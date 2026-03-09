# IMPACT

**Standardized Model Development and Deployment Platform**

A YAML-config-driven platform for financial lending credit modeling, providing standardized pipelines for entity data processing, model development, and deployment.

---

## Table of Contents

- [Installation](#installation)
- [User Guide — Entity Data Module](#user-guide--entity-data-module)
  - [Quick Start](#quick-start)
  - [Pipeline Result](#pipeline-result)
  - [YAML Config Reference](#yaml-config-reference)
    - [entity](#entity)
    - [parameters](#parameters)
    - [sources](#sources)
    - [joins](#joins)
    - [filters](#filters)
    - [fields](#fields)
    - [validations](#validations)
- [Developer Guide](#developer-guide)
  - [Architecture](#architecture)
  - [Pipeline Execution Order](#pipeline-execution-order)
  - [Adding a New Source Type](#adding-a-new-source-type)
  - [Adding a New Validator](#adding-a-new-validator)
  - [Adding a New Built-in Expression](#adding-a-new-built-in-expression)
  - [Exception Hierarchy](#exception-hierarchy)
  - [Running Tests](#running-tests)

---

## Installation

```bash
pip install -e ".[all]"        # everything (recommended)
pip install -e ".[snowflake]"  # core + Snowflake connector
pip install -e ".[dev]"        # core + dev/test tools
pip install -e "."             # core only (CSV, Parquet, Excel)
```

---

## User Guide — Entity Data Module

The Entity Data Module is a declarative pipeline for loading, transforming, validating, and structuring entity data (Facility, Obligor, etc.) from heterogeneous sources. Everything is declared in a single YAML config file — no Python code required for standard use cases.

### Quick Start

```python
from impact.entity.pipeline import EntityPipeline

# Run with config defaults
result = EntityPipeline("configs/facility_example.yaml").run()

# Override parameters at runtime (e.g. from an Airflow DAG)
result = EntityPipeline("configs/facility_example.yaml").run(
    parameters={"snapshot_date": "2025-06-30"}
)
```

### Pipeline Result

`pipeline.run()` returns a `PipelineResult` with the following attributes:

```python
result.entity_class       # dynamically-created dataclass (e.g. Facility)
result.entities           # list of entity instances, one per row
result.dataframe          # final processed pandas DataFrame
result.validation_report  # aggregated ValidationReport (warnings + errors)
result.metadata           # dict: entity_name, record_count, source_count, field_count
```

**Working with the validation report:**

```python
report = result.validation_report
report.has_errors      # True if any error-severity rule failed
report.has_warnings    # True if any warning-severity rule failed
report.error_count
report.warning_count

for r in report.results:
    print(r.rule_type, r.passed, r.severity, r.message)
    print(r.failing_row_count, r.failing_indices)
```

---

### YAML Config Reference

See `configs/facility_example.yaml` for a fully annotated working example. The sections below describe every option.

---

#### `entity`

Top-level metadata.

```yaml
entity:
  name: Facility          # used as the Python class name — must be a valid identifier
  description: "..."      # human-readable, optional
  version: "1.0"          # schema version string, optional
```

---

#### `parameters`

Global default values shared across all sources and filters. The orchestration layer (Airflow, CLI, etc.) overrides these at runtime via `pipeline.run(parameters={...})`. A source's own `parameters` block overrides the global default for that source only.

**Priority (highest to lowest):** `pipeline.run(parameters=...)` > source-level `parameters` > global `parameters`

```yaml
parameters:
  snapshot_date: "2025-12-31"    # static default; override at runtime via pipeline.run(parameters={...})
  active_status: "ACTIVE"
```

**Environment variables** use `${VAR}` syntax and are intended for infrastructure values — credentials, warehouse names — that vary by environment. They are set outside the config (CI secrets, `.env` files, etc.).

```yaml
connection:
  account: "${SNOWFLAKE_ACCOUNT}"            # required — set via: export SNOWFLAKE_ACCOUNT=myorg-myaccount
  warehouse: "${SNOWFLAKE_WH:ANALYTICS_WH}"  # optional — falls back to ANALYTICS_WH if unset
```

---

#### `sources`

One or more data sources. Exactly one must be marked `primary: true` — it is the anchor row count for all joins.

**Snowflake:**

```yaml
sources:
  - name: facility_main
    type: snowflake
    primary: true
    connection:
      account: "${SNOWFLAKE_ACCOUNT}"
      database: CREDIT_DB
      schema: LENDING
      warehouse: "${SNOWFLAKE_WH:ANALYTICS_WH}"
      role: null           # optional
      user: null           # optional — prefer authenticator for SSO environments
      password: null
      authenticator: null
    query: |
      SELECT facility_id, commitment_amount
      FROM facility_master
      WHERE snapshot_date = :snapshot_date   -- :name syntax binds from parameters.snapshot_date
```

**Parquet:**

```yaml
  - name: collateral
    type: parquet
    path: "data/collateral/{snapshot_date}/*.parquet"  # {name} interpolated from parameters
```

Glob patterns (`*`, `**`) are supported — all matched files are concatenated into one DataFrame.

**CSV / Excel:**

```yaml
  - name: rating_overrides
    type: csv              # or: excel
    path: "./data/overrides.csv"
    options:
      delimiter: ","
      encoding: "utf-8"
```

**Source-level parameter override:**

```yaml
  - name: alt_source
    type: parquet
    path: "data/{year}/*.parquet"
    parameters:
      year: "2024"         # overrides global parameters.year for this source only
```

---

#### `joins`

Combines sources after loading. Joins execute in the order listed; each result feeds the next.

**One-to-one join** — right side adds new columns, row count unchanged:

```yaml
joins:
  - left: facility_main
    right: rating_overrides
    how: left              # left | right | inner
    on:
      - left_col: facility_id
        right_col: facility_id
    relationship: one_to_one
```

**Composite join keys** — multiple key pairs are AND-ed (all must match):

```yaml
  - left: facility_main
    right: rating_overrides
    how: left
    on:
      - left_col: facility_id
        right_col: facility_id
      - left_col: product_type
        right_col: product_type
    relationship: one_to_one
```

**One-to-many join** — right rows are grouped and stored as a nested `pd.DataFrame` per left row, preserving the primary table's row count:

```yaml
  - left: facility_main
    right: collateral
    how: left
    on:
      - left_col: facility_id
        right_col: facility_id
    relationship: one_to_many
    nested_as: collateral_items   # name of the resulting nested DataFrame column
```

**Expression-based join condition:**

```yaml
    on:
      - condition: "left.origination_date <= right.effective_date"
```

---

#### `filters`

Row-level conditions applied **after all field processing** (Pass 1 + Pass 2). Each entry is a pandas eval expression. A row must pass **all** entries to be retained — multiple entries are AND-ed together implicitly.

```yaml
filters:
  - "commitment_amount > 0"        # static data-quality gate
  - "status == @active_status"     # @name references a value from parameters
```

**AND / OR logic within a single filter** — use Python boolean syntax with parentheses for grouping. When conditions are logically related, combine them into one entry:

```yaml
filters:
  - "commitment_amount > 0 and (product_category == 'TERM_LOAN' or product_category == 'REVOLVER')"
```

Use multiple entries when conditions are independent so failures are easier to identify in logs:

```yaml
filters:
  - "commitment_amount > 0"
  - "interest_rate >= 0 and interest_rate <= 1"
```

---

#### `fields`

Defines the output entity class. Each field declares its data origin, type, fill behaviour, and inline validation rules. Fields are processed in two ordered passes.

**Processing order:**

| Pass | Origin | Steps |
|---|---|---|
| Pass 1 | `source` | rename / expression eval → cast → fill_na → validation |
| Pass 2 | `derived` | expression eval → cast → fill_na → validation |
| Post | — | filters applied, `temp` fields dropped, entity class built |

**Every field requires exactly one of `source` or `derived`.**

---

**Full field reference:**

```yaml
fields:
  - name: facility_id           # REQUIRED — Python attribute name on the entity class
    source: facility_id         # REQUIRED (mutually exclusive with derived)
    dtype: str                  # REQUIRED — target type; cast is applied before fill_na
    description: "..."          # optional
    primary_key: true           # optional, default false
    entity_ref: CollateralItem  # optional — names the sub-entity type stored in a nested column;
                                #   must match the entity.name of the sub-entity's own YAML config;
                                #   used for documentation and future typed-access support
    temp: false                 # optional, default false — if true, the field participates in
                                #   processing and validation but is excluded from the entity class
    fill_na: "UNKNOWN"          # optional — scalar fill applied after cast; omit to skip
    validation_type: [not_null] # optional — list of validation types (see table below)
    validation_rule: {}         # optional — rules keyed by type (required for range, expression)
    validation_severity: {}     # optional — severity keyed by type; default is warning
```

**Supported `dtype` values:**

| dtype | Python / pandas type |
|---|---|
| `str` | `str` |
| `int32` / `int64` | `numpy.int32` / `numpy.int64` |
| `float32` / `float64` | `numpy.float32` / `numpy.float64` |
| `bool` | `bool` |
| `datetime` | `datetime64[ns]` via `pd.to_datetime` |
| `nested` | `pd.DataFrame` per cell — no cast applied |

---

**`source` — Pass 1 field (loaded from raw data)**

```yaml
# Simple pass-through (primary source column)
- name: facility_id
  source: facility_id
  dtype: str

# Rename — maps source column name to a different field name
- name: product_category
  source: product_type        # renames product_type → product_category
  dtype: str

# Non-primary source column — src_name.col_name format required
- name: rating_override
  source: rating_overrides.rating_override
  dtype: str

# Nested result from a one-to-many join — use the bare column name (no prefix)
# The join engine produces this column; it is not a raw source column
- name: collateral_items
  source: collateral_items
  dtype: nested
  entity_ref: CollateralItem

# Source expression — evaluated in Pass 1; src_name. prefixes are stripped automatically
- name: available_capacity
  source: "facility_main.commitment_amount - facility_main.outstanding_balance"
  dtype: float64
  temp: true                  # used by derived fields; excluded from the entity class
```

---

**`derived` — Pass 2 field (computed after all source fields are clean)**

Derived fields run after Pass 1 source fields are fully renamed, cast, filled, and validated. Reference field names only — no `src_name.` prefix.

```yaml
# pandas eval expression
- name: utilization_rate
  derived: "outstanding_balance / commitment_amount"
  dtype: float64

# Row-wise lambda — receives the full row as a pandas Series
- name: days_to_maturity
  derived: "lambda row: (row['maturity_date'] - row['origination_date']).days"
  dtype: int64

# Lambda accessing a nested DataFrame column
# row['collateral_items'] is always a pd.DataFrame (guaranteed by the join engine);
# facilities with no matching collateral receive an empty DataFrame, hence the .empty guard
- name: total_collateral_value
  derived: "lambda row: row['collateral_items']['collateral_value'].sum() if not row['collateral_items'].empty else 0.0"
  dtype: float64
```

---

**Inline validations:**

```yaml
- name: commitment_amount
  source: commitment_amount
  dtype: float64
  validation_type: [not_null, range]
  validation_rule:
    range: [0, null]           # null = unbounded; [min, max]
  validation_severity:
    not_null: error            # error halts the pipeline immediately
    range: warning             # warning logs and continues (this is the default if omitted)

- name: utilization_rate
  derived: "outstanding_balance / commitment_amount"
  dtype: float64
  validation_type: [range, expression]
  validation_rule:
    range: [0.0, 1.0]
    expression: "outstanding_balance <= commitment_amount"
  validation_severity:
    expression: warning
```

**Validation types:**

| Type | Rule key required | Description |
|---|---|---|
| `not_null` | — | No null values in this field |
| `unique` | — | No duplicate values in this field |
| `range` | `range: [min, max]` | Values within bounds; use `null` for unbounded |
| `expression` | `expression: "pandas expr"` | Boolean expression evaluated over the full DataFrame |
| `custom` | `custom: "pkg.module.fn"` | User-defined function returning a `ValidationResult` |

---

#### `validations`

Global validation rules applied to the full DataFrame after all field processing. Same types and syntax as inline field validations, but operate across multiple columns or at the dataset level.

```yaml
validations:
  - type: not_null
    columns: [facility_id, obligor_id]
    severity: error

  - type: expression
    rule: "outstanding_balance <= commitment_amount"
    message: "Balance cannot exceed commitment"
    severity: warning

  - type: custom
    function: "mypackage.validators.check_concentration"
    kwargs:
      threshold: 0.25
    severity: error
```

---

## Developer Guide

### Architecture

The Entity Data Module separates **configuration** (YAML + Pydantic schema) from **execution** (Python pipeline stages). All config shapes are defined once in `schema.py` as Pydantic v2 models and validated at parse time before any data is touched.

```
src/impact/
├── common/
│   ├── exceptions.py        # Exception hierarchy
│   ├── logging.py           # Structured logger factory (get_logger)
│   └── utils.py             # Shared utilities (import_dotted_path)
└── entity/
    ├── config/
    │   ├── schema.py        # Pydantic v2 models — single source of truth for all config shapes
    │   └── parser.py        # YAML loader, ${VAR} / built-in expression interpolation
    ├── source/
    │   ├── base.py          # DataSourceConnector ABC
    │   ├── registry.py      # ConnectorRegistry — @register decorator + singleton cache
    │   ├── snowflake.py     # SnowflakeConnector
    │   ├── parquet.py       # ParquetConnector (glob + path interpolation)
    │   └── csv_excel.py     # CsvConnector, ExcelConnector
    ├── join/
    │   ├── engine.py        # JoinEngine — flat (one_to_one) and nested (one_to_many) joins
    │   └── nesting.py       # nest_dataframe — collapses inflated rows into sub-DataFrames
    ├── transform/
    │   ├── base.py          # Transformer ABC
    │   ├── registry.py      # TransformRegistry — @register decorator + singleton cache
    │   └── builtin.py       # cast, rename, derive, fill_na, drop, filter, custom
    ├── validate/
    │   ├── base.py          # Validator ABC, ValidationResult, ValidationReport
    │   ├── registry.py      # ValidatorRegistry — @register decorator + singleton cache
    │   └── builtin.py       # not_null, unique, range, expression, custom
    ├── model/
    │   └── builder.py       # EntityBuilder — creates dataclasses at runtime from FieldConfig list
    └── pipeline.py          # EntityPipeline orchestrator — ties all stages together
```

**Key design patterns:**

- **Registry + Decorator** — Sources, transforms, and validators register themselves via `@Registry.register("type")`. New types plug in without modifying core code. Instances are cached as singletons at decoration time.
- **Strategy** — Each connector / transformer / validator is an interchangeable implementation behind an ABC. The pipeline calls the interface; it never knows the concrete type.
- **Builder** — `EntityBuilder` creates a Python `dataclass` type at runtime from the YAML `fields` list. Field names, types, and primary key metadata are embedded on the class.
- **Config-as-schema** — The YAML `fields` section doubles as the dynamic class definition. No separate schema file is needed.
- **Immutability** — Every pipeline stage receives a DataFrame and returns a new one. Inputs are never mutated in place.
- **Fail-fast** — The full config is validated by Pydantic v2 at parse time. Invalid configs raise `ConfigError` before any data is loaded.

---

### Pipeline Execution Order

```
EntityPipeline.run(parameters)
│
├── Merge parameters
│       effective = {**config.parameters, **runtime_parameters}
│
├── Stage 1 — Load sources
│       Per source: merged = {**global, **source_level, **runtime}
│       connector.load(source_cfg_with_merged_params)
│
├── Stage 2 — Execute joins (in config order)
│       JoinEngine.execute() — flat (one_to_one) or nested (one_to_many)
│
├── Stage 3 — Field processing
│   │
│   ├── Pass 1 — source fields
│   │       Batch rename (single rename() call for all source renames)
│   │       Per field: source expression eval → cast → fill_na
│   │       Source field validations → halt on any error-severity failure
│   │
│   └── Pass 2 — derived fields
│               Per field: df.eval() or lambda → cast → fill_na
│
├── Stage 3b — Row filters
│       Per filter string: df.eval(condition, local_dict=effective_params)
│
├── Stage 4 — Validations
│       Derived field inline validations
│       Global validations (config.validations section)
│       Halt if any error-severity failure
│
└── Stage 5 — Build entity
        Drop temp fields
        EntityBuilder.build_class(entity_name, non_temp_fields)
        EntityBuilder.to_entities(dataframe, entity_class)
```

Source field validations run **between Pass 1 and Pass 2** deliberately — derived fields must operate on clean, validated source data.

---

### Adding a New Source Type

1. Create a file in `src/impact/entity/source/` (or add a class to an existing file).
2. Subclass `DataSourceConnector` and decorate with `@ConnectorRegistry.register`:

```python
# src/impact/entity/source/my_source.py
from impact.entity.source.base import DataSourceConnector
from impact.entity.source.registry import ConnectorRegistry
from impact.entity.config.schema import SourceConfig
import pandas as pd

@ConnectorRegistry.register("my_source")
class MyConnector(DataSourceConnector):
    def load(self, config: SourceConfig) -> pd.DataFrame:
        # Available: config.path, config.parameters, config.options,
        #            config.connection (Snowflake only), config.query (Snowflake only)
        ...
        return df
```

3. Add the import to `pipeline.py` alongside the other registration imports so the decorator runs at startup:

```python
import impact.entity.source.my_source  # noqa: F401
```

4. Add `"my_source"` to the `type` `Literal` in `SourceConfig` in `schema.py`:

```python
type: Literal["snowflake", "parquet", "csv", "excel", "my_source"]
```

---

### Adding a New Validator

1. Add a class to `src/impact/entity/validate/builtin.py` (or a new file):

```python
from impact.entity.validate.base import ValidationResult, Validator
from impact.entity.validate.registry import ValidatorRegistry
from impact.entity.config.schema import ValidationConfig
import pandas as pd

@ValidatorRegistry.register("my_check")
class MyValidator(Validator):
    def validate(self, df: pd.DataFrame, config: ValidationConfig) -> ValidationResult:
        # Available config fields: columns, column, min, max, rule, message, function, kwargs
        passing = True  # your logic here
        return ValidationResult(
            rule_type="my_check",
            passed=passing,
            severity=config.severity,
            message="...",
            failing_row_count=0,
            failing_indices=[],
        )
```

2. Add `"my_check"` to `ValidationTypeLiteral` in `schema.py` so it is accepted in the YAML config:

```python
ValidationTypeLiteral = Literal["not_null", "unique", "range", "expression", "custom", "my_check"]
```

No import change needed in `pipeline.py` — `validate/builtin.py` is already imported at startup.

---

### Adding a New Built-in Expression

The config parser supports `${name}` interpolation in any YAML string value. In addition to environment variables (`${SNOWFLAKE_ACCOUNT}`), you can register **built-in expressions** that compute a value dynamically at parse time. These are defined in `_BUILTIN_EXPRESSIONS` in `src/impact/entity/config/parser.py`:

```python
# Currently available:
#   ${last_quarter_end}  — e.g. "2025-09-30"

# To add a new one:
def _last_month_end() -> str:
    from datetime import date, timedelta
    first_of_month = date.today().replace(day=1)
    return (first_of_month - timedelta(days=1)).strftime("%Y-%m-%d")

_BUILTIN_EXPRESSIONS: dict[str, Any] = {
    "last_quarter_end": _last_quarter_end,
    "last_month_end": _last_month_end,     # new
}
```

Usage in config:

```yaml
parameters:
  snapshot_date: "${last_quarter_end}"   # resolved at parse time; overridden by pipeline.run(parameters=...)
```

**Resolution priority** for `${name}` tokens:

| Priority | Mechanism | Intended for |
|---|---|---|
| 1 (highest) | Built-in expression | Dynamic computed defaults (`${last_quarter_end}`) |
| 2 | Environment variable | Infrastructure values (`${SNOWFLAKE_ACCOUNT}`) |
| 3 | Inline default after `:` | Static fallback (`${VAR:ANALYTICS_WH}`) |
| 4 (lowest) | Placeholder left as-is | Pydantic will surface it as an error if the field is required |

Runtime override of a built-in expression value is done via `pipeline.run(parameters={"snapshot_date": "..."})`, not via environment variables.

---

### Exception Hierarchy

All exceptions are defined in `src/impact/common/exceptions.py`.

```
ImpactError
├── ConfigError       — YAML parse failure, Pydantic validation failure
├── SourceError       — data source load failure
├── JoinError         — join execution failure
├── TransformError    — field expression / filter eval failure, cast failure
├── ValidationError   — error-severity validation failure; carries .report attribute
└── EntityBuildError  — dynamic dataclass creation failure
```

`ValidationError` carries the full `ValidationReport` on its `.report` attribute:

```python
try:
    result = pipeline.run()
except ValidationError as exc:
    for r in exc.report.results:
        if not r.passed:
            print(r.rule_type, r.severity, r.message, r.failing_indices)
```

---

### Running Tests

```bash
python3 -m pytest tests/ -v       # all tests with verbose output
python3 -m pytest tests/unit/ -v  # unit tests only
python3 -m pytest tests/ -q       # quiet summary
```

Tests are organised under `tests/unit/` by module. Shared fixtures (sample DataFrames, config dicts) live in `tests/conftest.py`.
