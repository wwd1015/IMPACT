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
    - [connections](#connections)
    - [pre_filters / post_filters](#pre_filters--post_filters)
    - [fields](#fields)
    - [validations](#validations)
  - [Config Merge — Custom Overrides](#config-merge--custom-overrides)
- [Developer Guide](#developer-guide)
  - [Architecture](#architecture)
  - [Pipeline Execution Order](#pipeline-execution-order)
  - [Sub-Entity Processing](#sub-entity-processing)
  - [Adding a New Source Type](#adding-a-new-source-type)
  - [Adding a New Validator](#adding-a-new-validator)
  - [Adding a New Built-in Expression](#adding-a-new-built-in-expression)
  - [Diagnostics and Debugging](#diagnostics-and-debugging)
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
result.entity_class        # dynamically-created dataclass (e.g. Facility)
result.entities            # list of entity instances, one per row
result.dataframe           # final processed pandas DataFrame
result.validation_report   # aggregated ValidationReport (warnings + errors)
result.metadata            # dict: entity_name, record_count, source_count, field_count
result.sub_entity_classes  # dict: field_name → sub-entity class (e.g. {"collateral_items": Collateral})
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
    print(r.field_name)         # which field triggered the failure
    print(r.context)            # parent row context for sub-entity validations
    print(r.failing_samples)    # sample rows (dicts) with the bad values

# Detailed diagnostic output (includes sample failing rows)
print(report.format_detail())
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
  active_product: "TERM_LOAN"
```

**Environment variables** use `${VAR}` syntax and are intended for infrastructure values — credentials, warehouse names — that vary by environment. They are set outside the config (CI secrets, `.env` files, etc.).

```yaml
connection:
  account: "${SNOWFLAKE_ACCOUNT}"            # required — set via: export SNOWFLAKE_ACCOUNT=myorg-myaccount
  warehouse: "${SNOWFLAKE_WH:ANALYTICS_WH}"  # optional — falls back to ANALYTICS_WH if unset
```

---

#### `connections`

Named connection configs — define once, reference by name in sources. Currently supports Snowflake. Multiple sources sharing the same connection reuse a single connection object at runtime.

```yaml
connections:
  lending_db:
    account: "${SNOWFLAKE_ACCOUNT}"
    database: CREDIT_DB
    schema: LENDING
    warehouse: "${SNOWFLAKE_WH:ANALYTICS_WH}"
```

Sources reference by name: `connection: lending_db`

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
      WHERE snapshot_date = '{snapshot_date}'
```

**SQLite:**

```yaml
  - name: facility_main
    type: sqlite
    primary: true
    path: "data/sample/lending.db"
    query: |
      SELECT facility_id, commitment_amount
      FROM {source_table}
      WHERE snapshot_date = '{snapshot_date}'
```

Query parameters use `{param}` syntax — interpolated from the merged parameters before execution.

> **Tip:** For Snowflake sources, you can define credentials once in the [`connections`](#connections) section and reference by name — see below.

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

**Pre-joins between non-primary sources** — joins execute in config order, and each result updates the left source. This allows enriching a secondary source before nesting it under the primary. For example, to nest facilities (with collateral) under each obligor:

```yaml
joins:
  # Step 1: enrich facility rows with collateral (secondary ↔ secondary)
  - left: facility_detail
    right: collateral
    how: left
    on:
      - left_col: facility_id
        right_col: facility_id
    relationship: one_to_many
    nested_as: collateral_items

  # Step 2: nest enriched facility rows under each obligor (primary ↔ secondary)
  - left: obligor_main
    right: facility_detail
    how: left
    on:
      - left_col: obligor_id
        right_col: obligor_id
    relationship: one_to_many
    nested_as: facilities
```

After these joins, each obligor row's `facilities` nested DataFrame contains facility rows that already have `collateral_items` — enabling recursive sub-entity processing (Obligor → Facility → Collateral).

**Expression-based join condition:**

```yaml
    on:
      - condition: "left.origination_date <= right.effective_date"
```

---

#### `pre_filters / post_filters`

Row-level filter conditions split into two stages. Each entry is a pandas eval expression. A row must pass **all** entries to be retained — multiple entries are AND-ed together implicitly.

**`pre_filters`** — applied **before field processing** (right after joins). Use raw source column names. Reduces the dataset early for better performance on large data.

```yaml
pre_filters:
  - "commitment_amount > 0"
```

**`post_filters`** — applied **after all field processing** (Pass 1 + Pass 2). Use processed field names.

```yaml
post_filters:
  - "product_category == @active_product"  # @name references a value from parameters
```

Both support `@param_name` syntax to reference runtime parameters. Parameters are also accessible in derived expressions via `@param` (eval) or as variables in lambdas.

**AND / OR logic within a single filter** — use Python boolean syntax with parentheses for grouping. When conditions are logically related, combine them into one entry:

```yaml
post_filters:
  - "commitment_amount > 0 and (product_category == 'TERM_LOAN' or product_category == 'REVOLVER')"
```

Use multiple entries when conditions are independent so failures are easier to identify in logs:

```yaml
post_filters:
  - "commitment_amount > 0"
  - "interest_rate >= 0 and interest_rate <= 1"
```

---

#### `fields`

Defines the output entity class. Each field declares its data origin, type, fill behaviour, and inline validation rules. Fields are processed in two ordered passes.

**Processing order:**

| Pass | Origin | Steps |
|---|---|---|
| Pre-filters | — | `pre_filters:` applied after joins, before field processing (raw column names) |
| Pass 1 | `source` | rename / expression eval → cast → fill_na → validation |
| Pass 2 | `derived` | expression eval → cast → fill_na → validation |
| Post-filters | — | `post_filters:` applied after field processing (processed field names) |
| Post | — | `temp` fields dropped, entity class built |

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
    entity_ref: Collateral      # optional — triggers sub-entity processing for nested fields.
                                #   Must match entity.name in the sub-entity's YAML config.
                                #   Each nested DataFrame cell is validated, transformed, and
                                #   converted into a list of sub-entity dataclass instances.
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
| `str` / `string` | `str` |
| `int32` / `int64` | `numpy.int32` / `numpy.int64` |
| `float32` / `float64` | `numpy.float32` / `numpy.float64` |
| `bool` | `bool` |
| `datetime` | `datetime64[ns]` via `pd.to_datetime` |
| `nested` | `list` of sub-entity instances when `entity_ref` is set; `pd.DataFrame` otherwise |

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
# entity_ref triggers sub-entity processing: the nested DataFrame in each cell
# is validated, transformed, and converted into a list of Collateral instances.
# The entity_ref value must match entity.name in the sub-entity config file.
- name: collateral_items
  source: collateral_items
  dtype: nested
  entity_ref: Collateral

# Source expression — evaluated in Pass 1; src_name. prefixes are stripped automatically
- name: available_capacity
  source: "commitment_amount - outstanding_balance"
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
    range: [0, null]           # [] = inclusive; null = unbounded → x >= 0
  validation_severity:
    not_null: error            # error halts the pipeline immediately
    range: warning             # warning logs and continues (this is the default if omitted)

- name: utilization_rate
  derived: "outstanding_balance / commitment_amount"
  dtype: float64
  validation_type: [range, expression]
  validation_rule:
    range: "[0.0, 1.0)"        # string with brackets: 0.0 <= x < 1.0
    expression: "outstanding_balance <= commitment_amount"
  validation_severity:
    expression: warning
```

**Validation types:**

| Type | Rule key required | Description |
|---|---|---|
| `not_null` | — | No null values in this field |
| `unique` | — | No duplicate values in this field |
| `range` | `range: [min, max]` or `"(min, max)"` | Values within bounds; `[]` inclusive, `()` exclusive, mix allowed; use `null` for unbounded |
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

### Config Merge — Custom Overrides

IMPACT provides standardized configs that define the primary data processing logic. Users can create custom override configs to add or replace specific sections without duplicating the entire config. The merge happens at the YAML level before parsing — the IMPACT config is always the primary base.

```python
from impact.entity.config.merger import merge_configs

# IMPACT config is always the primary, regardless of argument order
config = merge_configs(
    primary="configs/facility_example.yaml",     # IMPACT standard config
    custom="user_configs/facility_custom.yaml",  # user's sparse overrides
)

# Use the merged config in the pipeline
result = EntityPipeline(config=config).run()
```

**Merge semantics by section:**

| Section | Strategy | Example |
|---|---|---|
| `entity` | Custom overrides individual keys | Change `version` without touching `name` |
| `parameters` | Dict merge, custom wins on conflict | Override `active_product` while keeping `snapshot_date` |
| `sources` | Merge by `name`; same name = replace entirely; new = added | Swap a Snowflake source for CSV in dev |
| `joins` | Merge by `(left, right)` pair; same pair = replace; new = added | Change join type or add new joins |
| `pre_filters` | Custom pre_filters appended to primary's | Add early data reduction gates |
| `post_filters` | Custom post_filters appended to primary's | Add stricter post-processing gates |
| `fields` | Merge by `name`; same name = replace entirely; new = added | Override a field's dtype, source, or validation |
| `validations` | Custom validations appended to primary's | Add extra validation rules |
| `connections` | Dict merge, custom wins on conflict | Override or add named connections |

**Example custom config** — only includes what's different:

```yaml
# user_configs/facility_custom.yaml
# Sparse override — only the sections/fields you want to change

parameters:
  active_product: "REVOLVER"          # override default TERM_LOAN

sources:
  - name: collateral                   # replaces the primary's 'collateral' source
    type: csv
    path: "./data/collateral_local.csv" # use local CSV instead of Parquet

fields:
  # Override: change the commitment_amount validation to be stricter
  - name: commitment_amount
    source: commitment_amount
    dtype: float64
    validation_type: [not_null, range]
    validation_rule:
      range: (0, null]                  # exclusive lower bound (must be > 0)
    validation_severity:
      not_null: error
      range: error                      # upgrade from warning to error

  # Addition: new field not in the primary config
  - name: region
    source: region_code
    dtype: str
    fill_na: "UNKNOWN"

post_filters:
  - "region != 'EXCLUDED'"             # appended to primary's post_filters
```

**Merge logging** — the merger logs every adjustment so users can verify the effect:

```
INFO | [merge] parameters: overridden: ['active_product']
INFO | [merge] sources: overridden: ['collateral']
INFO | [merge] fields: overridden: ['commitment_amount']
INFO | [merge] fields: added: ['region']
INFO | [merge] post_filters: appended 1 custom entries
```

**Key design principles:**

- The IMPACT config is **always** the primary base — argument order doesn't matter
- Custom configs are **sparse** — only include what you want to change
- **Fields replaced entirely** — if you override a field, the custom definition replaces the primary's completely (including its validations). This avoids ambiguity about which validations apply
- **No removal** — the custom config can only override or add, not remove sections from the primary

**Key file:** `src/impact/entity/config/merger.py`

---

## Developer Guide

### Architecture

The Entity Data Module separates **configuration** (YAML + Pydantic schema) from **execution** (Python pipeline stages). All config shapes are defined once in `schema.py` as Pydantic v2 models and validated at parse time before any data is touched.

```
src/impact/
├── common/
│   ├── exceptions.py        # Exception hierarchy
│   ├── logging.py           # Structured logger factory (get_logger)
│   └── utils.py             # Shared utilities, cast/lambda diagnostic helpers
└── entity/
    ├── config/
    │   ├── schema.py        # Pydantic v2 models — single source of truth for all config shapes
    │   ├── parser.py        # YAML loader, ${VAR} / built-in expression interpolation
    │   └── merger.py        # Config merger — primary + custom override merge logic
    ├── source/
    │   ├── base.py          # DataSourceConnector ABC
    │   ├── registry.py      # ConnectorRegistry — @register decorator + singleton cache
    │   ├── snowflake.py     # SnowflakeConnector (shared connections, {param} interpolation)
    │   ├── sqlite.py        # SqliteConnector ({param} interpolation for tables + values)
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
    ├── sub_entity.py        # SubEntityProcessor — validates/transforms nested DataFrames into typed instances
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
│       Each join updates resolved[left] with the result
│       Supports pre-joins between non-primary sources
│       Shared Snowflake connections reused across sources with same connection name
│       JoinEngine.execute() — flat (one_to_one) or nested (one_to_many)
│
├── Stage 2b — Pre-filters (raw column names, before field processing)
│       Per filter: df.eval(condition, local_dict=effective_params)
│
├── Stage 3 — Field processing
│   │
│   ├── Pass 1 — source fields
│   │       Batch rename (single rename() call for all source renames)
│   │       Per field: source expression eval → cast → fill_na
│   │       Source field validations → halt on any error-severity failure
│   │
│   └── Pass 2 — derived fields
│               Per field: df.eval(expr, local_dict=params) or lambda (params in namespace)
│
├── Stage 3b — Post-filters (processed field names)
│       Per filter: df.eval(condition, local_dict=effective_params)
│
├── Stage 4 — Validations
│       Derived field inline validations
│       Global validations (config.validations section)
│       Halt if any error-severity failure
│
├── Stage 4b — Sub-entity processing
│       For each nested field with entity_ref:
│         Resolve sub-entity config (convention-based file lookup)
│         Process each cell's DataFrame through SubEntityProcessor
│           (rename → cast → fill_na → validate → derived → build)
│         Replace nested DataFrame cells with list[SubEntity]
│         Merge sub-entity validation results into parent report
│
└── Stage 5 — Build entity
        Drop temp fields
        EntityBuilder.build_class(entity_name, non_temp_fields, sub_entity_classes)
        EntityBuilder.to_entities(dataframe, entity_class)
```

Source field validations run **between Pass 1 and Pass 2** deliberately — derived fields must operate on clean, validated source data.

---

### Sub-Entity Processing

When a field has `dtype: nested` and `entity_ref` set, the pipeline automatically processes each nested DataFrame through a sub-entity pipeline. This validates, transforms, and converts nested data into typed dataclass instances.

**How it works:**

1. The parent pipeline's one-to-many join produces a nested `pd.DataFrame` in each cell of the nested column (e.g. `collateral_items`).
2. After parent validations pass, the pipeline resolves the sub-entity config by convention — it looks for `{snake_case(entity_ref)}.yaml` or `{snake_case(entity_ref)}_*.yaml` in the same directory as the parent config.
3. Each cell's DataFrame is processed through `SubEntityProcessor`, which applies the same two-pass field processing as the main pipeline (rename → cast → fill_na → validate → derived → build), but without sources, joins, or filters (pre/post).
4. The nested DataFrame cells are replaced with `list[SubEntity]` — each element is a dataclass instance of the sub-entity type.
5. Sub-entity validation results are merged into the parent's `ValidationReport`.

**Sub-entity config format** — same as a top-level config, but with no `sources`, `joins`, or `filters` (pre/post):

```yaml
# configs/collateral_example.yaml
entity:
  name: Collateral
  description: "Collateral position pledged against a facility"
  version: "1.0"

fields:
  - name: collateral_type
    source: collateral_type
    dtype: str
    validation_type: [not_null]
    validation_severity:
      not_null: error

  - name: collateral_value
    source: collateral_value
    dtype: float64
    validation_type: [not_null, range]
    validation_rule:
      range: [0, null]

  - name: value_bucket
    derived: "lambda row: 'HIGH' if row['collateral_value'] >= 500000 else ('MEDIUM' if row['collateral_value'] >= 100000 else 'LOW')"
    dtype: str
```

**Accessing sub-entities at runtime:**

```python
result = EntityPipeline("configs/facility_example.yaml").run()

facility = result.entities[0]
facility.collateral_items          # → [Collateral(...), Collateral(...), ...]
facility.collateral_items[0].collateral_type   # → 'REAL_ESTATE'
facility.collateral_items[0].collateral_value  # → 500000.0

result.sub_entity_classes          # → {"collateral_items": <class 'Collateral'>}
```

**Config file naming convention:**

For `entity_ref: Collateral`, the pipeline searches in the parent config's directory:
1. `collateral.yaml` (exact match)
2. `collateral_*.yaml` (glob match, e.g. `collateral_demo.yaml`)

For multi-word names like `FinancialStatement`, it also tries the plain lowercase variants:
1. `financial_statement.yaml`
2. `financial_statement_*.yaml`
3. `financialstatement.yaml`
4. `financialstatement_*.yaml`

**Recursive sub-entities** — sub-entity configs can themselves contain `entity_ref` fields, enabling multi-level nesting. For example, the obligor pipeline produces:

```
Obligor
├── obligor_id, legal_name, ...
├── total_commitment (derived — aggregated from facilities)
└── facilities: list[Facility]
    ├── facility_id, commitment_amount, ...
    ├── utilization_rate (derived)
    └── collateral_items: list[Collateral]
        ├── collateral_type, collateral_value
        └── value_bucket (derived)
```

**Reusing top-level configs as sub-entity configs** — a config with `sources` defined (like `facility_example.yaml`) can be used as a sub-entity config. The `SubEntityProcessor` ignores the `sources`, `joins`, and `filters` sections but uses the source names for prefix stripping (e.g. `source: rating_overrides.rating_override` → `rating_override`). This means a single config file defines both the standalone pipeline and the sub-entity schema.

**Key files:** `src/impact/entity/sub_entity.py` (processor + config resolver), `src/impact/entity/model/builder.py` (handles `list` type for nested fields).

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
    def load(self, config: SourceConfig, **kwargs) -> pd.DataFrame:
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
from impact.entity.validate.base import ValidationResult, Validator, collect_failing_samples
from impact.entity.validate.registry import ValidatorRegistry
from impact.entity.config.schema import ValidationConfig
import pandas as pd

@ValidatorRegistry.register("my_check")
class MyValidator(Validator):
    def validate(self, df: pd.DataFrame, config: ValidationConfig) -> ValidationResult:
        # Available config fields: columns, column, min, max, rule, message, function, kwargs
        failing = []  # compute failing row indices
        samples = collect_failing_samples(df, failing, columns=config.columns) if failing else []
        return ValidationResult(
            rule_type="my_check",
            passed=len(failing) == 0,
            severity=config.severity,
            message="...",
            failing_row_count=len(failing),
            failing_indices=failing,
            field_name=config.columns[0] if config.columns else None,
            failing_samples=samples,
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

### Diagnostics and Debugging

The pipeline includes a row-level diagnostic system that pinpoints exactly which field, which row, and what value caused a failure. All diagnostic work happens only on the **error path** — the happy path has zero overhead.

**What gets diagnosed:**

| Failure type | Diagnostic detail |
|---|---|
| Cast failure | Which rows have un-castable values, the bad values themselves |
| Lambda/derived expression error | Which row caused the lambda to throw, the row's data, the error message |
| Validation failure (not_null, unique, range, expression) | Failing row indices + sample rows showing the bad values |
| Sub-entity error | Parent row context (primary key values) so you can trace errors back to the parent record |

**Validation results carry diagnostic data:**

Each `ValidationResult` includes:

```python
result.field_name         # "commitment_amount"
result.failing_indices    # [3, 7, 12]
result.failing_samples    # [{"commitment_amount": -5.0}, {"commitment_amount": -3.0}, ...]
result.context            # "parent row 3 (facility_id='FAC-001'), sub-entity 'Collateral'"

# Human-readable diagnostic with sample rows
print(result.format_detail())
# [ERROR] range check on 'commitment_amount': >= 0 (3 failing rows)
#   Sample failing rows:
#     row 3: commitment_amount=-5.0
#     row 7: commitment_amount=-3.0
#     row 12: commitment_amount=-1.0
```

**Cast failure diagnostics** — when a dtype cast fails, the error message includes the specific rows and values that cannot be cast:

```
TransformError: field 'interest_rate': cast to 'float64' failed.
  Bad values (first 5): row 3: 'N/A', row 7: 'TBD', row 12: 'n/a'
```

The `TransformError` also carries structured data on `.field` and `.failing_samples` attributes for programmatic inspection.

**Lambda expression diagnostics** — when a row-wise lambda fails, the pipeline identifies which row caused it and includes the row's data:

```
TransformError: Field 'days_to_maturity': derived expression
  'lambda row: (row['maturity_date'] - row['origination_date']).days' failed.
  Failing rows: row 5: TypeError: ... (data: {maturity_date=NaT, origination_date=...})
```

**Sub-entity parent context** — when processing nested entities, errors and validation warnings include the parent row's identity:

```
TransformError: parent row 3 (facility_id='FAC-001'), sub-entity 'Collateral':
  field 'collateral_value': cast to 'float64' failed. Bad values: row 0: 'INVALID'
```

Validation warnings from sub-entities carry the same context on `result.context`.

**Full diagnostic report:**

```python
try:
    result = pipeline.run()
except ValidationError as exc:
    # Detailed report with sample rows for every failed rule
    print(exc.report.format_detail())
```

**Controlling sample count:**

By default, up to 5 sample failing rows are attached to each validation result. To change this globally:

```python
from impact.entity.validate.base import set_max_samples

set_max_samples(10)   # more samples for debugging
set_max_samples(0)    # disable sample collection entirely
```

**Key files:** `src/impact/entity/validate/base.py` (ValidationResult, collect_failing_samples), `src/impact/common/utils.py` (diagnose_cast_failure, diagnose_lambda_failure).

---

### Exception Hierarchy

All exceptions are defined in `src/impact/common/exceptions.py`.

```
ImpactError
├── ConfigError       — YAML parse failure, Pydantic validation failure
├── SourceError       — data source load failure
├── JoinError         — join execution failure
├── TransformError    — field expression / filter eval failure, cast failure
│                       carries .field and .failing_samples for row-level diagnostics
├── ValidationError   — error-severity validation failure; carries .report attribute
│                       .report.format_detail() for full diagnostic output
└── EntityBuildError  — dynamic dataclass creation failure
```

**Programmatic error inspection:**

```python
try:
    result = pipeline.run()
except TransformError as exc:
    print(exc.field)            # which field failed
    print(exc.failing_samples)  # sample rows with bad values
except ValidationError as exc:
    print(exc.report.format_detail())   # full diagnostic with sample rows
    for r in exc.report.results:
        if not r.passed:
            print(r.field_name, r.failing_samples, r.context)
```

---

### Running Tests

```bash
python3 -m pytest tests/ -v       # all tests with verbose output
python3 -m pytest tests/unit/ -v  # unit tests only
python3 -m pytest tests/ -q       # quiet summary
```

Tests are organised under `tests/unit/` by module. Shared fixtures (sample DataFrames, config dicts) live in `tests/conftest.py`.
