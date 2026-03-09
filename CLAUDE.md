# IMPACT — Developer Guide

## Project Overview

IMPACT is a standardized model development and deployment platform for financial lending credit modeling. The first component is the **Entity Data Module** — a YAML-config-driven pipeline for processing entity data (Facility/Obligor).

## Quick Commands

```bash
pip3 install -e ".[all]"       # Install with all dependencies
python3 -m pytest tests/ -v    # Run tests
```

## Architecture

The Entity Data Module follows a **5-stage pipeline**: Load → Join → Transform → Filter → Validate → Build.

All pipeline logic is declared in a single YAML config file. Python code is the execution engine.

### Package Structure

```
src/impact/
├── entity/
│   ├── config/          # Pydantic v2 schema + YAML parser (env-var interpolation)
│   ├── source/          # Data connectors: Snowflake, Parquet, CSV, Excel
│   ├── join/            # Join engine with nested DataFrame support (1-to-many)
│   ├── transform/       # 7 built-in types: cast, rename, derive, fill_na, drop, filter, custom
│   ├── validate/        # 5 built-in types: not_null, unique, range, expression, custom
│   ├── model/           # Dynamic dataclass builder from YAML field definitions
│   └── pipeline.py      # Orchestrator — ties all stages together
└── common/              # Shared exceptions, logging, and utilities
```

### Design Patterns

- **Registry + Decorator** — Sources, transforms, and validators use `@Registry.register("type")` for plugin-style extensibility. New types are added without modifying core code.
- **Strategy** — Each connector/transform/validator is an interchangeable implementation behind an ABC.
- **Builder** — `EntityBuilder` creates dataclasses at runtime from YAML `fields` definitions.
- **Pipeline** — Ordered, composable processing stages with immutable DataFrame semantics.

### Key Design Decisions

1. **Config-as-schema** — The YAML `fields` section doubles as the dynamic class definition; no separate schema file.
2. **Nested DataFrames** — 1-to-many joins store sub-DataFrames in cells to preserve primary table row count.
3. **Immutability** — Transform/validation steps produce new DataFrames; inputs are never mutated.
4. **Fail-fast** — Config is fully validated at parse time (Pydantic v2) before any data is touched.
5. **Severity levels** — Validations use `error` (halt pipeline) or `warning` (log and continue).

### YAML Config Structure

```yaml
entity:        # [required] Name, description, version
parameters:    # [optional] Global defaults; overridden by pipeline.run(parameters={...})
sources:       # [required] Data source definitions (exactly one must be primary: true)
joins:         # [optional] How to combine sources (one_to_one / one_to_many)
filters:       # [optional] Row-level filter expressions applied after field processing
fields:        # [required] Output schema — one entry per output column
```

`parameters`, `joins`, and `filters` are optional. Every pipeline must have `entity`, `sources`, and `fields`.

### Field Definition Reference

Each entry in `fields` defines one output column. **Required keys: `source` or `derived`, and `dtype`.** Everything else is optional.

```yaml
fields:
  - name: <field_name>           # [required] output column / entity attribute name

    # --- Data origin: pick exactly one ---
    #
    # source — processed in Pass 1, alongside all other source fields.
    #   Two forms (src_name. prefix always stripped automatically):
    source: <col>                # column ref — PRIMARY source: bare name, same → pass-through,
    source: <src_name>.<col>     #              NON-PRIMARY source: src_name.col required.
                                 #              different col name → rename.
    source: "<expression>"       # expression — evaluated via df.eval(); src_name.col notation
                                 #              works and is stripped before evaluation.
                                 #              e.g. "src.col_a - src.col_b"
    #
    # derived — processed in Pass 2, after ALL source fields are renamed, cast,
    #           filled, and validated. Use field names only — no src_name prefix.
    derived: "<expression>"      #   pandas expression: "col_a / col_b"
                                 #   row-wise lambda:   "lambda row: row['a'] * 2"

    # --- Type casting (required) ---
    dtype: <type>                # cast the column to this type after source/derived.
                                 # Options: str, int32, int64, float32, float64,
                                 #          bool, datetime, nested

    # --- Optional: fill missing values (applied after dtype cast) ---
    fill_na: <value>             # scalar fill value. Omit if no fill is needed.

    # --- Optional: temp flag ---
    temp: true                   # available during processing but dropped before entity build.
                                 # Use for intermediate calculations referenced by derived fields.

    # --- Optional: field metadata ---
    description: "<text>"
    primary_key: true/false      # default false; primary key fields have no default in entity class
    entity_ref: <EntityName>     # for nested dtype only — names the sub-entity type

    # --- Optional: inline validations ---
    validation_type: [<types>]   # one or more of: not_null, unique, range, expression, custom
    validation_rule:             # required for range and expression; omit for not_null/unique
      range: [<min>, <max>]      #   use null for unbounded: [0, null]
      expression: "<rule>"       #   pandas-eval boolean expression across the full DataFrame
    validation_severity:         # per-type severity override; default is "warning"
      <type>: error              #   "error" halts the pipeline; "warning" logs and continues
```

**Field processing order:**

| Pass | Step | What happens |
|---|---|---|
| **Pass 1** — source | rename / expression | Batch rename, then evaluate source expressions |
| | dtype cast | Cast to declared type |
| | fill_na | Fill NAs |
| | source validations | Validate source fields (halt on error before derived runs) |
| **Pass 2** — derived | expression / lambda | Evaluate using field names (no src_name prefix needed) |
| | dtype cast + fill_na | Cast and fill derived columns |
| Filters | row filters | `filters:` expressions applied with @param support |
| Validations | derived + global validations | Validate derived fields and any global rules |
| **Build** | drop temp fields | Fields with `temp: true` removed from entity class |
| | entity class | Built from remaining (non-temp) fields |

### Extending the System

**Add a new source type:**
```python
@ConnectorRegistry.register("my_source")
class MyConnector(DataSourceConnector):
    def load(self, config: SourceConfig) -> pd.DataFrame: ...
```

**Add a new transform:**
```python
@TransformRegistry.register("my_transform")
class MyTransformer(Transformer):
    def apply(self, df: pd.DataFrame, config: TransformConfig) -> pd.DataFrame: ...
```

**Add a new validator:**
```python
@ValidatorRegistry.register("my_check")
class MyValidator(Validator):
    def validate(self, df: pd.DataFrame, config: ValidationConfig) -> ValidationResult: ...
```

### Tech Stack

| Concern | Choice |
|---|---|
| DataFrame | pandas 2.x |
| Config validation | Pydantic v2 |
| YAML parsing | PyYAML (safe_load) |
| Snowflake | snowflake-connector-python (optional) |
| Parquet | pyarrow |
| Excel | openpyxl |
| Testing | pytest |

## Usage

```python
from impact.entity.pipeline import EntityPipeline

# From a YAML config file
result = EntityPipeline("configs/facility_example.yaml").run()

# Or pass a pre-parsed EntityConfig object
result = EntityPipeline(config=entity_config_obj).run()

# Override source parameters at runtime (e.g. snapshot date)
result = EntityPipeline("configs/facility_example.yaml").run(
    parameters={"snapshot_date": "2025-12-31"}
)

result.entity_class      # Dynamically-created Facility dataclass
result.entities          # List of Facility instances
result.dataframe         # Processed pandas DataFrame
result.validation_report # Aggregated validation results
result.metadata          # Dict: entity_name, entity_version, source_count, record_count, field_count
```

## Key Implementation Details

### Environment Variable Interpolation

YAML configs support `${VAR}` and `${VAR:default_value}` syntax. Unresolved vars without defaults are left as-is (Pydantic will reject them if required).

```yaml
account: "${SNOWFLAKE_ACCOUNT}"
warehouse: "${SNOWFLAKE_WH:ANALYTICS_WH}"  # fallback if not set
```

### Exception Hierarchy

All exceptions inherit from `ImpactError`:

```
ImpactError
├── ConfigError        — invalid/missing YAML config
├── SourceError        — data source load failure
├── JoinError          — join operation failure
├── TransformError     — transformation step failure
├── ValidationError    — severity=error validation failure (has .report attribute)
└── EntityBuildError   — dynamic class creation or instantiation failure
```

### Registry Auto-Registration

Built-in connectors, transforms, and validators are registered via `@Registry.register("type")` decorators at import time. `pipeline.py` explicitly imports all builtin modules to trigger registration:

```python
import impact.entity.source.csv_excel   # triggers @ConnectorRegistry.register(...)
import impact.entity.transform.builtin  # triggers @TransformRegistry.register(...)
import impact.entity.validate.builtin   # triggers @ValidatorRegistry.register(...)
```

Custom plugins must also be imported before the pipeline runs.

### Join Conditions

Multiple key pairs and expression conditions can be mixed in the same join. All entries are AND-ed:

```yaml
on:
  - left_col: facility_id      # simple equality key
    right_col: facility_id
  - left_col: product_type     # composite key — both must match
    right_col: product_type
  - condition: "left.amount >= right.min_threshold"  # expression (uses pd.eval)
```

Expression conditions use `left.col` / `right.col` notation; right-side columns get `_right` suffix after merge.

### FieldConfig Supported dtypes

`str`, `string`, `int32`, `int64`, `float32`, `float64`, `bool`, `datetime`, `nested`

- `nested` maps to `pd.DataFrame` (for one-to-many joined data)
- Primary key fields have no default in the generated dataclass; all others default to `None`
- The generated class gets `__entity_fields__`, `__primary_key__`, and `__entity_name__` attributes

### Custom Functions (Transform / Validator)

```yaml
# Custom transform — function must accept (df: pd.DataFrame, **kwargs) → pd.DataFrame
type: custom
function: "mypackage.transforms.my_func"
kwargs:
  param1: value1

# Custom validator — function must accept (df: pd.DataFrame, **kwargs) → ValidationResult
type: custom
function: "mypackage.validators.my_check"
```
