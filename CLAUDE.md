# IMPACT — Developer Guide

## Project Overview

IMPACT is a standardized model development and deployment platform for financial lending credit modeling. The first component is the **Entity Data Module** — a YAML-config-driven pipeline for processing entity data (Facility/Obligor).

## Quick Commands

```bash
pip3 install -e ".[all]"       # Install with all dependencies
python3 -m pytest tests/ -v    # Run tests
```

## Architecture

The Entity Data Module follows a **5-stage pipeline**: Load → Join → Transform → Validate → Build.

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
└── common/              # Shared exceptions and logging
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

A single config file defines the entire pipeline:

```yaml
entity:        # Name, description, version
sources:       # Data source definitions (one must be primary: true)
joins:         # How to combine sources (one_to_one / one_to_many)
transforms:    # Ordered transformation steps
validations:   # Validation rules with error/warning severity
fields:        # Entity class field definitions (= output schema)
```

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

result = EntityPipeline("configs/facility_example.yaml").run()
result.entity_class      # Dynamically-created Facility dataclass
result.entities           # List of Facility instances
result.dataframe          # Processed pandas DataFrame
result.validation_report  # Aggregated validation results
```
