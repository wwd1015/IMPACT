# IMPACT

**Standardized Model Development and Deployment Platform**

A YAML-config-driven platform for financial lending credit modeling, providing standardized pipelines for entity data processing, model development, and deployment.

## Entity Data Module

The first component of IMPACT — a declarative pipeline for loading, transforming, validating, and structuring entity data (Facility / Obligor) from heterogeneous sources.

### Quick Start

```python
from impact.entity.pipeline import EntityPipeline

result = EntityPipeline("configs/facility_example.yaml").run()

# Access the dynamically-created entity class
print(result.entity_class)

# Access entity instances
for entity in result.entities[:5]:
    print(entity)

# Access the processed DataFrame
print(result.dataframe.head())
```

### Installation

```bash
# Core installation
pip install -e .

# With Snowflake support
pip install -e ".[snowflake]"

# With dev tools
pip install -e ".[dev]"

# Everything
pip install -e ".[all]"
```

### Running Tests

```bash
pytest tests/ -v
```
