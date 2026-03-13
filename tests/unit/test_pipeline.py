"""Tests for the pipeline orchestrator."""

from __future__ import annotations

import pytest
import yaml

from impact.common.exceptions import ConfigError, TransformError, ValidationError
from impact.entity.config.schema import EntityConfig
from impact.entity.pipeline import EntityPipeline, PipelineResult

from conftest import write_yaml_config


def _build_csv_config(tmp_dir, csv_data, base_fields, extra_fields, **config_overrides):
    """Shared helper to create a simple CSV-based pipeline config for testing.

    Args:
        tmp_dir: Temporary directory for the CSV file.
        csv_data: Dict of column → values for the CSV.
        base_fields: Base field definitions (always included).
        extra_fields: Additional field definitions to append.
        **config_overrides: Top-level config keys (parameters, expression_packages, etc.).
    """
    import pandas as pd

    csv_path = tmp_dir / "data.csv"
    pd.DataFrame(csv_data).to_csv(csv_path, index=False)

    config_dict = {
        "entity": {"name": "TestEntity"},
        "sources": [{"name": "main", "type": "csv", "primary": True, "path": str(csv_path)}],
        "fields": base_fields + extra_fields,
        **config_overrides,
    }
    return EntityConfig.model_validate(config_dict)


class TestEntityPipeline:
    """Tests for the full pipeline orchestrator."""

    def test_pipeline_from_config_object(self, full_config_dict):
        config = EntityConfig.model_validate(full_config_dict)
        pipeline = EntityPipeline(config=config)
        assert pipeline.config.entity.name == "Facility"

    def test_pipeline_from_yaml_file(self, tmp_dir, full_config_dict):
        path = write_yaml_config(full_config_dict, tmp_dir / "config.yaml")
        pipeline = EntityPipeline(config_path=path)
        assert pipeline.config.entity.name == "Facility"

    def test_pipeline_no_config_raises(self):
        with pytest.raises(ConfigError):
            EntityPipeline()

    def test_pipeline_run_end_to_end(self, full_config_dict):
        """Full end-to-end pipeline test with CSV sources, joins, transforms, validations."""
        config = EntityConfig.model_validate(full_config_dict)
        pipeline = EntityPipeline(config=config)
        result = pipeline.run()

        assert isinstance(result, PipelineResult)

        # Entity class is created dynamically
        assert result.entity_class.__name__ == "Facility"
        assert result.entity_class.__primary_key__ == ["facility_id"]

        # 3 facility records preserved after 1-to-many join
        assert len(result.entities) == 3
        assert len(result.dataframe) == 3

        # Check fields exist
        entity = result.entities[0]
        assert hasattr(entity, "facility_id")
        assert hasattr(entity, "utilization_rate")
        assert hasattr(entity, "collateral_items")

        # Utilization rate derived correctly
        assert entity.utilization_rate is not None

        # Nested collateral items
        import pandas as pd
        assert isinstance(entity.collateral_items, pd.DataFrame)

    def test_pipeline_result_metadata(self, full_config_dict):
        config = EntityConfig.model_validate(full_config_dict)
        result = EntityPipeline(config=config).run()

        assert result.metadata["entity_name"] == "Facility"
        assert result.metadata["record_count"] == 3
        assert result.metadata["source_count"] == 2

    def test_pipeline_validation_report(self, full_config_dict):
        config = EntityConfig.model_validate(full_config_dict)
        result = EntityPipeline(config=config).run()

        assert result.validation_report is not None
        assert result.validation_report.has_errors is False

    def test_pipeline_validation_error_halts(self, tmp_dir, full_config_dict):
        """Pipeline should raise ValidationError when severity=error validation fails."""
        # Add a global validation that will fail
        full_config_dict.setdefault("validations", []).append(
            {
                "type": "expression",
                "rule": "commitment_amount > 9999999",  # No facility has this
                "message": "Commitment too low",
                "severity": "error",
            }
        )

        config = EntityConfig.model_validate(full_config_dict)
        pipeline = EntityPipeline(config=config)

        with pytest.raises(ValidationError):
            pipeline.run()


class TestPipelineWithoutOptionalSteps:
    """Test that pipeline works when optional sections are omitted."""

    def test_no_joins(self, tmp_dir):
        csv_path = tmp_dir / "data.csv"
        import pandas as pd
        pd.DataFrame({"id": ["A", "B"], "val": [1.0, 2.0]}).to_csv(csv_path, index=False)

        config_dict = {
            "entity": {"name": "Simple"},
            "sources": [{"name": "main", "type": "csv", "primary": True, "path": str(csv_path)}],
            "fields": [
                {"name": "id", "source": "id", "dtype": "str", "primary_key": True},
                {"name": "val", "source": "val", "dtype": "float64"},
            ],
        }
        config = EntityConfig.model_validate(config_dict)
        result = EntityPipeline(config=config).run()
        assert len(result.entities) == 2

    def test_no_transforms_or_validations(self, tmp_dir):
        csv_path = tmp_dir / "data.csv"
        import pandas as pd
        pd.DataFrame({"id": ["X"], "name": ["Test"]}).to_csv(csv_path, index=False)

        config_dict = {
            "entity": {"name": "Minimal"},
            "sources": [{"name": "src", "type": "csv", "primary": True, "path": str(csv_path)}],
            "fields": [
                {"name": "id", "source": "id", "dtype": "str", "primary_key": True},
                {"name": "name", "source": "name", "dtype": "str"},
            ],
        }
        config = EntityConfig.model_validate(config_dict)
        result = EntityPipeline(config=config).run()
        assert len(result.entities) == 1
        assert result.entities[0].id == "X"
        assert result.entities[0].name == "Test"


class TestParametersInExpressions:
    """Test that @param / param variables work in derived expressions and lambdas."""

    _CSV_DATA = {"id": ["A", "B", "C"], "amount": [100.0, 200.0, 300.0], "rate": [0.05, 0.10, 0.15]}
    _BASE_FIELDS = [
        {"name": "id", "source": "id", "dtype": "str", "primary_key": True},
        {"name": "amount", "source": "amount", "dtype": "float64"},
        {"name": "rate", "source": "rate", "dtype": "float64"},
    ]

    def _make_config(self, tmp_dir, fields, parameters=None, **extra):
        return _build_csv_config(tmp_dir, self._CSV_DATA, self._BASE_FIELDS, fields, parameters=parameters or {}, **extra)

    def test_param_in_derived_eval_expression(self, tmp_dir):
        """@param works in pandas eval derived expressions."""
        config = self._make_config(tmp_dir, [
            {"name": "scaled", "derived": "amount * @scale_factor", "dtype": "float64"},
        ], parameters={"scale_factor": 2.0})

        result = EntityPipeline(config=config).run()
        assert list(result.dataframe["scaled"]) == [200.0, 400.0, 600.0]

    def test_param_in_derived_lambda(self, tmp_dir):
        """Parameters are accessible as variables in lambda expressions."""
        config = self._make_config(tmp_dir, [
            {
                "name": "adjusted",
                "derived": "lambda row: row['amount'] * multiplier",
                "dtype": "float64",
            },
        ], parameters={"multiplier": 3.0})

        result = EntityPipeline(config=config).run()
        assert list(result.dataframe["adjusted"]) == [300.0, 600.0, 900.0]

    def test_param_in_source_expression(self, tmp_dir):
        """@param works in source expressions (Pass 1)."""
        config = self._make_config(tmp_dir, [
            {"name": "offset_amount", "source": "amount + @base_offset", "dtype": "float64"},
        ], parameters={"base_offset": 50.0})

        result = EntityPipeline(config=config).run()
        assert list(result.dataframe["offset_amount"]) == [150.0, 250.0, 350.0]

    def test_param_in_post_filter(self, tmp_dir):
        """@param works in post_filters."""
        config = self._make_config(tmp_dir, [],
            parameters={"min_amount": 150.0},
            post_filters=["amount >= @min_amount"],
        )
        result = EntityPipeline(config=config).run()
        assert len(result.entities) == 2
        assert [e.id for e in result.entities] == ["B", "C"]

    def test_param_in_pre_filter(self, tmp_dir):
        """@param works in pre_filters."""
        config = self._make_config(tmp_dir, [],
            parameters={"cutoff": 250.0},
            pre_filters=["amount <= @cutoff"],
        )
        result = EntityPipeline(config=config).run()
        assert len(result.entities) == 2
        assert [e.id for e in result.entities] == ["A", "B"]

    def test_runtime_param_overrides_config_default(self, tmp_dir):
        """Runtime parameters override config defaults in expressions."""
        config = self._make_config(tmp_dir, [
            {"name": "scaled", "derived": "amount * @factor", "dtype": "float64"},
        ], parameters={"factor": 1.0})

        # Config default factor=1.0, runtime overrides to 10.0
        result = EntityPipeline(config=config).run(parameters={"factor": 10.0})
        assert list(result.dataframe["scaled"]) == [1000.0, 2000.0, 3000.0]

    def test_lambda_with_multiple_params(self, tmp_dir):
        """Lambda can reference multiple parameters."""
        config = self._make_config(tmp_dir, [
            {
                "name": "calc",
                "derived": "lambda row: row['amount'] * scale + offset",
                "dtype": "float64",
            },
        ], parameters={"scale": 2.0, "offset": 10.0})

        result = EntityPipeline(config=config).run()
        assert list(result.dataframe["calc"]) == [210.0, 410.0, 610.0]


class TestDerivedAccessToFullDataFrame:
    """Test that derived fields can access all DataFrame columns, not just declared fields."""

    def test_derived_uses_undeclared_source_column(self, tmp_dir):
        """Derived field can reference a raw source column not declared in fields."""
        import pandas as pd

        csv_path = tmp_dir / "data.csv"
        pd.DataFrame({
            "id": ["A", "B"],
            "amount": [100.0, 200.0],
            "hidden_rate": [0.05, 0.10],  # not declared in fields
        }).to_csv(csv_path, index=False)

        config_dict = {
            "entity": {"name": "TestEntity"},
            "sources": [{"name": "main", "type": "csv", "primary": True, "path": str(csv_path)}],
            "fields": [
                {"name": "id", "source": "id", "dtype": "str", "primary_key": True},
                {"name": "amount", "source": "amount", "dtype": "float64"},
                # hidden_rate is NOT declared — but derived can still use it
                {"name": "interest", "derived": "amount * hidden_rate", "dtype": "float64"},
            ],
        }
        config = EntityConfig.model_validate(config_dict)
        result = EntityPipeline(config=config).run()
        assert list(result.dataframe["interest"]) == [5.0, 20.0]
        # hidden_rate should NOT appear in the final entity
        assert not hasattr(result.entities[0], "hidden_rate")

    def test_derived_uses_original_after_copy(self, tmp_dir):
        """After a source field copies a column, the original is still accessible."""
        import pandas as pd

        csv_path = tmp_dir / "data.csv"
        pd.DataFrame({
            "id": ["A", "B"],
            "product_type": ["LOAN", "REVOLVER"],
            "amount": [100.0, 200.0],
        }).to_csv(csv_path, index=False)

        config_dict = {
            "entity": {"name": "TestEntity"},
            "sources": [{"name": "main", "type": "csv", "primary": True, "path": str(csv_path)}],
            "fields": [
                {"name": "id", "source": "id", "dtype": "str", "primary_key": True},
                {"name": "product_category", "source": "product_type", "dtype": "str"},
                # Use the ORIGINAL column name in a derived expression
                {
                    "name": "is_loan",
                    "derived": "lambda row: row['product_type'] == 'LOAN'",
                    "dtype": "bool",
                },
            ],
        }
        config = EntityConfig.model_validate(config_dict)
        result = EntityPipeline(config=config).run()
        assert result.entities[0].product_category == "LOAN"
        assert result.entities[0].is_loan is True
        assert result.entities[1].is_loan is False
        # Original column not in final entity
        assert not hasattr(result.entities[0], "product_type")


class TestExpressionPackages:
    """Test configurable expression_packages feature."""

    _CSV_DATA = {"id": ["A", "B", "C"], "value": [1.0, 4.0, 9.0]}
    _BASE_FIELDS = [
        {"name": "id", "source": "id", "dtype": "str", "primary_key": True},
        {"name": "value", "source": "value", "dtype": "float64"},
    ]

    def _make_config(self, tmp_dir, fields, expression_packages=None, **extra):
        if expression_packages is not None:
            extra["expression_packages"] = expression_packages
        return _build_csv_config(tmp_dir, self._CSV_DATA, self._BASE_FIELDS, fields, **extra)

    def test_math_package_in_lambda(self, tmp_dir):
        """Custom package (math) works in lambda when declared."""
        config = self._make_config(tmp_dir, [
            {"name": "root", "derived": "lambda row: math.sqrt(row['value'])", "dtype": "float64"},
        ], expression_packages={"pd": "pandas", "np": "numpy", "math": "math"})
        result = EntityPipeline(config=config).run()
        assert [e.root for e in result.entities] == [1.0, 2.0, 3.0]

    def test_custom_alias(self, tmp_dir):
        """Custom alias (m → math) works in lambda."""
        config = self._make_config(tmp_dir, [
            {"name": "root", "derived": "lambda row: m.sqrt(row['value'])", "dtype": "float64"},
        ], expression_packages={"pd": "pandas", "np": "numpy", "m": "math"})
        result = EntityPipeline(config=config).run()
        assert [e.root for e in result.entities] == [1.0, 2.0, 3.0]

    def test_missing_package_helpful_error(self, tmp_dir):
        """Using an undeclared package gives a helpful hint in the error."""
        config = self._make_config(tmp_dir, [
            {"name": "root", "derived": "lambda row: math.sqrt(row['value'])", "dtype": "float64"},
        ])
        with pytest.raises(TransformError, match="expression_packages"):
            EntityPipeline(config=config).run()

    def test_source_name_conflicts_with_custom_package(self):
        """Source name that matches an expression_packages alias is rejected."""
        with pytest.raises(ValueError, match="conflicts with expression package"):
            EntityConfig.model_validate({
                "entity": {"name": "Test"},
                "expression_packages": {"pd": "pandas", "math": "math"},
                "sources": [{"name": "math", "type": "csv", "primary": True, "path": "/tmp/x.csv"}],
                "fields": [{"name": "id", "source": "id", "dtype": "str"}],
            })

    def test_default_packages_when_omitted(self, tmp_dir):
        """pd and np are available by default even without explicit expression_packages."""
        config = self._make_config(tmp_dir, [
            {"name": "root", "derived": "lambda row: np.sqrt(row['value'])", "dtype": "float64"},
        ])
        result = EntityPipeline(config=config).run()
        assert [e.root for e in result.entities] == [1.0, 2.0, 3.0]


class TestPandasNumpyInExpressions:
    """Test that pd/np functions work in derived expressions and lambdas."""

    _CSV_DATA = {"id": ["A", "B", "C"], "amount": [100.0, None, 300.0], "value": [1.0, 4.0, 9.0]}
    _BASE_FIELDS = [
        {"name": "id", "source": "id", "dtype": "str", "primary_key": True},
        {"name": "amount", "source": "amount", "dtype": "float64"},
        {"name": "value", "source": "value", "dtype": "float64"},
    ]

    def _make_config(self, tmp_dir, fields, parameters=None, **extra):
        return _build_csv_config(tmp_dir, self._CSV_DATA, self._BASE_FIELDS, fields, parameters=parameters or {}, **extra)

    def test_pd_isna_in_lambda(self, tmp_dir):
        """pd.isna() works inside lambda expressions."""
        config = self._make_config(tmp_dir, [
            {
                "name": "is_missing",
                "derived": "lambda row: pd.isna(row['amount'])",
                "dtype": "bool",
            },
        ])
        result = EntityPipeline(config=config).run()
        assert [e.is_missing for e in result.entities] == [False, True, False]

    def test_np_sqrt_in_lambda(self, tmp_dir):
        """np.sqrt() works inside lambda expressions."""
        config = self._make_config(tmp_dir, [
            {
                "name": "root",
                "derived": "lambda row: np.sqrt(row['value'])",
                "dtype": "float64",
            },
        ])
        result = EntityPipeline(config=config).run()
        assert [e.root for e in result.entities] == [1.0, 2.0, 3.0]

    def test_pd_isna_in_eval(self, tmp_dir):
        """pd.isna() works in pandas eval expressions (no @ prefix needed)."""
        config = self._make_config(tmp_dir, [
            {"name": "is_missing", "derived": "pd.isna(amount)", "dtype": "bool"},
        ])
        result = EntityPipeline(config=config).run()
        assert [e.is_missing for e in result.entities] == [False, True, False]

    def test_np_log_in_eval_via_param(self, tmp_dir):
        """Custom function passed as parameter works with @param syntax."""
        import numpy as np

        config = self._make_config(tmp_dir, [
            {"name": "log_val", "derived": "@log(value)", "dtype": "float64"},
        ], parameters={"log": np.log})
        result = EntityPipeline(config=config).run()
        expected = [float(np.log(v)) for v in [1.0, 4.0, 9.0]]
        assert list(result.dataframe["log_val"]) == expected

    def test_pd_isna_in_source_expression(self, tmp_dir):
        """pd.isna() works in source expressions (Pass 1, no @ prefix needed)."""
        config = self._make_config(tmp_dir, [
            {"name": "amount_missing", "source": "pd.isna(amount)", "dtype": "bool"},
        ])
        result = EntityPipeline(config=config).run()
        assert list(result.dataframe["amount_missing"]) == [False, True, False]

    def test_pd_fillna_in_lambda(self, tmp_dir):
        """pd functions work for complex operations in lambdas."""
        config = self._make_config(tmp_dir, [
            {
                "name": "safe_amount",
                "derived": "lambda row: 0.0 if pd.isna(row['amount']) else row['amount']",
                "dtype": "float64",
            },
        ])
        result = EntityPipeline(config=config).run()
        assert list(result.dataframe["safe_amount"]) == [100.0, 0.0, 300.0]

    def test_np_where_in_lambda(self, tmp_dir):
        """np.where logic works in lambda expressions."""
        config = self._make_config(tmp_dir, [
            {
                "name": "capped",
                "derived": "lambda row: min(row['value'], np.float64(5.0))",
                "dtype": "float64",
            },
        ])
        result = EntityPipeline(config=config).run()
        assert list(result.dataframe["capped"]) == [1.0, 4.0, 5.0]

    def test_param_in_pd_function_in_lambda(self, tmp_dir):
        """External parameter works as argument to pd function inside lambda."""
        config = self._make_config(tmp_dir, [
            {
                "name": "safe_amount",
                "derived": "lambda row: default_val if pd.isna(row['amount']) else row['amount']",
                "dtype": "float64",
            },
        ], parameters={"default_val": -1.0})
        result = EntityPipeline(config=config).run(parameters={"default_val": -1.0})
        assert list(result.dataframe["safe_amount"]) == [100.0, -1.0, 300.0]

    def test_at_param_in_pd_function_in_lambda(self, tmp_dir):
        """@param syntax works inside pd function calls in lambdas."""
        config = self._make_config(tmp_dir, [
            {
                "name": "result",
                "derived": "lambda row: str(pd.to_datetime(@snapshot_date))",
                "dtype": "str",
            },
        ], parameters={"snapshot_date": "2025-01-01"})
        result = EntityPipeline(config=config).run(parameters={"snapshot_date": "2025-01-01"})
        assert all(e.result == "2025-01-01 00:00:00" for e in result.entities)

    def test_at_param_as_fallback_in_lambda(self, tmp_dir):
        """@param works as a fallback value with pd.isna() in lambdas."""
        config = self._make_config(tmp_dir, [
            {
                "name": "filled",
                "derived": "lambda row: @default_val if pd.isna(row['amount']) else row['amount']",
                "dtype": "float64",
            },
        ], parameters={"default_val": 0.0})
        result = EntityPipeline(config=config).run(parameters={"default_val": 0.0})
        assert list(result.dataframe["filled"]) == [100.0, 0.0, 300.0]

    def test_at_param_multiple_in_lambda(self, tmp_dir):
        """Multiple @params work in the same lambda expression."""
        config = self._make_config(tmp_dir, [
            {
                "name": "scaled",
                "derived": "lambda row: (row['amount'] * @factor + @offset) if not pd.isna(row['amount']) else @offset",
                "dtype": "float64",
            },
        ], parameters={"factor": 2.0, "offset": 10.0})
        result = EntityPipeline(config=config).run(parameters={"factor": 2.0, "offset": 10.0})
        assert list(result.dataframe["scaled"]) == [210.0, 10.0, 610.0]
