"""Tests for the pipeline orchestrator."""

from __future__ import annotations

import pytest
import yaml

from impact.common.exceptions import ConfigError, ValidationError
from impact.entity.config.schema import EntityConfig
from impact.entity.pipeline import EntityPipeline, PipelineResult

from conftest import write_yaml_config


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

    def _make_config(self, tmp_dir, fields, parameters=None, **extra):
        """Helper to create a simple config with parameters.

        Args:
            extra: Additional top-level config keys (e.g. pre_filters, post_filters).
        """
        import pandas as pd

        csv_path = tmp_dir / "data.csv"
        pd.DataFrame({
            "id": ["A", "B", "C"],
            "amount": [100.0, 200.0, 300.0],
            "rate": [0.05, 0.10, 0.15],
        }).to_csv(csv_path, index=False)

        config_dict = {
            "entity": {"name": "TestEntity"},
            "parameters": parameters or {},
            "sources": [{"name": "main", "type": "csv", "primary": True, "path": str(csv_path)}],
            "fields": [
                {"name": "id", "source": "id", "dtype": "str", "primary_key": True},
                {"name": "amount", "source": "amount", "dtype": "float64"},
                {"name": "rate", "source": "rate", "dtype": "float64"},
            ] + fields,
            **extra,
        }
        return EntityConfig.model_validate(config_dict)

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
