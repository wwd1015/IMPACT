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
        # Add a validation that will fail
        full_config_dict["validations"].append(
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
                {"name": "id", "dtype": "str", "primary_key": True},
                {"name": "val", "dtype": "float64"},
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
                {"name": "id", "dtype": "str", "primary_key": True},
                {"name": "name", "dtype": "str"},
            ],
        }
        config = EntityConfig.model_validate(config_dict)
        result = EntityPipeline(config=config).run()
        assert len(result.entities) == 1
        assert result.entities[0].id == "X"
        assert result.entities[0].name == "Test"
