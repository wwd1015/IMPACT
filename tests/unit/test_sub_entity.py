"""Tests for sub-entity processing."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
import pytest
import yaml

from impact.entity.config.schema import EntityConfig, FieldConfig, ValidationConfig
from impact.entity.model.builder import EntityBuilder
from impact.entity.sub_entity import (
    SubEntityProcessor,
    SubEntityResult,
    _camel_to_snake,
    resolve_sub_entity_config,
)
from impact.entity.validate.base import ValidationReport
from impact.common.exceptions import ConfigError, TransformError, ValidationError


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def collateral_config_dict() -> dict:
    """Minimal sub-entity config (no sources)."""
    return {
        "entity": {"name": "Collateral", "version": "1.0"},
        "fields": [
            {"name": "collateral_type", "source": "collateral_type", "dtype": "str",
             "validation_type": ["not_null"], "validation_severity": {"not_null": "error"}},
            {"name": "collateral_value", "source": "collateral_value", "dtype": "float64",
             "validation_type": ["range"], "validation_rule": {"range": [0, None]},
             "validation_severity": {"range": "warning"}},
            {"name": "value_bucket", "derived": "lambda row: 'HIGH' if row['collateral_value'] >= 500000 else 'LOW'",
             "dtype": "str"},
        ],
    }


@pytest.fixture
def collateral_config(collateral_config_dict) -> EntityConfig:
    return EntityConfig(**collateral_config_dict)


@pytest.fixture
def nested_collateral_df() -> pd.DataFrame:
    return pd.DataFrame({
        "collateral_type": ["REAL_ESTATE", "EQUIPMENT"],
        "collateral_value": [500000.0, 100000.0],
    })


@pytest.fixture
def facility_with_sources_config_dict() -> dict:
    """Top-level config reused as sub-entity — has sources for prefix stripping."""
    return {
        "entity": {"name": "Facility", "version": "1.0"},
        "sources": [
            {"name": "facility_main", "type": "csv", "primary": True, "path": "/tmp/f.csv"},
            {"name": "rating_overrides", "type": "csv", "path": "/tmp/r.csv"},
        ],
        "fields": [
            {"name": "facility_id", "source": "facility_id", "dtype": "str", "primary_key": True},
            {"name": "commitment_amount", "source": "commitment_amount", "dtype": "float64"},
            {"name": "rating_override", "source": "rating_overrides.rating_override", "dtype": "str"},
            {"name": "available_capacity",
             "source": "facility_main.commitment_amount - facility_main.outstanding_balance",
             "dtype": "float64", "temp": True},
        ],
    }


# ---------------------------------------------------------------------------
# SubEntityProcessor tests
# ---------------------------------------------------------------------------

class TestSubEntityProcessor:
    def test_process_basic(self, collateral_config, nested_collateral_df):
        processor = SubEntityProcessor(collateral_config)
        result = processor.process(nested_collateral_df)

        assert isinstance(result, SubEntityResult)
        assert result.entity_class.__name__ == "Collateral"
        assert len(result.entities) == 2
        assert result.entities[0].collateral_type == "REAL_ESTATE"
        assert result.entities[0].collateral_value == 500000.0
        assert result.entities[1].collateral_type == "EQUIPMENT"

    def test_process_derived_fields(self, collateral_config, nested_collateral_df):
        processor = SubEntityProcessor(collateral_config)
        result = processor.process(nested_collateral_df)

        assert result.entities[0].value_bucket == "HIGH"
        assert result.entities[1].value_bucket == "LOW"

    def test_process_empty_dataframe(self, collateral_config):
        processor = SubEntityProcessor(collateral_config)
        result = processor.process(pd.DataFrame())

        assert result.entity_class.__name__ == "Collateral"
        assert result.entities == []
        assert result.validation_report.has_errors is False

    def test_process_validation_report(self, collateral_config, nested_collateral_df):
        processor = SubEntityProcessor(collateral_config)
        result = processor.process(nested_collateral_df)

        # not_null and range validations should have run
        assert len(result.validation_report.results) > 0
        assert result.validation_report.has_errors is False

    def test_process_validation_error_halts(self):
        """not_null with error severity halts on null values."""
        config = EntityConfig(**{
            "entity": {"name": "Test"},
            "fields": [
                {"name": "val", "source": "val", "dtype": "float64",
                 "validation_type": ["not_null"], "validation_severity": {"not_null": "error"}},
            ],
        })
        df = pd.DataFrame({"val": [1.0, float("nan"), 3.0]})
        processor = SubEntityProcessor(config)
        with pytest.raises(ValidationError):
            processor.process(df)

    def test_source_prefix_stripping(self, facility_with_sources_config_dict):
        """Top-level config with sources works as sub-entity via prefix stripping."""
        config = EntityConfig(**facility_with_sources_config_dict)
        df = pd.DataFrame({
            "facility_id": ["F001"],
            "commitment_amount": [1000000.0],
            "outstanding_balance": [750000.0],
            "rating_override": ["A+"],
        })
        processor = SubEntityProcessor(config)
        result = processor.process(df)

        assert result.entities[0].facility_id == "F001"
        assert result.entities[0].commitment_amount == 1000000.0
        assert result.entities[0].rating_override == "A+"

    def test_source_expression_with_prefix_stripping(self, facility_with_sources_config_dict):
        """Source expression with src_name. prefix is stripped and evaluated."""
        config = EntityConfig(**facility_with_sources_config_dict)
        df = pd.DataFrame({
            "facility_id": ["F001"],
            "commitment_amount": [1000000.0],
            "outstanding_balance": [750000.0],
            "rating_override": ["A+"],
        })
        processor = SubEntityProcessor(config)
        result = processor.process(df)

        # available_capacity = commitment - outstanding = 250000, but temp so not on entity
        assert not hasattr(result.entities[0], "available_capacity")

    def test_rename_in_sub_entity(self):
        config = EntityConfig(**{
            "entity": {"name": "Test"},
            "fields": [
                {"name": "product_category", "source": "product_type", "dtype": "str"},
            ],
        })
        df = pd.DataFrame({"product_type": ["TERM_LOAN", "REVOLVER"]})
        processor = SubEntityProcessor(config)
        result = processor.process(df)

        assert result.entities[0].product_category == "TERM_LOAN"
        assert result.entities[1].product_category == "REVOLVER"

    def test_derived_expression_failure(self):
        config = EntityConfig(**{
            "entity": {"name": "Test"},
            "fields": [
                {"name": "a", "source": "a", "dtype": "float64"},
                {"name": "bad", "derived": "nonexistent_col / a", "dtype": "float64"},
            ],
        })
        df = pd.DataFrame({"a": [1.0, 2.0]})
        processor = SubEntityProcessor(config)
        with pytest.raises(TransformError, match="derived expression failed"):
            processor.process(df)


# ---------------------------------------------------------------------------
# Recursive sub-entity processing
# ---------------------------------------------------------------------------

class TestRecursiveSubEntity:
    def test_recursive_processing(self, tmp_dir):
        """Facility sub-entity containing Collateral sub-entity."""
        # Write collateral config
        collateral_config = {
            "entity": {"name": "Collateral", "version": "1.0"},
            "fields": [
                {"name": "collateral_type", "source": "collateral_type", "dtype": "str"},
                {"name": "collateral_value", "source": "collateral_value", "dtype": "float64"},
            ],
        }
        collateral_path = tmp_dir / "collateral.yaml"
        with open(collateral_path, "w") as f:
            yaml.dump(collateral_config, f)

        # Facility config with entity_ref: Collateral
        facility_config = EntityConfig(**{
            "entity": {"name": "Facility", "version": "1.0"},
            "fields": [
                {"name": "facility_id", "source": "facility_id", "dtype": "str", "primary_key": True},
                {"name": "collateral_items", "source": "collateral_items", "dtype": "nested",
                 "entity_ref": "Collateral"},
            ],
        })

        nested_collateral = pd.DataFrame({
            "collateral_type": ["REAL_ESTATE", "EQUIPMENT"],
            "collateral_value": [500000.0, 100000.0],
        })
        df = pd.DataFrame({
            "facility_id": ["F001"],
            "collateral_items": [nested_collateral],
        })

        processor = SubEntityProcessor(facility_config, config_path=tmp_dir / "facility.yaml")
        result = processor.process(df)

        # collateral_items should be a list of Collateral instances
        assert isinstance(result.entities[0].collateral_items, list)
        assert len(result.entities[0].collateral_items) == 2
        assert result.entities[0].collateral_items[0].collateral_type == "REAL_ESTATE"


# ---------------------------------------------------------------------------
# resolve_sub_entity_config tests
# ---------------------------------------------------------------------------

class TestResolveSubEntityConfig:
    def test_resolve_snake_case(self, tmp_dir):
        config_dict = {
            "entity": {"name": "Collateral"},
            "fields": [{"name": "x", "source": "x", "dtype": "str"}],
        }
        config_path = tmp_dir / "collateral.yaml"
        with open(config_path, "w") as f:
            yaml.dump(config_dict, f)

        result = resolve_sub_entity_config("Collateral", tmp_dir / "parent.yaml")
        assert result.entity.name == "Collateral"

    def test_resolve_example_suffix(self, tmp_dir):
        config_dict = {
            "entity": {"name": "Collateral"},
            "fields": [{"name": "x", "source": "x", "dtype": "str"}],
        }
        config_path = tmp_dir / "collateral_example.yaml"
        with open(config_path, "w") as f:
            yaml.dump(config_dict, f)

        result = resolve_sub_entity_config("Collateral", tmp_dir / "parent.yaml")
        assert result.entity.name == "Collateral"

    def test_resolve_camel_case(self, tmp_dir):
        config_dict = {
            "entity": {"name": "FinancialStatement"},
            "fields": [{"name": "x", "source": "x", "dtype": "str"}],
        }
        config_path = tmp_dir / "financial_statement.yaml"
        with open(config_path, "w") as f:
            yaml.dump(config_dict, f)

        result = resolve_sub_entity_config("FinancialStatement", tmp_dir / "parent.yaml")
        assert result.entity.name == "FinancialStatement"

    def test_resolve_no_parent_path_raises(self):
        with pytest.raises(ConfigError, match="no parent config path"):
            resolve_sub_entity_config("Collateral", None)

    def test_resolve_not_found_raises(self, tmp_dir):
        with pytest.raises(ConfigError, match="not found"):
            resolve_sub_entity_config("NonExistent", tmp_dir / "parent.yaml")


# ---------------------------------------------------------------------------
# _camel_to_snake tests
# ---------------------------------------------------------------------------

class TestCamelToSnake:
    def test_simple(self):
        assert _camel_to_snake("Collateral") == "collateral"

    def test_multi_word(self):
        assert _camel_to_snake("FinancialStatement") == "financial_statement"

    def test_consecutive_capitals(self):
        assert _camel_to_snake("HTTPServer") == "http_server"

    def test_already_snake(self):
        assert _camel_to_snake("already_snake") == "already_snake"

    def test_single_word_lower(self):
        assert _camel_to_snake("test") == "test"
