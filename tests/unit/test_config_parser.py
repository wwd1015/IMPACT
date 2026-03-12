"""Tests for the config parser and schema validation."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml

from impact.common.exceptions import ConfigError
from impact.entity.config.parser import ConfigParser
from impact.entity.config.schema import EntityConfig, SourceConfig

from conftest import write_yaml_config


class TestConfigSchema:
    """Tests for Pydantic schema validation."""

    def test_minimal_valid_config(self, minimal_config_dict):
        """A minimal config with required fields should validate."""
        config = EntityConfig.model_validate(minimal_config_dict)
        assert config.entity.name == "TestEntity"
        assert len(config.sources) == 1
        assert config.sources[0].primary is True
        assert len(config.fields) == 2

    def test_missing_entity_name_fails(self, minimal_config_dict):
        del minimal_config_dict["entity"]["name"]
        with pytest.raises(Exception):
            EntityConfig.model_validate(minimal_config_dict)

    def test_no_primary_source_fails(self, minimal_config_dict):
        minimal_config_dict["sources"][0]["primary"] = False
        with pytest.raises(ValueError, match="primary"):
            EntityConfig.model_validate(minimal_config_dict)

    def test_multiple_primary_sources_fails(self, minimal_config_dict):
        minimal_config_dict["sources"].append(
            {"name": "extra", "type": "csv", "primary": True, "path": "/tmp/x.csv"}
        )
        with pytest.raises(ValueError, match="primary"):
            EntityConfig.model_validate(minimal_config_dict)

    def test_snowflake_source_requires_connection(self):
        with pytest.raises(ValueError, match="connection"):
            SourceConfig.model_validate(
                {"name": "sf", "type": "snowflake", "primary": True}
            )

    def test_csv_source_requires_path(self):
        with pytest.raises(ValueError, match="path"):
            SourceConfig.model_validate(
                {"name": "c", "type": "csv", "primary": True}
            )

    @pytest.mark.parametrize("unsafe_name", ["os", "sys", "subprocess", "shutil"])
    def test_unsafe_source_name_rejected(self, unsafe_name):
        """Source names that are unsafe are rejected at SourceConfig level."""
        with pytest.raises(ValueError, match="reserved"):
            SourceConfig.model_validate(
                {"name": unsafe_name, "type": "csv", "primary": True, "path": "/tmp/x.csv"}
            )

    @pytest.mark.parametrize("pkg_alias", ["pd", "np"])
    def test_source_name_conflicts_with_expression_package(self, pkg_alias, minimal_config_dict):
        """Source names that conflict with expression_packages are rejected at EntityConfig level."""
        minimal_config_dict["sources"][0]["name"] = pkg_alias
        with pytest.raises(ValueError, match="conflicts with expression package"):
            EntityConfig.model_validate(minimal_config_dict)

    def test_one_to_many_requires_nested_as(self, minimal_config_dict):
        minimal_config_dict["sources"].append(
            {"name": "secondary", "type": "csv", "path": "/tmp/s.csv"}
        )
        minimal_config_dict["joins"] = [
            {
                "left": "main_source",
                "right": "secondary",
                "how": "left",
                "on": [{"left_col": "id", "right_col": "id"}],
                "relationship": "one_to_many",
                # missing nested_as
            }
        ]
        with pytest.raises(ValueError, match="nested_as"):
            EntityConfig.model_validate(minimal_config_dict)

    def test_join_unknown_source_fails(self, minimal_config_dict):
        minimal_config_dict["joins"] = [
            {
                "left": "main_source",
                "right": "nonexistent",
                "how": "left",
                "on": [{"left_col": "id", "right_col": "id"}],
            }
        ]
        with pytest.raises(ValueError, match="nonexistent"):
            EntityConfig.model_validate(minimal_config_dict)

    def test_full_config_validates(self, full_config_dict):
        config = EntityConfig.model_validate(full_config_dict)
        assert config.entity.name == "Facility"
        assert len(config.sources) == 2
        assert len(config.joins) == 1
        assert len(config.fields) == 8


class TestConfigParser:
    """Tests for YAML loading and environment variable interpolation."""

    def test_parse_valid_yaml(self, tmp_dir, minimal_config_dict):
        path = write_yaml_config(minimal_config_dict, tmp_dir / "config.yaml")
        parser = ConfigParser()
        config = parser.parse(path)
        assert config.entity.name == "TestEntity"

    def test_parse_nonexistent_file(self):
        parser = ConfigParser()
        with pytest.raises(ConfigError, match="not found"):
            parser.parse("/nonexistent/config.yaml")

    def test_parse_invalid_yaml(self, tmp_dir):
        bad_yaml = tmp_dir / "bad.yaml"
        bad_yaml.write_text("{ invalid yaml: [")
        parser = ConfigParser()
        with pytest.raises(ConfigError, match="YAML parse error"):
            parser.parse(bad_yaml)

    def test_env_var_interpolation(self, tmp_dir, minimal_config_dict):
        minimal_config_dict["entity"]["name"] = "${TEST_ENTITY_NAME}"
        path = write_yaml_config(minimal_config_dict, tmp_dir / "config.yaml")

        os.environ["TEST_ENTITY_NAME"] = "InterpolatedEntity"
        try:
            parser = ConfigParser()
            config = parser.parse(path)
            assert config.entity.name == "InterpolatedEntity"
        finally:
            del os.environ["TEST_ENTITY_NAME"]

    def test_env_var_with_default(self, tmp_dir, minimal_config_dict):
        minimal_config_dict["entity"]["name"] = "${NONEXISTENT_VAR:DefaultName}"
        path = write_yaml_config(minimal_config_dict, tmp_dir / "config.yaml")

        parser = ConfigParser()
        config = parser.parse(path)
        assert config.entity.name == "DefaultName"

    def test_parse_full_config(self, tmp_dir, full_config_dict):
        path = write_yaml_config(full_config_dict, tmp_dir / "full.yaml")
        parser = ConfigParser()
        config = parser.parse(path)
        assert config.entity.name == "Facility"
        assert len(config.joins) == 1
        assert config.joins[0].relationship == "one_to_many"
