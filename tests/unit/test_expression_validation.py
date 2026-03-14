"""Tests for parse-time expression validation and derived function references."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from impact.common.exceptions import ConfigError, TransformError
from impact.entity.config.schema import DerivedFunctionRef, EntityConfig, FieldConfig
from impact.entity.pipeline import EntityPipeline
from tests.conftest import write_yaml_config


# ---------------------------------------------------------------------------
# #1: Parse-time expression syntax validation
# ---------------------------------------------------------------------------


class TestExpressionSyntaxValidation:
    """Syntax errors in expressions are caught at config parse time."""

    def test_valid_derived_expression(self):
        """Valid expression parses without error."""
        field = FieldConfig(name="x", derived="a + b", dtype="float64")
        assert field.derived == "a + b"

    def test_valid_lambda_expression(self):
        """Valid lambda parses without error."""
        field = FieldConfig(name="x", derived="lambda row: row['a'] * 2", dtype="float64")
        assert field.derived == "lambda row: row['a'] * 2"

    def test_mismatched_parens_caught(self):
        """Mismatched parentheses caught at parse time."""
        with pytest.raises(Exception, match="syntax error"):
            FieldConfig(name="x", derived="(a + b", dtype="float64")

    def test_mismatched_brackets_caught(self):
        """Mismatched brackets in lambda caught at parse time."""
        with pytest.raises(Exception, match="syntax error"):
            FieldConfig(name="x", derived="lambda row: row['a'", dtype="float64")

    def test_invalid_lambda_syntax_caught(self):
        """Bad lambda syntax caught at parse time."""
        with pytest.raises(Exception, match="syntax error"):
            FieldConfig(name="x", derived="lambda row: if row['a']", dtype="float64")

    def test_valid_source_expression(self):
        """Valid source expression parses without error."""
        field = FieldConfig(name="x", source="col_a + col_b", dtype="float64")
        assert field.source == "col_a + col_b"

    def test_source_simple_column_not_validated(self):
        """Simple column names (identifiers) are not compiled."""
        field = FieldConfig(name="x", source="my_column", dtype="str")
        assert field.source == "my_column"

    def test_source_expression_mismatched_parens(self):
        """Source expression syntax errors caught at parse time."""
        with pytest.raises(Exception, match="syntax error"):
            FieldConfig(name="x", source="(col_a + col_b", dtype="float64")

    def test_valid_complex_expression(self):
        """Complex but valid expression parses fine."""
        field = FieldConfig(
            name="x",
            derived="lambda row: 'HIGH' if row['amount'] > 800 else 'MEDIUM'",
            dtype="str",
        )
        assert "HIGH" in field.derived

    def test_error_message_includes_field_name(self):
        """Error message includes the field name for easy debugging."""
        with pytest.raises(Exception, match="my_field"):
            FieldConfig(name="my_field", derived="(unclosed", dtype="float64")

    def test_error_message_includes_expression(self):
        """Error message includes the offending expression."""
        with pytest.raises(Exception, match="unclosed"):
            FieldConfig(name="x", derived="(unclosed", dtype="float64")

    def test_full_config_catches_syntax_error(self):
        """Syntax error caught during full EntityConfig validation."""
        config_dict = {
            "entity": {"name": "Test", "version": "1.0"},
            "sources": [{"name": "main", "type": "csv", "primary": True, "path": "/tmp/x.csv"}],
            "fields": [
                {"name": "id", "source": "id", "dtype": "str", "primary_key": True},
                {"name": "bad", "derived": "lambda row: row['x'", "dtype": "float64"},
            ],
        }
        with pytest.raises(Exception, match="syntax error"):
            EntityConfig.model_validate(config_dict)


# ---------------------------------------------------------------------------
# #2: DerivedFunctionRef — function references for derived fields
# ---------------------------------------------------------------------------


class TestDerivedFunctionRef:
    """Derived fields can reference Python functions instead of inline expressions."""

    def test_function_ref_schema(self):
        """DerivedFunctionRef parses from dict."""
        ref = DerivedFunctionRef(function="mypackage.module.compute", kwargs={"threshold": 0.5})
        assert ref.function == "mypackage.module.compute"
        assert ref.kwargs == {"threshold": 0.5}

    def test_function_ref_no_kwargs(self):
        """kwargs defaults to empty dict."""
        ref = DerivedFunctionRef(function="pkg.func")
        assert ref.kwargs == {}

    def test_field_with_function_ref(self):
        """FieldConfig accepts DerivedFunctionRef as derived value."""
        field = FieldConfig(
            name="score",
            derived=DerivedFunctionRef(function="pkg.compute_score"),
            dtype="float64",
        )
        assert isinstance(field.derived, DerivedFunctionRef)
        assert field.derived.function == "pkg.compute_score"

    def test_field_from_dict_yaml_style(self):
        """FieldConfig parses function ref from YAML-style dict."""
        field = FieldConfig.model_validate({
            "name": "score",
            "derived": {"function": "pkg.compute_score", "kwargs": {"x": 1}},
            "dtype": "float64",
        })
        assert isinstance(field.derived, DerivedFunctionRef)
        assert field.derived.function == "pkg.compute_score"
        assert field.derived.kwargs == {"x": 1}

    def test_mutually_exclusive_with_source(self):
        """source and derived (function ref) are still mutually exclusive."""
        with pytest.raises(Exception, match="mutually exclusive"):
            FieldConfig(
                name="x",
                source="col",
                derived=DerivedFunctionRef(function="pkg.func"),
                dtype="str",
            )


# ---------------------------------------------------------------------------
# Pipeline integration — function references
# ---------------------------------------------------------------------------


# A real function for testing — lives in this module
def _test_compute_doubled(row):
    """Test function: doubles the amount."""
    return row["amount"] * 2


def _test_compute_label(row, threshold=500):
    """Test function with kwargs: labels rows based on threshold."""
    return "HIGH" if row["amount"] > threshold else "LOW"


class TestPipelineDerivedFunction:
    """Pipeline integration tests for derived function references."""

    @pytest.fixture
    def setup_data(self, tmp_dir):
        """Create CSV and config for function ref testing."""
        df = pd.DataFrame({
            "id": ["A", "B", "C", "D"],
            "amount": [100.0, 300.0, 700.0, 1000.0],
        })
        csv_path = tmp_dir / "data.csv"
        df.to_csv(csv_path, index=False)

        primary_dict = {
            "entity": {"name": "TestEntity", "version": "1.0"},
            "sources": [{"name": "main", "type": "csv", "primary": True, "path": str(csv_path)}],
            "fields": [
                {"name": "id", "source": "id", "dtype": "str", "primary_key": True},
                {"name": "amount", "source": "amount", "dtype": "float64"},
            ],
        }
        return primary_dict, tmp_dir

    def test_function_ref_basic(self, setup_data):
        """Function ref computes derived field correctly."""
        primary_dict, tmp_dir = setup_data
        primary_dict["fields"].append({
            "name": "doubled",
            "derived": {
                "function": "tests.unit.test_expression_validation._test_compute_doubled",
            },
            "dtype": "float64",
        })
        primary_path = write_yaml_config(primary_dict, tmp_dir / "config.yaml")
        result = EntityPipeline(primary_path).run()

        assert len(result.entities) == 4
        assert result.entities[0].doubled == 200.0
        assert result.entities[2].doubled == 1400.0

    def test_function_ref_with_kwargs(self, setup_data):
        """Function ref passes kwargs through correctly."""
        primary_dict, tmp_dir = setup_data
        primary_dict["fields"].append({
            "name": "label",
            "derived": {
                "function": "tests.unit.test_expression_validation._test_compute_label",
                "kwargs": {"threshold": 500},
            },
            "dtype": "str",
        })
        primary_path = write_yaml_config(primary_dict, tmp_dir / "config.yaml")
        result = EntityPipeline(primary_path).run()

        labels = [e.label for e in result.entities]
        assert labels == ["LOW", "LOW", "HIGH", "HIGH"]

    def test_function_ref_kwargs_override(self, setup_data):
        """Different kwargs change behavior."""
        primary_dict, tmp_dir = setup_data
        primary_dict["fields"].append({
            "name": "label",
            "derived": {
                "function": "tests.unit.test_expression_validation._test_compute_label",
                "kwargs": {"threshold": 200},
            },
            "dtype": "str",
        })
        primary_path = write_yaml_config(primary_dict, tmp_dir / "config.yaml")
        result = EntityPipeline(primary_path).run()

        labels = [e.label for e in result.entities]
        assert labels == ["LOW", "HIGH", "HIGH", "HIGH"]

    def test_function_ref_bad_import(self, setup_data):
        """Bad function path raises TransformError."""
        primary_dict, tmp_dir = setup_data
        primary_dict["fields"].append({
            "name": "bad",
            "derived": {
                "function": "nonexistent.module.func",
            },
            "dtype": "float64",
        })
        primary_path = write_yaml_config(primary_dict, tmp_dir / "config.yaml")
        with pytest.raises(TransformError, match="Cannot import"):
            EntityPipeline(primary_path).run()

    def test_function_ref_runtime_error(self, setup_data):
        """Function that raises at runtime gives TransformError with diagnostics."""
        primary_dict, tmp_dir = setup_data
        primary_dict["fields"].append({
            "name": "bad",
            "derived": {
                "function": "tests.unit.test_expression_validation._test_failing_func",
            },
            "dtype": "float64",
        })
        primary_path = write_yaml_config(primary_dict, tmp_dir / "config.yaml")
        with pytest.raises(TransformError, match="_test_failing_func"):
            EntityPipeline(primary_path).run()

    def test_function_ref_receives_runtime_parameters(self, setup_data):
        """Runtime parameters are passed via @param references in kwargs."""
        primary_dict, tmp_dir = setup_data
        primary_dict["fields"].append({
            "name": "tagged",
            "derived": {
                "function": "tests.unit.test_expression_validation._test_compute_with_date",
                "kwargs": {"snapshot_date": "@snapshot_date"},
            },
            "dtype": "str",
        })
        primary_path = write_yaml_config(primary_dict, tmp_dir / "config.yaml")
        result = EntityPipeline(primary_path).run(
            parameters={"snapshot_date": "2026-06-15"},
        )

        assert result.entities[0].tagged == "A_2026-06-15"
        assert result.entities[1].tagged == "B_2026-06-15"

    def test_function_ref_literal_kwargs(self, setup_data):
        """Literal kwargs are passed directly (no @param resolution)."""
        primary_dict, tmp_dir = setup_data
        primary_dict["fields"].append({
            "name": "tagged",
            "derived": {
                "function": "tests.unit.test_expression_validation._test_compute_with_date",
                "kwargs": {"snapshot_date": "2025-12-31"},
            },
            "dtype": "str",
        })
        primary_path = write_yaml_config(primary_dict, tmp_dir / "config.yaml")
        result = EntityPipeline(primary_path).run()

        assert result.entities[0].tagged == "A_2025-12-31"

    def test_function_ref_mixed_literal_and_param_kwargs(self, setup_data):
        """Mix of literal values and @param references in kwargs."""
        primary_dict, tmp_dir = setup_data
        primary_dict["parameters"] = {"factor": 2.0}
        primary_dict["fields"].append({
            "name": "scaled",
            "derived": {
                "function": "tests.unit.test_expression_validation._test_compute_scaled",
                "kwargs": {"factor": "@factor", "offset": 10.0},
            },
            "dtype": "float64",
        })
        primary_path = write_yaml_config(primary_dict, tmp_dir / "config.yaml")
        result = EntityPipeline(primary_path).run()

        # factor=2.0 from @factor parameter, offset=10.0 literal
        # A: 100 * 2.0 + 10.0 = 210.0
        assert result.entities[0].scaled == 210.0

    def test_function_ref_param_override_at_runtime(self, setup_data):
        """Runtime parameters override config defaults for @param refs."""
        primary_dict, tmp_dir = setup_data
        primary_dict["parameters"] = {"snapshot_date": "2025-01-01"}
        primary_dict["fields"].append({
            "name": "tagged",
            "derived": {
                "function": "tests.unit.test_expression_validation._test_compute_with_date",
                "kwargs": {"snapshot_date": "@snapshot_date"},
            },
            "dtype": "str",
        })
        primary_path = write_yaml_config(primary_dict, tmp_dir / "config.yaml")
        # Runtime overrides config default
        result = EntityPipeline(primary_path).run(
            parameters={"snapshot_date": "2026-06-15"},
        )

        assert result.entities[0].tagged == "A_2026-06-15"

    def test_function_ref_missing_param_raises(self, setup_data):
        """@param referencing undefined parameter raises TransformError."""
        primary_dict, tmp_dir = setup_data
        primary_dict["fields"].append({
            "name": "tagged",
            "derived": {
                "function": "tests.unit.test_expression_validation._test_compute_with_date",
                "kwargs": {"snapshot_date": "@nonexistent_param"},
            },
            "dtype": "str",
        })
        primary_path = write_yaml_config(primary_dict, tmp_dir / "config.yaml")
        with pytest.raises(TransformError, match="nonexistent_param"):
            EntityPipeline(primary_path).run()

    def test_mixed_inline_and_function_ref(self, setup_data):
        """Mix of inline expressions and function refs in same config."""
        primary_dict, tmp_dir = setup_data
        primary_dict["fields"].extend([
            {
                "name": "doubled",
                "derived": {
                    "function": "tests.unit.test_expression_validation._test_compute_doubled",
                },
                "dtype": "float64",
            },
            {
                "name": "tripled",
                "derived": "amount * 3",
                "dtype": "float64",
            },
        ])
        primary_path = write_yaml_config(primary_dict, tmp_dir / "config.yaml")
        result = EntityPipeline(primary_path).run()

        assert result.entities[0].doubled == 200.0
        assert result.entities[0].tripled == 300.0


def _test_compute_with_date(row, snapshot_date="2025-01-01"):
    """Test function that uses a runtime parameter."""
    return f"{row['id']}_{snapshot_date}"


def _test_compute_scaled(row, factor=1.0, offset=0.0):
    """Test function with multiple kwargs, some from parameters."""
    return row["amount"] * factor + offset


def _test_failing_func(row):
    """Test function that always raises."""
    raise ValueError("intentional failure")
