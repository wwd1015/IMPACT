"""Tests for range validation with exclusive bounds and bracket notation."""

from __future__ import annotations

import pandas as pd
import pytest

from impact.entity.config.schema import FieldConfig, ValidationConfig
from impact.entity.validate.builtin import RangeValidator


# ---------------------------------------------------------------------------
# Bracket notation parsing in FieldConfig.build_validation_configs
# ---------------------------------------------------------------------------

class TestRangeBracketNotation:
    def _build_range_config(self, range_value) -> ValidationConfig:
        """Helper to build a range ValidationConfig from a field."""
        field = FieldConfig(
            name="x", source="x", dtype="float64",
            validation_type=["range"],
            validation_rule={"range": range_value},
        )
        configs = field.build_validation_configs()
        assert len(configs) == 1
        return configs[0]

    def test_list_inclusive(self):
        cfg = self._build_range_config([0.0, 1.0])
        assert cfg.min == 0.0
        assert cfg.max == 1.0
        assert cfg.min_exclusive is False
        assert cfg.max_exclusive is False

    def test_list_with_null(self):
        cfg = self._build_range_config([0, None])
        assert cfg.min == 0
        assert cfg.max is None
        assert cfg.min_exclusive is False

    def test_string_inclusive(self):
        cfg = self._build_range_config("[0.0, 1.0]")
        assert cfg.min == 0.0
        assert cfg.max == 1.0
        assert cfg.min_exclusive is False
        assert cfg.max_exclusive is False

    def test_string_exclusive_both(self):
        cfg = self._build_range_config("(0.0, 1.0)")
        assert cfg.min == 0.0
        assert cfg.max == 1.0
        assert cfg.min_exclusive is True
        assert cfg.max_exclusive is True

    def test_string_mixed_inclusive_min_exclusive_max(self):
        cfg = self._build_range_config("[0.0, 1.0)")
        assert cfg.min == 0.0
        assert cfg.max == 1.0
        assert cfg.min_exclusive is False
        assert cfg.max_exclusive is True

    def test_string_mixed_exclusive_min_inclusive_max(self):
        cfg = self._build_range_config("(0.0, 1.0]")
        assert cfg.min == 0.0
        assert cfg.max == 1.0
        assert cfg.min_exclusive is True
        assert cfg.max_exclusive is False

    def test_string_with_null(self):
        cfg = self._build_range_config("(0, null]")
        assert cfg.min == 0
        assert cfg.max is None
        assert cfg.min_exclusive is True
        assert cfg.max_exclusive is False

    def test_dict_with_exclusive(self):
        cfg = self._build_range_config({
            "min": 0, "max": 100,
            "min_exclusive": True, "max_exclusive": False,
        })
        assert cfg.min == 0
        assert cfg.max == 100
        assert cfg.min_exclusive is True
        assert cfg.max_exclusive is False


# ---------------------------------------------------------------------------
# RangeValidator with exclusive bounds
# ---------------------------------------------------------------------------

class TestRangeValidatorExclusive:
    def test_inclusive_pass(self):
        df = pd.DataFrame({"x": [0.0, 0.5, 1.0]})
        config = ValidationConfig(
            type="range", column="x", min=0.0, max=1.0,
            min_exclusive=False, max_exclusive=False, severity="error",
        )
        result = RangeValidator().validate(df, config)
        assert result.passed is True

    def test_inclusive_fail_at_boundary(self):
        df = pd.DataFrame({"x": [0.0, 0.5, 1.1]})
        config = ValidationConfig(
            type="range", column="x", min=0.0, max=1.0,
            min_exclusive=False, max_exclusive=False, severity="error",
        )
        result = RangeValidator().validate(df, config)
        assert result.passed is False
        assert result.failing_row_count == 1

    def test_exclusive_min_boundary_fails(self):
        df = pd.DataFrame({"x": [0.0, 0.5, 1.0]})
        config = ValidationConfig(
            type="range", column="x", min=0.0, max=1.0,
            min_exclusive=True, max_exclusive=False, severity="error",
        )
        result = RangeValidator().validate(df, config)
        assert result.passed is False
        assert result.failing_row_count == 1  # x=0.0 fails exclusive min

    def test_exclusive_max_boundary_fails(self):
        df = pd.DataFrame({"x": [0.0, 0.5, 1.0]})
        config = ValidationConfig(
            type="range", column="x", min=0.0, max=1.0,
            min_exclusive=False, max_exclusive=True, severity="error",
        )
        result = RangeValidator().validate(df, config)
        assert result.passed is False
        assert result.failing_row_count == 1  # x=1.0 fails exclusive max

    def test_exclusive_both_boundaries_fail(self):
        df = pd.DataFrame({"x": [0.0, 0.5, 1.0]})
        config = ValidationConfig(
            type="range", column="x", min=0.0, max=1.0,
            min_exclusive=True, max_exclusive=True, severity="error",
        )
        result = RangeValidator().validate(df, config)
        assert result.passed is False
        assert result.failing_row_count == 2  # x=0.0 and x=1.0

    def test_exclusive_both_interior_passes(self):
        df = pd.DataFrame({"x": [0.1, 0.5, 0.9]})
        config = ValidationConfig(
            type="range", column="x", min=0.0, max=1.0,
            min_exclusive=True, max_exclusive=True, severity="error",
        )
        result = RangeValidator().validate(df, config)
        assert result.passed is True

    def test_exclusive_min_unbounded_max(self):
        df = pd.DataFrame({"x": [0.0, 50.0, 1000.0]})
        config = ValidationConfig(
            type="range", column="x", min=0.0, max=None,
            min_exclusive=True, severity="error",
        )
        result = RangeValidator().validate(df, config)
        assert result.passed is False
        assert result.failing_row_count == 1  # x=0.0 fails

    def test_message_exclusive(self):
        df = pd.DataFrame({"x": [0.5]})
        config = ValidationConfig(
            type="range", column="x", min=0.0, max=1.0,
            min_exclusive=True, max_exclusive=True, severity="warning",
        )
        result = RangeValidator().validate(df, config)
        assert "> 0.0" in result.message
        assert "< 1.0" in result.message
