"""Tests for validation engine."""

from __future__ import annotations

import pandas as pd
import pytest

from impact.entity.config.schema import ValidationConfig
from impact.entity.validate.base import ValidationReport, ValidationResult
from impact.entity.validate.builtin import (
    ExpressionValidator,
    NotNullValidator,
    RangeValidator,
    UniqueValidator,
)
from impact.entity.validate.registry import ValidatorRegistry


@pytest.fixture
def df():
    return pd.DataFrame(
        {
            "id": ["A", "B", "C", "D"],
            "value": [10.0, 20.0, 30.0, 40.0],
            "category": ["X", None, "Z", "X"],
        }
    )


class TestValidatorRegistry:
    def test_not_null_registered(self):
        v = ValidatorRegistry.get("not_null")
        assert isinstance(v, NotNullValidator)

    def test_available_types(self):
        types = ValidatorRegistry.available_types()
        assert "not_null" in types
        assert "unique" in types
        assert "range" in types
        assert "expression" in types


class TestNotNullValidator:
    def test_pass_no_nulls(self, df):
        config = ValidationConfig(type="not_null", columns=["id", "value"], severity="error")
        result = NotNullValidator().validate(df, config)
        assert result.passed is True

    def test_fail_with_nulls(self, df):
        config = ValidationConfig(type="not_null", columns=["category"], severity="error")
        result = NotNullValidator().validate(df, config)
        assert result.passed is False
        assert result.failing_row_count == 1

    def test_severity_propagated(self, df):
        config = ValidationConfig(type="not_null", columns=["category"], severity="warning")
        result = NotNullValidator().validate(df, config)
        assert result.severity == "warning"


class TestUniqueValidator:
    def test_pass_unique(self, df):
        config = ValidationConfig(type="unique", columns=["id"], severity="error")
        result = UniqueValidator().validate(df, config)
        assert result.passed is True

    def test_fail_duplicates(self, df):
        config = ValidationConfig(type="unique", columns=["category"], severity="error")
        result = UniqueValidator().validate(df, config)
        assert result.passed is False
        # "X" appears twice
        assert result.failing_row_count == 2


class TestRangeValidator:
    def test_pass_in_range(self, df):
        config = ValidationConfig(
            type="range", column="value", min=0.0, max=50.0, severity="warning"
        )
        result = RangeValidator().validate(df, config)
        assert result.passed is True

    def test_fail_out_of_range(self, df):
        config = ValidationConfig(
            type="range", column="value", min=0.0, max=25.0, severity="warning"
        )
        result = RangeValidator().validate(df, config)
        assert result.passed is False
        assert result.failing_row_count == 2  # 30 and 40 are out of range

    def test_min_only(self, df):
        config = ValidationConfig(
            type="range", column="value", min=15.0, severity="warning"
        )
        result = RangeValidator().validate(df, config)
        assert result.passed is False
        assert result.failing_row_count == 1  # 10 < 15


class TestExpressionValidator:
    def test_pass_expression(self, df):
        config = ValidationConfig(
            type="expression", rule="value > 0", severity="error"
        )
        result = ExpressionValidator().validate(df, config)
        assert result.passed is True

    def test_fail_expression(self, df):
        config = ValidationConfig(
            type="expression", rule="value > 15", severity="warning",
            message="Value must exceed 15",
        )
        result = ExpressionValidator().validate(df, config)
        assert result.passed is False
        assert result.failing_row_count == 1

    def test_custom_message(self, df):
        config = ValidationConfig(
            type="expression", rule="value > 0", severity="error",
            message="Custom message",
        )
        result = ExpressionValidator().validate(df, config)
        assert result.message == "Custom message"


class TestValidationReport:
    def test_report_no_errors(self):
        report = ValidationReport(
            results=[
                ValidationResult(
                    rule_type="not_null", passed=True, severity="error", message="ok"
                )
            ]
        )
        assert report.has_errors is False
        assert report.error_count == 0

    def test_report_with_errors(self):
        report = ValidationReport(
            results=[
                ValidationResult(
                    rule_type="not_null", passed=False, severity="error",
                    message="nulls found", failing_row_count=3,
                )
            ]
        )
        assert report.has_errors is True
        assert report.error_count == 1

    def test_report_warnings_vs_errors(self):
        report = ValidationReport(
            results=[
                ValidationResult(
                    rule_type="range", passed=False, severity="warning",
                    message="out of range", failing_row_count=2,
                ),
                ValidationResult(
                    rule_type="not_null", passed=True, severity="error", message="ok"
                ),
            ]
        )
        assert report.has_errors is False
        assert report.has_warnings is True
        assert report.warning_count == 1
