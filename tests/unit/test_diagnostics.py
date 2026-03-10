"""Tests for the row-level diagnostic / debugging system.

Covers: failing_samples on ValidationResult, cast failure diagnostics,
lambda failure diagnostics, parent row context in sub-entity errors,
and format_detail output.
"""

from __future__ import annotations

import math

import pandas as pd
import pytest

from impact.common.exceptions import TransformError
from impact.common.utils import cast_and_fill, diagnose_cast_failure, diagnose_lambda_failure
from impact.entity.config.schema import EntityConfig, FieldConfig, ValidationConfig
from impact.entity.sub_entity import SubEntityProcessor, _build_row_context
from impact.entity.transform.builtin import CastTransformer
from impact.entity.validate.base import (
    ValidationReport,
    ValidationResult,
    collect_failing_samples,
    set_max_samples,
)
from impact.entity.validate.builtin import (
    ExpressionValidator,
    NotNullValidator,
    RangeValidator,
    UniqueValidator,
)


# =====================================================================
# collect_failing_samples
# =====================================================================

class TestCollectFailingSamples:
    def test_basic(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        samples = collect_failing_samples(df, [0, 2], columns=["a", "b"])
        assert len(samples) == 2
        assert samples[0] == {"a": 1, "b": "x"}
        assert samples[1] == {"a": 3, "b": "z"}

    def test_respects_max_samples(self):
        df = pd.DataFrame({"a": range(100)})
        samples = collect_failing_samples(df, list(range(100)), max_samples=3)
        assert len(samples) == 3

    def test_empty_indices(self):
        df = pd.DataFrame({"a": [1, 2]})
        assert collect_failing_samples(df, []) == []

    def test_all_columns_when_none(self):
        df = pd.DataFrame({"a": [1], "b": [2]})
        samples = collect_failing_samples(df, [0])
        assert "a" in samples[0] and "b" in samples[0]


# =====================================================================
# ValidationResult.format_detail
# =====================================================================

class TestFormatDetail:
    def test_passing_result(self):
        r = ValidationResult(
            rule_type="not_null", passed=True, severity="warning", message="ok"
        )
        assert "PASS" in r.format_detail()
        assert "failing" not in r.format_detail()

    def test_failing_with_samples(self):
        r = ValidationResult(
            rule_type="range",
            passed=False,
            severity="error",
            message="range check on 'amount'",
            failing_row_count=10,
            failing_indices=[0, 1, 2],
            field_name="amount",
            failing_samples=[
                {"amount": -5.0},
                {"amount": -3.0},
                {"amount": -1.0},
            ],
        )
        detail = r.format_detail()
        assert "ERROR" in detail
        assert "10 failing rows" in detail
        assert "row 0: amount=-5.0" in detail
        assert "and 7 more rows" in detail

    def test_context_included(self):
        r = ValidationResult(
            rule_type="not_null",
            passed=False,
            severity="warning",
            message="check",
            failing_row_count=1,
            failing_indices=[0],
            context="parent row 3 (facility_id='FAC-001'), sub-entity 'Collateral'",
        )
        detail = r.format_detail()
        assert "FAC-001" in detail
        assert "Collateral" in detail


# =====================================================================
# ValidationReport.format_detail
# =====================================================================

class TestReportFormatDetail:
    def test_includes_samples(self):
        report = ValidationReport(results=[
            ValidationResult(
                rule_type="not_null", passed=False, severity="error",
                message="not_null on col_a", failing_row_count=2,
                failing_indices=[0, 1],
                failing_samples=[{"col_a": None}, {"col_a": None}],
            ),
        ])
        detail = report.format_detail()
        assert "col_a=None" in detail
        assert "1 errors" in detail


# =====================================================================
# Validators produce failing_samples
# =====================================================================

class TestValidatorSamples:
    def test_not_null_samples(self):
        df = pd.DataFrame({"x": [1.0, float("nan"), 3.0, float("nan")]})
        cfg = ValidationConfig(type="not_null", columns=["x"], severity="error")
        result = NotNullValidator().validate(df, cfg)
        assert not result.passed
        assert result.failing_row_count == 2
        assert len(result.failing_samples) == 2
        assert math.isnan(result.failing_samples[0]["x"])

    def test_unique_samples(self):
        df = pd.DataFrame({"id": ["A", "B", "A", "C"]})
        cfg = ValidationConfig(type="unique", columns=["id"], severity="error")
        result = UniqueValidator().validate(df, cfg)
        assert not result.passed
        assert len(result.failing_samples) == 2
        assert result.failing_samples[0]["id"] == "A"

    def test_range_samples(self):
        df = pd.DataFrame({"val": [1.0, -5.0, 3.0, -2.0]})
        cfg = ValidationConfig(type="range", column="val", min=0, severity="warning")
        result = RangeValidator().validate(df, cfg)
        assert not result.passed
        assert result.field_name == "val"
        assert len(result.failing_samples) == 2
        assert result.failing_samples[0]["val"] == -5.0

    def test_expression_samples(self):
        df = pd.DataFrame({"a": [10, 5, 20], "b": [5, 10, 15]})
        cfg = ValidationConfig(type="expression", rule="a >= b", severity="warning")
        result = ExpressionValidator().validate(df, cfg)
        assert not result.passed
        assert result.failing_row_count == 1
        # Expression samples include all columns
        assert "a" in result.failing_samples[0]
        assert "b" in result.failing_samples[0]

    def test_passing_produces_no_samples(self):
        df = pd.DataFrame({"x": [1.0, 2.0, 3.0]})
        cfg = ValidationConfig(type="not_null", columns=["x"], severity="error")
        result = NotNullValidator().validate(df, cfg)
        assert result.passed
        assert result.failing_samples == []


# =====================================================================
# diagnose_cast_failure
# =====================================================================

class TestDiagnoseCastFailure:
    def test_numeric_bad_values(self):
        df = pd.DataFrame({"x": ["1.0", "abc", "3.0", "def"]})
        bad = diagnose_cast_failure(df, "x", "float64")
        assert len(bad) == 2
        assert bad[0] == (1, "abc")
        assert bad[1] == (3, "def")

    def test_datetime_bad_values(self):
        df = pd.DataFrame({"d": ["2025-01-01", "not-a-date", "2025-03-01"]})
        bad = diagnose_cast_failure(df, "d", "datetime")
        assert len(bad) == 1
        assert bad[0][0] == 1

    def test_string_always_ok(self):
        df = pd.DataFrame({"x": [1, 2, 3]})
        assert diagnose_cast_failure(df, "x", "str") == []

    def test_missing_column(self):
        df = pd.DataFrame({"x": [1]})
        assert diagnose_cast_failure(df, "missing", "float64") == []


# =====================================================================
# diagnose_lambda_failure
# =====================================================================

class TestDiagnoseLambdaFailure:
    def test_finds_failing_row(self):
        df = pd.DataFrame({"a": [10, "bad", 5]})
        fn = lambda row: int(row["a"]) * 2  # noqa: E731
        failures = diagnose_lambda_failure(df, fn)
        assert len(failures) == 1
        idx, data, err = failures[0]
        assert idx == 1

    def test_no_failure(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        fn = lambda row: row["a"] * 2  # noqa: E731
        assert diagnose_lambda_failure(df, fn) == []


# =====================================================================
# cast_and_fill with diagnostics
# =====================================================================

class TestCastAndFillDiagnostics:
    def test_cast_failure_includes_bad_values(self):
        df = pd.DataFrame({"x": ["1.0", "bad", "3.0"]})
        caster = CastTransformer()
        with pytest.raises(TransformError) as exc_info:
            cast_and_fill(df, "x", "float64", None, caster)
        err = exc_info.value
        assert "bad" in str(err)
        assert err.field == "x"
        assert len(err.failing_samples) > 0


# =====================================================================
# Parent row context in sub-entity processing
# =====================================================================

class TestBuildRowContext:
    def test_with_pk(self):
        df = pd.DataFrame({"facility_id": ["FAC-001", "FAC-002"], "amount": [100, 200]})
        ctx = _build_row_context(df, 0, ["facility_id"], "Collateral", "")
        assert "facility_id='FAC-001'" in ctx
        assert "Collateral" in ctx

    def test_with_parent_context(self):
        df = pd.DataFrame({"id": ["A"]})
        ctx = _build_row_context(df, 0, ["id"], "Collateral", "parent row 5 (obligor_id='OB-1')")
        assert "OB-1" in ctx
        assert "→" in ctx
        assert "Collateral" in ctx

    def test_no_pk(self):
        df = pd.DataFrame({"amount": [100]})
        ctx = _build_row_context(df, 0, [], "Collateral", "")
        assert "parent row 0" in ctx


# =====================================================================
# Sub-entity processor tags validation results with context
# =====================================================================

class TestSubEntityContext:
    def test_validation_results_get_context(self):
        """Validation warnings from sub-entities should carry parent context."""
        config = EntityConfig(
            entity={"name": "Child", "version": "1.0"},
            fields=[
                FieldConfig(name="value", source="value", dtype="float64",
                            validation_type=["range"],
                            validation_rule={"range": [0, None]},
                            validation_severity={"range": "warning"}),
            ],
        )
        # Nested DF with a bad value (will produce a warning, not error)
        nested = pd.DataFrame({"value": [-1.0, 5.0]})
        processor = SubEntityProcessor(config)
        result = processor.process(nested)
        # The validation result should exist (range warning)
        warnings = [r for r in result.validation_report.results if not r.passed]
        assert len(warnings) >= 1
        assert warnings[0].failing_samples  # should have sample data
        assert warnings[0].failing_samples[0]["value"] == -1.0
