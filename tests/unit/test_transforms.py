"""Tests for transformation pipeline."""

from __future__ import annotations

import pandas as pd
import pytest

from impact.common.exceptions import TransformError
from impact.entity.config.schema import TransformConfig
from impact.entity.transform.builtin import (
    CastTransformer,
    DeriveTransformer,
    FillNaTransformer,
    FilterTransformer,
    RenameTransformer,
    DropTransformer,
)
from impact.entity.transform.registry import TransformRegistry


@pytest.fixture
def df():
    return pd.DataFrame(
        {
            "id": ["A", "B", "C"],
            "amount": ["100.5", "200.0", "300.75"],
            "category": ["X", None, "Z"],
            "score": [1, 2, 3],
        }
    )


class TestTransformRegistry:
    def test_cast_registered(self):
        t = TransformRegistry.get("cast")
        assert isinstance(t, CastTransformer)

    def test_unknown_type_raises(self):
        with pytest.raises(TransformError, match="No transformer registered"):
            TransformRegistry.get("nonexistent")

    def test_available_types(self):
        types = TransformRegistry.available_types()
        assert "cast" in types
        assert "derive" in types
        assert "rename" in types


class TestCastTransformer:
    def test_cast_to_float(self, df):
        config = TransformConfig(type="cast", columns={"amount": "float64"})
        result = CastTransformer().apply(df, config)
        assert result["amount"].dtype == "float64"
        assert result["amount"].iloc[0] == 100.5

    def test_cast_missing_column(self, df):
        config = TransformConfig(type="cast", columns={"nonexistent": "float64"})
        with pytest.raises(TransformError, match="not found"):
            CastTransformer().apply(df, config)

    def test_cast_no_columns(self, df):
        config = TransformConfig(type="cast")
        with pytest.raises(TransformError, match="requires 'columns'"):
            CastTransformer().apply(df, config)


class TestRenameTransformer:
    def test_rename_columns(self, df):
        config = TransformConfig(type="rename", mapping={"category": "cat", "score": "s"})
        result = RenameTransformer().apply(df, config)
        assert "cat" in result.columns
        assert "s" in result.columns
        assert "category" not in result.columns

    def test_rename_missing_column(self, df):
        config = TransformConfig(type="rename", mapping={"nonexistent": "new"})
        with pytest.raises(TransformError, match="not found"):
            RenameTransformer().apply(df, config)


class TestDeriveTransformer:
    def test_derive_column(self):
        df = pd.DataFrame({"a": [10.0, 20.0], "b": [2.0, 4.0]})
        config = TransformConfig(
            type="derive", name="ratio", expression="a / b", dtype="float64"
        )
        result = DeriveTransformer().apply(df, config)
        assert "ratio" in result.columns
        assert result["ratio"].iloc[0] == 5.0
        assert result["ratio"].iloc[1] == 5.0

    def test_derive_bad_expression(self):
        df = pd.DataFrame({"a": [1]})
        config = TransformConfig(
            type="derive", name="bad", expression="nonexistent_col + 1"
        )
        with pytest.raises(TransformError, match="Failed to derive"):
            DeriveTransformer().apply(df, config)


class TestFillNaTransformer:
    def test_fill_na(self, df):
        config = TransformConfig(type="fill_na", strategy={"category": "MISSING"})
        result = FillNaTransformer().apply(df, config)
        assert result["category"].iloc[1] == "MISSING"

    def test_fill_na_numeric(self):
        df = pd.DataFrame({"val": [1.0, None, 3.0]})
        config = TransformConfig(type="fill_na", strategy={"val": 0.0})
        result = FillNaTransformer().apply(df, config)
        assert result["val"].iloc[1] == 0.0


class TestFilterTransformer:
    def test_filter_rows(self, df):
        config = TransformConfig(type="filter", condition="score > 1")
        result = FilterTransformer().apply(df, config)
        assert len(result) == 2

    def test_filter_bad_condition(self, df):
        config = TransformConfig(type="filter", condition="nonexistent > 1")
        with pytest.raises(TransformError, match="Failed to filter"):
            FilterTransformer().apply(df, config)


class TestDropTransformer:
    def test_drop_columns(self, df):
        config = TransformConfig(type="drop", drop_columns=["score", "category"])
        result = DropTransformer().apply(df, config)
        assert "score" not in result.columns
        assert "category" not in result.columns
        assert "id" in result.columns

    def test_drop_missing_column(self, df):
        config = TransformConfig(type="drop", drop_columns=["nonexistent"])
        with pytest.raises(TransformError, match="not found"):
            DropTransformer().apply(df, config)


class TestTransformImmutability:
    def test_transforms_dont_mutate_input(self, df):
        original = df.copy()
        config = TransformConfig(type="fill_na", strategy={"category": "FILLED"})
        FillNaTransformer().apply(df, config)
        pd.testing.assert_frame_equal(df, original)
