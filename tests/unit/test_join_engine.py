"""Tests for the join engine and nesting logic."""

from __future__ import annotations

import pandas as pd
import pytest

from impact.entity.config.schema import JoinConfig, JoinKeyCondition
from impact.entity.join.engine import JoinEngine


@pytest.fixture
def engine():
    return JoinEngine()


class TestFlatJoin:
    """Tests for one-to-one (flat) joins."""

    def test_inner_join(self, engine, sample_facility_df, sample_rating_df):
        config = JoinConfig(
            left="facility",
            right="rating",
            how="inner",
            on=[JoinKeyCondition(left_col="facility_id", right_col="facility_id")],
            relationship="one_to_one",
        )
        result = engine.execute(sample_facility_df, sample_rating_df, config)
        # Only F001 and F003 have rating overrides
        assert len(result) == 2

    def test_left_join(self, engine, sample_facility_df, sample_rating_df):
        config = JoinConfig(
            left="facility",
            right="rating",
            how="left",
            on=[JoinKeyCondition(left_col="facility_id", right_col="facility_id")],
            relationship="one_to_one",
        )
        result = engine.execute(sample_facility_df, sample_rating_df, config)
        # All 3 facilities preserved
        assert len(result) == 3
        assert "rating_override" in result.columns

    def test_right_join(self, engine, sample_facility_df, sample_rating_df):
        config = JoinConfig(
            left="facility",
            right="rating",
            how="right",
            on=[JoinKeyCondition(left_col="facility_id", right_col="facility_id")],
            relationship="one_to_one",
        )
        result = engine.execute(sample_facility_df, sample_rating_df, config)
        assert len(result) == 2


class TestNestedJoin:
    """Tests for one-to-many joins with nested DataFrames."""

    def test_nested_join_preserves_row_count(
        self, engine, sample_facility_df, sample_collateral_df
    ):
        config = JoinConfig(
            left="facility",
            right="collateral",
            how="left",
            on=[JoinKeyCondition(left_col="facility_id", right_col="facility_id")],
            relationship="one_to_many",
            nested_as="collateral_items",
        )
        result = engine.execute(sample_facility_df, sample_collateral_df, config)

        # Row count must match left side
        assert len(result) == len(sample_facility_df)
        assert "collateral_items" in result.columns

    def test_nested_column_contains_dataframes(
        self, engine, sample_facility_df, sample_collateral_df
    ):
        config = JoinConfig(
            left="facility",
            right="collateral",
            how="left",
            on=[JoinKeyCondition(left_col="facility_id", right_col="facility_id")],
            relationship="one_to_many",
            nested_as="collateral_items",
        )
        result = engine.execute(sample_facility_df, sample_collateral_df, config)

        # Each nested value should be a DataFrame
        for _, row in result.iterrows():
            assert isinstance(row["collateral_items"], pd.DataFrame)

    def test_nested_join_correct_nesting_counts(
        self, engine, sample_facility_df, sample_collateral_df
    ):
        config = JoinConfig(
            left="facility",
            right="collateral",
            how="left",
            on=[JoinKeyCondition(left_col="facility_id", right_col="facility_id")],
            relationship="one_to_many",
            nested_as="collateral_items",
        )
        result = engine.execute(sample_facility_df, sample_collateral_df, config)

        # F001 has 2 collaterals, F002 has 1, F003 has 3
        f001 = result.loc[result["facility_id"] == "F001", "collateral_items"].iloc[0]
        f002 = result.loc[result["facility_id"] == "F002", "collateral_items"].iloc[0]
        f003 = result.loc[result["facility_id"] == "F003", "collateral_items"].iloc[0]

        assert len(f001) == 2
        assert len(f002) == 1
        assert len(f003) == 3

    def test_nested_join_with_no_matches(self, engine, sample_facility_df):
        """Facilities with no collateral should get empty nested DataFrames."""
        empty_collateral = pd.DataFrame(
            {"facility_id": ["F999"], "collateral_type": ["X"], "collateral_value": [0]}
        )
        config = JoinConfig(
            left="facility",
            right="collateral",
            how="left",
            on=[JoinKeyCondition(left_col="facility_id", right_col="facility_id")],
            relationship="one_to_many",
            nested_as="collateral_items",
        )
        result = engine.execute(sample_facility_df, empty_collateral, config)
        assert len(result) == len(sample_facility_df)

        # All nested frames should be empty
        for _, row in result.iterrows():
            assert isinstance(row["collateral_items"], pd.DataFrame)
            assert len(row["collateral_items"]) == 0
