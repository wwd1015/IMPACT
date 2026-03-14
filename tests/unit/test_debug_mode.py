"""Tests for debug mode features: explain(), snapshots, and debug context."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from impact.common.exceptions import DebugContext, TransformError, ValidationError
from impact.entity.config.schema import DerivedFunctionRef, EntityConfig
from impact.entity.pipeline import (
    SNAP_AFTER_DERIVED_FIELDS,
    SNAP_AFTER_JOIN,
    SNAP_AFTER_POST_FILTERS,
    SNAP_AFTER_PRE_FILTERS,
    SNAP_AFTER_SOURCE_FIELDS,
    SNAP_AFTER_VALIDATION,
    SNAP_FINAL,
    STAGE_DERIVED_FIELDS,
    STAGE_PRE_FILTERS,
    STAGE_SOURCE_VALIDATION,
    EntityPipeline,
)
from tests.conftest import write_yaml_config


# ---------------------------------------------------------------------------
# Helper data
# ---------------------------------------------------------------------------

def _make_csv_and_config(tmp_dir: Path, extra_fields=None, post_filters=None, pre_filters=None):
    """Create a simple CSV + config for testing."""
    df = pd.DataFrame({
        "id": ["A", "B", "C", "D"],
        "amount": [100.0, 300.0, 700.0, 1000.0],
    })
    csv_path = tmp_dir / "data.csv"
    df.to_csv(csv_path, index=False)

    config_dict = {
        "entity": {"name": "TestEntity", "version": "1.0"},
        "sources": [{"name": "main", "type": "csv", "primary": True, "path": str(csv_path)}],
        "fields": [
            {"name": "id", "source": "id", "dtype": "str", "primary_key": True},
            {"name": "amount", "source": "amount", "dtype": "float64"},
            *(extra_fields or []),
        ],
    }
    if post_filters:
        config_dict["post_filters"] = post_filters
    if pre_filters:
        config_dict["pre_filters"] = pre_filters
    return config_dict, tmp_dir


# ---------------------------------------------------------------------------
# A: explain() — execution plan without loading data
# ---------------------------------------------------------------------------


class TestExplain:
    """pipeline.explain() shows the execution plan."""

    def test_explain_basic(self, tmp_dir):
        """explain() returns a non-empty string with entity info."""
        config_dict, _ = _make_csv_and_config(tmp_dir)
        path = write_yaml_config(config_dict, tmp_dir / "config.yaml")
        plan = EntityPipeline(path).explain()

        assert "TestEntity" in plan
        assert "v1.0" in plan
        assert "main (csv)" in plan
        assert "Pass 1" in plan
        assert "source fields" in plan.lower()

    def test_explain_shows_derived_fields(self, tmp_dir):
        """explain() lists derived fields in Pass 2."""
        config_dict, _ = _make_csv_and_config(tmp_dir, extra_fields=[
            {"name": "doubled", "derived": "amount * 2", "dtype": "float64"},
        ])
        path = write_yaml_config(config_dict, tmp_dir / "config.yaml")
        plan = EntityPipeline(path).explain()

        assert "doubled" in plan
        assert "amount * 2" in plan
        assert "Pass 2" in plan

    def test_explain_shows_function_ref(self, tmp_dir):
        """explain() shows function path for DerivedFunctionRef."""
        config_dict, _ = _make_csv_and_config(tmp_dir, extra_fields=[
            {
                "name": "score",
                "derived": {"function": "my.module.compute", "kwargs": {"x": 1}},
                "dtype": "float64",
            },
        ])
        path = write_yaml_config(config_dict, tmp_dir / "config.yaml")
        plan = EntityPipeline(path).explain()

        assert "fn:my.module.compute" in plan
        assert "x" in plan

    def test_explain_shows_filters(self, tmp_dir):
        """explain() lists pre- and post-filters."""
        config_dict, _ = _make_csv_and_config(
            tmp_dir,
            pre_filters=["amount > 0"],
            post_filters=["amount < 900"],
        )
        path = write_yaml_config(config_dict, tmp_dir / "config.yaml")
        plan = EntityPipeline(path).explain()

        assert "Pre-filters" in plan
        assert "amount > 0" in plan
        assert "Post-filters" in plan
        assert "amount < 900" in plan

    def test_explain_shows_validations(self, tmp_dir):
        """explain() lists field-level validations."""
        config_dict, _ = _make_csv_and_config(tmp_dir)
        config_dict["fields"][1]["validation_type"] = ["not_null", "range"]
        config_dict["fields"][1]["validation_rule"] = {"range": [0.0, None]}
        config_dict["fields"][1]["validation_severity"] = {"not_null": "error"}
        path = write_yaml_config(config_dict, tmp_dir / "config.yaml")
        plan = EntityPipeline(path).explain()

        assert "not_null" in plan
        assert "range" in plan

    def test_explain_shows_execution_order(self, tmp_dir):
        """explain() has numbered execution steps."""
        config_dict, _ = _make_csv_and_config(tmp_dir)
        path = write_yaml_config(config_dict, tmp_dir / "config.yaml")
        plan = EntityPipeline(path).explain()

        assert "Execution order:" in plan
        assert "1." in plan
        assert "Load sources" in plan
        assert "Build TestEntity" in plan

    def test_explain_shows_parameters(self, tmp_dir):
        """explain() lists global parameters."""
        config_dict, _ = _make_csv_and_config(tmp_dir)
        config_dict["parameters"] = {"snapshot_date": "2025-01-01"}
        path = write_yaml_config(config_dict, tmp_dir / "config.yaml")
        plan = EntityPipeline(path).explain()

        assert "Parameters:" in plan
        assert "snapshot_date" in plan

    def test_explain_no_data_loaded(self, tmp_dir):
        """explain() works even if the data file does not exist."""
        config_dict = {
            "entity": {"name": "Ghost", "version": "2.0"},
            "sources": [{"name": "main", "type": "csv", "primary": True, "path": "/nonexistent/data.csv"}],
            "fields": [
                {"name": "id", "source": "id", "dtype": "str", "primary_key": True},
            ],
        }
        path = write_yaml_config(config_dict, tmp_dir / "config.yaml")
        plan = EntityPipeline(path).explain()
        assert "Ghost" in plan


# ---------------------------------------------------------------------------
# B: run(debug=True) — stage snapshots
# ---------------------------------------------------------------------------


class TestDebugSnapshots:
    """run(debug=True) saves DataFrame snapshots at each stage."""

    def test_snapshots_populated(self, tmp_dir):
        """Debug mode populates result.snapshots."""
        config_dict, _ = _make_csv_and_config(tmp_dir, extra_fields=[
            {"name": "doubled", "derived": "amount * 2", "dtype": "float64"},
        ])
        path = write_yaml_config(config_dict, tmp_dir / "config.yaml")
        result = EntityPipeline(path).run(debug=True)

        assert SNAP_AFTER_JOIN in result.snapshots
        assert SNAP_AFTER_SOURCE_FIELDS in result.snapshots
        assert SNAP_AFTER_DERIVED_FIELDS in result.snapshots
        assert SNAP_AFTER_VALIDATION in result.snapshots
        assert SNAP_FINAL in result.snapshots

    def test_snapshots_empty_without_debug(self, tmp_dir):
        """Without debug=True, snapshots is empty."""
        config_dict, _ = _make_csv_and_config(tmp_dir)
        path = write_yaml_config(config_dict, tmp_dir / "config.yaml")
        result = EntityPipeline(path).run()

        assert result.snapshots == {}

    def test_snapshots_are_copies(self, tmp_dir):
        """Snapshots are independent copies, not references."""
        config_dict, _ = _make_csv_and_config(tmp_dir, extra_fields=[
            {"name": "doubled", "derived": "amount * 2", "dtype": "float64"},
        ])
        path = write_yaml_config(config_dict, tmp_dir / "config.yaml")
        result = EntityPipeline(path).run(debug=True)

        # after_source_fields should not have 'doubled'
        assert "doubled" not in result.snapshots[SNAP_AFTER_SOURCE_FIELDS].columns
        # after_derived_fields should have 'doubled'
        assert "doubled" in result.snapshots[SNAP_AFTER_DERIVED_FIELDS].columns

    def test_snapshots_with_filters(self, tmp_dir):
        """Post-filter snapshot is captured."""
        config_dict, _ = _make_csv_and_config(
            tmp_dir, post_filters=["amount > 200"],
        )
        path = write_yaml_config(config_dict, tmp_dir / "config.yaml")
        result = EntityPipeline(path).run(debug=True)

        assert "after_post_filters" in result.snapshots
        # post-filter removed rows with amount <= 200
        assert len(result.snapshots[SNAP_AFTER_POST_FILTERS]) == 3

    def test_snapshots_with_pre_filters(self, tmp_dir):
        """Pre-filter snapshot is captured."""
        config_dict, _ = _make_csv_and_config(
            tmp_dir, pre_filters=["amount > 200"],
        )
        path = write_yaml_config(config_dict, tmp_dir / "config.yaml")
        result = EntityPipeline(path).run(debug=True)

        assert "after_pre_filters" in result.snapshots
        assert len(result.snapshots[SNAP_AFTER_PRE_FILTERS]) == 3

    def test_snapshot_row_counts_progress(self, tmp_dir):
        """Snapshots show progressive data transformation."""
        config_dict, _ = _make_csv_and_config(tmp_dir, extra_fields=[
            {"name": "doubled", "derived": "amount * 2", "dtype": "float64"},
        ])
        path = write_yaml_config(config_dict, tmp_dir / "config.yaml")
        result = EntityPipeline(path).run(debug=True)

        # All snapshots should have 4 rows (no filters)
        for name, snap in result.snapshots.items():
            assert len(snap) == 4, f"snapshot '{name}' has {len(snap)} rows"

        # after_join has raw columns
        assert "id" in result.snapshots[SNAP_AFTER_JOIN].columns
        # final has only entity columns
        assert set(result.snapshots[SNAP_FINAL].columns) == {"id", "amount", "doubled"}


# ---------------------------------------------------------------------------
# C: Debug context on exceptions
# ---------------------------------------------------------------------------


def _test_always_fails(row):
    """Test function that always raises."""
    raise ValueError("boom")


class TestDebugContext:
    """debug=True attaches DebugContext to exceptions."""

    def test_transform_error_has_debug_context(self, tmp_dir):
        """TransformError gets debug_context when debug=True."""
        config_dict, _ = _make_csv_and_config(tmp_dir, extra_fields=[
            {"name": "bad", "derived": "nonexistent_col * 2", "dtype": "float64"},
        ])
        path = write_yaml_config(config_dict, tmp_dir / "config.yaml")
        with pytest.raises(TransformError) as exc_info:
            EntityPipeline(path).run(debug=True)

        ctx = exc_info.value.debug_context
        assert ctx is not None
        assert isinstance(ctx, DebugContext)
        assert isinstance(ctx.dataframe, pd.DataFrame)
        assert len(ctx.dataframe) == 4
        assert ctx.stage == STAGE_DERIVED_FIELDS
        assert ctx.parameters == {}
        assert ctx.field == "bad"
        assert ctx.expression == "nonexistent_col * 2"

    def test_no_debug_context_without_flag(self, tmp_dir):
        """Without debug=True, debug_context is None."""
        config_dict, _ = _make_csv_and_config(tmp_dir, extra_fields=[
            {"name": "bad", "derived": "nonexistent_col * 2", "dtype": "float64"},
        ])
        path = write_yaml_config(config_dict, tmp_dir / "config.yaml")
        with pytest.raises(TransformError) as exc_info:
            EntityPipeline(path).run()

        assert exc_info.value.debug_context is None

    def test_debug_context_has_parameters(self, tmp_dir):
        """Debug context includes effective parameters."""
        config_dict, _ = _make_csv_and_config(tmp_dir, extra_fields=[
            {"name": "bad", "derived": "nonexistent_col * 2", "dtype": "float64"},
        ])
        config_dict["parameters"] = {"snapshot_date": "2025-01-01"}
        path = write_yaml_config(config_dict, tmp_dir / "config.yaml")
        with pytest.raises(TransformError) as exc_info:
            EntityPipeline(path).run(debug=True, parameters={"extra": 42})

        ctx = exc_info.value.debug_context
        assert ctx.parameters["snapshot_date"] == "2025-01-01"
        assert ctx.parameters["extra"] == 42

    def test_debug_context_on_function_ref_error(self, tmp_dir):
        """Function ref errors also get debug context."""
        config_dict, _ = _make_csv_and_config(tmp_dir, extra_fields=[
            {
                "name": "bad",
                "derived": {
                    "function": "tests.unit.test_debug_mode._test_always_fails",
                },
                "dtype": "float64",
            },
        ])
        path = write_yaml_config(config_dict, tmp_dir / "config.yaml")
        with pytest.raises(TransformError) as exc_info:
            EntityPipeline(path).run(debug=True)

        ctx = exc_info.value.debug_context
        assert ctx is not None
        assert ctx.stage == STAGE_DERIVED_FIELDS
        assert ctx.expression == "fn:tests.unit.test_debug_mode._test_always_fails"

    def test_debug_context_on_validation_error(self, tmp_dir):
        """ValidationError gets debug_context when debug=True."""
        config_dict, _ = _make_csv_and_config(tmp_dir)
        # Add a validation that will fail
        config_dict["fields"][1]["validation_type"] = ["range"]
        config_dict["fields"][1]["validation_rule"] = {"range": [500.0, None]}
        config_dict["fields"][1]["validation_severity"] = {"range": "error"}
        path = write_yaml_config(config_dict, tmp_dir / "config.yaml")
        with pytest.raises(ValidationError) as exc_info:
            EntityPipeline(path).run(debug=True)

        ctx = exc_info.value.debug_context
        assert ctx is not None
        assert isinstance(ctx.dataframe, pd.DataFrame)
        assert ctx.stage == STAGE_SOURCE_VALIDATION

    def test_debug_context_on_filter_error(self, tmp_dir):
        """Filter errors get debug context."""
        config_dict, _ = _make_csv_and_config(
            tmp_dir, pre_filters=["nonexistent_col > 0"],
        )
        path = write_yaml_config(config_dict, tmp_dir / "config.yaml")
        with pytest.raises(TransformError) as exc_info:
            EntityPipeline(path).run(debug=True)

        ctx = exc_info.value.debug_context
        assert ctx is not None
        assert ctx.stage == STAGE_PRE_FILTERS
        assert isinstance(ctx.dataframe, pd.DataFrame)

    def test_debug_context_dataframe_is_copy(self, tmp_dir):
        """Debug context DataFrame is a copy, not a reference."""
        config_dict, _ = _make_csv_and_config(tmp_dir, extra_fields=[
            {"name": "bad", "derived": "nonexistent_col * 2", "dtype": "float64"},
        ])
        path = write_yaml_config(config_dict, tmp_dir / "config.yaml")
        with pytest.raises(TransformError) as exc_info:
            EntityPipeline(path).run(debug=True)

        ctx = exc_info.value.debug_context
        # Modifying the context df should not affect anything
        original_len = len(ctx.dataframe)
        ctx.dataframe.drop(ctx.dataframe.index, inplace=True)
        assert len(ctx.dataframe) == 0
        assert original_len == 4  # was 4 rows before we dropped
