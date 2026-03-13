"""Tests for custom_filter_mode: flag — selective space application.

In flag mode, custom config filters act as row selectors rather than reducers.
Matching rows get the custom space applied; non-matching rows keep primary
fields only. The full dataset is preserved.
"""
from __future__ import annotations

import dataclasses
from pathlib import Path

import pandas as pd
import pytest

from impact.common.exceptions import ConfigError, TransformError
from impact.entity.config.merger import merge_configs, merge_raw_configs
from impact.entity.pipeline import EntityPipeline
from tests.conftest import write_yaml_config


# ---------------------------------------------------------------------------
# Merger tests — flag mode filter handling
# ---------------------------------------------------------------------------


class TestMergerFlagMode:
    def test_flag_mode_filters_become_selectors(self):
        """In flag mode, post_filters are NOT appended — stored as selectors."""
        primary = {
            "post_filters": ["amount > 0"],
            "fields": [{"name": "id", "source": "id", "dtype": "str"}],
        }
        custom_spaces = {
            "risk": {
                "post_filters": ["product == 'TERM_LOAN'"],
                "fields": [{"name": "score", "derived": "amount * 0.1", "dtype": "float64"}],
            },
        }
        merged, selectors = merge_raw_configs(primary, custom_spaces, custom_filter_mode="flag")

        # Primary post_filters preserved, custom NOT appended
        assert merged["post_filters"] == ["amount > 0"]

        # Custom filters stored as selectors
        assert selectors["risk"] == ["product == 'TERM_LOAN'"]

    def test_flag_mode_pre_filters_become_selectors(self):
        """pre_filters in flag mode are also stored as selectors."""
        primary = {"fields": [{"name": "id", "source": "id", "dtype": "str"}]}
        custom_spaces = {
            "risk": {
                "pre_filters": ["status == 'ACTIVE'"],
                "post_filters": ["amount > 100"],
                "fields": [{"name": "score", "derived": "amount * 0.1", "dtype": "float64"}],
            },
        }
        merged, selectors = merge_raw_configs(primary, custom_spaces, custom_filter_mode="flag")
        assert merged["pre_filters"] == []
        assert merged["post_filters"] == []
        assert set(selectors["risk"]) == {
            "status == 'ACTIVE'", "amount > 100",
        }

    def test_filter_mode_default_appends(self):
        """Default (filter mode) still appends as before."""
        primary = {"post_filters": ["amount > 0"]}
        custom_spaces = {
            "risk": {
                "post_filters": ["product == 'TERM_LOAN'"],
                "fields": [{"name": "score", "derived": "amount * 0.1", "dtype": "float64"}],
            },
        }
        merged, selectors = merge_raw_configs(primary, custom_spaces)
        assert merged["post_filters"] == ["amount > 0", "product == 'TERM_LOAN'"]
        assert selectors == {}

    def test_invalid_mode_raises(self):
        primary = {}
        custom_spaces = {
            "risk": {
                "fields": [{"name": "x", "dtype": "str"}],
            },
        }
        with pytest.raises(ConfigError, match="Invalid custom_filter_mode"):
            merge_raw_configs(primary, custom_spaces, custom_filter_mode="invalid")

    def test_flag_mode_validations_still_appended(self):
        """Even in flag mode, validations are appended (not treated as selectors)."""
        primary = {"validations": [{"type": "not_null", "columns": ["id"]}]}
        custom_spaces = {
            "risk": {
                "validations": [{"type": "expression", "rule": "x > 0"}],
                "fields": [],
            },
        }
        merged, _ = merge_raw_configs(primary, custom_spaces, custom_filter_mode="flag")
        assert len(merged["validations"]) == 2

    def test_merge_configs_sets_space_selectors(self, tmp_dir):
        """merge_configs() sets space_selectors on the returned EntityConfig."""
        primary_dict = {
            "entity": {"name": "Test", "version": "1.0"},
            "sources": [{"name": "s", "type": "csv", "primary": True, "path": str(tmp_dir / "x.csv")}],
            "fields": [
                {"name": "id", "source": "id", "dtype": "str", "primary_key": True},
                {"name": "amount", "source": "amount", "dtype": "float64"},
            ],
        }
        custom_dict = {
            "post_filters": ["amount > 500"],
            "fields": [
                {"name": "risk_score", "derived": "amount * 0.01", "dtype": "float64"},
            ],
        }
        primary_path = write_yaml_config(primary_dict, tmp_dir / "primary.yaml")
        custom_path = write_yaml_config(custom_dict, tmp_dir / "custom_risk.yaml")

        config = merge_configs(primary=primary_path, custom=custom_path, custom_filter_mode="flag")
        assert config.space_selectors is not None
        assert "custom_risk" in config.space_selectors
        assert config.space_selectors["custom_risk"] == ["amount > 500"]

        # post_filters should NOT have the custom filter
        assert config.post_filters is None or "amount > 500" not in (config.post_filters or [])


# ---------------------------------------------------------------------------
# Pipeline integration tests — flag mode selective space application
# ---------------------------------------------------------------------------


class TestPipelineFlagMode:
    @pytest.fixture
    def setup_data(self, tmp_dir):
        """Create CSV data and configs for flag mode testing."""
        # 10 rows, 3 match the flag condition (amount > 500)
        df = pd.DataFrame({
            "id": [f"E{i:03d}" for i in range(10)],
            "amount": [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
            "category": ["A", "B", "A", "B", "A", "B", "A", "B", "A", "B"],
        })
        csv_path = tmp_dir / "data.csv"
        df.to_csv(csv_path, index=False)

        primary_dict = {
            "entity": {"name": "TestEntity", "version": "1.0"},
            "sources": [{"name": "main", "type": "csv", "primary": True, "path": str(csv_path)}],
            "fields": [
                {"name": "id", "source": "id", "dtype": "str", "primary_key": True},
                {"name": "amount", "source": "amount", "dtype": "float64"},
                {"name": "category", "source": "category", "dtype": "str"},
            ],
        }
        primary_path = write_yaml_config(primary_dict, tmp_dir / "primary.yaml")

        custom_dict = {
            "post_filters": ["amount > 500"],
            "fields": [
                {"name": "risk_score", "derived": "amount * 0.001", "dtype": "float64"},
                {"name": "risk_label", "derived": "lambda row: 'HIGH' if row['amount'] > 800 else 'MEDIUM'", "dtype": "str"},
            ],
        }
        custom_path = write_yaml_config(custom_dict, tmp_dir / "custom_risk.yaml")

        return primary_path, custom_path

    def test_full_dataset_preserved(self, setup_data):
        """Flag mode keeps all rows, not just matching ones."""
        primary_path, custom_path = setup_data
        result = EntityPipeline(
            config=primary_path,
            custom={"risk": custom_path},
            custom_filter_mode="flag",
        ).run()

        # All 10 rows preserved
        assert len(result.entities) == 10
        assert result.metadata["record_count"] == 10

    def test_matching_rows_get_space(self, setup_data):
        """Rows matching the selector get the custom space applied."""
        primary_path, custom_path = setup_data
        result = EntityPipeline(
            config=primary_path,
            custom={"risk": custom_path},
            custom_filter_mode="flag",
        ).run()

        # Rows with amount > 500 (600, 700, 800, 900, 1000) get risk space
        matching = [e for e in result.entities if hasattr(e, "spaces") and "risk" in e.spaces]
        assert len(matching) == 5

        # Check values on a matching entity
        e800 = next(e for e in result.entities if e.id == "E007")
        assert e800.amount == 800.0
        assert e800.risk_score == 0.8
        assert e800.risk_label == "MEDIUM"

        e900 = next(e for e in result.entities if e.id == "E008")
        assert e900.risk_score == 0.9
        assert e900.risk_label == "HIGH"

    def test_non_matching_rows_no_space(self, setup_data):
        """Rows not matching the selector have no space (primary only)."""
        primary_path, custom_path = setup_data
        result = EntityPipeline(
            config=primary_path,
            custom={"risk": custom_path},
            custom_filter_mode="flag",
        ).run()

        # Rows with amount <= 500 should NOT have risk space
        non_matching = [e for e in result.entities if not hasattr(e, "spaces") or "risk" not in e.spaces]
        assert len(non_matching) == 5

        # Primary fields still accessible
        e100 = next(e for e in result.entities if e.id == "E000")
        assert e100.amount == 100.0
        assert e100.category == "A"

    def test_space_selector_counts_in_metadata(self, setup_data):
        """Metadata includes space selector match counts."""
        primary_path, custom_path = setup_data
        result = EntityPipeline(
            config=primary_path,
            custom={"risk": custom_path},
            custom_filter_mode="flag",
        ).run()

        assert "space_selector_counts" in result.metadata
        assert result.metadata["space_selector_counts"]["risk"] == 5

    def test_filter_mode_backward_compat(self, setup_data, tmp_dir):
        """Without custom_filter_mode, filter mode (default) still reduces dataset."""
        primary_path, _ = setup_data

        custom_filter_dict = {
            "post_filters": ["amount > 500"],
            "fields": [
                {"name": "risk_score", "derived": "amount * 0.001", "dtype": "float64"},
            ],
        }
        custom_path = write_yaml_config(custom_filter_dict, tmp_dir / "filter_mode.yaml")

        # Default custom_filter_mode="filter"
        result = EntityPipeline(
            config=primary_path,
            custom={"risk": custom_path},
        ).run()

        # Only 5 rows survive (amount > 500)
        assert len(result.entities) == 5
        # All have the space
        for e in result.entities:
            assert "risk" in e.spaces

    def test_same_config_both_modes(self, setup_data):
        """Same custom config works in both filter and flag mode."""
        primary_path, custom_path = setup_data

        # Flag mode — full dataset
        flag_result = EntityPipeline(
            config=primary_path,
            custom={"risk": custom_path},
            custom_filter_mode="flag",
        ).run()
        assert len(flag_result.entities) == 10

        # Filter mode — reduced dataset
        filter_result = EntityPipeline(
            config=primary_path,
            custom={"risk": custom_path},
            custom_filter_mode="filter",
        ).run()
        assert len(filter_result.entities) == 5

    def test_primary_only_strips_flagged_space(self, setup_data):
        """primary_only() works on entities with/without spaces."""
        primary_path, custom_path = setup_data
        result = EntityPipeline(
            config=primary_path,
            custom={"risk": custom_path},
            custom_filter_mode="flag",
        ).run()

        # Entity with space
        with_space = next(e for e in result.entities if hasattr(e, "spaces") and "risk" in e.spaces)
        primary = with_space.primary_only()
        assert not hasattr(primary, "spaces")
        assert primary.id == with_space.id

        # Entity without space — primary_only still works
        without_space = next(e for e in result.entities if not hasattr(e, "spaces") or "risk" not in e.spaces)
        primary2 = without_space.primary_only()
        assert primary2.id == without_space.id
