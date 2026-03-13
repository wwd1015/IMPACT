"""Tests for config merger — merging primary IMPACT config with custom overrides."""

from __future__ import annotations

import pytest

from impact.common.exceptions import ConfigError
from impact.entity.config.merger import merge_raw_configs


def _single_space(custom_raw: dict, space_name: str = "custom") -> dict[str, dict]:
    """Wrap a raw custom config dict into a single-space dict for merge_raw_configs."""
    return {space_name: custom_raw}


class TestMergeEntity:
    def test_custom_overrides_keys(self):
        primary = {"entity": {"name": "Facility", "version": "1.0", "description": "Standard"}}
        custom = _single_space({"entity": {"version": "2.0"}})
        merged, _ = merge_raw_configs(primary, custom)
        assert merged["entity"]["name"] == "Facility"
        assert merged["entity"]["version"] == "2.0"
        assert merged["entity"]["description"] == "Standard"

    def test_no_custom_entity(self):
        primary = {"entity": {"name": "Facility"}}
        merged, _ = merge_raw_configs(primary, {})
        assert merged["entity"]["name"] == "Facility"


class TestMergeParameters:
    def test_custom_overrides_and_adds(self):
        primary = {"parameters": {"snapshot_date": "2025-12-31", "active_product": "TERM_LOAN"}}
        custom = _single_space({"parameters": {"active_product": "REVOLVER", "region": "US"}})
        merged, _ = merge_raw_configs(primary, custom)
        assert merged["parameters"]["snapshot_date"] == "2025-12-31"
        assert merged["parameters"]["active_product"] == "REVOLVER"
        assert merged["parameters"]["region"] == "US"

    def test_no_custom_parameters(self):
        primary = {"parameters": {"x": 1}}
        merged, _ = merge_raw_configs(primary, {})
        assert merged["parameters"] == {"x": 1}


class TestMergeSources:
    def test_same_name_replaced(self):
        primary = {"sources": [
            {"name": "src_a", "type": "csv", "path": "old.csv"},
            {"name": "src_b", "type": "parquet", "path": "b.parquet"},
        ]}
        custom = _single_space({"sources": [
            {"name": "src_a", "type": "parquet", "path": "new.parquet"},
        ]})
        merged, _ = merge_raw_configs(primary, custom)
        assert len(merged["sources"]) == 2
        assert merged["sources"][0]["name"] == "src_a"
        assert merged["sources"][0]["type"] == "parquet"
        assert merged["sources"][0]["path"] == "new.parquet"
        assert merged["sources"][1]["name"] == "src_b"

    def test_new_source_added(self):
        primary = {"sources": [{"name": "src_a", "type": "csv", "path": "a.csv"}]}
        custom = _single_space({"sources": [{"name": "src_c", "type": "csv", "path": "c.csv"}]})
        merged, _ = merge_raw_configs(primary, custom)
        assert len(merged["sources"]) == 2
        names = [s["name"] for s in merged["sources"]]
        assert names == ["src_a", "src_c"]

    def test_ordering_preserved(self):
        primary = {"sources": [
            {"name": "b", "type": "csv", "path": "b.csv"},
            {"name": "a", "type": "csv", "path": "a.csv"},
        ]}
        custom = _single_space({"sources": [
            {"name": "a", "type": "parquet", "path": "a_new.parquet"},
        ]})
        merged, _ = merge_raw_configs(primary, custom)
        names = [s["name"] for s in merged["sources"]]
        assert names == ["b", "a"]  # primary order preserved

    def test_no_custom_sources(self):
        primary = {"sources": [{"name": "x", "type": "csv", "path": "x.csv"}]}
        merged, _ = merge_raw_configs(primary, {})
        assert len(merged["sources"]) == 1


class TestMergeJoins:
    def test_same_pair_replaced(self):
        primary = {"joins": [
            {"left": "A", "right": "B", "how": "left", "relationship": "one_to_one"},
        ]}
        custom = _single_space({"joins": [
            {"left": "A", "right": "B", "how": "inner", "relationship": "one_to_many"},
        ]})
        merged, _ = merge_raw_configs(primary, custom)
        assert len(merged["joins"]) == 1
        assert merged["joins"][0]["how"] == "inner"
        assert merged["joins"][0]["relationship"] == "one_to_many"

    def test_new_pair_added(self):
        primary = {"joins": [
            {"left": "A", "right": "B", "how": "left", "relationship": "one_to_one"},
        ]}
        custom = _single_space({"joins": [
            {"left": "A", "right": "C", "how": "left", "relationship": "one_to_many"},
        ]})
        merged, _ = merge_raw_configs(primary, custom)
        assert len(merged["joins"]) == 2

    def test_no_custom_joins(self):
        primary = {"joins": [{"left": "A", "right": "B", "how": "left"}]}
        merged, _ = merge_raw_configs(primary, {})
        assert len(merged["joins"]) == 1


class TestMergePreFilters:
    def test_custom_appended(self):
        primary = {"pre_filters": ["amount > 0"]}
        custom = _single_space({"pre_filters": ["status == 'ACTIVE'"]})
        merged, _ = merge_raw_configs(primary, custom)
        assert merged["pre_filters"] == ["amount > 0", "status == 'ACTIVE'"]

    def test_no_custom(self):
        primary = {"pre_filters": ["amount > 0"]}
        merged, _ = merge_raw_configs(primary, {})
        assert merged["pre_filters"] == ["amount > 0"]

    def test_no_primary(self):
        primary = {}
        custom = _single_space({"pre_filters": ["x > 0"]})
        merged, _ = merge_raw_configs(primary, custom)
        assert merged["pre_filters"] == ["x > 0"]


class TestMergePostFilters:
    def test_custom_appended(self):
        primary = {"post_filters": ["amount > 0"]}
        custom = _single_space({"post_filters": ["region == 'US'"]})
        merged, _ = merge_raw_configs(primary, custom)
        assert merged["post_filters"] == ["amount > 0", "region == 'US'"]

    def test_no_custom_filters(self):
        primary = {"post_filters": ["amount > 0"]}
        merged, _ = merge_raw_configs(primary, {})
        assert merged["post_filters"] == ["amount > 0"]

    def test_no_primary_filters(self):
        primary = {}
        custom = _single_space({"post_filters": ["x > 0"]})
        merged, _ = merge_raw_configs(primary, custom)
        assert merged["post_filters"] == ["x > 0"]


class TestMergeFields:
    def test_overlap_with_primary_raises_error(self):
        """Custom field with same name as primary field raises ConfigError."""
        primary = {"fields": [
            {"name": "facility_id", "source": "facility_id", "dtype": "str"},
            {"name": "amount", "source": "amount", "dtype": "float64"},
        ]}
        custom = _single_space({"fields": [
            {"name": "facility_id", "source": "fac_id", "dtype": "str"},
        ]})
        with pytest.raises(ConfigError, match="already exists in the primary config"):
            merge_raw_configs(primary, custom)

    def test_new_field_added_with_space_tag(self):
        primary = {"fields": [
            {"name": "id", "source": "id", "dtype": "str"},
        ]}
        custom = _single_space({"fields": [
            {"name": "region", "source": "region", "dtype": "str"},
        ]}, space_name="reporting")
        merged, _ = merge_raw_configs(primary, custom)
        assert len(merged["fields"]) == 2
        assert merged["fields"][0]["name"] == "id"
        assert "space" not in merged["fields"][0]  # primary field, no space tag
        assert merged["fields"][1]["name"] == "region"
        assert merged["fields"][1]["space"] == "reporting"

    def test_field_ordering_primary_first_then_spaces(self):
        """Primary fields first, then custom space fields appended."""
        primary = {"fields": [
            {"name": "b", "source": "b", "dtype": "str"},
            {"name": "a", "source": "a", "dtype": "str"},
        ]}
        custom = _single_space({"fields": [
            {"name": "c", "source": "c", "dtype": "str"},
        ]})
        merged, _ = merge_raw_configs(primary, custom)
        names = [f["name"] for f in merged["fields"]]
        assert names == ["b", "a", "c"]
        assert merged["fields"][2]["space"] == "custom"

    def test_no_custom_fields(self):
        primary = {"fields": [
            {"name": "id", "source": "id", "dtype": "str"},
        ]}
        merged, _ = merge_raw_configs(primary, {})
        assert len(merged["fields"]) == 1
        assert "space" not in merged["fields"][0]


class TestMergeValidations:
    def test_custom_appended(self):
        primary = {"validations": [
            {"type": "not_null", "columns": ["id"], "severity": "error"},
        ]}
        custom = _single_space({"validations": [
            {"type": "expression", "rule": "x > 0", "severity": "warning"},
        ]})
        merged, _ = merge_raw_configs(primary, custom)
        assert len(merged["validations"]) == 2

    def test_no_custom_validations(self):
        primary = {"validations": [{"type": "not_null", "columns": ["id"]}]}
        merged, _ = merge_raw_configs(primary, {})
        assert len(merged["validations"]) == 1


class TestMergeFullConfig:
    def test_empty_custom_returns_primary(self):
        primary = {
            "entity": {"name": "Facility", "version": "1.0"},
            "parameters": {"date": "2025-01-01"},
            "sources": [{"name": "main", "type": "csv", "path": "x.csv", "primary": True}],
            "fields": [{"name": "id", "source": "id", "dtype": "str"}],
        }
        merged, _ = merge_raw_configs(primary, {})
        assert merged["entity"]["name"] == "Facility"
        assert len(merged["sources"]) == 1
        assert len(merged["fields"]) == 1

    def test_primary_not_mutated(self):
        primary = {
            "entity": {"name": "Facility"},
            "parameters": {"x": 1},
            "sources": [{"name": "a", "type": "csv", "path": "a.csv"}],
            "fields": [{"name": "id", "source": "id", "dtype": "str"}],
        }
        custom = _single_space({
            "parameters": {"x": 2},
            "fields": [{"name": "custom_field", "source": "cf", "dtype": "str"}],
        })
        merge_raw_configs(primary, custom)
        # Primary should not be mutated
        assert primary["parameters"]["x"] == 1
        assert len(primary["fields"]) == 1

    def test_both_empty(self):
        merged, _ = merge_raw_configs({}, {})
        assert merged["entity"] == {}
        assert merged["parameters"] == {}
        assert merged["sources"] == []
        assert merged["joins"] == []
        assert merged["pre_filters"] == []
        assert merged["post_filters"] == []
        assert merged["fields"] == []
        assert merged["validations"] == []

    def test_custom_only_sections(self):
        """Custom has sections that primary doesn't."""
        primary = {"entity": {"name": "Facility"}}
        custom = _single_space({
            "parameters": {"region": "US"},
            "post_filters": ["amount > 0"],
        })
        merged, _ = merge_raw_configs(primary, custom)
        assert merged["parameters"]["region"] == "US"
        assert merged["post_filters"] == ["amount > 0"]

    def test_duplicate_filters_appended(self):
        """Same filter in both should appear twice (append, not deduplicate)."""
        primary = {"post_filters": ["amount > 0"]}
        custom = _single_space({"post_filters": ["amount > 0"]})
        merged, _ = merge_raw_configs(primary, custom)
        assert merged["post_filters"] == ["amount > 0", "amount > 0"]


class TestMergeEdgeCases:
    def test_source_without_name_key_preserved(self):
        """Source items missing the merge key are kept as-is."""
        primary = {"sources": [{"type": "csv", "path": "a.csv"}]}
        custom = _single_space({"sources": [{"name": "new", "type": "parquet", "path": "b.parquet"}]})
        merged, _ = merge_raw_configs(primary, custom)
        assert len(merged["sources"]) == 2

    def test_join_symmetric_pair_not_matched(self):
        """(A, B) and (B, A) are different join pairs."""
        primary = {"joins": [{"left": "A", "right": "B", "how": "left"}]}
        custom = _single_space({"joins": [{"left": "B", "right": "A", "how": "inner"}]})
        merged, _ = merge_raw_configs(primary, custom)
        assert len(merged["joins"]) == 2

    def test_unknown_custom_sections_ignored(self):
        """Unknown sections in custom config are silently ignored."""
        primary = {"entity": {"name": "Facility"}}
        custom = _single_space({"typo_sorces": [{"name": "x"}], "transforms": []})
        merged, _ = merge_raw_configs(primary, custom)
        assert "typo_sorces" not in merged
        assert "transforms" not in merged


class TestMultipleCustomSpaces:
    """Tests for merging multiple custom configs, each with its own space."""

    def test_two_spaces_fields_tagged_separately(self):
        primary = {"fields": [
            {"name": "id", "source": "id", "dtype": "str"},
        ]}
        custom_spaces = {
            "risk": {"fields": [
                {"name": "risk_score", "source": "risk_score", "dtype": "float64"},
            ]},
            "reporting": {"fields": [
                {"name": "report_flag", "source": "report_flag", "dtype": "bool"},
            ]},
        }
        merged, _ = merge_raw_configs(primary, custom_spaces)
        assert len(merged["fields"]) == 3
        assert merged["fields"][0]["name"] == "id"
        assert "space" not in merged["fields"][0]
        assert merged["fields"][1]["name"] == "risk_score"
        assert merged["fields"][1]["space"] == "risk"
        assert merged["fields"][2]["name"] == "report_flag"
        assert merged["fields"][2]["space"] == "reporting"

    def test_cross_space_field_overlap_allowed(self):
        """Same field name in two different custom spaces is allowed (separate layers)."""
        primary = {"fields": [
            {"name": "id", "source": "id", "dtype": "str"},
        ]}
        custom_spaces = {
            "risk": {"fields": [
                {"name": "score", "source": "risk_score", "dtype": "float64"},
            ]},
            "reporting": {"fields": [
                {"name": "score", "source": "report_score", "dtype": "float64"},
            ]},
        }
        merged, _ = merge_raw_configs(primary, custom_spaces)
        score_fields = [f for f in merged["fields"] if f["name"] == "score"]
        assert len(score_fields) == 2
        spaces = {f["space"] for f in score_fields}
        assert spaces == {"risk", "reporting"}

    def test_parameters_merged_across_all_spaces(self):
        primary = {"parameters": {"x": 1}}
        custom_spaces = {
            "a": {"parameters": {"y": 2}},
            "b": {"parameters": {"z": 3}},
        }
        merged, _ = merge_raw_configs(primary, custom_spaces)
        assert merged["parameters"] == {"x": 1, "y": 2, "z": 3}

    def test_filters_appended_from_all_spaces(self):
        primary = {"post_filters": ["a > 0"]}
        custom_spaces = {
            "risk": {"post_filters": ["risk_score > 0.5"]},
            "reporting": {"post_filters": ["report_flag == True"]},
        }
        merged, _ = merge_raw_configs(primary, custom_spaces)
        assert len(merged["post_filters"]) == 3
        assert "a > 0" in merged["post_filters"]
        assert "risk_score > 0.5" in merged["post_filters"]

    def test_sources_merged_across_all_spaces(self):
        primary = {"sources": [{"name": "main", "type": "csv", "path": "main.csv"}]}
        custom_spaces = {
            "risk": {"sources": [{"name": "risk_src", "type": "csv", "path": "risk.csv"}]},
            "reporting": {"sources": [{"name": "report_src", "type": "csv", "path": "report.csv"}]},
        }
        merged, _ = merge_raw_configs(primary, custom_spaces)
        names = [s["name"] for s in merged["sources"]]
        assert "main" in names
        assert "risk_src" in names
        assert "report_src" in names
