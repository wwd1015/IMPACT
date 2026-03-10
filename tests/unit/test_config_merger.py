"""Tests for config merger — merging primary IMPACT config with custom overrides."""

from __future__ import annotations

import pytest

from impact.entity.config.merger import merge_raw_configs


class TestMergeEntity:
    def test_custom_overrides_keys(self):
        primary = {"entity": {"name": "Facility", "version": "1.0", "description": "Standard"}}
        custom = {"entity": {"version": "2.0"}}
        merged = merge_raw_configs(primary, custom)
        assert merged["entity"]["name"] == "Facility"
        assert merged["entity"]["version"] == "2.0"
        assert merged["entity"]["description"] == "Standard"

    def test_no_custom_entity(self):
        primary = {"entity": {"name": "Facility"}}
        merged = merge_raw_configs(primary, {})
        assert merged["entity"]["name"] == "Facility"


class TestMergeParameters:
    def test_custom_overrides_and_adds(self):
        primary = {"parameters": {"snapshot_date": "2025-12-31", "active_product": "TERM_LOAN"}}
        custom = {"parameters": {"active_product": "REVOLVER", "region": "US"}}
        merged = merge_raw_configs(primary, custom)
        assert merged["parameters"]["snapshot_date"] == "2025-12-31"
        assert merged["parameters"]["active_product"] == "REVOLVER"
        assert merged["parameters"]["region"] == "US"

    def test_no_custom_parameters(self):
        primary = {"parameters": {"x": 1}}
        merged = merge_raw_configs(primary, {})
        assert merged["parameters"] == {"x": 1}


class TestMergeSources:
    def test_same_name_replaced(self):
        primary = {"sources": [
            {"name": "src_a", "type": "csv", "path": "old.csv"},
            {"name": "src_b", "type": "parquet", "path": "b.parquet"},
        ]}
        custom = {"sources": [
            {"name": "src_a", "type": "parquet", "path": "new.parquet"},
        ]}
        merged = merge_raw_configs(primary, custom)
        assert len(merged["sources"]) == 2
        assert merged["sources"][0]["name"] == "src_a"
        assert merged["sources"][0]["type"] == "parquet"
        assert merged["sources"][0]["path"] == "new.parquet"
        assert merged["sources"][1]["name"] == "src_b"

    def test_new_source_added(self):
        primary = {"sources": [{"name": "src_a", "type": "csv", "path": "a.csv"}]}
        custom = {"sources": [{"name": "src_c", "type": "csv", "path": "c.csv"}]}
        merged = merge_raw_configs(primary, custom)
        assert len(merged["sources"]) == 2
        names = [s["name"] for s in merged["sources"]]
        assert names == ["src_a", "src_c"]

    def test_ordering_preserved(self):
        primary = {"sources": [
            {"name": "b", "type": "csv", "path": "b.csv"},
            {"name": "a", "type": "csv", "path": "a.csv"},
        ]}
        custom = {"sources": [
            {"name": "a", "type": "parquet", "path": "a_new.parquet"},
        ]}
        merged = merge_raw_configs(primary, custom)
        names = [s["name"] for s in merged["sources"]]
        assert names == ["b", "a"]  # primary order preserved

    def test_no_custom_sources(self):
        primary = {"sources": [{"name": "x", "type": "csv", "path": "x.csv"}]}
        merged = merge_raw_configs(primary, {})
        assert len(merged["sources"]) == 1


class TestMergeJoins:
    def test_same_pair_replaced(self):
        primary = {"joins": [
            {"left": "A", "right": "B", "how": "left", "relationship": "one_to_one"},
        ]}
        custom = {"joins": [
            {"left": "A", "right": "B", "how": "inner", "relationship": "one_to_many"},
        ]}
        merged = merge_raw_configs(primary, custom)
        assert len(merged["joins"]) == 1
        assert merged["joins"][0]["how"] == "inner"
        assert merged["joins"][0]["relationship"] == "one_to_many"

    def test_new_pair_added(self):
        primary = {"joins": [
            {"left": "A", "right": "B", "how": "left", "relationship": "one_to_one"},
        ]}
        custom = {"joins": [
            {"left": "A", "right": "C", "how": "left", "relationship": "one_to_many"},
        ]}
        merged = merge_raw_configs(primary, custom)
        assert len(merged["joins"]) == 2

    def test_no_custom_joins(self):
        primary = {"joins": [{"left": "A", "right": "B", "how": "left"}]}
        merged = merge_raw_configs(primary, {})
        assert len(merged["joins"]) == 1


class TestMergeFilters:
    def test_custom_appended(self):
        primary = {"filters": ["amount > 0"]}
        custom = {"filters": ["region == 'US'"]}
        merged = merge_raw_configs(primary, custom)
        assert merged["filters"] == ["amount > 0", "region == 'US'"]

    def test_no_custom_filters(self):
        primary = {"filters": ["amount > 0"]}
        merged = merge_raw_configs(primary, {})
        assert merged["filters"] == ["amount > 0"]

    def test_no_primary_filters(self):
        primary = {}
        custom = {"filters": ["x > 0"]}
        merged = merge_raw_configs(primary, custom)
        assert merged["filters"] == ["x > 0"]


class TestMergeFields:
    def test_same_name_replaced_entirely(self):
        primary = {"fields": [
            {"name": "facility_id", "source": "facility_id", "dtype": "str",
             "validation_type": ["not_null"], "validation_severity": {"not_null": "error"}},
            {"name": "amount", "source": "amount", "dtype": "float64"},
        ]}
        custom = {"fields": [
            {"name": "facility_id", "source": "fac_id", "dtype": "str"},
        ]}
        merged = merge_raw_configs(primary, custom)
        assert len(merged["fields"]) == 2
        fid = merged["fields"][0]
        assert fid["source"] == "fac_id"
        # Validation should be gone — entire field replaced
        assert "validation_type" not in fid

    def test_new_field_added(self):
        primary = {"fields": [
            {"name": "id", "source": "id", "dtype": "str"},
        ]}
        custom = {"fields": [
            {"name": "region", "source": "region", "dtype": "str"},
        ]}
        merged = merge_raw_configs(primary, custom)
        assert len(merged["fields"]) == 2
        names = [f["name"] for f in merged["fields"]]
        assert names == ["id", "region"]

    def test_field_ordering(self):
        """Primary field order preserved; custom additions at end."""
        primary = {"fields": [
            {"name": "b", "source": "b", "dtype": "str"},
            {"name": "a", "source": "a", "dtype": "str"},
        ]}
        custom = {"fields": [
            {"name": "c", "source": "c", "dtype": "str"},
            {"name": "a", "source": "a_custom", "dtype": "int64"},
        ]}
        merged = merge_raw_configs(primary, custom)
        names = [f["name"] for f in merged["fields"]]
        assert names == ["b", "a", "c"]
        assert merged["fields"][1]["source"] == "a_custom"


class TestMergeValidations:
    def test_custom_appended(self):
        primary = {"validations": [
            {"type": "not_null", "columns": ["id"], "severity": "error"},
        ]}
        custom = {"validations": [
            {"type": "expression", "rule": "x > 0", "severity": "warning"},
        ]}
        merged = merge_raw_configs(primary, custom)
        assert len(merged["validations"]) == 2

    def test_no_custom_validations(self):
        primary = {"validations": [{"type": "not_null", "columns": ["id"]}]}
        merged = merge_raw_configs(primary, {})
        assert len(merged["validations"]) == 1


class TestMergeFullConfig:
    def test_empty_custom_returns_primary(self):
        primary = {
            "entity": {"name": "Facility", "version": "1.0"},
            "parameters": {"date": "2025-01-01"},
            "sources": [{"name": "main", "type": "csv", "path": "x.csv", "primary": True}],
            "fields": [{"name": "id", "source": "id", "dtype": "str"}],
        }
        merged = merge_raw_configs(primary, {})
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
        custom = {
            "parameters": {"x": 2},
            "fields": [{"name": "id", "source": "custom_id", "dtype": "str"}],
        }
        merge_raw_configs(primary, custom)
        # Primary should not be mutated
        assert primary["parameters"]["x"] == 1
        assert primary["fields"][0]["source"] == "id"

    def test_both_empty(self):
        merged = merge_raw_configs({}, {})
        assert merged["entity"] == {}
        assert merged["parameters"] == {}
        assert merged["sources"] == []
        assert merged["joins"] == []
        assert merged["filters"] == []
        assert merged["fields"] == []
        assert merged["validations"] == []

    def test_custom_only_sections(self):
        """Custom has sections that primary doesn't."""
        primary = {"entity": {"name": "Facility"}}
        custom = {
            "parameters": {"region": "US"},
            "filters": ["amount > 0"],
        }
        merged = merge_raw_configs(primary, custom)
        assert merged["parameters"]["region"] == "US"
        assert merged["filters"] == ["amount > 0"]

    def test_duplicate_filters_appended(self):
        """Same filter in both should appear twice (append, not deduplicate)."""
        primary = {"filters": ["amount > 0"]}
        custom = {"filters": ["amount > 0"]}
        merged = merge_raw_configs(primary, custom)
        assert merged["filters"] == ["amount > 0", "amount > 0"]


class TestMergeEdgeCases:
    def test_source_without_name_key_preserved(self):
        """Source items missing the merge key are kept as-is."""
        primary = {"sources": [{"type": "csv", "path": "a.csv"}]}
        custom = {"sources": [{"name": "new", "type": "parquet", "path": "b.parquet"}]}
        merged = merge_raw_configs(primary, custom)
        assert len(merged["sources"]) == 2

    def test_join_symmetric_pair_not_matched(self):
        """(A, B) and (B, A) are different join pairs."""
        primary = {"joins": [{"left": "A", "right": "B", "how": "left"}]}
        custom = {"joins": [{"left": "B", "right": "A", "how": "inner"}]}
        merged = merge_raw_configs(primary, custom)
        assert len(merged["joins"]) == 2

    def test_unknown_custom_sections_ignored(self):
        """Unknown sections in custom config are silently ignored."""
        primary = {"entity": {"name": "Facility"}}
        custom = {"typo_sorces": [{"name": "x"}], "transforms": []}
        merged = merge_raw_configs(primary, custom)
        assert "typo_sorces" not in merged
        assert "transforms" not in merged

    def test_multiple_fields_mixed_override_and_add(self):
        primary = {"fields": [
            {"name": "a", "source": "a", "dtype": "str"},
            {"name": "b", "source": "b", "dtype": "int64"},
            {"name": "c", "source": "c", "dtype": "float64"},
        ]}
        custom = {"fields": [
            {"name": "b", "source": "b_custom", "dtype": "str"},
            {"name": "d", "source": "d", "dtype": "bool"},
        ]}
        merged = merge_raw_configs(primary, custom)
        names = [f["name"] for f in merged["fields"]]
        assert names == ["a", "b", "c", "d"]
        assert merged["fields"][1]["source"] == "b_custom"
        assert merged["fields"][1]["dtype"] == "str"
