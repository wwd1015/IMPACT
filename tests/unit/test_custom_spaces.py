"""Tests for custom field spaces — merged config fields in separate namespaces."""

from __future__ import annotations

import dataclasses

import pandas as pd
import pytest

from impact.common.exceptions import ConfigError
from impact.entity.config.schema import EntityConfig, FieldConfig
from impact.entity.model.builder import EntityBuilder
from impact.entity.pipeline import EntityPipeline


# ---------------------------------------------------------------------------
# EntityBuilder with spaces
# ---------------------------------------------------------------------------


class TestBuildClassWithSpaces:
    """Test EntityBuilder produces spaced entity classes correctly."""

    def _make_fields(self):
        return [
            FieldConfig(name="id", source="id", dtype="str", primary_key=True),
            FieldConfig(name="amount", source="amount", dtype="float64"),
            FieldConfig(name="risk_score", source="risk_score", dtype="float64", space="risk"),
            FieldConfig(name="risk_grade", source="risk_grade", dtype="str", space="risk"),
            FieldConfig(name="report_flag", source="report_flag", dtype="bool", space="reporting"),
        ]

    def test_no_spaces_returns_plain_dataclass(self):
        """Without custom spaces, build_class returns a plain dataclass."""
        fields = [
            FieldConfig(name="id", source="id", dtype="str", primary_key=True),
            FieldConfig(name="amount", source="amount", dtype="float64"),
        ]
        builder = EntityBuilder()
        cls = builder.build_class("Facility", fields)
        assert dataclasses.is_dataclass(cls)
        assert cls.__name__ == "Facility"

    def test_spaces_returns_dataclass_with_spaces(self):
        """With custom spaces, build_class returns a dataclass with spaces dict."""
        fields = self._make_fields()
        builder = EntityBuilder()
        cls = builder.build_class("Facility", fields)
        assert dataclasses.is_dataclass(cls)
        assert cls.__entity_name__ == "Facility"
        assert hasattr(cls, "__space_field_names__")
        assert "risk" in cls.__space_field_names__
        assert "reporting" in cls.__space_field_names__

    def test_entity_fields_metadata(self):
        """All fields (primary + spaces) are in __entity_fields__."""
        fields = self._make_fields()
        builder = EntityBuilder()
        cls = builder.build_class("Facility", fields)
        assert len(cls.__entity_fields__) == 5
        assert cls.__primary_key__ == ["id"]

    def test_space_field_names_metadata(self):
        """__space_field_names__ maps space name to field name list."""
        fields = self._make_fields()
        builder = EntityBuilder()
        cls = builder.build_class("Facility", fields)
        assert set(cls.__space_field_names__["risk"]) == {"risk_score", "risk_grade"}
        assert cls.__space_field_names__["reporting"] == ["report_flag"]


class TestSpacedEntityAccess:
    """Test attribute/item access on spaced entity instances."""

    def _build_entity(self):
        fields = [
            FieldConfig(name="id", source="id", dtype="str", primary_key=True),
            FieldConfig(name="amount", source="amount", dtype="float64"),
            FieldConfig(name="risk_score", source="risk_score", dtype="float64", space="risk"),
            FieldConfig(name="report_flag", source="report_flag", dtype="bool", space="reporting"),
        ]
        builder = EntityBuilder()
        cls = builder.build_class("Facility", fields)
        df = pd.DataFrame({
            "id": ["FAC-001"],
            "amount": [1000.0],
            "risk_score": [0.85],
            "report_flag": [True],
        })
        entities = builder.to_entities(df, cls)
        return entities[0]

    def test_primary_attribute_access(self):
        entity = self._build_entity()
        assert entity.id == "FAC-001"
        assert entity.amount == 1000.0

    def test_space_attribute_access_transparent(self):
        """Unique space field names accessible as direct attributes."""
        entity = self._build_entity()
        assert entity.risk_score == 0.85
        assert entity.report_flag is True

    def test_spaces_dict_access(self):
        """Space fields accessible via spaces dict."""
        entity = self._build_entity()
        assert entity.spaces["risk"]["risk_score"] == 0.85
        assert entity.spaces["reporting"]["report_flag"] is True

    def test_getitem_primary(self):
        entity = self._build_entity()
        assert entity["id"] == "FAC-001"

    def test_getitem_space(self):
        entity = self._build_entity()
        assert entity["risk_score"] == 0.85

    def test_getitem_qualified(self):
        """Qualified access with space_name.field_name."""
        entity = self._build_entity()
        assert entity["risk.risk_score"] == 0.85
        assert entity["reporting.report_flag"] is True

    def test_getattr_unknown_raises(self):
        entity = self._build_entity()
        with pytest.raises(AttributeError, match="has no field"):
            _ = entity.nonexistent

    def test_getitem_unknown_raises(self):
        entity = self._build_entity()
        with pytest.raises(KeyError):
            _ = entity["nonexistent"]

    def test_is_dataclass(self):
        """Spaced entity is a real dataclass."""
        entity = self._build_entity()
        assert dataclasses.is_dataclass(entity)


class TestSpacedEntityAmbiguousNames:
    """Test behavior when the same field name exists in multiple spaces."""

    def _build_entity(self):
        fields = [
            FieldConfig(name="id", source="id", dtype="str", primary_key=True),
            FieldConfig(name="score", source="score", dtype="float64", space="risk"),
            FieldConfig(name="score", source="score", dtype="float64", space="reporting"),
            FieldConfig(name="grade", source="grade", dtype="str", space="risk"),
        ]
        builder = EntityBuilder()
        cls = builder.build_class("Facility", fields)
        df = pd.DataFrame({
            "id": ["FAC-001"],
            "score": [0.85],
            "grade": ["B"],
        })
        entities = builder.to_entities(df, cls)
        return entities[0]

    def test_ambiguous_name_raises_error(self):
        """Accessing a field in multiple spaces raises AttributeError."""
        entity = self._build_entity()
        with pytest.raises(AttributeError, match="multiple spaces"):
            _ = entity.score

    def test_ambiguous_name_explicit_access(self):
        """Ambiguous names accessible via explicit spaces dict."""
        entity = self._build_entity()
        assert entity.spaces["risk"]["score"] == 0.85
        assert entity.spaces["reporting"]["score"] == 0.85

    def test_ambiguous_name_qualified_getitem(self):
        """Ambiguous names accessible via qualified getitem."""
        entity = self._build_entity()
        assert entity["risk.score"] == 0.85
        assert entity["reporting.score"] == 0.85

    def test_unique_name_still_works(self):
        """Unique names (grade) still work transparently."""
        entity = self._build_entity()
        assert entity.grade == "B"


class TestSpacedEntityDropSpace:
    """Test drop_space, select_space, and primary_only methods."""

    def _build_entity(self):
        fields = [
            FieldConfig(name="id", source="id", dtype="str", primary_key=True),
            FieldConfig(name="amount", source="amount", dtype="float64"),
            FieldConfig(name="risk_score", source="risk_score", dtype="float64", space="risk"),
            FieldConfig(name="report_flag", source="report_flag", dtype="bool", space="reporting"),
        ]
        builder = EntityBuilder()
        cls = builder.build_class("Facility", fields)
        df = pd.DataFrame({
            "id": ["FAC-001"],
            "amount": [1000.0],
            "risk_score": [0.85],
            "report_flag": [True],
        })
        return builder.to_entities(df, cls)[0]

    def test_drop_space_returns_copy(self):
        """drop_space returns a new entity, original unchanged."""
        entity = self._build_entity()
        reduced = entity.drop_space("reporting")
        # Original still has both spaces
        assert entity.report_flag is True
        assert entity.risk_score == 0.85
        # Reduced has only risk
        assert reduced.id == "FAC-001"
        assert reduced.risk_score == 0.85
        assert "reporting" not in reduced.spaces

    def test_drop_space_removes_fields(self):
        entity = self._build_entity()
        reduced = entity.drop_space("reporting")
        with pytest.raises(AttributeError):
            _ = reduced.report_flag

    def test_drop_last_space_returns_primary(self):
        """Dropping the only remaining space returns a plain primary dataclass."""
        fields = [
            FieldConfig(name="id", source="id", dtype="str", primary_key=True),
            FieldConfig(name="score", source="score", dtype="float64", space="risk"),
        ]
        builder = EntityBuilder()
        cls = builder.build_class("Test", fields)
        df = pd.DataFrame({"id": ["A"], "score": [0.5]})
        entity = builder.to_entities(df, cls)[0]

        primary = entity.drop_space("risk")
        assert dataclasses.is_dataclass(primary)
        assert primary.id == "A"
        assert not hasattr(primary, "spaces")

    def test_drop_unknown_space_raises(self):
        entity = self._build_entity()
        with pytest.raises(KeyError, match="not found"):
            entity.drop_space("nonexistent")

    def test_select_space(self):
        """select_space returns a new entity with only the named space."""
        entity = self._build_entity()
        risk_only = entity.select_space("risk")
        # Primary fields preserved
        assert risk_only.id == "FAC-001"
        assert risk_only.amount == 1000.0
        # Only risk space
        assert risk_only.risk_score == 0.85
        assert "risk" in risk_only.spaces
        assert "reporting" not in risk_only.spaces

    def test_select_space_returns_copy(self):
        """select_space returns a new entity, original unchanged."""
        entity = self._build_entity()
        risk_only = entity.select_space("risk")
        # Original still has both
        assert entity.report_flag is True
        # Selected has only risk
        with pytest.raises(AttributeError):
            _ = risk_only.report_flag

    def test_select_unknown_space_raises(self):
        entity = self._build_entity()
        with pytest.raises(KeyError, match="not found"):
            entity.select_space("nonexistent")

    def test_primary_only(self):
        entity = self._build_entity()
        primary = entity.primary_only()
        assert dataclasses.is_dataclass(primary)
        assert primary.id == "FAC-001"
        assert primary.amount == 1000.0
        assert not hasattr(primary, "risk_score")
        assert not hasattr(primary, "spaces")

    def test_primary_only_returns_copy(self):
        """primary_only returns a new entity, original unchanged."""
        entity = self._build_entity()
        primary = entity.primary_only()
        # Original still has spaces
        assert entity.risk_score == 0.85
        # Primary has no spaces
        assert not hasattr(primary, "spaces")

    def test_to_dict(self):
        entity = self._build_entity()
        d = entity.to_dict()
        assert d["id"] == "FAC-001"
        assert d["amount"] == 1000.0
        assert d["risk_score"] == 0.85
        assert d["report_flag"] is True

    def test_repr(self):
        entity = self._build_entity()
        r = repr(entity)
        assert "Facility" in r
        assert "FAC-001" in r


# ---------------------------------------------------------------------------
# Pipeline integration with spaces
# ---------------------------------------------------------------------------


class TestPipelineWithSpaces:
    """End-to-end pipeline tests with custom field spaces."""

    def _make_config(self, tmp_dir, extra_fields=None, space=None):
        """Create a CSV-based config, optionally with spaced fields."""
        csv_data = {"id": ["A", "B"], "amount": [100.0, 200.0], "rate": [0.05, 0.08]}
        csv_path = tmp_dir / "data.csv"
        pd.DataFrame(csv_data).to_csv(csv_path, index=False)

        fields = [
            {"name": "id", "source": "id", "dtype": "str", "primary_key": True},
            {"name": "amount", "source": "amount", "dtype": "float64"},
            {"name": "rate", "source": "rate", "dtype": "float64"},
        ]

        if extra_fields:
            for f in extra_fields:
                if space:
                    f["space"] = space
                fields.append(f)

        config_dict = {
            "entity": {"name": "TestEntity"},
            "sources": [{"name": "main", "type": "csv", "primary": True, "path": str(csv_path)}],
            "fields": fields,
        }
        return EntityConfig.model_validate(config_dict)

    def test_pipeline_with_spaced_derived_field(self, tmp_dir):
        """Derived field in a custom space works through the pipeline."""
        config = self._make_config(tmp_dir, extra_fields=[
            {"name": "adjusted", "derived": "amount * rate", "dtype": "float64"},
        ], space="risk")
        result = EntityPipeline(config=config).run()
        entity = result.entities[0]

        # Primary fields accessible
        assert entity.id == "A"
        assert entity.amount == 100.0

        # Space field accessible transparently
        assert entity.adjusted == 100.0 * 0.05

    def test_pipeline_primary_only_drops_spaces(self, tmp_dir):
        """primary_only() returns entity without custom space fields."""
        config = self._make_config(tmp_dir, extra_fields=[
            {"name": "custom_score", "derived": "amount / 10", "dtype": "float64"},
        ], space="scoring")
        result = EntityPipeline(config=config).run()
        entity = result.entities[0]

        primary = entity.primary_only()
        assert primary.id == "A"
        assert primary.amount == 100.0
        assert not hasattr(primary, "custom_score")

    def test_pipeline_no_spaces_backward_compatible(self, tmp_dir):
        """Pipeline without spaces produces plain dataclass entities."""
        config = self._make_config(tmp_dir)
        result = EntityPipeline(config=config).run()
        entity = result.entities[0]
        assert dataclasses.is_dataclass(entity)
        assert entity.id == "A"

    def test_pipeline_spaced_fields_in_dataframe(self, tmp_dir):
        """All fields (primary + space) appear as columns in result DataFrame."""
        config = self._make_config(tmp_dir, extra_fields=[
            {"name": "risk_score", "derived": "amount * 0.01", "dtype": "float64"},
        ], space="risk")
        result = EntityPipeline(config=config).run()
        assert "risk_score" in result.dataframe.columns
        assert "id" in result.dataframe.columns

    def test_pipeline_select_space(self, tmp_dir):
        """select_space() returns entity with only the named space."""
        config = self._make_config(tmp_dir, extra_fields=[
            {"name": "custom_score", "derived": "amount / 10", "dtype": "float64"},
        ], space="scoring")
        result = EntityPipeline(config=config).run()
        entity = result.entities[0]

        selected = entity.select_space("scoring")
        assert selected.id == "A"
        assert selected.custom_score == 10.0
