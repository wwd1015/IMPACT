"""Tests for the dynamic entity builder."""

from __future__ import annotations

import dataclasses

import pandas as pd
import pytest

from impact.common.exceptions import EntityBuildError
from impact.entity.config.schema import FieldConfig
from impact.entity.model.builder import EntityBuilder


@pytest.fixture
def builder():
    return EntityBuilder()


@pytest.fixture
def basic_fields():
    return [
        FieldConfig(name="id", source="id", dtype="str", primary_key=True),
        FieldConfig(name="amount", source="amount", dtype="float64"),
        FieldConfig(name="name", source="name", dtype="str"),
        FieldConfig(name="active", source="active", dtype="bool"),
    ]


class TestEntityBuilder:
    def test_build_class(self, builder, basic_fields):
        cls = builder.build_class("TestEntity", basic_fields)
        assert cls.__name__ == "TestEntity"
        assert dataclasses.is_dataclass(cls)
        assert cls.__entity_name__ == "TestEntity"

    def test_build_class_has_primary_key(self, builder, basic_fields):
        cls = builder.build_class("TestEntity", basic_fields)
        assert cls.__primary_key__ == ["id"]

    def test_build_class_fields(self, builder, basic_fields):
        cls = builder.build_class("TestEntity", basic_fields)
        field_names = [f.name for f in dataclasses.fields(cls)]
        assert field_names == ["id", "amount", "name", "active"]

    def test_instantiate_entity(self, builder, basic_fields):
        cls = builder.build_class("TestEntity", basic_fields)
        obj = cls(id="E001", amount=1000.0, name="Test", active=True)
        assert obj.id == "E001"
        assert obj.amount == 1000.0
        assert obj.name == "Test"
        assert obj.active is True

    def test_optional_fields_default_none(self, builder, basic_fields):
        cls = builder.build_class("TestEntity", basic_fields)
        obj = cls(id="E001")
        assert obj.amount is None
        assert obj.name is None
        assert obj.active is None

    def test_build_class_unknown_dtype(self, builder):
        fields = [FieldConfig(name="x", source="x", dtype="unknown_type")]
        with pytest.raises(EntityBuildError, match="Unknown dtype"):
            builder.build_class("Bad", fields)

    def test_nested_field(self, builder):
        fields = [
            FieldConfig(name="id", source="id", dtype="str", primary_key=True),
            FieldConfig(name="items", source="items", dtype="nested"),
        ]
        cls = builder.build_class("Parent", fields)
        nested_df = pd.DataFrame({"col": [1, 2]})
        obj = cls(id="P001", items=nested_df)
        assert isinstance(obj.items, pd.DataFrame)
        assert len(obj.items) == 2


class TestToEntities:
    def test_to_entities(self, builder, basic_fields):
        cls = builder.build_class("TestEntity", basic_fields)
        df = pd.DataFrame(
            {
                "id": ["E001", "E002"],
                "amount": [100.0, 200.0],
                "name": ["Alice", "Bob"],
                "active": [True, False],
            }
        )
        entities = builder.to_entities(df, cls)
        assert len(entities) == 2
        assert entities[0].id == "E001"
        assert entities[1].amount == 200.0

    def test_to_entities_with_nulls(self, builder, basic_fields):
        cls = builder.build_class("TestEntity", basic_fields)
        df = pd.DataFrame(
            {
                "id": ["E001"],
                "amount": [None],
                "name": [None],
                "active": [None],
            }
        )
        entities = builder.to_entities(df, cls)
        assert len(entities) == 1
        assert entities[0].amount is None

    def test_to_entities_with_nested_df(self, builder):
        fields = [
            FieldConfig(name="id", source="id", dtype="str", primary_key=True),
            FieldConfig(name="children", source="children", dtype="nested"),
        ]
        cls = builder.build_class("Parent", fields)

        nested1 = pd.DataFrame({"val": [1, 2]})
        nested2 = pd.DataFrame({"val": [3]})

        df = pd.DataFrame({"id": ["P1", "P2"], "children": [nested1, nested2]})
        entities = builder.to_entities(df, cls)

        assert len(entities) == 2
        assert isinstance(entities[0].children, pd.DataFrame)
        assert len(entities[0].children) == 2
        assert len(entities[1].children) == 1

    def test_to_entities_missing_columns_warns(self, builder, basic_fields):
        cls = builder.build_class("TestEntity", basic_fields)
        # DataFrame missing 'active' column
        df = pd.DataFrame({"id": ["E001"], "amount": [100.0], "name": ["Alice"]})
        entities = builder.to_entities(df, cls)
        assert len(entities) == 1
        assert entities[0].active is None  # default
