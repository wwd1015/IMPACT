"""Tests for EntityBuilder with sub-entity classes and list handling."""

from __future__ import annotations

import dataclasses

import pandas as pd
import pytest

from impact.entity.config.schema import FieldConfig
from impact.entity.model.builder import EntityBuilder


class TestBuildClassWithSubEntities:
    def test_nested_field_becomes_list_type(self):
        fields = [
            FieldConfig(name="id", source="id", dtype="str", primary_key=True),
            FieldConfig(name="items", source="items", dtype="nested", entity_ref="Item"),
        ]
        # Simulate a sub-entity class
        ItemClass = dataclasses.make_dataclass("Item", [("x", str)])
        sub_entity_classes = {"items": ItemClass}

        builder = EntityBuilder()
        cls = builder.build_class("Parent", fields, sub_entity_classes=sub_entity_classes)

        # The items field should be typed as list
        field_types = {f.name: f.type for f in dataclasses.fields(cls)}
        assert field_types["items"] is list

    def test_nested_field_without_sub_entity_stays_dataframe(self):
        fields = [
            FieldConfig(name="id", source="id", dtype="str", primary_key=True),
            FieldConfig(name="items", source="items", dtype="nested"),
        ]

        builder = EntityBuilder()
        cls = builder.build_class("Parent", fields)

        field_types = {f.name: f.type for f in dataclasses.fields(cls)}
        assert field_types["items"] is pd.DataFrame

    def test_no_sub_entity_classes_default(self):
        fields = [
            FieldConfig(name="id", source="id", dtype="str", primary_key=True),
        ]

        builder = EntityBuilder()
        cls = builder.build_class("Test", fields)
        assert cls.__name__ == "Test"


class TestToEntitiesWithLists:
    def test_list_values_passed_through(self):
        fields = [
            FieldConfig(name="id", source="id", dtype="str", primary_key=True),
            FieldConfig(name="items", source="items", dtype="nested", entity_ref="Item"),
        ]
        ItemClass = dataclasses.make_dataclass("Item", [("x", str)])
        sub_entity_classes = {"items": ItemClass}

        builder = EntityBuilder()
        cls = builder.build_class("Parent", fields, sub_entity_classes=sub_entity_classes)

        item1 = ItemClass(x="a")
        item2 = ItemClass(x="b")
        df = pd.DataFrame({
            "id": ["P001"],
            "items": [[item1, item2]],
        })

        entities = builder.to_entities(df, cls)
        assert len(entities) == 1
        assert entities[0].items == [item1, item2]

    def test_empty_list(self):
        fields = [
            FieldConfig(name="id", source="id", dtype="str", primary_key=True),
            FieldConfig(name="items", source="items", dtype="nested", entity_ref="Item"),
        ]
        ItemClass = dataclasses.make_dataclass("Item", [("x", str)])
        sub_entity_classes = {"items": ItemClass}

        builder = EntityBuilder()
        cls = builder.build_class("Parent", fields, sub_entity_classes=sub_entity_classes)

        df = pd.DataFrame({
            "id": ["P001"],
            "items": [[]],
        })

        entities = builder.to_entities(df, cls)
        assert entities[0].items == []
