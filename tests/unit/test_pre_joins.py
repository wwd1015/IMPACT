"""Tests for pre-joins between non-primary sources."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
import yaml

from impact.entity.config.schema import EntityConfig
from impact.entity.pipeline import EntityPipeline

from tests.conftest import write_yaml_config


class TestPreJoins:
    def test_non_primary_left_join(self, tmp_dir):
        """Join between two non-primary sources before nesting under primary."""
        # Create data files
        obligor_csv = tmp_dir / "obligors.csv"
        pd.DataFrame({
            "obligor_id": ["O001", "O002"],
            "legal_name": ["Acme Corp", "Beta Inc"],
        }).to_csv(obligor_csv, index=False)

        facility_csv = tmp_dir / "facilities.csv"
        pd.DataFrame({
            "facility_id": ["F001", "F002", "F003"],
            "obligor_id": ["O001", "O001", "O002"],
            "commitment_amount": [1000000.0, 500000.0, 2000000.0],
        }).to_csv(facility_csv, index=False)

        collateral_csv = tmp_dir / "collateral.csv"
        pd.DataFrame({
            "facility_id": ["F001", "F001", "F003"],
            "collateral_type": ["REAL_ESTATE", "EQUIPMENT", "INVENTORY"],
            "collateral_value": [500000.0, 100000.0, 200000.0],
        }).to_csv(collateral_csv, index=False)

        config_dict = {
            "entity": {"name": "Obligor", "version": "1.0"},
            "sources": [
                {"name": "obligor_main", "type": "csv", "primary": True,
                 "path": str(obligor_csv)},
                {"name": "facility_detail", "type": "csv",
                 "path": str(facility_csv)},
                {"name": "collateral", "type": "csv",
                 "path": str(collateral_csv)},
            ],
            "joins": [
                # Step 1: pre-join facility ↔ collateral (non-primary left)
                {
                    "left": "facility_detail", "right": "collateral",
                    "how": "left",
                    "on": [{"left_col": "facility_id", "right_col": "facility_id"}],
                    "relationship": "one_to_many",
                    "nested_as": "collateral_items",
                },
                # Step 2: nest enriched facilities under obligor
                {
                    "left": "obligor_main", "right": "facility_detail",
                    "how": "left",
                    "on": [{"left_col": "obligor_id", "right_col": "obligor_id"}],
                    "relationship": "one_to_many",
                    "nested_as": "facilities",
                },
            ],
            "fields": [
                {"name": "obligor_id", "source": "obligor_id", "dtype": "str",
                 "primary_key": True},
                {"name": "legal_name", "source": "legal_name", "dtype": "str"},
                {"name": "facilities", "source": "facilities", "dtype": "nested"},
                {"name": "facility_count",
                 "derived": "lambda row: len(row['facilities']) if not row['facilities'].empty else 0",
                 "dtype": "int64"},
            ],
        }

        config_path = write_yaml_config(config_dict, tmp_dir / "obligor.yaml")
        pipeline = EntityPipeline(config=config_path)
        result = pipeline.run()

        assert len(result.entities) == 2

        # O001 has 2 facilities
        o001 = next(e for e in result.entities if e.obligor_id == "O001")
        assert o001.facility_count == 2

        # O002 has 1 facility
        o002 = next(e for e in result.entities if e.obligor_id == "O002")
        assert o002.facility_count == 1

        # The nested facilities DataFrame should contain collateral_items column
        facilities_df = result.dataframe.loc[
            result.dataframe["obligor_id"] == "O001", "facilities"
        ].iloc[0]
        assert "collateral_items" in facilities_df.columns

    def test_join_order_matters(self, tmp_dir):
        """Without pre-join, collateral wouldn't be in the nested facilities."""
        left_csv = tmp_dir / "left.csv"
        pd.DataFrame({"id": ["A", "B"], "val": [1, 2]}).to_csv(left_csv, index=False)

        right_csv = tmp_dir / "right.csv"
        pd.DataFrame({"id": ["A", "A", "B"], "detail": ["x", "y", "z"]}).to_csv(right_csv, index=False)

        config_dict = {
            "entity": {"name": "Test", "version": "1.0"},
            "sources": [
                {"name": "main", "type": "csv", "primary": True, "path": str(left_csv)},
                {"name": "details", "type": "csv", "path": str(right_csv)},
            ],
            "joins": [
                {
                    "left": "main", "right": "details", "how": "left",
                    "on": [{"left_col": "id", "right_col": "id"}],
                    "relationship": "one_to_many", "nested_as": "detail_items",
                },
            ],
            "fields": [
                {"name": "id", "source": "id", "dtype": "str", "primary_key": True},
                {"name": "val", "source": "val", "dtype": "int64"},
                {"name": "detail_items", "source": "detail_items", "dtype": "nested"},
            ],
        }

        config_path = write_yaml_config(config_dict, tmp_dir / "test.yaml")
        result = EntityPipeline(config=config_path).run()

        assert len(result.entities) == 2
        # A has 2 detail items
        a_details = result.dataframe.loc[result.dataframe["id"] == "A", "detail_items"].iloc[0]
        assert len(a_details) == 2


class TestPipelineConfigPath:
    def test_config_path_stored(self, tmp_dir):
        config_dict = {
            "entity": {"name": "Test"},
            "sources": [{"name": "s", "type": "csv", "primary": True, "path": "/tmp/x.csv"}],
            "fields": [{"name": "x", "source": "x", "dtype": "str"}],
        }
        config_path = write_yaml_config(config_dict, tmp_dir / "test.yaml")
        pipeline = EntityPipeline(config=config_path)
        assert pipeline.config_path == config_path

    def test_config_path_none_when_config_object(self):
        config = EntityConfig(**{
            "entity": {"name": "Test"},
            "sources": [{"name": "s", "type": "csv", "primary": True, "path": "/tmp/x.csv"}],
            "fields": [{"name": "x", "source": "x", "dtype": "str"}],
        })
        pipeline = EntityPipeline(config=config)
        assert pipeline.config_path is None
