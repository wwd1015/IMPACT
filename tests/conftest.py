"""Shared pytest fixtures for the IMPACT test suite."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
import pytest
import yaml


@pytest.fixture
def tmp_dir():
    """Provide a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def sample_facility_df() -> pd.DataFrame:
    """Sample facility DataFrame for testing."""
    return pd.DataFrame(
        {
            "facility_id": ["F001", "F002", "F003"],
            "obligor_id": ["O001", "O001", "O002"],
            "product_type": ["TERM_LOAN", "REVOLVER", "TERM_LOAN"],
            "commitment_amount": [1000000.0, 500000.0, 2000000.0],
            "outstanding_balance": [750000.0, 200000.0, 1500000.0],
            "interest_rate": [0.05, 0.04, 0.06],
            "origination_date": pd.to_datetime(
                ["2023-01-15", "2023-06-01", "2024-01-10"]
            ),
            "maturity_date": pd.to_datetime(
                ["2028-01-15", "2026-06-01", "2029-01-10"]
            ),
        }
    )


@pytest.fixture
def sample_collateral_df() -> pd.DataFrame:
    """Sample collateral DataFrame (1-to-many with facility)."""
    return pd.DataFrame(
        {
            "facility_id": ["F001", "F001", "F002", "F003", "F003", "F003"],
            "collateral_type": [
                "REAL_ESTATE",
                "EQUIPMENT",
                "REAL_ESTATE",
                "INVENTORY",
                "RECEIVABLES",
                "EQUIPMENT",
            ],
            "collateral_value": [
                500000.0,
                100000.0,
                300000.0,
                200000.0,
                150000.0,
                80000.0,
            ],
        }
    )


@pytest.fixture
def sample_rating_df() -> pd.DataFrame:
    """Sample rating overrides DataFrame (1-to-1 with facility)."""
    return pd.DataFrame(
        {
            "facility_id": ["F001", "F003"],
            "rating_override": ["A+", "B-"],
        }
    )


@pytest.fixture
def minimal_config_dict() -> dict:
    """Minimal valid config dict for testing."""
    return {
        "entity": {"name": "TestEntity", "version": "1.0"},
        "sources": [
            {
                "name": "main_source",
                "type": "csv",
                "primary": True,
                "path": "/tmp/test.csv",
            }
        ],
        "fields": [
            {"name": "id", "source": "id", "dtype": "str", "primary_key": True},
            {"name": "value", "source": "value", "dtype": "float64"},
        ],
    }


@pytest.fixture
def full_config_dict(tmp_dir) -> dict:
    """Full config dict with joins, transforms, and validations for end-to-end testing."""
    # Create test CSV files
    facility_csv = tmp_dir / "facilities.csv"
    pd.DataFrame(
        {
            "facility_id": ["F001", "F002", "F003"],
            "obligor_id": ["O001", "O001", "O002"],
            "product_type": ["TERM_LOAN", "REVOLVER", "TERM_LOAN"],
            "commitment_amount": [1000000, 500000, 2000000],
            "outstanding_balance": [750000, 200000, 1500000],
            "interest_rate": [0.05, 0.04, 0.06],
        }
    ).to_csv(facility_csv, index=False)

    collateral_csv = tmp_dir / "collateral.csv"
    pd.DataFrame(
        {
            "facility_id": ["F001", "F001", "F002", "F003", "F003", "F003"],
            "collateral_type": [
                "REAL_ESTATE", "EQUIPMENT", "REAL_ESTATE",
                "INVENTORY", "RECEIVABLES", "EQUIPMENT",
            ],
            "collateral_value": [500000, 100000, 300000, 200000, 150000, 80000],
        }
    ).to_csv(collateral_csv, index=False)

    return {
        "entity": {"name": "Facility", "version": "1.0"},
        "sources": [
            {
                "name": "facility_main",
                "type": "csv",
                "primary": True,
                "path": str(facility_csv),
            },
            {
                "name": "collateral",
                "type": "csv",
                "path": str(collateral_csv),
            },
        ],
        "joins": [
            {
                "left": "facility_main",
                "right": "collateral",
                "how": "left",
                "on": [{"left_col": "facility_id", "right_col": "facility_id"}],
                "relationship": "one_to_many",
                "nested_as": "collateral_items",
            }
        ],
        "fields": [
            {
                "name": "facility_id",
                "source": "facility_id",
                "dtype": "str",
                "primary_key": True,
                "validation_type": ["not_null", "unique"],
                "validation_severity": {"not_null": "error", "unique": "error"},
            },
            {
                "name": "obligor_id",
                "source": "obligor_id",
                "dtype": "str",
                "validation_type": ["not_null"],
                "validation_severity": {"not_null": "error"},
            },
            {
                "name": "product_category",
                "source": "product_type",
                "dtype": "str",
            },
            {
                "name": "commitment_amount",
                "source": "commitment_amount",
                "dtype": "float64",
            },
            {
                "name": "outstanding_balance",
                "source": "outstanding_balance",
                "dtype": "float64",
            },
            {
                "name": "interest_rate",
                "source": "interest_rate",
                "dtype": "float64",
                "fill_na": 0.0,
                "validation_type": ["range"],
                "validation_rule": {"range": [0.0, 1.0]},
                "validation_severity": {"range": "warning"},
            },
            {
                "name": "collateral_items",
                "source": "collateral_items",
                "dtype": "nested",
            },
            {
                "name": "utilization_rate",
                "derived": "outstanding_balance / commitment_amount",
                "dtype": "float64",
            },
        ],
    }


def write_yaml_config(config_dict: dict, path: Path) -> Path:
    """Helper to write a config dict to a YAML file."""
    with open(path, "w") as f:
        yaml.dump(config_dict, f, default_flow_style=False)
    return path
