"""Tests for data source connectors."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from impact.common.exceptions import SourceError
from impact.entity.config.schema import SourceConfig
from impact.entity.source.csv_excel import CsvConnector, ExcelConnector
from impact.entity.source.parquet import ParquetConnector
from impact.entity.source.registry import ConnectorRegistry


class TestConnectorRegistry:
    """Tests for the connector plugin registry."""

    def test_csv_registered(self):
        connector = ConnectorRegistry.get("csv")
        assert isinstance(connector, CsvConnector)

    def test_parquet_registered(self):
        connector = ConnectorRegistry.get("parquet")
        assert isinstance(connector, ParquetConnector)

    def test_unknown_type_raises(self):
        with pytest.raises(SourceError, match="No connector registered"):
            ConnectorRegistry.get("nonexistent")

    def test_available_types(self):
        types = ConnectorRegistry.available_types()
        assert "csv" in types
        assert "parquet" in types
        assert "excel" in types
        assert "snowflake" in types


class TestCsvConnector:
    """Tests for the CSV connector."""

    def test_load_csv(self, tmp_dir):
        csv_path = tmp_dir / "test.csv"
        pd.DataFrame({"a": [1, 2], "b": [3, 4]}).to_csv(csv_path, index=False)

        config = SourceConfig(name="test", type="csv", path=str(csv_path))
        df = CsvConnector().load(config)
        assert len(df) == 2
        assert list(df.columns) == ["a", "b"]

    def test_load_csv_with_options(self, tmp_dir):
        csv_path = tmp_dir / "test.tsv"
        pd.DataFrame({"a": [1, 2], "b": [3, 4]}).to_csv(
            csv_path, index=False, sep="\t"
        )

        config = SourceConfig(
            name="test", type="csv", path=str(csv_path), options={"delimiter": "\t"}
        )
        df = CsvConnector().load(config)
        assert len(df) == 2

    def test_load_csv_missing_file(self):
        config = SourceConfig(name="test", type="csv", path="/nonexistent/file.csv")
        with pytest.raises(SourceError, match="not found"):
            CsvConnector().load(config)

    def test_load_csv_missing_path(self):
        config = SourceConfig.model_validate(
            {"name": "test", "type": "parquet", "path": "/tmp/x.parquet"}
        )
        config.path = None
        with pytest.raises(SourceError, match="missing 'path'"):
            CsvConnector().load(config)


class TestParquetConnector:
    """Tests for the Parquet connector."""

    def test_load_parquet(self, tmp_dir):
        pq_path = tmp_dir / "test.parquet"
        pd.DataFrame({"x": [10, 20, 30]}).to_parquet(pq_path)

        config = SourceConfig(name="test", type="parquet", path=str(pq_path))
        df = ParquetConnector().load(config)
        assert len(df) == 3
        assert "x" in df.columns

    def test_load_parquet_glob(self, tmp_dir):
        for i in range(3):
            pq_path = tmp_dir / f"part_{i}.parquet"
            pd.DataFrame({"val": [i * 10]}).to_parquet(pq_path)

        config = SourceConfig(
            name="test", type="parquet", path=str(tmp_dir / "*.parquet")
        )
        df = ParquetConnector().load(config)
        assert len(df) == 3

    def test_load_parquet_with_parameter_interpolation(self, tmp_dir):
        sub_dir = tmp_dir / "2025"
        sub_dir.mkdir()
        pq_path = sub_dir / "data.parquet"
        pd.DataFrame({"v": [1]}).to_parquet(pq_path)

        config = SourceConfig(
            name="test",
            type="parquet",
            path=str(tmp_dir / "{year}" / "*.parquet"),
            parameters={"year": "2025"},
        )
        df = ParquetConnector().load(config)
        assert len(df) == 1

    def test_load_parquet_missing_file(self):
        config = SourceConfig(name="test", type="parquet", path="/nonexistent.parquet")
        with pytest.raises(SourceError, match="not found"):
            ParquetConnector().load(config)

    def test_load_parquet_no_glob_match(self, tmp_dir):
        config = SourceConfig(
            name="test", type="parquet", path=str(tmp_dir / "*.no_match")
        )
        with pytest.raises(SourceError, match="no files matched"):
            ParquetConnector().load(config)


class TestExcelConnector:
    """Tests for the Excel connector."""

    def test_load_excel(self, tmp_dir):
        xlsx_path = tmp_dir / "test.xlsx"
        pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]}).to_excel(
            xlsx_path, index=False
        )

        config = SourceConfig(name="test", type="excel", path=str(xlsx_path))
        df = ExcelConnector().load(config)
        assert len(df) == 2
        assert "col1" in df.columns

    def test_load_excel_missing_file(self):
        config = SourceConfig(name="test", type="excel", path="/nonexistent.xlsx")
        with pytest.raises(SourceError, match="not found"):
            ExcelConnector().load(config)
