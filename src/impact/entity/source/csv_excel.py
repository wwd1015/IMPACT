"""CSV and Excel data source connectors."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from impact.common.exceptions import SourceError
from impact.common.logging import get_logger
from impact.entity.config.schema import SourceConfig
from impact.entity.source.base import DataSourceConnector
from impact.entity.source.registry import ConnectorRegistry

logger = get_logger(__name__)


@ConnectorRegistry.register("csv")
class CsvConnector(DataSourceConnector):
    """Loads data from CSV files.

    Supports all ``pandas.read_csv`` options via the ``options`` config block.
    """

    def load(self, config: SourceConfig, **kwargs) -> pd.DataFrame:
        if not config.path:
            raise SourceError(f"Source '{config.name}': missing 'path' for csv type")

        resolved_path = config.path.format(**config.parameters) if config.parameters else config.path
        path = Path(resolved_path)

        if not path.exists():
            raise SourceError(f"Source '{config.name}': file not found '{resolved_path}'")

        logger.info("Loading CSV from: %s", resolved_path)

        try:
            df = pd.read_csv(path, **config.options)
        except Exception as exc:
            raise SourceError(
                f"Failed to read CSV for source '{config.name}': {exc}"
            ) from exc

        logger.info("Loaded %d rows from CSV source '%s'", len(df), config.name)
        return df


@ConnectorRegistry.register("excel")
class ExcelConnector(DataSourceConnector):
    """Loads data from Excel files (.xlsx, .xls).

    Supports all ``pandas.read_excel`` options via the ``options`` config block.
    The ``openpyxl`` engine is used by default.
    """

    def load(self, config: SourceConfig, **kwargs) -> pd.DataFrame:
        if not config.path:
            raise SourceError(f"Source '{config.name}': missing 'path' for excel type")

        resolved_path = config.path.format(**config.parameters) if config.parameters else config.path
        path = Path(resolved_path)

        if not path.exists():
            raise SourceError(f"Source '{config.name}': file not found '{resolved_path}'")

        logger.info("Loading Excel from: %s", resolved_path)

        # Default to openpyxl engine if not specified
        options = dict(config.options)
        options.setdefault("engine", "openpyxl")

        try:
            df = pd.read_excel(path, **options)
        except Exception as exc:
            raise SourceError(
                f"Failed to read Excel for source '{config.name}': {exc}"
            ) from exc

        logger.info("Loaded %d rows from Excel source '%s'", len(df), config.name)
        return df
