"""Parquet data source connector."""

from __future__ import annotations

import glob
from pathlib import Path

import pandas as pd

from impact.common.exceptions import SourceError
from impact.common.logging import get_logger
from impact.entity.config.schema import SourceConfig
from impact.entity.source.base import DataSourceConnector
from impact.entity.source.registry import ConnectorRegistry

logger = get_logger(__name__)


@ConnectorRegistry.register("parquet")
class ParquetConnector(DataSourceConnector):
    """Loads data from Parquet files using ``pyarrow``.

    Supports:
    - Single file paths
    - Glob patterns (e.g. ``data/*.parquet``)
    - Path parameter interpolation (e.g. ``data/{snapshot_date}/*.parquet``)
    """

    def load(self, config: SourceConfig) -> pd.DataFrame:
        if not config.path:
            raise SourceError(f"Source '{config.name}': missing 'path' for parquet type")

        # Interpolate parameters into path
        resolved_path = config.path.format(**config.parameters) if config.parameters else config.path

        logger.info("Loading parquet from: %s", resolved_path)

        # Handle glob patterns
        if any(c in resolved_path for c in ("*", "?", "[")):
            files = sorted(glob.glob(resolved_path, recursive=True))
            if not files:
                raise SourceError(
                    f"Source '{config.name}': no files matched pattern '{resolved_path}'"
                )
            logger.info("Matched %d parquet files", len(files))
            frames = [pd.read_parquet(f) for f in files]
            df = pd.concat(frames, ignore_index=True)
        else:
            path = Path(resolved_path)
            if not path.exists():
                raise SourceError(f"Source '{config.name}': file not found '{resolved_path}'")
            df = pd.read_parquet(path)

        logger.info("Loaded %d rows from parquet source '%s'", len(df), config.name)
        return df
