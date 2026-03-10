"""SQLite data source connector."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from impact.common.exceptions import SourceError
from impact.common.logging import get_logger
from impact.entity.config.schema import SourceConfig
from impact.entity.source.base import DataSourceConnector
from impact.entity.source.registry import ConnectorRegistry

logger = get_logger(__name__)


@ConnectorRegistry.register("sqlite")
class SqliteConnector(DataSourceConnector):
    """Loads data from a SQLite database file.

    Requires ``path`` (to the ``.db`` file) and ``query`` (SQL statement)
    in the source config. Query parameters are passed as bind variables.

    Example YAML config::

        sources:
          - name: facility_main
            type: sqlite
            primary: true
            path: "data/sample/lending.db"
            query: |
              SELECT facility_id, obligor_id, product_type,
                     commitment_amount, outstanding_balance,
                     origination_date, maturity_date, interest_rate
              FROM facility_main
              WHERE snapshot_date = :snapshot_date
    """

    def load(self, config: SourceConfig) -> pd.DataFrame:
        if not config.path:
            raise SourceError(f"Source '{config.name}': missing 'path' for sqlite type")
        if not config.query:
            raise SourceError(f"Source '{config.name}': missing 'query' for sqlite type")

        resolved_path = config.path.format(**config.parameters) if config.parameters else config.path
        path = Path(resolved_path)

        if not path.exists():
            raise SourceError(f"Source '{config.name}': database not found '{resolved_path}'")

        logger.info("Loading from SQLite: %s", resolved_path)

        try:
            conn = sqlite3.connect(str(path))
            try:
                df = pd.read_sql_query(config.query, conn, params=config.parameters or {})
            finally:
                conn.close()
        except Exception as exc:
            raise SourceError(
                f"SQLite query failed for source '{config.name}': {exc}"
            ) from exc

        logger.info("Loaded %d rows from SQLite source '%s'", len(df), config.name)
        return df
