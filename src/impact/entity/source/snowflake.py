"""Snowflake data source connector."""

from __future__ import annotations

import pandas as pd

from impact.common.exceptions import SourceError
from impact.common.logging import get_logger
from impact.entity.config.schema import SourceConfig
from impact.entity.source.base import DataSourceConnector
from impact.entity.source.registry import ConnectorRegistry

logger = get_logger(__name__)


@ConnectorRegistry.register("snowflake")
class SnowflakeConnector(DataSourceConnector):
    """Loads data from Snowflake using ``snowflake-connector-python``.

    Connection parameters are read from the ``connection`` block in the
    source config. Query parameters use ``{param}`` syntax — interpolated
    into the query string before execution.
    """

    def load(self, config: SourceConfig) -> pd.DataFrame:
        try:
            import snowflake.connector  # noqa: F811
        except ImportError as exc:
            raise SourceError(
                "Snowflake connector not installed. "
                "Install with: pip install impact[snowflake]"
            ) from exc

        if not config.connection:
            raise SourceError(f"Source '{config.name}': missing 'connection' for snowflake type")
        if not config.query:
            raise SourceError(f"Source '{config.name}': missing 'query' for snowflake type")

        conn_cfg = config.connection
        connect_kwargs: dict = {
            "account": conn_cfg.account,
            "database": conn_cfg.database,
            "schema": conn_cfg.schema_,
            "warehouse": conn_cfg.warehouse,
        }
        # Optional auth fields
        if conn_cfg.user:
            connect_kwargs["user"] = conn_cfg.user
        if conn_cfg.password:
            connect_kwargs["password"] = conn_cfg.password
        if conn_cfg.role:
            connect_kwargs["role"] = conn_cfg.role
        if conn_cfg.authenticator:
            connect_kwargs["authenticator"] = conn_cfg.authenticator

        logger.info(
            "Loading from Snowflake: account=%s, database=%s, schema=%s",
            conn_cfg.account,
            conn_cfg.database,
            conn_cfg.schema_,
        )

        try:
            conn = snowflake.connector.connect(**connect_kwargs)
            try:
                cursor = conn.cursor()
                params = config.parameters or {}
                resolved_query = config.query.format(**params) if params else config.query
                cursor.execute(resolved_query)
                df = cursor.fetch_pandas_all()
            finally:
                conn.close()
        except Exception as exc:
            raise SourceError(f"Snowflake query failed for source '{config.name}': {exc}") from exc

        logger.info("Loaded %d rows from Snowflake source '%s'", len(df), config.name)
        return df
