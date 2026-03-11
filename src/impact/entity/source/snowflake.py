"""Snowflake data source connector."""

from __future__ import annotations

from typing import Any

import pandas as pd

from impact.common.exceptions import SourceError
from impact.common.logging import get_logger
from impact.entity.config.schema import SnowflakeConnectionConfig, SourceConfig
from impact.entity.source.base import DataSourceConnector
from impact.entity.source.registry import ConnectorRegistry

logger = get_logger(__name__)


def _build_connect_kwargs(conn_cfg: SnowflakeConnectionConfig) -> dict[str, Any]:
    """Build the kwargs dict for ``snowflake.connector.connect()``."""
    kwargs: dict[str, Any] = {
        "account": conn_cfg.account,
        "database": conn_cfg.database,
        "schema": conn_cfg.schema_,
        "warehouse": conn_cfg.warehouse,
    }
    if conn_cfg.user:
        kwargs["user"] = conn_cfg.user
    if conn_cfg.password:
        kwargs["password"] = conn_cfg.password
    if conn_cfg.role:
        kwargs["role"] = conn_cfg.role
    if conn_cfg.authenticator:
        kwargs["authenticator"] = conn_cfg.authenticator
    return kwargs


def _connection_key(conn_cfg: SnowflakeConnectionConfig) -> tuple:
    """Return a hashable key identifying a unique Snowflake connection config."""
    return (
        conn_cfg.account,
        conn_cfg.database,
        conn_cfg.schema_,
        conn_cfg.warehouse,
        conn_cfg.user,
        conn_cfg.role,
        conn_cfg.authenticator,
    )


def create_snowflake_connection(conn_cfg: SnowflakeConnectionConfig) -> Any:
    """Create a Snowflake connection from a connection config.

    Returns:
        A ``snowflake.connector.SnowflakeConnection`` object.

    Raises:
        SourceError: If the snowflake-connector-python package is not installed
            or the connection fails.
    """
    try:
        import snowflake.connector
    except ImportError as exc:
        raise SourceError(
            "Snowflake connector not installed. "
            "Install with: pip install impact[snowflake]"
        ) from exc

    kwargs = _build_connect_kwargs(conn_cfg)
    logger.info(
        "Connecting to Snowflake: account=%s, database=%s, schema=%s",
        conn_cfg.account,
        conn_cfg.database,
        conn_cfg.schema_,
    )
    try:
        return snowflake.connector.connect(**kwargs)
    except Exception as exc:
        raise SourceError(f"Snowflake connection failed: {exc}") from exc


@ConnectorRegistry.register("snowflake")
class SnowflakeConnector(DataSourceConnector):
    """Loads data from Snowflake using ``snowflake-connector-python``.

    Connection parameters are read from the ``connection`` block in the
    source config (inline or resolved from the top-level ``connections`` section).
    Query parameters use ``{param}`` syntax — interpolated into the query string
    before execution.

    When multiple sources share the same connection config, the pipeline
    creates a single connection and passes it via ``load(config, connection=conn)``.
    """

    def load(self, config: SourceConfig, connection: Any | None = None) -> pd.DataFrame:
        """Load data from Snowflake.

        Args:
            config: Source configuration with connection and query details.
            connection: Optional existing Snowflake connection to reuse.
                If not provided, a new connection is created and closed after use.
        """
        if not config.connection or isinstance(config.connection, str):
            raise SourceError(f"Source '{config.name}': missing or unresolved 'connection' for snowflake type")
        if not config.query:
            raise SourceError(f"Source '{config.name}': missing 'query' for snowflake type")

        conn_cfg = config.connection
        params = config.parameters or {}
        resolved_query = config.query.format(**params) if params else config.query

        # Use shared connection or create a new one
        own_connection = connection is None
        if own_connection:
            connection = create_snowflake_connection(conn_cfg)

        try:
            cursor = connection.cursor()
            cursor.execute(resolved_query)
            df = cursor.fetch_pandas_all()
        except Exception as exc:
            raise SourceError(f"Snowflake query failed for source '{config.name}': {exc}") from exc
        finally:
            if own_connection:
                connection.close()

        logger.info("Loaded %d rows from Snowflake source '%s'", len(df), config.name)
        return df
