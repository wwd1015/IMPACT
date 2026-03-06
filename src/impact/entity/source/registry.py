"""Connector registry — plugin system for data source connectors.

New connector types are registered via the ``@ConnectorRegistry.register``
decorator and retrieved at runtime by source type string.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from impact.common.exceptions import SourceError

if TYPE_CHECKING:
    from impact.entity.source.base import DataSourceConnector


class ConnectorRegistry:
    """Registry of available data source connectors.

    Usage::

        @ConnectorRegistry.register("snowflake")
        class SnowflakeConnector(DataSourceConnector):
            ...

        connector = ConnectorRegistry.get("snowflake")
    """

    _registry: dict[str, type[DataSourceConnector]] = {}

    @classmethod
    def register(cls, source_type: str):
        """Decorator to register a connector class for a given source type.

        Args:
            source_type: The type string used in YAML configs (e.g. "snowflake").
        """

        def wrapper(connector_cls: type[DataSourceConnector]):
            cls._registry[source_type] = connector_cls
            return connector_cls

        return wrapper

    @classmethod
    def get(cls, source_type: str) -> DataSourceConnector:
        """Instantiate and return a connector for the given source type.

        Args:
            source_type: The type string from the config.

        Returns:
            An instance of the registered connector.

        Raises:
            SourceError: If no connector is registered for the type.
        """
        connector_cls = cls._registry.get(source_type)
        if connector_cls is None:
            available = ", ".join(sorted(cls._registry.keys()))
            raise SourceError(
                f"No connector registered for source type '{source_type}'. "
                f"Available types: {available}"
            )
        return connector_cls()

    @classmethod
    def available_types(cls) -> list[str]:
        """Return a sorted list of all registered source types."""
        return sorted(cls._registry.keys())
