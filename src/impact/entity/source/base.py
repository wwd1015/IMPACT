"""Abstract base class for data source connectors."""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd

from impact.entity.config.schema import SourceConfig


class DataSourceConnector(ABC):
    """Base class for all data source connectors.

    Subclasses must implement :meth:`load` to return a ``pd.DataFrame``
    from the configured source.
    """

    @abstractmethod
    def load(self, config: SourceConfig, **kwargs) -> pd.DataFrame:
        """Load data from the configured source.

        Args:
            config: Source configuration parsed from YAML.
            **kwargs: Optional connector-specific arguments (e.g. ``connection``
                for Snowflake to reuse a shared connection object).

        Returns:
            DataFrame containing the loaded data.

        Raises:
            SourceError: If the data cannot be loaded.
        """
        ...
