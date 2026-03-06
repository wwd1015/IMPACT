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
    def load(self, config: SourceConfig) -> pd.DataFrame:
        """Load data from the configured source.

        Args:
            config: Source configuration parsed from YAML.

        Returns:
            DataFrame containing the loaded data.

        Raises:
            SourceError: If the data cannot be loaded.
        """
        ...
