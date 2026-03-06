"""Abstract base class for DataFrame transformers."""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd

from impact.entity.config.schema import TransformConfig


class Transformer(ABC):
    """Base class for all transformation steps.

    Each transformer receives a DataFrame and a :class:`TransformConfig`,
    applies its logic, and returns a **new** DataFrame (input is never mutated).
    """

    @abstractmethod
    def apply(self, df: pd.DataFrame, config: TransformConfig) -> pd.DataFrame:
        """Apply the transformation.

        Args:
            df: Input DataFrame.
            config: Transformation configuration from YAML.

        Returns:
            Transformed DataFrame.

        Raises:
            TransformError: If the transformation fails.
        """
        ...
