"""Built-in transformation implementations.

Includes: cast, rename, derive, fill_na, drop, filter, custom.
"""

from __future__ import annotations

import importlib
from typing import Any

import pandas as pd

from impact.common.exceptions import TransformError
from impact.common.logging import get_logger
from impact.entity.config.schema import TransformConfig
from impact.entity.transform.base import Transformer
from impact.entity.transform.registry import TransformRegistry

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Cast
# ---------------------------------------------------------------------------
@TransformRegistry.register("cast")
class CastTransformer(Transformer):
    """Cast columns to specified data types.

    Config example::

        type: cast
        columns:
          amount: float64
          date_col: datetime
    """

    _DTYPE_MAP: dict[str, Any] = {
        "str": "str",
        "string": "string",
        "int32": "int32",
        "int64": "int64",
        "float32": "float32",
        "float64": "float64",
        "bool": "bool",
        "datetime": "datetime64[ns]",
    }

    def apply(self, df: pd.DataFrame, config: TransformConfig) -> pd.DataFrame:
        if not config.columns:
            raise TransformError("CastTransformer requires 'columns' mapping")

        result = df.copy()
        for col, dtype_str in config.columns.items():
            if col not in result.columns:
                raise TransformError(f"Column '{col}' not found for casting")

            target_dtype = self._DTYPE_MAP.get(dtype_str, dtype_str)

            try:
                if target_dtype == "datetime64[ns]":
                    result[col] = pd.to_datetime(result[col])
                else:
                    result[col] = result[col].astype(target_dtype)
            except Exception as exc:
                raise TransformError(
                    f"Failed to cast column '{col}' to '{dtype_str}': {exc}"
                ) from exc

        logger.info("Cast %d columns", len(config.columns))
        return result


# ---------------------------------------------------------------------------
# Rename
# ---------------------------------------------------------------------------
@TransformRegistry.register("rename")
class RenameTransformer(Transformer):
    """Rename columns.

    Config example::

        type: rename
        mapping:
          old_name: new_name
    """

    def apply(self, df: pd.DataFrame, config: TransformConfig) -> pd.DataFrame:
        if not config.mapping:
            raise TransformError("RenameTransformer requires 'mapping'")

        missing = set(config.mapping.keys()) - set(df.columns)
        if missing:
            raise TransformError(f"Columns not found for renaming: {missing}")

        result = df.rename(columns=config.mapping)
        logger.info("Renamed %d columns", len(config.mapping))
        return result


# ---------------------------------------------------------------------------
# Derive (new computed column)
# ---------------------------------------------------------------------------
@TransformRegistry.register("derive")
class DeriveTransformer(Transformer):
    """Derive a new column from an expression.

    Config example::

        type: derive
        name: utilization_rate
        expression: "outstanding_balance / commitment_amount"
        dtype: float64
    """

    def apply(self, df: pd.DataFrame, config: TransformConfig) -> pd.DataFrame:
        if not config.name or not config.expression:
            raise TransformError("DeriveTransformer requires 'name' and 'expression'")

        result = df.copy()
        try:
            result[config.name] = result.eval(config.expression)
        except Exception as exc:
            raise TransformError(
                f"Failed to derive column '{config.name}' with expression "
                f"'{config.expression}': {exc}"
            ) from exc

        if config.dtype:
            cast_cfg = TransformConfig(type="cast", columns={config.name: config.dtype})
            result = CastTransformer().apply(result, cast_cfg)

        logger.info("Derived column '%s'", config.name)
        return result


# ---------------------------------------------------------------------------
# Fill NA
# ---------------------------------------------------------------------------
@TransformRegistry.register("fill_na")
class FillNaTransformer(Transformer):
    """Fill missing values with specified defaults.

    Config example::

        type: fill_na
        strategy:
          interest_rate: 0.0
          product_type: "UNKNOWN"
    """

    def apply(self, df: pd.DataFrame, config: TransformConfig) -> pd.DataFrame:
        if not config.strategy:
            raise TransformError("FillNaTransformer requires 'strategy'")

        result = df.copy()
        for col, fill_value in config.strategy.items():
            if col not in result.columns:
                raise TransformError(f"Column '{col}' not found for fill_na")
            result[col] = result[col].fillna(fill_value)

        logger.info("Filled NA in %d columns", len(config.strategy))
        return result


# ---------------------------------------------------------------------------
# Drop columns
# ---------------------------------------------------------------------------
@TransformRegistry.register("drop")
class DropTransformer(Transformer):
    """Drop specified columns.

    Config example::

        type: drop
        drop_columns: [temp_col1, temp_col2]
    """

    def apply(self, df: pd.DataFrame, config: TransformConfig) -> pd.DataFrame:
        if not config.drop_columns:
            raise TransformError("DropTransformer requires 'drop_columns'")

        missing = set(config.drop_columns) - set(df.columns)
        if missing:
            raise TransformError(f"Columns not found for dropping: {missing}")

        result = df.drop(columns=config.drop_columns)
        logger.info("Dropped %d columns", len(config.drop_columns))
        return result


# ---------------------------------------------------------------------------
# Filter rows
# ---------------------------------------------------------------------------
@TransformRegistry.register("filter")
class FilterTransformer(Transformer):
    """Filter rows based on an expression.

    Config example::

        type: filter
        condition: "amount > 0"
    """

    def apply(self, df: pd.DataFrame, config: TransformConfig) -> pd.DataFrame:
        if not config.condition:
            raise TransformError("FilterTransformer requires 'condition'")

        try:
            mask = df.eval(config.condition)
            result = df.loc[mask].reset_index(drop=True)
        except Exception as exc:
            raise TransformError(
                f"Failed to filter with condition '{config.condition}': {exc}"
            ) from exc

        logger.info(
            "Filter '%s': %d → %d rows", config.condition, len(df), len(result)
        )
        return result


# ---------------------------------------------------------------------------
# Custom (user-defined function)
# ---------------------------------------------------------------------------
@TransformRegistry.register("custom")
class CustomTransformer(Transformer):
    """Invoke a user-defined transformation function.

    The function must accept ``(df: pd.DataFrame, **kwargs)`` and return
    a ``pd.DataFrame``.

    Config example::

        type: custom
        function: "mypackage.transforms.compute_risk_weight"
        kwargs:
          model_version: "v2"
    """

    def apply(self, df: pd.DataFrame, config: TransformConfig) -> pd.DataFrame:
        if not config.function:
            raise TransformError("CustomTransformer requires 'function'")

        func = self._import_function(config.function)

        try:
            result = func(df, **config.kwargs)
        except Exception as exc:
            raise TransformError(
                f"Custom transform '{config.function}' failed: {exc}"
            ) from exc

        if not isinstance(result, pd.DataFrame):
            raise TransformError(
                f"Custom transform '{config.function}' must return a DataFrame, "
                f"got {type(result).__name__}"
            )

        logger.info("Applied custom transform '%s'", config.function)
        return result

    @staticmethod
    def _import_function(dotted_path: str):
        """Dynamically import a function from a dotted module path."""
        parts = dotted_path.rsplit(".", 1)
        if len(parts) != 2:
            raise TransformError(
                f"Invalid function path '{dotted_path}'. "
                "Expected format: 'module.path.function_name'"
            )
        module_path, func_name = parts
        try:
            module = importlib.import_module(module_path)
            return getattr(module, func_name)
        except (ImportError, AttributeError) as exc:
            raise TransformError(
                f"Cannot import function '{dotted_path}': {exc}"
            ) from exc
