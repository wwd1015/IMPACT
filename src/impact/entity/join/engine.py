"""Core join engine for combining multiple data sources.

Supports left, right, and inner joins with simple equality keys and complex
expression-based conditions.  For ``one_to_many`` relationships, the engine
uses nested DataFrames to preserve the primary table's row count.
"""

from __future__ import annotations

import pandas as pd

from impact.common.exceptions import JoinError
from impact.common.logging import get_logger
from impact.entity.config.schema import JoinConfig
from impact.entity.join.nesting import nest_dataframe

logger = get_logger(__name__)


class JoinEngine:
    """Executes join operations between DataFrames based on :class:`JoinConfig`.

    Usage::

        engine = JoinEngine()
        result = engine.execute(left_df, right_df, join_config)
    """

    def execute(
        self,
        left: pd.DataFrame,
        right: pd.DataFrame,
        config: JoinConfig,
    ) -> pd.DataFrame:
        """Execute a single join operation.

        Args:
            left: Left DataFrame (typically the primary source).
            right: Right DataFrame.
            config: Join configuration from YAML.

        Returns:
            Joined DataFrame with row count preserved for one-to-many joins.

        Raises:
            JoinError: If the join operation fails.
        """
        logger.info(
            "Joining '%s' ↔ '%s' (how=%s, relationship=%s)",
            config.left,
            config.right,
            config.how,
            config.relationship,
        )

        try:
            if config.relationship == "one_to_many":
                return self._nested_join(left, right, config)
            return self._flat_join(left, right, config)
        except JoinError:
            raise
        except Exception as exc:
            raise JoinError(
                f"Join '{config.left}' ↔ '{config.right}' failed: {exc}"
            ) from exc

    # ------------------------------------------------------------------
    # Flat join (one_to_one)
    # ------------------------------------------------------------------

    def _flat_join(
        self, left: pd.DataFrame, right: pd.DataFrame, config: JoinConfig
    ) -> pd.DataFrame:
        """Standard merge for one-to-one relationships."""
        # Separate simple keys from expression conditions
        simple_keys, expr_conditions = self._partition_conditions(config)

        if simple_keys and not expr_conditions:
            result = pd.merge(
                left,
                right,
                left_on=[k[0] for k in simple_keys],
                right_on=[k[1] for k in simple_keys],
                how=config.how,
                suffixes=("", "_right"),
            )
        else:
            result = self._conditional_join(left, right, config, simple_keys, expr_conditions)

        logger.info(
            "Flat join result: %d rows (left had %d, right had %d)",
            len(result),
            len(left),
            len(right),
        )
        return result

    # ------------------------------------------------------------------
    # Nested join (one_to_many)
    # ------------------------------------------------------------------

    def _nested_join(
        self, left: pd.DataFrame, right: pd.DataFrame, config: JoinConfig
    ) -> pd.DataFrame:
        """Join with nesting for one-to-many — preserves left row count."""
        simple_keys, expr_conditions = self._partition_conditions(config)

        # Perform the raw merge first
        if simple_keys and not expr_conditions:
            merged = pd.merge(
                left,
                right,
                left_on=[k[0] for k in simple_keys],
                right_on=[k[1] for k in simple_keys],
                how=config.how,
                suffixes=("", "_right"),
            )
        else:
            merged = self._conditional_join(left, right, config, simple_keys, expr_conditions)

        # Identify columns that came only from the right side
        left_cols_set = set(left.columns)
        right_only_cols = [c for c in merged.columns if c not in left_cols_set]

        # Remove duplicate key columns from right_only_cols
        right_key_cols = {k[1] for k in simple_keys if k[1] != k[0]}
        right_only_cols = [c for c in right_only_cols if c not in right_key_cols]

        left_key_cols = [k[0] for k in simple_keys]

        result = nest_dataframe(
            merged=merged,
            join_keys_left=left_key_cols,
            right_only_cols=right_only_cols,
            nested_col_name=config.nested_as,  # type: ignore[arg-type]
            original_left=left,
        )

        assert len(result) == len(left), (
            f"Nested join row count mismatch: expected {len(left)}, got {len(result)}"
        )

        logger.info(
            "Nested join result: %d rows preserved, nested column '%s'",
            len(result),
            config.nested_as,
        )
        return result

    # ------------------------------------------------------------------
    # Conditional join (complex expressions)
    # ------------------------------------------------------------------

    def _conditional_join(
        self,
        left: pd.DataFrame,
        right: pd.DataFrame,
        config: JoinConfig,
        simple_keys: list[tuple[str, str]],
        expr_conditions: list[str],
    ) -> pd.DataFrame:
        """Handle joins with expression-based conditions.

        Strategy: first merge on simple keys (or cross join if none), then
        filter by expression conditions.
        """
        if simple_keys:
            merged = pd.merge(
                left,
                right,
                left_on=[k[0] for k in simple_keys],
                right_on=[k[1] for k in simple_keys],
                how="inner",
                suffixes=("", "_right"),
            )
        else:
            # Cross join — add temporary key
            left_tmp = left.assign(_cross_key=1)
            right_tmp = right.assign(_cross_key=1)
            merged = pd.merge(left_tmp, right_tmp, on="_cross_key").drop(columns=["_cross_key"])

        # Apply expression filters
        for expr in expr_conditions:
            # Normalize left.col / right.col references to actual column names
            normalized = self._normalize_expression(expr, left.columns, right.columns)
            try:
                mask = merged.eval(normalized)
                merged = merged.loc[mask].reset_index(drop=True)
            except Exception as exc:
                raise JoinError(
                    f"Failed to evaluate join condition '{expr}': {exc}"
                ) from exc

        # For left/right joins, merge back to preserve unmatched rows
        if config.how == "left" and simple_keys:
            left_keys = [k[0] for k in simple_keys]
            merged = left.merge(
                merged.drop(columns=[c for c in left.columns if c not in left_keys], errors="ignore"),
                on=left_keys,
                how="left",
            )

        return merged

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _partition_conditions(
        config: JoinConfig,
    ) -> tuple[list[tuple[str, str]], list[str]]:
        """Split join conditions into simple key pairs and expression strings."""
        simple_keys: list[tuple[str, str]] = []
        expr_conditions: list[str] = []

        for cond in config.on:
            if cond.left_col and cond.right_col:
                simple_keys.append((cond.left_col, cond.right_col))
            if cond.condition:
                expr_conditions.append(cond.condition)

        return simple_keys, expr_conditions

    @staticmethod
    def _normalize_expression(
        expr: str,
        left_columns: pd.Index,
        right_columns: pd.Index,
    ) -> str:
        """Normalize ``left.col`` / ``right.col`` references for pd.eval.

        Replaces ``left.col_name`` with ``col_name`` and ``right.col_name``
        with ``col_name_right`` (matching the merge suffix convention).
        """
        normalized = expr
        # Replace "right.col" first to avoid partial matches with "left."
        for col in right_columns:
            normalized = normalized.replace(f"right.{col}", f"{col}_right")
        for col in left_columns:
            normalized = normalized.replace(f"left.{col}", col)
        return normalized
