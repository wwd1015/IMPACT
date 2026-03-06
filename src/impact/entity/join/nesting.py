"""Nesting utilities for one-to-many join results.

When a join has ``relationship: one_to_many``, the right-side rows are grouped
by the join key and packed into sub-DataFrames stored as cell values.  This
preserves the primary (left) table's row count.
"""

from __future__ import annotations

import pandas as pd


def nest_dataframe(
    merged: pd.DataFrame,
    join_keys_left: list[str],
    right_only_cols: list[str],
    nested_col_name: str,
    original_left: pd.DataFrame,
) -> pd.DataFrame:
    """Collapse one-to-many merged rows into nested sub-DataFrames.

    Args:
        merged: The merged DataFrame (left + right, with row inflation).
        join_keys_left: Left-side join key column names.
        right_only_cols: Column names that came from the right side only.
        nested_col_name: Name of the new column holding sub-DataFrames.
        original_left: The original left DataFrame before merge (row-count reference).

    Returns:
        DataFrame with the same row count as *original_left*, with a new column
        ``nested_col_name`` containing a ``pd.DataFrame`` for each group.
    """
    result = original_left.copy()
    empty_df = pd.DataFrame(columns=right_only_cols)

    if not right_only_cols:
        result[nested_col_name] = [empty_df.copy() for _ in range(len(result))]
        return result

    # Build a dict mapping join-key tuples to their sub-DataFrames
    nested_map: dict[tuple, pd.DataFrame] = {}

    if right_only_cols:
        # Only consider rows that actually have right-side data
        has_right_data = merged[right_only_cols].notna().any(axis=1)
        merged_with_data = merged.loc[has_right_data]

        if not merged_with_data.empty:
            for key_vals, group in merged_with_data.groupby(join_keys_left, sort=False):
                if not isinstance(key_vals, tuple):
                    key_vals = (key_vals,)
                sub_df = group[right_only_cols].reset_index(drop=True)
                nested_map[key_vals] = sub_df

    # Map nested DataFrames onto each row of the original left frame
    nested_series = []
    for _, row in result.iterrows():
        if len(join_keys_left) == 1:
            key = (row[join_keys_left[0]],)
        else:
            key = tuple(row[k] for k in join_keys_left)

        nested_series.append(nested_map.get(key, empty_df.copy()))

    result[nested_col_name] = nested_series
    return result
