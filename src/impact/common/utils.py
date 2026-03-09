"""Shared utility functions for the IMPACT platform."""

from __future__ import annotations

import importlib
from typing import Any

import pandas as pd

from impact.common.exceptions import ImpactError, TransformError
from impact.common.logging import get_logger

logger = get_logger(__name__)


def import_dotted_path(dotted_path: str, error_class: type[ImpactError] = ImpactError) -> Any:
    """Dynamically import a callable from a dotted module path.

    Args:
        dotted_path: ``"module.path.function_name"`` format.
        error_class: Exception class to raise on failure (default: ImpactError).

    Returns:
        The imported callable.

    Raises:
        error_class: If the path is malformed or the import fails.
    """
    parts = dotted_path.rsplit(".", 1)
    if len(parts) != 2:
        raise error_class(
            f"Invalid dotted path '{dotted_path}'. "
            "Expected format: 'module.path.callable_name'"
        )
    module_path, attr_name = parts
    try:
        module = importlib.import_module(module_path)
        return getattr(module, attr_name)
    except (ImportError, AttributeError) as exc:
        raise error_class(f"Cannot import '{dotted_path}': {exc}") from exc


def strip_source_prefixes(expr: str, source_names: set[str]) -> str:
    """Strip known source-name prefixes from an expression or column reference.

    Sorts source names longest-first so that ``"other_facility."`` is stripped
    before ``"facility."`` — avoiding partial substring matches.
    """
    for src in sorted(source_names, key=len, reverse=True):
        expr = expr.replace(f"{src}.", "")
    return expr


def cast_and_fill(
    df: pd.DataFrame,
    field_name: str,
    dtype: str,
    fill_na: Any,
    caster: Any,
    error_context: str = "",
) -> pd.DataFrame:
    """Apply dtype cast then fill_na for a single field.

    Args:
        df: DataFrame to modify.
        field_name: Column to cast/fill.
        dtype: Target dtype string (e.g. ``"float64"``). ``"nested"`` is skipped.
        fill_na: Scalar fill value, or ``None`` to skip.
        caster: A ``CastTransformer`` instance.
        error_context: Prefix for error messages (e.g. ``"Sub-entity 'X', "``).
    """
    from impact.entity.config.schema import TransformConfig

    if dtype != "nested" and field_name in df.columns:
        try:
            cast_cfg = TransformConfig(type="cast", columns={field_name: dtype})
            df = caster.apply(df, cast_cfg)
        except Exception as exc:
            raise TransformError(
                f"{error_context}field '{field_name}': cast to '{dtype}' failed: {exc}"
            ) from exc
    if fill_na is not None and field_name in df.columns:
        df[field_name] = df[field_name].fillna(fill_na)
    return df
