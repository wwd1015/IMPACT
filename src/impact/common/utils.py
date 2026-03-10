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

    On cast failure, runs a diagnostic pass to identify the specific rows and
    values that cannot be cast — included in the error message and on the
    ``TransformError.failing_samples`` attribute.

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
            bad_rows = diagnose_cast_failure(df, field_name, dtype)
            detail = ""
            if bad_rows:
                detail = " Bad values (first 5): " + ", ".join(
                    f"row {idx}: {val!r}" for idx, val in bad_rows
                )
            raise TransformError(
                f"{error_context}field '{field_name}': cast to '{dtype}' failed.{detail}",
                field=field_name,
                failing_samples=[{"row": idx, field_name: val} for idx, val in bad_rows],
            ) from exc
    if fill_na is not None and field_name in df.columns:
        df[field_name] = df[field_name].fillna(fill_na)
    return df


def diagnose_cast_failure(
    df: pd.DataFrame, field_name: str, dtype: str, max_samples: int = 5,
) -> list[tuple[int, Any]]:
    """Identify rows with values that cannot be cast to the target dtype.

    Uses vectorized coercion where possible (``pd.to_numeric``, ``pd.to_datetime``).
    Only called on the error path — zero cost when casts succeed.

    Returns:
        List of ``(row_index, bad_value)`` tuples, up to ``max_samples``.
    """
    if field_name not in df.columns:
        return []

    col = df[field_name]

    if dtype in ("float32", "float64", "int32", "int64"):
        coerced = pd.to_numeric(col, errors="coerce")
        bad_mask = coerced.isna() & col.notna()
    elif dtype == "datetime":
        coerced = pd.to_datetime(col, errors="coerce")
        bad_mask = coerced.isna() & col.notna()
    elif dtype in ("bool",):
        try:
            col.astype("bool")
            return []
        except (ValueError, TypeError):
            bad_mask = pd.Series(True, index=col.index)
    elif dtype in ("str", "string"):
        return []  # anything can be cast to string
    else:
        bad_mask = pd.Series(False, index=col.index)

    bad_indices = col.index[bad_mask].tolist()[:max_samples]
    return [(idx, col.loc[idx]) for idx in bad_indices]


def format_lambda_diagnostic(
    df: pd.DataFrame, fn: Any,
) -> tuple[str, list[dict[str, Any]]]:
    """Diagnose a failed row-wise lambda and return a formatted message + samples.

    Only called on the error path. Returns ``("", [])`` if no row-level
    failures can be identified.
    """
    failures = diagnose_lambda_failure(df, fn)
    if not failures:
        return "", []
    parts = [
        f"row {idx}: {err} (data: {data})"
        for idx, data, err in failures
    ]
    diag = " Failing rows: " + "; ".join(parts)
    samples = [{"row": idx, **data} for idx, data, _ in failures]
    return diag, samples


def diagnose_lambda_failure(
    df: pd.DataFrame, fn: Any, max_samples: int = 3,
) -> list[tuple[int, dict[str, Any], str]]:
    """Find the rows where a row-wise lambda fails.

    Iterates rows one-by-one (only called on the error path) and returns
    the first ``max_samples`` failures.

    Returns:
        List of ``(row_index, row_data_dict, error_message)`` tuples.
    """
    failures: list[tuple[int, dict[str, Any], str]] = []
    for idx in df.index:
        try:
            fn(df.loc[idx])
        except Exception as exc:
            row_data = df.loc[idx].to_dict()
            # Truncate row data to avoid enormous output
            truncated = {
                k: (repr(v)[:80] + "..." if len(repr(v)) > 80 else v)
                for k, v in row_data.items()
            }
            failures.append((idx, truncated, str(exc)))
            if len(failures) >= max_samples:
                break
    return failures
