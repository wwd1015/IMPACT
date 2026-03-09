"""Shared utility functions for the IMPACT platform."""

from __future__ import annotations

import importlib
from typing import Any

from impact.common.exceptions import ImpactError


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
