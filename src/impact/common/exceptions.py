"""Custom exceptions for the IMPACT platform."""

from __future__ import annotations

from dataclasses import dataclass, field as dc_field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd


@dataclass
class DebugContext:
    """Debug context attached to exceptions when ``debug=True``.

    Captures the live pipeline state at the point of failure so users can
    inspect it interactively in notebooks or scripts.

    Attributes:
        dataframe: DataFrame snapshot at the point of failure.
        field: The field name being processed when the error occurred.
        expression: The expression or function path that failed.
        parameters: Effective parameters at the time of failure.
        stage: Pipeline stage name (e.g. 'source_fields', 'derived_fields').
    """

    dataframe: pd.DataFrame | None = None
    field: str | None = None
    expression: str | None = None
    parameters: dict[str, Any] = dc_field(default_factory=dict)
    stage: str | None = None


class ImpactError(Exception):
    """Base exception for all IMPACT errors."""


class ConfigError(ImpactError):
    """Raised when a configuration file is invalid or cannot be parsed."""


class SourceError(ImpactError):
    """Raised when a data source cannot be loaded."""


class JoinError(ImpactError):
    """Raised when a join operation fails."""


class TransformError(ImpactError):
    """Raised when a transformation step fails.

    Attributes:
        field: The field name that caused the failure (if known).
        failing_samples: Sample rows (dicts) showing the bad values.
        debug_context: Pipeline state at failure (populated when ``debug=True``).
    """

    def __init__(
        self,
        message: str = "",
        field: str | None = None,
        failing_samples: list[dict[str, Any]] | None = None,
        debug_context: DebugContext | None = None,
    ):
        self.field = field
        self.failing_samples = failing_samples or []
        self.debug_context = debug_context
        super().__init__(message)


class ValidationError(ImpactError):
    """Raised when data validation fails with severity=error."""

    def __init__(
        self,
        report: object | None = None,
        message: str = "",
        debug_context: DebugContext | None = None,
    ):
        self.report = report
        self.debug_context = debug_context
        super().__init__(message or str(report))


class EntityBuildError(ImpactError):
    """Raised when dynamic entity class creation or instantiation fails."""
