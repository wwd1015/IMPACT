"""Custom exceptions for the IMPACT platform."""

from __future__ import annotations


class ImpactError(Exception):
    """Base exception for all IMPACT errors."""


class ConfigError(ImpactError):
    """Raised when a configuration file is invalid or cannot be parsed."""


class SourceError(ImpactError):
    """Raised when a data source cannot be loaded."""


class JoinError(ImpactError):
    """Raised when a join operation fails."""


class TransformError(ImpactError):
    """Raised when a transformation step fails."""


class ValidationError(ImpactError):
    """Raised when data validation fails with severity=error."""

    def __init__(self, report: object | None = None, message: str = ""):
        self.report = report
        super().__init__(message or str(report))


class EntityBuildError(ImpactError):
    """Raised when dynamic entity class creation or instantiation fails."""
