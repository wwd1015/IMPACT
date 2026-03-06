"""Abstract base class for validators and result models."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Literal

import pandas as pd

from impact.entity.config.schema import ValidationConfig


@dataclass
class ValidationResult:
    """Result of a single validation rule."""

    rule_type: str
    passed: bool
    severity: Literal["error", "warning"]
    message: str
    failing_row_count: int = 0
    failing_indices: list[int] = field(default_factory=list)

    def __str__(self) -> str:
        status = "PASS" if self.passed else self.severity.upper()
        detail = f" ({self.failing_row_count} failing rows)" if not self.passed else ""
        return f"[{status}] {self.message}{detail}"


@dataclass
class ValidationReport:
    """Aggregated report of all validation results for a pipeline run."""

    results: list[ValidationResult] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        """Whether any validation with severity=error failed."""
        return any(
            not r.passed and r.severity == "error" for r in self.results
        )

    @property
    def has_warnings(self) -> bool:
        """Whether any validation with severity=warning failed."""
        return any(
            not r.passed and r.severity == "warning" for r in self.results
        )

    @property
    def error_count(self) -> int:
        return sum(1 for r in self.results if not r.passed and r.severity == "error")

    @property
    def warning_count(self) -> int:
        return sum(1 for r in self.results if not r.passed and r.severity == "warning")

    def __str__(self) -> str:
        lines = [f"Validation Report ({len(self.results)} rules):"]
        for r in self.results:
            lines.append(f"  {r}")
        lines.append(
            f"Summary: {self.error_count} errors, {self.warning_count} warnings"
        )
        return "\n".join(lines)


class Validator(ABC):
    """Base class for all validation rules."""

    @abstractmethod
    def validate(self, df: pd.DataFrame, config: ValidationConfig) -> ValidationResult:
        """Run this validation rule against the DataFrame.

        Args:
            df: The DataFrame to validate.
            config: Validation configuration from YAML.

        Returns:
            A ValidationResult indicating pass/fail.
        """
        ...
