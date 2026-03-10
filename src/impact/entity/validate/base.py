"""Abstract base class for validators and result models."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Literal

import pandas as pd

from impact.entity.config.schema import ValidationConfig


# ---------------------------------------------------------------------------
# Diagnostic configuration — controls how many failing-row samples to attach
# ---------------------------------------------------------------------------
_MAX_SAMPLES: int = 5


def set_max_samples(n: int) -> None:
    """Set the global max failing-row samples attached to validation results."""
    global _MAX_SAMPLES
    _MAX_SAMPLES = max(0, n)


def get_max_samples() -> int:
    return _MAX_SAMPLES


def collect_failing_samples(
    df: pd.DataFrame,
    failing_indices: list[int],
    columns: list[str] | None = None,
    max_samples: int | None = None,
) -> list[dict[str, Any]]:
    """Extract sample rows from a DataFrame at the given failing indices.

    Only called when validation fails — zero cost on the happy path.

    Args:
        df: Source DataFrame.
        failing_indices: Row indices that failed validation.
        columns: Columns to include in each sample dict. ``None`` = all columns.
        max_samples: Cap on samples returned (default: global ``_MAX_SAMPLES``).

    Returns:
        List of dicts, one per sample row.
    """
    if not failing_indices:
        return []
    cap = max_samples if max_samples is not None else _MAX_SAMPLES
    if cap <= 0:
        return []
    sample_idx = failing_indices[:cap]
    cols = columns if columns else list(df.columns)
    # Use .iloc for positional safety when index may be non-default
    rows = df.loc[sample_idx, cols]
    return rows.to_dict(orient="records")


@dataclass
class ValidationResult:
    """Result of a single validation rule.

    Attributes:
        rule_type: Validator name (e.g. ``"not_null"``, ``"range"``).
        passed: Whether the rule passed.
        severity: ``"error"`` halts the pipeline; ``"warning"`` logs and continues.
        message: Human-readable summary.
        failing_row_count: Total number of rows that failed.
        failing_indices: Row indices that failed (for programmatic access).
        field_name: The field this validation applies to (if field-level).
        context: Parent context string (e.g. ``"Sub-entity 'Collateral', parent 'FAC-001'"``).
        failing_samples: Up to N sample rows (dicts) showing the bad values.
    """

    rule_type: str
    passed: bool
    severity: Literal["error", "warning"]
    message: str
    failing_row_count: int = 0
    failing_indices: list[int] = field(default_factory=list)
    field_name: str | None = None
    context: str = ""
    failing_samples: list[dict[str, Any]] = field(default_factory=list)

    def format_detail(self, max_samples: int | None = None) -> str:
        """Format a human-readable detail string with sample failing rows.

        Returns a multi-line string showing the validation result and, if it
        failed, a table of sample rows with their bad values.
        """
        status = "PASS" if self.passed else self.severity.upper()
        prefix = f"[{status}] {self.message}"
        if self.context:
            prefix = f"[{status}] ({self.context}) {self.message}"

        if self.passed:
            return prefix

        lines = [f"{prefix} ({self.failing_row_count} failing rows)"]

        cap = max_samples if max_samples is not None else _MAX_SAMPLES
        samples = self.failing_samples[:cap]
        if samples:
            lines.append("  Sample failing rows:")
            for i, row in enumerate(samples):
                row_idx = self.failing_indices[i] if i < len(self.failing_indices) else "?"
                vals = ", ".join(f"{k}={v!r}" for k, v in row.items())
                lines.append(f"    row {row_idx}: {vals}")
            remaining = self.failing_row_count - len(samples)
            if remaining > 0:
                lines.append(f"    ... and {remaining} more rows")

        return "\n".join(lines)

    def __str__(self) -> str:
        status = "PASS" if self.passed else self.severity.upper()
        ctx = f"({self.context}) " if self.context else ""
        detail = f" ({self.failing_row_count} failing rows)" if not self.passed else ""
        return f"[{status}] {ctx}{self.message}{detail}"


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

    def format_detail(self) -> str:
        """Full diagnostic report with sample failing rows for each failed rule."""
        lines = [f"Validation Report ({len(self.results)} rules):"]
        for r in self.results:
            lines.append(f"  {r.format_detail()}")
        lines.append(
            f"Summary: {self.error_count} errors, {self.warning_count} warnings"
        )
        return "\n".join(lines)

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
