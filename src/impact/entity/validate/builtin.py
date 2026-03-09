"""Built-in validation rule implementations.

Includes: not_null, unique, range, expression, custom.
"""

from __future__ import annotations

import pandas as pd

from impact.common.logging import get_logger
from impact.common.utils import import_dotted_path
from impact.entity.config.schema import ValidationConfig
from impact.entity.validate.base import ValidationResult, Validator
from impact.entity.validate.registry import ValidatorRegistry

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Not Null
# ---------------------------------------------------------------------------
@ValidatorRegistry.register("not_null")
class NotNullValidator(Validator):
    """Checks that specified columns have no null values."""

    def validate(self, df: pd.DataFrame, config: ValidationConfig) -> ValidationResult:
        if not config.columns:
            return ValidationResult(
                rule_type="not_null",
                passed=True,
                severity=config.severity,
                message="not_null: no columns specified",
            )

        null_mask = df[config.columns].isnull().any(axis=1)
        failing = df.index[null_mask].tolist()

        return ValidationResult(
            rule_type="not_null",
            passed=len(failing) == 0,
            severity=config.severity,
            message=f"not_null check on columns {config.columns}",
            failing_row_count=len(failing),
            failing_indices=failing,
        )


# ---------------------------------------------------------------------------
# Unique
# ---------------------------------------------------------------------------
@ValidatorRegistry.register("unique")
class UniqueValidator(Validator):
    """Checks that specified columns form a unique key."""

    def validate(self, df: pd.DataFrame, config: ValidationConfig) -> ValidationResult:
        if not config.columns:
            return ValidationResult(
                rule_type="unique",
                passed=True,
                severity=config.severity,
                message="unique: no columns specified",
            )

        duplicated = df.duplicated(subset=config.columns, keep=False)
        failing = df.index[duplicated].tolist()

        return ValidationResult(
            rule_type="unique",
            passed=len(failing) == 0,
            severity=config.severity,
            message=f"unique check on columns {config.columns}",
            failing_row_count=len(failing),
            failing_indices=failing,
        )


# ---------------------------------------------------------------------------
# Range
# ---------------------------------------------------------------------------
@ValidatorRegistry.register("range")
class RangeValidator(Validator):
    """Checks that a column's values fall within a range.

    Supports inclusive ``[`` and exclusive ``(`` bounds:
    ``[0, 1]`` → 0 ≤ x ≤ 1, ``(0, 1)`` → 0 < x < 1, ``[0, 1)`` → 0 ≤ x < 1.
    """

    def validate(self, df: pd.DataFrame, config: ValidationConfig) -> ValidationResult:
        if not config.column:
            return ValidationResult(
                rule_type="range",
                passed=True,
                severity=config.severity,
                message="range: no column specified",
            )

        col = df[config.column]
        mask = pd.Series(True, index=df.index)

        if config.min is not None:
            mask &= col > config.min if config.min_exclusive else col >= config.min
        if config.max is not None:
            mask &= col < config.max if config.max_exclusive else col <= config.max

        failing = df.index[~mask].tolist()

        bounds = []
        if config.min is not None:
            op = ">" if config.min_exclusive else ">="
            bounds.append(f"{op} {config.min}")
        if config.max is not None:
            op = "<" if config.max_exclusive else "<="
            bounds.append(f"{op} {config.max}")
        bounds_str = " and ".join(bounds)

        return ValidationResult(
            rule_type="range",
            passed=len(failing) == 0,
            severity=config.severity,
            message=f"range check on '{config.column}': {bounds_str}",
            failing_row_count=len(failing),
            failing_indices=failing,
        )


# ---------------------------------------------------------------------------
# Expression
# ---------------------------------------------------------------------------
@ValidatorRegistry.register("expression")
class ExpressionValidator(Validator):
    """Checks that a boolean expression holds for all rows."""

    def validate(self, df: pd.DataFrame, config: ValidationConfig) -> ValidationResult:
        if not config.rule:
            return ValidationResult(
                rule_type="expression",
                passed=True,
                severity=config.severity,
                message="expression: no rule specified",
            )

        try:
            mask = df.eval(config.rule)
        except Exception as exc:
            return ValidationResult(
                rule_type="expression",
                passed=False,
                severity=config.severity,
                message=f"expression eval failed: {exc}",
            )

        failing = df.index[~mask].tolist()
        msg = config.message or f"expression: {config.rule}"

        return ValidationResult(
            rule_type="expression",
            passed=len(failing) == 0,
            severity=config.severity,
            message=msg,
            failing_row_count=len(failing),
            failing_indices=failing,
        )


# ---------------------------------------------------------------------------
# Custom
# ---------------------------------------------------------------------------
@ValidatorRegistry.register("custom")
class CustomValidator(Validator):
    """Invokes a user-defined validation function.

    The function must accept ``(df: pd.DataFrame, **kwargs)`` and return
    a ``ValidationResult``.
    """

    def validate(self, df: pd.DataFrame, config: ValidationConfig) -> ValidationResult:
        if not config.function:
            return ValidationResult(
                rule_type="custom",
                passed=True,
                severity=config.severity,
                message="custom: no function specified",
            )

        func = self._import_function(config.function)

        try:
            result = func(df, **config.kwargs)
        except Exception as exc:
            return ValidationResult(
                rule_type="custom",
                passed=False,
                severity=config.severity,
                message=f"custom validator '{config.function}' failed: {exc}",
            )

        if not isinstance(result, ValidationResult):
            return ValidationResult(
                rule_type="custom",
                passed=False,
                severity=config.severity,
                message=(
                    f"custom validator '{config.function}' must return ValidationResult, "
                    f"got {type(result).__name__}"
                ),
            )

        # Override severity from config
        result.severity = config.severity
        return result

    @staticmethod
    def _import_function(dotted_path: str):
        from impact.common.exceptions import ValidationError
        return import_dotted_path(dotted_path, error_class=ValidationError)
