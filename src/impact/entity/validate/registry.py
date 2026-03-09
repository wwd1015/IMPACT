"""Validator registry — plugin system for validation rules."""

from __future__ import annotations

from typing import TYPE_CHECKING

from impact.common.exceptions import ValidationError

if TYPE_CHECKING:
    from impact.entity.validate.base import Validator


class ValidatorRegistry:
    """Registry of available validation types."""

    _registry: dict[str, type[Validator]] = {}
    _instances: dict[str, Validator] = {}

    @classmethod
    def register(cls, validation_type: str):
        """Decorator to register a validator class for a given type."""

        def wrapper(validator_cls: type[Validator]):
            cls._registry[validation_type] = validator_cls
            cls._instances[validation_type] = validator_cls()
            return validator_cls

        return wrapper

    @classmethod
    def get(cls, validation_type: str) -> Validator:
        """Return the singleton validator instance for the given type.

        Raises:
            ValidationError: If no validator is registered for the type.
        """
        instance = cls._instances.get(validation_type)
        if instance is None:
            available = ", ".join(sorted(cls._registry.keys()))
            raise ValidationError(
                message=f"No validator registered for type '{validation_type}'. "
                f"Available: {available}"
            )
        return instance

    @classmethod
    def available_types(cls) -> list[str]:
        return sorted(cls._registry.keys())
