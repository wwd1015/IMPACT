"""Transform registry — plugin system for transformation steps."""

from __future__ import annotations

from typing import TYPE_CHECKING

from impact.common.exceptions import TransformError

if TYPE_CHECKING:
    from impact.entity.transform.base import Transformer


class TransformRegistry:
    """Registry of available transformation types.

    Usage::

        @TransformRegistry.register("cast")
        class CastTransformer(Transformer):
            ...

        transformer = TransformRegistry.get("cast")
    """

    _registry: dict[str, type[Transformer]] = {}

    @classmethod
    def register(cls, transform_type: str):
        """Decorator to register a transformer class for a given type.

        Args:
            transform_type: The type string used in YAML configs.
        """

        def wrapper(transformer_cls: type[Transformer]):
            cls._registry[transform_type] = transformer_cls
            return transformer_cls

        return wrapper

    @classmethod
    def get(cls, transform_type: str) -> Transformer:
        """Instantiate and return a transformer for the given type.

        Raises:
            TransformError: If no transformer is registered for the type.
        """
        transformer_cls = cls._registry.get(transform_type)
        if transformer_cls is None:
            available = ", ".join(sorted(cls._registry.keys()))
            raise TransformError(
                f"No transformer registered for type '{transform_type}'. "
                f"Available: {available}"
            )
        return transformer_cls()

    @classmethod
    def available_types(cls) -> list[str]:
        """Return a sorted list of all registered transform types."""
        return sorted(cls._registry.keys())
