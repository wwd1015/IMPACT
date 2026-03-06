"""Dynamic entity class builder.

Creates Python dataclasses at runtime from the ``fields`` section of a YAML
config.  Each row in the processed DataFrame is converted into an instance
of the dynamically-created entity class.
"""

from __future__ import annotations

import dataclasses
from datetime import datetime
from typing import Any

import pandas as pd

from impact.common.exceptions import EntityBuildError
from impact.common.logging import get_logger
from impact.entity.config.schema import FieldConfig

logger = get_logger(__name__)

# Mapping from YAML dtype strings to Python types
DTYPE_MAP: dict[str, type] = {
    "str": str,
    "string": str,
    "int32": int,
    "int64": int,
    "float32": float,
    "float64": float,
    "bool": bool,
    "datetime": datetime,
    "nested": pd.DataFrame,
}


class EntityBuilder:
    """Dynamically creates entity classes and converts DataFrames to entity instances.

    Usage::

        builder = EntityBuilder()
        FacilityClass = builder.build_class("Facility", field_configs)
        entities = builder.to_entities(processed_df, FacilityClass)
    """

    def build_class(self, entity_name: str, fields: list[FieldConfig]) -> type:
        """Create a dataclass-based entity class from field configurations.

        Args:
            entity_name: Name for the generated class (e.g. ``"Facility"``).
            fields: List of field definitions from the YAML config.

        Returns:
            A new dataclass type with the specified fields.

        Raises:
            EntityBuildError: If the class cannot be created.
        """
        try:
            dc_fields: list[tuple[str, type, Any]] = []

            for f in fields:
                py_type = DTYPE_MAP.get(f.dtype)
                if py_type is None:
                    raise EntityBuildError(
                        f"Unknown dtype '{f.dtype}' for field '{f.name}'. "
                        f"Available: {list(DTYPE_MAP.keys())}"
                    )

                # Primary keys have no default; optional fields default to None
                if f.primary_key:
                    dc_fields.append((f.name, py_type, dataclasses.MISSING))
                else:
                    dc_fields.append(
                        (f.name, py_type, dataclasses.field(default=None))
                    )

            # Build the dataclass
            cls = dataclasses.make_dataclass(
                entity_name,
                [
                    (name, ann, default)
                    for name, ann, default in dc_fields
                ],
            )

            # Attach metadata to the class for introspection
            cls.__entity_fields__ = fields
            cls.__primary_key__ = [f.name for f in fields if f.primary_key]
            cls.__entity_name__ = entity_name

            logger.info(
                "Built entity class '%s' with %d fields (pk: %s)",
                entity_name,
                len(fields),
                cls.__primary_key__,
            )
            return cls

        except EntityBuildError:
            raise
        except Exception as exc:
            raise EntityBuildError(
                f"Failed to build entity class '{entity_name}': {exc}"
            ) from exc

    def to_entities(
        self,
        df: pd.DataFrame,
        entity_class: type,
        fields: list[FieldConfig] | None = None,
    ) -> list:
        """Convert each row of a DataFrame into an entity instance.

        Args:
            df: Processed DataFrame with columns matching the entity fields.
            entity_class: The dynamically-created entity class.
            fields: Optional field configs for column filtering. If not
                provided, uses ``entity_class.__entity_fields__``.

        Returns:
            List of entity instances (one per row).

        Raises:
            EntityBuildError: If conversion fails.
        """
        if fields is None:
            fields = getattr(entity_class, "__entity_fields__", [])

        field_names = [f.name for f in fields]

        # Only use columns that exist in the DataFrame
        available_cols = [c for c in field_names if c in df.columns]
        missing_cols = set(field_names) - set(available_cols)
        if missing_cols:
            logger.warning(
                "Entity '%s': columns missing from DataFrame: %s",
                getattr(entity_class, "__entity_name__", "Unknown"),
                missing_cols,
            )

        entities = []
        try:
            for _, row in df.iterrows():
                kwargs = {}
                for col in available_cols:
                    val = row[col]
                    # Convert numpy/pandas types to Python builtins where appropriate
                    if isinstance(val, pd.DataFrame):
                        kwargs[col] = val
                    elif pd.isna(val):
                        kwargs[col] = None
                    else:
                        kwargs[col] = _to_python_type(val)
                entities.append(entity_class(**kwargs))
        except Exception as exc:
            raise EntityBuildError(
                f"Failed to convert DataFrame rows to entity instances: {exc}"
            ) from exc

        logger.info("Created %d entity instances", len(entities))
        return entities


def _to_python_type(val: Any) -> Any:
    """Convert numpy/pandas scalar to native Python type."""
    import numpy as np

    if isinstance(val, (np.integer,)):
        return int(val)
    if isinstance(val, (np.floating,)):
        return float(val)
    if isinstance(val, (np.bool_,)):
        return bool(val)
    if isinstance(val, pd.Timestamp):
        return val.to_pydatetime()
    return val
