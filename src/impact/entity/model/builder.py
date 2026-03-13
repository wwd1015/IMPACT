"""Dynamic entity class builder.

Creates Python dataclasses at runtime from the ``fields`` section of a YAML
config.  Each row in the processed DataFrame is converted into an instance
of the dynamically-created entity class.

When fields have a ``space`` attribute (set by the config merger for custom
configs), primary fields become direct dataclass attributes and space fields
are stored in a ``spaces`` dict (plain dicts, not dataclasses).  Unique space
field names are accessible transparently via ``__getattr__``; ambiguous names
(present in multiple spaces) raise ``AttributeError`` directing the user to
``entity.spaces["space_name"]["field"]``.
"""

from __future__ import annotations

import dataclasses
from collections import defaultdict
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


def _resolve_dc_fields(
    fields: list[FieldConfig],
    sub_entity_classes: dict[str, type],
) -> list[tuple[str, type, Any]]:
    """Build dataclass field tuples from FieldConfig list.

    Shared by plain dataclass builder, spaced dataclass builder, and
    primary_only() reconstruction to avoid triplication.
    """
    dc_fields: list[tuple[str, type, Any]] = []
    for f in fields:
        if f.dtype == "nested" and f.name in sub_entity_classes:
            py_type = list
        else:
            py_type = DTYPE_MAP.get(f.dtype)
            if py_type is None:
                raise EntityBuildError(
                    f"Unknown dtype '{f.dtype}' for field '{f.name}'. "
                    f"Available: {list(DTYPE_MAP.keys())}"
                )
        if f.primary_key:
            dc_fields.append((f.name, py_type, dataclasses.MISSING))
        else:
            dc_fields.append(
                (f.name, py_type, dataclasses.field(default=None))
            )
    return dc_fields


def _convert_cell_value(val: Any) -> Any:
    """Convert a single DataFrame cell value to a Python-native type.

    Shared by plain and spaced entity conversion to avoid triplication.
    """
    if isinstance(val, (pd.DataFrame, list)):
        return val
    if pd.isna(val):
        return None
    return _to_python_type(val)


class EntityBuilder:
    """Dynamically creates entity classes and converts DataFrames to entity instances.

    Usage::

        builder = EntityBuilder()
        FacilityClass = builder.build_class("Facility", field_configs)
        entities = builder.to_entities(processed_df, FacilityClass)
    """

    def build_class(
        self,
        entity_name: str,
        fields: list[FieldConfig],
        sub_entity_classes: dict[str, type] | None = None,
    ) -> type:
        """Create an entity class from field configurations.

        When all fields are primary (``space=None``), returns a plain dataclass
        (backward compatible). When custom spaces exist, returns a dataclass
        with primary fields as direct attributes and a ``spaces`` dict holding
        space fields as plain dicts. Transparent ``__getattr__`` provides
        shorthand access for unique space field names.

        Args:
            entity_name: Name for the generated class (e.g. ``"Facility"``).
            fields: List of field definitions from the YAML config.
            sub_entity_classes: Mapping of field name -> sub-entity class.

        Returns:
            A new entity class with the specified fields.

        Raises:
            EntityBuildError: If the class cannot be created.
        """
        try:
            sub_classes = sub_entity_classes or {}

            # Partition fields by space
            primary_fields = [f for f in fields if f.space is None]
            space_fields: dict[str, list[FieldConfig]] = defaultdict(list)
            for f in fields:
                if f.space is not None:
                    space_fields[f.space].append(f)

            if not space_fields:
                # No custom spaces — return plain dataclass (backward compatible)
                dc_fields = _resolve_dc_fields(primary_fields, sub_classes)
                cls = dataclasses.make_dataclass(entity_name, dc_fields)
                cls.__entity_fields__ = fields
                cls.__primary_key__ = [f.name for f in primary_fields if f.primary_key]
                cls.__entity_name__ = entity_name
                logger.info(
                    "Built entity class '%s' with %d fields (pk: %s)",
                    entity_name, len(fields), cls.__primary_key__,
                )
                return cls

            # Build a dataclass with primary fields + spaces dict
            cls = _build_spaced_dataclass(
                entity_name, primary_fields, space_fields, fields, sub_classes,
            )

            logger.info(
                "Built entity class '%s' with %d fields (pk: %s, spaces: %s)",
                entity_name, len(fields),
                cls.__primary_key__,
                sorted(space_fields.keys()),
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

        # Check if this is a spaced entity class
        is_spaced = hasattr(entity_class, "__space_field_names__")

        entities = []
        try:
            if is_spaced:
                entities = self._to_spaced_entities(df, entity_class, fields, available_cols)
            else:
                entities = self._to_plain_entities(df, entity_class, available_cols)
        except EntityBuildError:
            raise
        except Exception as exc:
            raise EntityBuildError(
                f"Failed to convert DataFrame rows to entity instances: {exc}"
            ) from exc

        logger.info("Created %d entity instances", len(entities))
        return entities

    def _to_plain_entities(
        self, df: pd.DataFrame, entity_class: type, available_cols: list[str],
    ) -> list:
        """Convert rows to plain dataclass entities."""
        entities = []
        for _, row in df.iterrows():
            kwargs = {col: _convert_cell_value(row[col]) for col in available_cols}
            entities.append(entity_class(**kwargs))
        return entities

    def _to_spaced_entities(
        self, df: pd.DataFrame, entity_class: type,
        fields: list[FieldConfig], available_cols: list[str],
    ) -> list:
        """Convert rows to spaced entity instances (dataclass + spaces dict)."""
        # Pre-partition available columns by space (computed once, not per-row)
        primary_field_names = {f.name for f in fields if f.space is None}
        space_field_names: dict[str, set[str]] = defaultdict(set)
        for f in fields:
            if f.space is not None:
                space_field_names[f.space].add(f.name)

        primary_cols = [c for c in available_cols if c in primary_field_names]
        space_cols: dict[str, list[str]] = {
            sn: [c for c in available_cols if c in sf]
            for sn, sf in space_field_names.items()
        }

        entities = []
        for _, row in df.iterrows():
            primary_kwargs = {col: _convert_cell_value(row[col]) for col in primary_cols}

            # Build spaces dict, omitting spaces where all values are None
            # (flag mode: non-matching rows don't get the space)
            spaces: dict[str, dict[str, Any]] = {}
            for space_name, cols in space_cols.items():
                space_dict = {col: _convert_cell_value(row[col]) for col in cols}
                if any(v is not None for v in space_dict.values()):
                    spaces[space_name] = space_dict

            entities.append(entity_class(**primary_kwargs, spaces=spaces))

        return entities


def _build_spaced_dataclass(
    entity_name: str,
    primary_fields: list[FieldConfig],
    space_fields: dict[str, list[FieldConfig]],
    all_fields: list[FieldConfig],
    sub_entity_classes: dict[str, type],
) -> type:
    """Build a dataclass with primary fields as attributes and a ``spaces`` dict.

    Primary fields are direct dataclass attributes. Space fields are stored in
    ``self.spaces`` as ``{space_name: {field_name: value, ...}, ...}``.

    ``__getattr__`` provides transparent access for unique space field names.
    Ambiguous names (in multiple spaces) raise ``AttributeError`` with guidance.

    Methods:
        - ``primary_only()`` — returns a new entity with only primary fields
        - ``drop_space(name)`` — returns a new entity without the named space
        - ``select_space(name)`` — returns a new entity with only the named space
        - ``to_dict()`` — returns a flat dict of all fields (primary + all spaces)
    """
    # Build the primary dataclass fields + spaces dict field
    dc_fields = _resolve_dc_fields(primary_fields, sub_entity_classes)
    dc_fields.append(("spaces", dict, dataclasses.field(default_factory=dict, repr=False)))

    # Build the base dataclass
    base_cls = dataclasses.make_dataclass(entity_name, dc_fields)

    # Pre-compute the space field lookup: field_name → list of space names
    _space_field_lookup: dict[str, list[str]] = defaultdict(list)
    for f in all_fields:
        if f.space is not None:
            _space_field_lookup[f.name].append(f.space)

    _primary_field_names = {f.name for f in primary_fields}
    _entity_name = entity_name

    # Pre-compute space field names per space for metadata
    _space_field_names: dict[str, list[str]] = {
        space_name: [f.name for f in s_fields]
        for space_name, s_fields in space_fields.items()
    }

    # Store original primary fields for primary_only() reconstruction
    _primary_field_configs = primary_fields

    # Cache the plain primary class (built lazily on first primary_only() call)
    _cached_primary_cls: list[type | None] = [None]  # list for closure mutability

    def __getattr__(self, name: str) -> Any:
        # This is only called when normal attribute lookup fails,
        # so primary fields are already handled by the dataclass.

        # Check spaces dict
        spaces = object.__getattribute__(self, "spaces")

        if name in _space_field_lookup:
            candidate_spaces = _space_field_lookup[name]
            # Filter to spaces actually present on this instance
            present = [s for s in candidate_spaces if s in spaces and name in spaces[s]]
            if len(present) == 1:
                return spaces[present[0]][name]
            elif len(present) > 1:
                raise AttributeError(
                    f"'{_entity_name}' field '{name}' exists in multiple spaces: "
                    f"{sorted(present)}. Use entity.spaces[\"<space>\"][\"{name}\"]"
                )

        raise AttributeError(
            f"'{_entity_name}' has no field '{name}'. "
            f"Available: primary={sorted(_primary_field_names)}, "
            f"spaces={sorted(spaces.keys())}"
        )

    def __getitem__(self, key: str) -> Any:
        # Support "space_name.field_name" for explicit access
        if "." in key:
            space_name, field_name = key.split(".", 1)
            spaces = object.__getattribute__(self, "spaces")
            if space_name not in spaces:
                raise KeyError(
                    f"Space '{space_name}' not found. "
                    f"Available: {sorted(spaces.keys())}"
                )
            if field_name not in spaces[space_name]:
                raise KeyError(
                    f"Field '{field_name}' not found in space '{space_name}'. "
                    f"Available: {sorted(spaces[space_name].keys())}"
                )
            return spaces[space_name][field_name]

        # Try direct attribute first (covers primary fields)
        if key in _primary_field_names:
            return getattr(self, key)

        # Try transparent space access
        try:
            return self.__getattr__(key)
        except AttributeError as exc:
            raise KeyError(str(exc)) from exc

    def drop_space(self, name: str) -> Any:
        """Return a new entity without the named custom space (original unchanged)."""
        spaces = object.__getattribute__(self, "spaces")
        if name not in spaces:
            raise KeyError(
                f"Space '{name}' not found. Available: {sorted(spaces.keys())}"
            )
        new_spaces = {k: dict(v) for k, v in spaces.items() if k != name}
        # Copy primary field values
        kwargs = {f.name: getattr(self, f.name) for f in _primary_field_configs
                  if hasattr(self, f.name)}
        if new_spaces:
            kwargs["spaces"] = new_spaces
            return type(self)(**kwargs)
        else:
            # No spaces left — return a plain primary-only instance
            return _build_primary_instance(self)

    def select_space(self, name: str) -> Any:
        """Return a new entity with only the named space (original unchanged)."""
        spaces = object.__getattribute__(self, "spaces")
        if name not in spaces:
            raise KeyError(
                f"Space '{name}' not found. Available: {sorted(spaces.keys())}"
            )
        kwargs = {f.name: getattr(self, f.name) for f in _primary_field_configs
                  if hasattr(self, f.name)}
        kwargs["spaces"] = {name: dict(spaces[name])}
        return type(self)(**kwargs)

    def primary_only(self) -> Any:
        """Return a new plain dataclass entity with only primary fields (original unchanged)."""
        return _build_primary_instance(self)

    def _build_primary_instance(entity_instance) -> Any:
        """Build a plain dataclass with only primary fields (class is cached)."""
        if _cached_primary_cls[0] is None:
            plain_dc_fields = _resolve_dc_fields(_primary_field_configs, sub_entity_classes)
            plain_cls = dataclasses.make_dataclass(_entity_name, plain_dc_fields)
            plain_cls.__entity_fields__ = _primary_field_configs
            plain_cls.__primary_key__ = [f.name for f in _primary_field_configs if f.primary_key]
            plain_cls.__entity_name__ = _entity_name
            _cached_primary_cls[0] = plain_cls

        kwargs = {f.name: getattr(entity_instance, f.name)
                  for f in _primary_field_configs if hasattr(entity_instance, f.name)}
        return _cached_primary_cls[0](**kwargs)

    def to_dict(self) -> dict[str, Any]:
        """Convert entity to a flat dict including all spaces."""
        result = {}
        for f in _primary_field_configs:
            if hasattr(self, f.name):
                result[f.name] = getattr(self, f.name)
        spaces = object.__getattribute__(self, "spaces")
        for space_name, space_dict in spaces.items():
            result.update(space_dict)
        return result

    def __repr__(self) -> str:
        # Build primary part
        parts = []
        for f in _primary_field_configs:
            if hasattr(self, f.name):
                parts.append(f"{f.name}={getattr(self, f.name)!r}")
        spaces = object.__getattribute__(self, "spaces")
        if spaces:
            space_parts = []
            for sn in sorted(spaces):
                space_parts.append(f"{sn}={spaces[sn]}")
            parts.append(f"spaces={{{', '.join(space_parts)}}}")
        return f"{_entity_name}({', '.join(parts)})"

    # Patch the class with our methods
    base_cls.__getattr__ = __getattr__
    base_cls.__getitem__ = __getitem__
    base_cls.drop_space = drop_space
    base_cls.select_space = select_space
    base_cls.primary_only = primary_only
    base_cls.to_dict = to_dict
    base_cls.__repr__ = __repr__

    # Set class-level metadata
    base_cls.__entity_fields__ = all_fields
    base_cls.__primary_key__ = [f.name for f in primary_fields if f.primary_key]
    base_cls.__entity_name__ = entity_name
    base_cls.__space_field_names__ = _space_field_names

    return base_cls


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
