"""Config merger — combines a primary (IMPACT standard) config with custom overrides.

The primary config is always the base. Custom configs are sparse — users only
define what's different. Each custom config's fields occupy a named "space"
on the entity, isolated from primary fields and from each other.

Merge semantics by section:

- ``entity``: custom overrides individual keys (name, version, description)
- ``parameters``: dict merge, custom wins on key conflict
- ``expression_packages``: dict merge, custom wins on key conflict
- ``connections``: dict merge, custom wins on key conflict
- ``sources``: merge by ``name``; same name = custom replaces; new names added
- ``joins``: merge by ``(left, right)``; same pair = custom replaces; new pairs added
- ``pre_filters``: custom entries appended to primary
- ``post_filters``: custom entries appended to primary
- ``fields``: custom fields are tagged with their space name and appended.
  Field name overlap between primary and custom raises ``ConfigError``.
- ``validations``: custom entries appended to primary

Usage::

    from impact.entity.config.merger import merge_configs

    # Single custom config (space name auto-derived from filename)
    config = merge_configs("configs/facility.yaml", custom="custom_risk.yaml")

    # Single custom config with explicit space name
    config = merge_configs("configs/facility.yaml", custom={"risk": "custom_risk.yaml"})

    # Multiple custom configs
    config = merge_configs(
        "configs/facility.yaml",
        custom={"risk": "risk.yaml", "reporting": "report.yaml"},
    )

    # Multiple custom configs without explicit names (auto from filenames)
    config = merge_configs(
        "configs/facility.yaml",
        custom=["risk_fields.yaml", "report_fields.yaml"],
    )
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Literal

from impact.common.exceptions import ConfigError
from impact.common.logging import get_logger
from impact.entity.config.parser import ConfigParser
from impact.entity.config.schema import EntityConfig

logger = get_logger(__name__)

_KNOWN_SECTIONS = {"entity", "expression_packages", "parameters", "connections", "sources", "joins", "pre_filters", "post_filters", "fields", "validations"}


def merge_configs(
    primary: str | Path,
    custom: str | Path | list[str | Path] | dict[str, str | Path] | None = None,
    custom_filter_mode: Literal["filter", "flag"] = "filter",
) -> EntityConfig:
    """Merge a primary IMPACT config with one or more custom overrides.

    Each custom config's fields occupy a named "space" on the entity.
    Field names in custom configs must not overlap with primary field names.

    Args:
        primary: Path to the IMPACT standard config (the base).
        custom: Custom override config(s). Accepts:
            - ``str | Path`` — single config, space name = filename stem
            - ``list[str | Path]`` — multiple configs, each space name = filename stem
            - ``dict[str, str | Path]`` — explicit space names as keys
            - ``None`` — no custom configs (returns primary as-is)
        custom_filter_mode: How custom config filters are applied:
            - ``"filter"`` (default) — filters reduce the dataset.
            - ``"flag"`` — filters become row selectors; full dataset preserved,
              only matching rows get the custom space applied.

    Returns:
        A validated ``EntityConfig`` with merged content.

    Raises:
        ConfigError: If files cannot be read/parsed, field names overlap,
            or the merged result fails validation.
    """
    primary_raw = ConfigParser.load_yaml(Path(primary))

    # Normalize custom into dict[space_name, raw_config]
    custom_spaces = _normalize_custom_input(custom)

    if not custom_spaces:
        merged, space_selectors = merge_raw_configs(primary_raw, {})
    else:
        merged, space_selectors = merge_raw_configs(primary_raw, custom_spaces, custom_filter_mode=custom_filter_mode)

    # Interpolate env vars on the merged result
    merged = ConfigParser.interpolate_env(merged)

    try:
        config = EntityConfig.model_validate(merged)
    except Exception as exc:
        raise ConfigError(f"Merged config validation failed: {exc}") from exc

    # Store primary config path for sub-entity resolution
    config.config_path = Path(primary)

    # Store space selectors (flag mode filters) if any
    if space_selectors:
        config.space_selectors = space_selectors

    spaces = sorted({f.get("space") for f in merged.get("fields", []) if f.get("space")})
    logger.info(
        "Merged config parsed: entity=%s, sources=%d, joins=%d, pre_filters=%d, "
        "post_filters=%d, fields=%d (spaces: %s)",
        config.entity.name,
        len(config.sources),
        len(config.joins or []),
        len(config.pre_filters or []),
        len(config.post_filters or []),
        len(config.fields),
        spaces or ["primary only"],
    )
    return config


def _normalize_custom_input(
    custom: str | Path | list[str | Path] | dict[str, str | Path] | None,
) -> dict[str, dict[str, Any]]:
    """Convert custom input to a dict of space_name → raw YAML config."""
    if custom is None:
        return {}

    if isinstance(custom, dict):
        # Explicit space names
        return {
            name: ConfigParser.load_yaml(Path(path))
            for name, path in custom.items()
        }

    if isinstance(custom, (str, Path)):
        # Single path — auto name from filename stem
        path = Path(custom)
        space_name = path.stem
        return {space_name: ConfigParser.load_yaml(path)}

    if isinstance(custom, list):
        # List of paths — auto name from filename stems
        result: dict[str, dict[str, Any]] = {}
        for item in custom:
            path = Path(item)
            space_name = path.stem
            if space_name in result:
                raise ConfigError(
                    f"Duplicate space name '{space_name}' derived from filenames. "
                    f"Use a dict with explicit space names to disambiguate."
                )
            result[space_name] = ConfigParser.load_yaml(path)
        return result

    raise ConfigError(
        f"Invalid custom config type: {type(custom).__name__}. "
        f"Expected str, Path, list, or dict."
    )


def merge_raw_configs(
    primary: dict[str, Any],
    custom_spaces: dict[str, dict[str, Any]],
    custom_filter_mode: Literal["filter", "flag"] = "filter",
) -> dict[str, Any]:
    """Merge a primary config with multiple custom space configs.

    Custom fields are tagged with their space name and appended (never override
    primary fields). Field name overlap between primary and custom raises
    ``ConfigError``. Non-field sections are merged sequentially across all
    custom configs.

    Args:
        primary: Raw YAML dict for the primary config.
        custom_spaces: Mapping of space_name → raw YAML dict for each custom config.
            Pass an empty dict for no custom configs.
        custom_filter_mode: How custom config filters are applied:
            ``"filter"`` (default) — appended to primary filters.
            ``"flag"`` — stored as space selectors for selective application.

    Returns:
        A tuple of ``(merged_dict, space_selectors)`` where ``space_selectors``
        is a dict mapping space names to their selector expressions (empty dict
        when ``custom_filter_mode="filter"``). Inputs are not mutated.
    """
    if custom_filter_mode not in ("filter", "flag"):
        raise ConfigError(
            f"Invalid custom_filter_mode '{custom_filter_mode}'. "
            f"Expected 'filter' or 'flag'."
        )
    # Warn on unknown sections in custom configs
    for space_name, custom in custom_spaces.items():
        unknown = set(custom) - _KNOWN_SECTIONS
        if unknown:
            logger.warning(
                "  [merge] custom config '%s' has unknown sections (ignored): %s",
                space_name, sorted(unknown),
            )

    merged: dict[str, Any] = {}

    # --- entity --- (custom can override entity metadata like version)
    result_entity = dict(primary.get("entity", {}))
    for custom in custom_spaces.values():
        result_entity = _merge_dict(result_entity, custom.get("entity", {}), section="entity")
    merged["entity"] = result_entity

    # --- dict-merge sections: expression_packages, parameters, connections ---
    for section in ("expression_packages", "parameters", "connections"):
        result_section = dict(primary.get(section, {}))
        for custom in custom_spaces.values():
            result_section = _merge_dict(result_section, custom.get(section, {}), section=section)
        merged[section] = result_section

    # --- sources: merge by name across all custom configs ---
    result_sources = list(primary.get("sources", []))
    for custom in custom_spaces.values():
        result_sources = _merge_by_key(result_sources, custom.get("sources", []), key="name", section="sources")
    merged["sources"] = result_sources

    # --- joins: merge by (left, right) ---
    result_joins = list(primary.get("joins", []))
    for custom in custom_spaces.values():
        result_joins = _merge_by_key(
            result_joins, custom.get("joins", []),
            key=lambda j: (j.get("left", ""), j.get("right", "")),
            section="joins",
        )
    merged["joins"] = result_joins

    # --- append sections: pre_filters, post_filters, validations ---
    # In flag mode, custom filters become space selectors (not appended).
    space_selectors: dict[str, list[str]] = {}
    for section in ("pre_filters", "post_filters", "validations"):
        result_list = list(primary.get(section, []))
        for space_name, custom in custom_spaces.items():
            custom_entries = custom.get(section, [])
            if not custom_entries:
                continue
            if custom_filter_mode == "flag" and section in ("pre_filters", "post_filters"):
                # Flag mode: store filters as selectors, don't append
                space_selectors.setdefault(space_name, []).extend(custom_entries)
                logger.info(
                    "  [merge] %s: space '%s' (flag mode): %d entries stored as selectors",
                    section, space_name, len(custom_entries),
                )
            else:
                # Filter mode (default) or validations: append as before
                result_list = _merge_append(result_list, custom_entries, section=section)
        merged[section] = result_list

    # --- fields: tag custom fields with space, validate no overlap ---
    merged["fields"] = _merge_fields_with_spaces(
        primary.get("fields", []),
        {name: cfg.get("fields", []) for name, cfg in custom_spaces.items()},
    )

    return merged, space_selectors


def _merge_fields_with_spaces(
    primary_fields: list[dict[str, Any]],
    custom_field_spaces: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    """Merge primary fields with custom space fields.

    Custom fields are tagged with ``space`` and appended. Raises ``ConfigError``
    if any custom field name overlaps with a primary field name.

    Same field names across different custom spaces are allowed — each space
    is a separate layer. Transparent attribute access works for unique names;
    ambiguous names require explicit ``entity.spaces["space"]["field"]`` access.
    """
    # Copy primary fields (no space tag = primary)
    result = [dict(f) for f in primary_fields]
    primary_names = {f.get("name") for f in primary_fields if "name" in f}

    for space_name, fields in custom_field_spaces.items():
        if not fields:
            continue

        added: list[str] = []
        for field in fields:
            field_name = field.get("name")
            if not field_name:
                continue

            # Check overlap with primary fields
            if field_name in primary_names:
                raise ConfigError(
                    f"Custom space '{space_name}': field '{field_name}' already exists "
                    f"in the primary config. Use the primary field directly or choose "
                    f"a different name for the custom field."
                )

            tagged = {**field, "space": space_name}
            result.append(tagged)
            added.append(field_name)

        if added:
            logger.info("  [merge] fields: space '%s' added: %s", space_name, added)

    return result


# ---------------------------------------------------------------------------
# Section-specific merge logic
# ---------------------------------------------------------------------------


def _merge_dict(
    primary: dict[str, Any] | None, custom: dict[str, Any] | None,
    section: str,
) -> dict[str, Any]:
    """Merge two dicts — custom wins on key conflict (entity, parameters)."""
    p = primary or {}
    c = custom or {}
    if not c:
        return dict(p)
    result = {**p, **c}
    overridden = [k for k in c if k in p and c[k] != p.get(k)]
    added = [k for k in c if k not in p]
    if overridden:
        logger.info("  [merge] %s: overridden: %s", section, overridden)
    if added:
        logger.info("  [merge] %s: added: %s", section, added)
    return result


def _merge_by_key(
    primary: list[dict[str, Any]] | None,
    custom: list[dict[str, Any]] | None,
    key: str | Callable[[dict[str, Any]], Any],
    section: str,
) -> list[dict[str, Any]]:
    """Merge two lists of dicts by a key (string field name or callable extractor).

    Same key = custom replaces primary entry. New keys = appended.
    Preserves primary ordering; custom additions go at the end.
    """
    p = list(primary or [])
    c = list(custom or [])
    if not c:
        return [dict(item) for item in p]

    key_fn: Callable[[dict[str, Any]], Any] = (
        (lambda item, k=key: item.get(k)) if isinstance(key, str) else key
    )

    custom_map = {key_fn(item): item for item in c}
    overridden: list[str] = []
    added: list[str] = []

    # Start with primary, replacing where custom overrides
    result = []
    for item in p:
        item_key = key_fn(item)
        if item_key in custom_map:
            result.append(custom_map.pop(item_key))
            overridden.append(str(item_key))
        else:
            result.append(dict(item))

    # Append remaining custom entries (new additions)
    for item_key, item in custom_map.items():
        result.append(item)
        added.append(str(item_key))

    if overridden:
        logger.info("  [merge] %s: overridden: %s", section, overridden)
    if added:
        logger.info("  [merge] %s: added: %s", section, added)
    return result


def _merge_append(
    primary: list[Any] | None,
    custom: list[Any] | None,
    section: str,
) -> list[Any]:
    """Append custom entries to primary (filters, validations)."""
    p = list(primary or [])
    c = list(custom or [])
    if not c:
        return p
    logger.info("  [merge] %s: appended %d custom entries", section, len(c))
    return p + c
