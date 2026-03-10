"""Config merger — combines a primary (IMPACT standard) config with a custom override.

The primary config is always the base. The custom config is sparse — users only
define what's different. Merge happens at the raw YAML dict level, before
Pydantic parsing.

Merge semantics by section:

- ``entity``: custom overrides individual keys (name, version, description)
- ``parameters``: dict merge, custom wins on key conflict
- ``sources``: merge by ``name``; same name = custom replaces; new names added
- ``joins``: merge by ``(left, right)``; same pair = custom replaces; new pairs added
- ``filters``: custom's filters are appended to primary's
- ``fields``: merge by ``name``; same name = custom replaces entirely; new names added
- ``validations``: custom's validations are appended to primary's

Usage::

    from impact.entity.config.merger import merge_configs

    config = merge_configs(
        primary="configs/facility_example.yaml",
        custom="user_configs/facility_custom.yaml",
    )
    result = EntityPipeline(config=config).run()
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from impact.common.exceptions import ConfigError
from impact.common.logging import get_logger
from impact.entity.config.parser import ConfigParser
from impact.entity.config.schema import EntityConfig

logger = get_logger(__name__)


def merge_configs(
    primary: str | Path,
    custom: str | Path,
) -> EntityConfig:
    """Merge a primary IMPACT config with a custom override and return a parsed config.

    The primary config is always the base, regardless of argument order. The
    custom config is sparse — users only include sections and fields they want
    to change or add.

    Args:
        primary: Path to the IMPACT standard config (the base).
        custom: Path to the user's custom override config.

    Returns:
        A validated ``EntityConfig`` with merged content.

    Raises:
        ConfigError: If either file cannot be read, parsed, or the merged result
            fails validation.
    """
    parser = ConfigParser()
    primary_raw = parser._load_yaml(Path(primary))
    custom_raw = parser._load_yaml(Path(custom))

    merged = merge_raw_configs(primary_raw, custom_raw)

    # Interpolate env vars on the merged result
    merged = parser._interpolate_env(merged)

    try:
        config = EntityConfig.model_validate(merged)
    except Exception as exc:
        raise ConfigError(f"Merged config validation failed: {exc}") from exc

    logger.info(
        "Merged config parsed: entity=%s, sources=%d, joins=%d, filters=%d, fields=%d",
        config.entity.name,
        len(config.sources),
        len(config.joins or []),
        len(config.filters or []),
        len(config.fields),
    )
    return config


def merge_raw_configs(
    primary: dict[str, Any],
    custom: dict[str, Any],
) -> dict[str, Any]:
    """Merge two raw YAML config dicts (before Pydantic parsing).

    The primary dict is the base. The custom dict overrides or extends it.
    Returns a new dict — neither input is mutated.
    """
    merged: dict[str, Any] = {}

    # --- entity ---
    merged["entity"] = _merge_entity(
        primary.get("entity", {}),
        custom.get("entity", {}),
    )

    # --- parameters ---
    merged["parameters"] = _merge_parameters(
        primary.get("parameters", {}),
        custom.get("parameters", {}),
    )

    # --- sources ---
    merged["sources"] = _merge_by_key(
        primary.get("sources", []),
        custom.get("sources", []),
        key="name",
        section="sources",
    )

    # --- joins ---
    merged["joins"] = _merge_joins(
        primary.get("joins", []),
        custom.get("joins", []),
    )

    # --- filters ---
    merged["filters"] = _merge_append(
        primary.get("filters", []),
        custom.get("filters", []),
        section="filters",
    )

    # --- fields ---
    merged["fields"] = _merge_by_key(
        primary.get("fields", []),
        custom.get("fields", []),
        key="name",
        section="fields",
    )

    # --- validations ---
    merged["validations"] = _merge_append(
        primary.get("validations", []),
        custom.get("validations", []),
        section="validations",
    )

    return merged


# ---------------------------------------------------------------------------
# Section-specific merge logic
# ---------------------------------------------------------------------------


def _merge_entity(
    primary: dict[str, Any], custom: dict[str, Any],
) -> dict[str, Any]:
    """Merge entity metadata — custom overrides individual keys."""
    if not custom:
        return dict(primary)
    result = {**primary, **custom}
    overridden = [k for k in custom if k in primary and custom[k] != primary.get(k)]
    if overridden:
        logger.info("  [merge] entity: overridden keys: %s", overridden)
    return result


def _merge_parameters(
    primary: dict[str, Any] | None, custom: dict[str, Any] | None,
) -> dict[str, Any]:
    """Merge parameters — custom wins on key conflict."""
    p = primary or {}
    c = custom or {}
    if not c:
        return dict(p)
    result = {**p, **c}
    overridden = [k for k in c if k in p and c[k] != p.get(k)]
    added = [k for k in c if k not in p]
    if overridden:
        logger.info("  [merge] parameters: overridden: %s", overridden)
    if added:
        logger.info("  [merge] parameters: added: %s", added)
    return result


def _merge_by_key(
    primary: list[dict[str, Any]],
    custom: list[dict[str, Any]],
    key: str,
    section: str,
) -> list[dict[str, Any]]:
    """Merge two lists of dicts by a key field (e.g. 'name').

    Same key = custom replaces primary entry. New keys = appended.
    Preserves primary ordering; custom additions go at the end.
    """
    if not custom:
        return list(primary)

    custom_map = {item[key]: item for item in custom if key in item}
    overridden: list[str] = []
    added: list[str] = []

    # Start with primary, replacing where custom overrides
    result = []
    for item in primary:
        item_key = item.get(key)
        if item_key in custom_map:
            result.append(custom_map.pop(item_key))
            overridden.append(item_key)
        else:
            result.append(dict(item))

    # Append remaining custom entries (new additions)
    for item_key, item in custom_map.items():
        result.append(item)
        added.append(item_key)

    if overridden:
        logger.info("  [merge] %s: overridden: %s", section, overridden)
    if added:
        logger.info("  [merge] %s: added: %s", section, added)
    return result


def _merge_joins(
    primary: list[dict[str, Any]] | None,
    custom: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    """Merge joins by (left, right) key pair.

    Same pair = custom replaces. New pairs = appended after primary joins.
    """
    p = primary or []
    c = custom or []
    if not c:
        return list(p)

    def _join_key(j: dict) -> tuple[str, str]:
        return (j.get("left", ""), j.get("right", ""))

    custom_map = {_join_key(j): j for j in c}
    overridden: list[str] = []
    added: list[str] = []

    result = []
    for j in p:
        jk = _join_key(j)
        if jk in custom_map:
            result.append(custom_map.pop(jk))
            overridden.append(f"{jk[0]}↔{jk[1]}")
        else:
            result.append(dict(j))

    for jk, j in custom_map.items():
        result.append(j)
        added.append(f"{jk[0]}↔{jk[1]}")

    if overridden:
        logger.info("  [merge] joins: overridden: %s", overridden)
    if added:
        logger.info("  [merge] joins: added: %s", added)
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
