"""Config merger — combines a primary (IMPACT standard) config with a custom override.

The primary config is always the base. The custom config is sparse — users only
define what's different. Merge happens at the raw YAML dict level, before
Pydantic parsing.

Merge semantics by section:

- ``entity``: custom overrides individual keys (name, version, description)
- ``parameters``: dict merge, custom wins on key conflict
- ``sources``: merge by ``name``; same name = custom replaces; new names added
- ``joins``: merge by ``(left, right)``; same pair = custom replaces; new pairs added
- ``pre_filters``: custom's pre_filters are appended to primary's
- ``post_filters``: custom's post_filters are appended to primary's
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
from typing import Any, Callable

from impact.common.exceptions import ConfigError
from impact.common.logging import get_logger
from impact.entity.config.parser import ConfigParser
from impact.entity.config.schema import EntityConfig

logger = get_logger(__name__)

_KNOWN_SECTIONS = {"entity", "expression_packages", "parameters", "connections", "sources", "joins", "pre_filters", "post_filters", "fields", "validations"}


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
    primary_raw = ConfigParser.load_yaml(Path(primary))
    custom_raw = ConfigParser.load_yaml(Path(custom))

    merged = merge_raw_configs(primary_raw, custom_raw)

    # Interpolate env vars on the merged result
    merged = ConfigParser.interpolate_env(merged)

    try:
        config = EntityConfig.model_validate(merged)
    except Exception as exc:
        raise ConfigError(f"Merged config validation failed: {exc}") from exc

    logger.info(
        "Merged config parsed: entity=%s, sources=%d, joins=%d, pre_filters=%d, post_filters=%d, fields=%d",
        config.entity.name,
        len(config.sources),
        len(config.joins or []),
        len(config.pre_filters or []),
        len(config.post_filters or []),
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
    # Warn on unknown sections in custom config
    unknown = set(custom) - _KNOWN_SECTIONS
    if unknown:
        logger.warning("  [merge] custom config has unknown sections (ignored): %s", sorted(unknown))

    merged: dict[str, Any] = {}

    # --- entity ---
    merged["entity"] = _merge_dict(
        primary.get("entity", {}),
        custom.get("entity", {}),
        section="entity",
    )

    # --- expression_packages ---
    merged["expression_packages"] = _merge_dict(
        primary.get("expression_packages", {}),
        custom.get("expression_packages", {}),
        section="expression_packages",
    )

    # --- parameters ---
    merged["parameters"] = _merge_dict(
        primary.get("parameters", {}),
        custom.get("parameters", {}),
        section="parameters",
    )

    # --- connections ---
    merged["connections"] = _merge_dict(
        primary.get("connections", {}),
        custom.get("connections", {}),
        section="connections",
    )

    # --- sources ---
    merged["sources"] = _merge_by_key(
        primary.get("sources", []),
        custom.get("sources", []),
        key="name",
        section="sources",
    )

    # --- joins ---
    merged["joins"] = _merge_by_key(
        primary.get("joins", []),
        custom.get("joins", []),
        key=lambda j: (j.get("left", ""), j.get("right", "")),
        section="joins",
    )

    # --- pre_filters ---
    merged["pre_filters"] = _merge_append(
        primary.get("pre_filters", []),
        custom.get("pre_filters", []),
        section="pre_filters",
    )

    # --- post_filters ---
    merged["post_filters"] = _merge_append(
        primary.get("post_filters", []),
        custom.get("post_filters", []),
        section="post_filters",
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
