"""YAML config loader and parser.

Handles file loading, environment variable interpolation, and Pydantic validation.
"""

from __future__ import annotations

import os
import re
from calendar import monthrange
from datetime import date
from pathlib import Path
from typing import Any

import yaml

from impact.common.exceptions import ConfigError
from impact.common.logging import get_logger
from impact.entity.config.schema import EntityConfig

logger = get_logger(__name__)

# Pattern for ${ENV_VAR} or ${ENV_VAR:default_value}
_ENV_PATTERN = re.compile(r"\$\{(\w+)(?::([^}]*))?\}")


def _last_quarter_end() -> str:
    """Return the last calendar quarter-end date as YYYY-MM-DD."""
    today = date.today()
    # Quarter end months: 3 (Q1), 6 (Q2), 9 (Q3), 12 (Q4)
    quarter_end_month = ((today.month - 1) // 3) * 3  # last completed quarter's end month
    if quarter_end_month == 0:
        # We're in Q1 — last quarter end was Dec 31 of previous year
        return date(today.year - 1, 12, 31).strftime("%Y-%m-%d")
    _, last_day = monthrange(today.year, quarter_end_month)
    return date(today.year, quarter_end_month, last_day).strftime("%Y-%m-%d")


# Built-in computed expressions available as ${expression_name} in any config value.
# Resolution order: environment variable → built-in expression → original placeholder.
_BUILTIN_EXPRESSIONS: dict[str, Any] = {
    "last_quarter_end": _last_quarter_end,   # e.g. "2025-09-30"
}


class ConfigParser:
    """Parses and validates Entity Data Module YAML configuration files.

    Usage::

        parser = ConfigParser()
        config = parser.parse("configs/facility.yaml")
    """

    def parse(self, path: str | Path) -> EntityConfig:
        """Load, interpolate, and validate a YAML config file.

        Args:
            path: Path to the YAML configuration file.

        Returns:
            A validated ``EntityConfig`` instance.

        Raises:
            ConfigError: If the file cannot be read, parsed, or validated.
        """
        path = Path(path)
        if not path.exists():
            raise ConfigError(f"Config file not found: {path}")

        logger.info("Parsing config: %s", path)
        raw = self.load_yaml(path)
        raw = self.interpolate_env(raw)

        try:
            config = EntityConfig.model_validate(raw)
        except Exception as exc:
            raise ConfigError(f"Config validation failed: {exc}") from exc

        logger.info(
            "Config parsed successfully: entity=%s, sources=%d, joins=%d, filters=%d, validations=%d, fields=%d",
            config.entity.name,
            len(config.sources),
            len(config.joins or []),
            len(config.filters or []),
            len(config.validations or []),
            len(config.fields),
        )
        return config

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def load_yaml(path: Path) -> dict[str, Any]:
        """Load a YAML file using safe_load."""
        try:
            with path.open("r", encoding="utf-8") as fh:
                data = yaml.safe_load(fh)
        except yaml.YAMLError as exc:
            raise ConfigError(f"YAML parse error in {path}: {exc}") from exc

        if not isinstance(data, dict):
            raise ConfigError(f"Config root must be a mapping, got {type(data).__name__}")

        return data

    @classmethod
    def interpolate_env(cls, obj: Any) -> Any:
        """Recursively replace ``${VAR}`` / ``${VAR:default}`` with resolved values."""
        if isinstance(obj, str):
            return cls._replace_env_vars(obj)
        if isinstance(obj, dict):
            return {k: cls.interpolate_env(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [cls.interpolate_env(item) for item in obj]
        return obj

    @staticmethod
    def _replace_env_vars(value: str) -> str:
        """Replace all ``${VAR}`` patterns in a string.

        Resolution order for each ``${name}`` token:
        1. Built-in computed expression (e.g. ``last_quarter_end``) — evaluated fresh at parse time.
           Runtime override is done via ``pipeline.run(parameters=...)``, not env vars.
        2. Environment variable ``name`` — for infrastructure values like ``${SNOWFLAKE_ACCOUNT}``.
        3. Inline default after ``:`` (e.g. ``${SNOWFLAKE_WH:ANALYTICS_WH}``).
        4. Original placeholder left as-is — Pydantic will surface it if required.
        """

        def _replacer(match: re.Match) -> str:
            var_name = match.group(1)
            default = match.group(2)

            # 1. Built-in computed expression — evaluated fresh at parse time.
            #    Runtime override happens via pipeline.run(parameters=...), not here.
            builtin = _BUILTIN_EXPRESSIONS.get(var_name)
            if builtin is not None:
                return builtin() if callable(builtin) else str(builtin)

            # 2. Environment variable — for infrastructure values like ${SNOWFLAKE_ACCOUNT}
            env_val = os.environ.get(var_name)
            if env_val is not None:
                return env_val

            # 3. Inline default (e.g. ${SNOWFLAKE_WH:ANALYTICS_WH})
            if default is not None:
                return default

            # 4. Leave placeholder intact
            return match.group(0)

        return _ENV_PATTERN.sub(_replacer, value)
