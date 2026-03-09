"""Sub-entity processor for nested DataFrames.

When a field has ``dtype: nested`` and ``entity_ref`` set, the pipeline uses
this processor to validate, transform, and build sub-entity instances from
each nested DataFrame cell.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from impact.common.exceptions import TransformError, ValidationError
from impact.common.logging import get_logger
from impact.entity.config.parser import ConfigParser
from impact.entity.config.schema import EntityConfig, FieldConfig, TransformConfig
from impact.entity.model.builder import EntityBuilder
from impact.entity.transform.builtin import CastTransformer
from impact.entity.validate.base import ValidationReport
from impact.entity.validate.registry import ValidatorRegistry

logger = get_logger(__name__)


@dataclass
class SubEntityResult:
    """Result of processing a single nested DataFrame."""

    entity_class: type
    entities: list[Any]
    dataframe: pd.DataFrame
    validation_report: ValidationReport


class SubEntityProcessor:
    """Processes nested DataFrames against a sub-entity config.

    Applies the same field processing as the main pipeline (rename → cast →
    fill_na → validate → derived → build) but without sources, joins, or filters.

    Usage::

        processor = SubEntityProcessor(config)
        result = processor.process(nested_df)
    """

    def __init__(self, config: EntityConfig, config_path: Path | None = None):
        self.config = config
        self.config_path = config_path
        self._caster = CastTransformer()
        self._builder = EntityBuilder()

    def process(self, df: pd.DataFrame) -> SubEntityResult:
        """Process a nested DataFrame through the sub-entity pipeline.

        Args:
            df: A single nested DataFrame (one cell from the parent entity).

        Returns:
            SubEntityResult with processed DataFrame, entity class, and instances.

        Raises:
            TransformError: If field processing fails.
            ValidationError: If validation fails with severity=error.
        """
        if df.empty:
            entity_fields = [f for f in self.config.fields if not f.temp]
            entity_cls = self._builder.build_class(
                self.config.entity.name, entity_fields
            )
            return SubEntityResult(
                entity_class=entity_cls,
                entities=[],
                dataframe=df,
                validation_report=ValidationReport(),
            )

        result = df.copy()

        # Pass 1: source fields (rename → cast → fill_na)
        result = self._apply_source_fields(result)

        # Source field validations
        report = self._run_field_validations(result, pass_filter="source")
        if report.has_errors:
            raise ValidationError(report=report, message=str(report))

        # Pass 2: derived fields (eval → cast → fill_na)
        result = self._apply_derived_fields(result)

        # Derived field validations
        derived_report = self._run_field_validations(result, pass_filter="derived")
        report.results.extend(derived_report.results)
        if report.has_errors:
            raise ValidationError(report=report, message=str(report))

        # Recursive sub-entity processing (e.g. Facility → Collateral)
        sub_entity_classes = self._process_nested_sub_entities(result, report)

        # Build entity class and instances
        entity_fields = [f for f in self.config.fields if not f.temp]
        entity_cols = [f.name for f in entity_fields if f.name in result.columns]
        entity_df = result[entity_cols]

        entity_cls = self._builder.build_class(
            self.config.entity.name, entity_fields,
            sub_entity_classes=sub_entity_classes,
        )
        entities = self._builder.to_entities(entity_df, entity_cls)

        return SubEntityResult(
            entity_class=entity_cls,
            entities=entities,
            dataframe=entity_df,
            validation_report=report,
        )

    # ------------------------------------------------------------------
    # Field processing (mirrors EntityPipeline logic)
    # ------------------------------------------------------------------

    def _apply_source_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pass 1 — rename/expr → cast → fill_na for source fields.

        If the config defines sources (reused top-level config), their names are
        used for source-prefix stripping — same logic as the main pipeline.
        """
        result = df
        source_prefixes = {s.name for s in self.config.sources}

        # Batch renames
        rename_map: dict[str, str] = {}
        for field in self.config.fields:
            if field.source is None:
                continue
            stripped = self._strip_source_prefixes(field.source, source_prefixes)
            if stripped.isidentifier() and stripped != field.name and stripped in result.columns:
                rename_map[stripped] = field.name
        if rename_map:
            result = result.rename(columns=rename_map)

        # Per field: expression eval → cast → fill_na
        for field in self.config.fields:
            if field.source is None:
                continue
            stripped = self._strip_source_prefixes(field.source, source_prefixes)

            if not stripped.isidentifier():
                # Source expression
                try:
                    result[field.name] = result.eval(stripped)
                except Exception as exc:
                    raise TransformError(
                        f"Sub-entity '{self.config.entity.name}', field '{field.name}': "
                        f"source expression failed: {exc}"
                    ) from exc
            result = self._cast_and_fill(result, field)

        return result

    def _apply_derived_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pass 2 — derived expressions after all source fields are clean."""
        result = df
        for field in self.config.fields:
            if field.derived is None:
                continue
            expr = field.derived.strip()
            try:
                if expr.startswith("lambda"):
                    fn = eval(expr)  # noqa: S307
                    result[field.name] = result.apply(fn, axis=1)
                else:
                    result[field.name] = result.eval(expr)
            except Exception as exc:
                raise TransformError(
                    f"Sub-entity '{self.config.entity.name}', field '{field.name}': "
                    f"derived expression failed: {exc}"
                ) from exc
            result = self._cast_and_fill(result, field)
        return result

    def _cast_and_fill(self, df: pd.DataFrame, field: FieldConfig) -> pd.DataFrame:
        """Apply dtype cast then fill_na for a single field."""
        if field.dtype != "nested" and field.name in df.columns:
            try:
                cast_cfg = TransformConfig(type="cast", columns={field.name: field.dtype})
                df = self._caster.apply(df, cast_cfg)
            except Exception as exc:
                raise TransformError(
                    f"Sub-entity '{self.config.entity.name}', field '{field.name}': "
                    f"cast to '{field.dtype}' failed: {exc}"
                ) from exc
        if field.fill_na is not None and field.name in df.columns:
            df[field.name] = df[field.name].fillna(field.fill_na)
        return df

    @staticmethod
    def _strip_source_prefixes(expr: str, source_names: set[str]) -> str:
        """Strip known source-name prefixes, longest-first."""
        for src in sorted(source_names, key=len, reverse=True):
            expr = expr.replace(f"{src}.", "")
        return expr

    def _process_nested_sub_entities(
        self, df: pd.DataFrame, report: ValidationReport
    ) -> dict[str, type]:
        """Recursively process nested fields with entity_ref."""
        sub_entity_classes: dict[str, type] = {}
        nested_fields = [
            f for f in self.config.fields
            if f.dtype == "nested" and f.entity_ref is not None
        ]
        if not nested_fields:
            return sub_entity_classes

        for field in nested_fields:
            logger.info(
                "  Sub-entity '%s': processing nested '%s'",
                self.config.entity.name, field.entity_ref,
            )
            sub_config = resolve_sub_entity_config(field.entity_ref, self.config_path)
            processor = SubEntityProcessor(sub_config, config_path=self.config_path)

            entity_lists: list[list] = []
            for idx, cell in enumerate(df[field.name]):
                nested_df = cell if isinstance(cell, pd.DataFrame) else pd.DataFrame()
                result = processor.process(nested_df)
                entity_lists.append(result.entities)
                report.results.extend(result.validation_report.results)
                if idx == 0:
                    sub_entity_classes[field.name] = result.entity_class

            df[field.name] = entity_lists

        return sub_entity_classes

    def _run_field_validations(
        self, df: pd.DataFrame, pass_filter: str | None = None
    ) -> ValidationReport:
        """Run inline validation rules for fields matching pass_filter."""
        report = ValidationReport()
        for field in self.config.fields:
            for v_cfg in field.build_validation_configs(pass_filter=pass_filter):
                validator = ValidatorRegistry.get(v_cfg.type)
                result = validator.validate(df, v_cfg)
                report.results.append(result)
        return report


def resolve_sub_entity_config(
    entity_ref: str, parent_config_path: Path | None
) -> EntityConfig:
    """Resolve and parse a sub-entity config by entity_ref name.

    Looks for ``{snake_case(entity_ref)}_example.yaml`` or
    ``{snake_case(entity_ref)}.yaml`` in the same directory as the parent config.

    Args:
        entity_ref: The entity_ref value (e.g. ``"Collateral"``).
        parent_config_path: Path to the parent YAML config file.

    Returns:
        Parsed EntityConfig for the sub-entity.

    Raises:
        ConfigError: If the sub-entity config cannot be found or parsed.
    """
    from impact.common.exceptions import ConfigError

    if parent_config_path is None:
        raise ConfigError(
            f"Cannot resolve sub-entity '{entity_ref}': no parent config path. "
            "Use config_path= when creating the pipeline, or set entity_ref_path on the field."
        )

    config_dir = parent_config_path.parent

    # Convert CamelCase to snake_case
    snake_name = _camel_to_snake(entity_ref)

    # Try common naming conventions
    candidates = [
        config_dir / f"{snake_name}.yaml",
        config_dir / f"{snake_name}_example.yaml",
        config_dir / f"{entity_ref.lower()}.yaml",
        config_dir / f"{entity_ref.lower()}_example.yaml",
    ]

    for candidate in candidates:
        if candidate.exists():
            logger.info(
                "Resolved sub-entity '%s' config: %s", entity_ref, candidate
            )
            return ConfigParser().parse(candidate)

    raise ConfigError(
        f"Sub-entity config for '{entity_ref}' not found. "
        f"Searched: {[str(c) for c in candidates]}"
    )


def _camel_to_snake(name: str) -> str:
    """Convert CamelCase to snake_case."""
    import re

    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    return s.lower()
