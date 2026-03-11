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
from impact.common.utils import cast_and_fill, format_lambda_diagnostic, strip_source_prefixes
from impact.entity.config.parser import ConfigParser
from impact.entity.config.schema import EntityConfig, FieldConfig
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

        When a top-level config is reused as a sub-entity config, fields
        whose source columns don't exist in the nested DataFrame are
        automatically skipped (they belong to the parent pipeline context).

        Args:
            df: A single nested DataFrame (one cell from the parent entity).

        Returns:
            SubEntityResult with processed DataFrame, entity class, and instances.

        Raises:
            TransformError: If field processing fails.
            ValidationError: If validation fails with severity=error.
        """
        # Filter fields to only those resolvable from the nested DataFrame
        available_fields = self._resolve_available_fields(df)

        if df.empty:
            entity_fields = [f for f in available_fields if not f.temp]
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
        result = self._apply_source_fields(result, available_fields)

        # Source field validations
        report = self._run_field_validations(result, available_fields, pass_filter="source")
        if report.has_errors:
            raise ValidationError(report=report, message=report.format_detail())

        # Pass 2: derived fields (eval → cast → fill_na)
        result = self._apply_derived_fields(result, available_fields)

        # Derived field validations
        derived_report = self._run_field_validations(result, available_fields, pass_filter="derived")
        report.results.extend(derived_report.results)
        if report.has_errors:
            raise ValidationError(report=report, message=report.format_detail())

        # Recursive sub-entity processing (e.g. Facility → Collateral)
        sub_entity_classes = process_sub_entity_fields(
            available_fields, result, report, self.config_path
        )

        # Build entity class and instances
        entity_fields = [f for f in available_fields if not f.temp]
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

    def _resolve_available_fields(self, df: pd.DataFrame) -> list[FieldConfig]:
        """Filter config fields to those resolvable from the nested DataFrame.

        Source fields are kept if their resolved column exists in the DataFrame.
        Derived fields are kept if all referenced source fields are available.
        This allows a top-level pipeline config to be reused as a sub-entity
        config — fields from the parent context are automatically skipped.
        """
        if df.empty:
            return list(self.config.fields)

        available_cols = set(df.columns)
        source_prefixes = {s.name for s in self.config.sources}

        # Pass 1: determine which source fields are available
        available: list[FieldConfig] = []
        available_names: set[str] = set(available_cols)

        for field in self.config.fields:
            if field.source is not None:
                stripped = strip_source_prefixes(field.source, source_prefixes)
                # Column reference (identifier) — must exist in DataFrame
                if stripped.isidentifier():
                    col = field.name if stripped == field.name else stripped
                    if col in available_cols or field.name in available_cols:
                        available.append(field)
                        available_names.add(field.name)
                    else:
                        logger.debug(
                            "  Sub-entity '%s': skipping field '%s' (column '%s' not in nested DataFrame)",
                            self.config.entity.name, field.name, col,
                        )
                else:
                    # Expression — try to check if referenced columns exist
                    available.append(field)
                    available_names.add(field.name)
            elif field.derived is not None:
                # Derived fields are included; they'll fail at eval time if deps missing
                available.append(field)
                available_names.add(field.name)

        return available

    # ------------------------------------------------------------------
    # Field processing (mirrors EntityPipeline logic)
    # ------------------------------------------------------------------

    def _apply_source_fields(
        self, df: pd.DataFrame, fields: list[FieldConfig],
    ) -> pd.DataFrame:
        """Pass 1 — copy/expr → cast → fill_na for source fields."""
        result = df
        source_prefixes = {s.name for s in self.config.sources}
        err_ctx = f"Sub-entity '{self.config.entity.name}', "

        stripped_map = {
            f.name: strip_source_prefixes(f.source, source_prefixes)
            for f in fields if f.source is not None
        }

        # Copy columns (originals preserved for derived field access)
        for field in fields:
            if field.source is None:
                continue
            stripped = stripped_map[field.name]
            if stripped.isidentifier() and stripped != field.name and stripped in result.columns:
                result[field.name] = result[stripped]
                logger.info("  Field '%s': copied from '%s'", field.name, stripped)

        # Per field: expression eval → cast → fill_na
        for field in fields:
            if field.source is None:
                continue
            stripped = stripped_map[field.name]

            if not stripped.isidentifier():
                try:
                    result[field.name] = result.eval(stripped)
                except Exception as exc:
                    raise TransformError(
                        f"{err_ctx}field '{field.name}': source expression failed: {exc}"
                    ) from exc
            result = cast_and_fill(
                result, field.name, field.dtype, field.fill_na,
                self._caster, error_context=err_ctx,
            )

        return result

    def _apply_derived_fields(
        self, df: pd.DataFrame, fields: list[FieldConfig],
    ) -> pd.DataFrame:
        """Pass 2 — derived expressions after all source fields are clean."""
        result = df
        err_ctx = f"Sub-entity '{self.config.entity.name}', "
        for field in fields:
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
                diag, samples = "", []
                if expr.startswith("lambda"):
                    diag, samples = format_lambda_diagnostic(result, fn)
                raise TransformError(
                    f"{err_ctx}field '{field.name}': derived expression failed: {exc}.{diag}",
                    field=field.name,
                    failing_samples=samples,
                ) from exc
            result = cast_and_fill(
                result, field.name, field.dtype, field.fill_na,
                self._caster, error_context=err_ctx,
            )
        return result

    def _run_field_validations(
        self, df: pd.DataFrame, fields: list[FieldConfig],
        pass_filter: str | None = None,
    ) -> ValidationReport:
        """Run inline validation rules for fields matching pass_filter."""
        report = ValidationReport()
        for field in fields:
            for v_cfg in field.build_validation_configs(pass_filter=pass_filter):
                validator = ValidatorRegistry.get(v_cfg.type)
                result = validator.validate(df, v_cfg)
                report.results.append(result)
        return report


def process_sub_entity_fields(
    fields: list[FieldConfig],
    df: pd.DataFrame,
    report: ValidationReport,
    config_path: Path | None,
    parent_context: str = "",
) -> dict[str, type]:
    """Process nested fields with entity_ref through sub-entity pipelines.

    Shared by both ``EntityPipeline`` and ``SubEntityProcessor`` to avoid
    duplication. When a sub-entity cell fails, the error includes the parent
    row context (primary key values) so the user can locate the exact record.

    Args:
        fields: All field configs (filtered internally for nested + entity_ref).
        df: DataFrame whose nested columns will be replaced with entity lists.
        report: Parent validation report to merge sub-entity results into.
        config_path: Path to the parent config file (for sub-entity resolution).
        parent_context: Optional context prefix from a higher-level parent.

    Returns:
        Mapping of field name → sub-entity class.
    """
    sub_entity_classes: dict[str, type] = {}
    nested_fields = [
        f for f in fields
        if f.dtype == "nested" and f.entity_ref is not None
    ]
    if not nested_fields:
        return sub_entity_classes

    # Find primary key columns for parent context
    pk_cols = [f.name for f in fields if f.primary_key and f.name in df.columns]

    for field in nested_fields:
        logger.info("  Processing sub-entity '%s' for field '%s'", field.entity_ref, field.name)
        sub_config = resolve_sub_entity_config(field.entity_ref, config_path)
        processor = SubEntityProcessor(sub_config, config_path=config_path)

        entity_lists: list[list] = []
        for idx, cell in enumerate(df[field.name]):
            nested_df = cell if isinstance(cell, pd.DataFrame) else pd.DataFrame()

            try:
                result = processor.process(nested_df)
            except (TransformError, ValidationError) as exc:
                row_ctx = _build_row_context(df, idx, pk_cols, field.entity_ref, parent_context)
                new_exc = type(exc)(f"{row_ctx}: {exc}")
                if isinstance(exc, TransformError):
                    new_exc.field = exc.field
                    new_exc.failing_samples = exc.failing_samples
                elif isinstance(exc, ValidationError):
                    new_exc.report = exc.report
                raise new_exc from exc

            # Tag sub-entity validation results with parent context (only if any exist)
            if result.validation_report.results:
                row_ctx = _build_row_context(df, idx, pk_cols, field.entity_ref, parent_context)
                for v_result in result.validation_report.results:
                    v_result.context = row_ctx

            entity_lists.append(result.entities)
            report.results.extend(result.validation_report.results)
            if idx == 0:
                sub_entity_classes[field.name] = result.entity_class

        df[field.name] = entity_lists
        logger.info("  Sub-entity '%s': processed %d cells", field.entity_ref, len(entity_lists))

    return sub_entity_classes


def _build_row_context(
    df: pd.DataFrame, idx: int, pk_cols: list[str],
    entity_ref: str, parent_context: str,
) -> str:
    """Build a human-readable context string identifying a parent row.

    Example output: ``"Parent row 3 (facility_id='FAC-001'), sub-entity 'Collateral'"``
    """
    parts = []
    if parent_context:
        parts.append(parent_context)

    pk_vals = ""
    if pk_cols:
        kv = ", ".join(f"{c}={df.at[idx, c]!r}" for c in pk_cols if c in df.columns)
        pk_vals = f" ({kv})" if kv else ""

    parts.append(f"parent row {idx}{pk_vals}, sub-entity '{entity_ref}'")
    return " → ".join(parts)


def resolve_sub_entity_config(
    entity_ref: str, parent_config_path: Path | None
) -> EntityConfig:
    """Resolve and parse a sub-entity config by entity_ref name.

    Searches the parent config's directory for any YAML file matching
    ``{snake_case(entity_ref)}_*.yaml`` or ``{snake_case(entity_ref)}.yaml``.
    For example, ``entity_ref: Collateral`` matches ``collateral_demo.yaml``,
    ``collateral_example.yaml``, ``collateral.yaml``, etc.

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
    snake_name = _camel_to_snake(entity_ref)
    lower_name = entity_ref.lower()

    # Collect candidate files: exact match first, then glob
    prefixes = sorted({snake_name, lower_name})  # deduplicate if same
    candidate_paths: list[Path] = []
    for prefix in prefixes:
        exact = config_dir / f"{prefix}.yaml"
        if exact.exists():
            candidate_paths.append(exact)
        candidate_paths.extend(sorted(config_dir.glob(f"{prefix}_*.yaml")))

    if not candidate_paths:
        raise ConfigError(
            f"Sub-entity config for '{entity_ref}' not found in {config_dir}. "
            f"Expected a file matching '{snake_name}.yaml' or '{snake_name}_*.yaml'."
        )

    chosen = candidate_paths[0]
    logger.info("Resolved sub-entity '%s' config: %s", entity_ref, chosen)
    return ConfigParser().parse(chosen)


def _camel_to_snake(name: str) -> str:
    """Convert CamelCase to snake_case."""
    import re

    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    return s.lower()
