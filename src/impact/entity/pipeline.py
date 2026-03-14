"""Pipeline orchestrator — ties all Entity Data Module components together.

This is the main entry point for running an entity processing pipeline.
It loads the YAML config, executes all stages (load → join → transform →
filter → validate → build), and returns a structured result.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import pandas as pd

from impact.common.exceptions import ConfigError, DebugContext, SourceError, TransformError, ValidationError
from impact.common.logging import get_logger
from impact.common.utils import (
    build_expression_namespace,
    cast_and_fill,
    enhance_expression_error,
    import_dotted_path,
    normalize_lambda_at_params,
    format_lambda_diagnostic,
    strip_source_prefixes,
)
from impact.entity.config.merger import merge_configs
from impact.entity.config.parser import ConfigParser
from impact.entity.config.schema import DerivedFunctionRef, EntityConfig, SnowflakeConnectionConfig
from impact.entity.join.engine import JoinEngine
from impact.entity.model.builder import EntityBuilder
from impact.entity.sub_entity import process_sub_entity_fields
from impact.entity.transform.builtin import CastTransformer
from impact.entity.validate.base import ValidationReport

# Import builtins to trigger decorator registration
import impact.entity.source.csv_excel  # noqa: F401
import impact.entity.source.parquet  # noqa: F401
import impact.entity.source.snowflake  # noqa: F401
import impact.entity.source.sqlite  # noqa: F401
import impact.entity.transform.builtin  # noqa: F401
import impact.entity.validate.builtin  # noqa: F401

from impact.entity.source.registry import ConnectorRegistry
from impact.entity.validate.registry import ValidatorRegistry

logger = get_logger(__name__)

# Stage names used in DebugContext.stage and snapshot keys
STAGE_SOURCE_FIELDS = "source_fields"
STAGE_DERIVED_FIELDS = "derived_fields"
STAGE_PRE_FILTERS = "pre_filters"
STAGE_POST_FILTERS = "post_filters"
STAGE_SOURCE_VALIDATION = "source_validation"
STAGE_VALIDATION = "validation"

SNAP_AFTER_JOIN = "after_join"
SNAP_AFTER_PRE_FILTERS = "after_pre_filters"
SNAP_AFTER_SOURCE_FIELDS = "after_source_fields"
SNAP_AFTER_DERIVED_FIELDS = "after_derived_fields"
SNAP_AFTER_POST_FILTERS = "after_post_filters"
SNAP_AFTER_VALIDATION = "after_validation"
SNAP_FINAL = "final"


@dataclass
class PipelineResult:
    """Structured result of a pipeline run.

    Attributes:
        entity_class: The dynamically-created entity class.
        entities: List of entity instances (one per row).
        dataframe: The final processed DataFrame.
        validation_report: Aggregated validation results.
        metadata: Additional metadata about the pipeline run.
    """

    entity_class: type
    entities: list[Any]
    dataframe: pd.DataFrame
    validation_report: ValidationReport
    metadata: dict[str, Any]
    sub_entity_classes: dict[str, type] = dataclasses.field(default_factory=dict)
    snapshots: dict[str, pd.DataFrame] = dataclasses.field(default_factory=dict)


class EntityPipeline:
    """Orchestrates the full entity data processing pipeline.

    Stages:
        1. **Load** — Load data from all configured sources
        2. **Join** — Combine sources via configured joins
        3. **Transform** — Apply ordered transformation steps
        4. **Validate** — Run validation rules, halt on errors
        5. **Build** — Create dynamic entity class and instances

    Usage::

        pipeline = EntityPipeline("configs/facility.yaml")
        result = pipeline.run()

        # Or pass parameters to override config
        result = pipeline.run(parameters={"snapshot_date": "2025-06-30"})
    """

    def __init__(
        self,
        config: str | Path | EntityConfig | None = None,
        custom: str | Path | list[str | Path] | dict[str, str | Path] | None = None,
        sub_entity_custom: dict[str, str | Path | list[str | Path] | dict[str, str | Path]] | None = None,
        custom_filter_mode: Literal["filter", "flag"] = "filter",
    ):
        """Initialize the pipeline.

        Args:
            config: Path to a YAML config file, or a pre-parsed ``EntityConfig``.
            custom: Custom override config(s) for the parent entity. Merged with
                the primary config via ``merge_configs``. Accepts the same formats:
                ``str | Path`` (auto-named), ``list`` (auto-named each),
                ``dict[str, Path]`` (explicit space names).
                Ignored when ``config`` is already an ``EntityConfig``.
            sub_entity_custom: Custom override config(s) for sub-entities, keyed
                by ``entity_ref`` name. Each value uses the same format as
                ``custom``. Example::

                    sub_entity_custom={
                        "Collateral": {"risk": "collateral_risk.yaml"},
                    }
            custom_filter_mode: How custom config filters are applied:
                ``"filter"`` (default) — filters reduce the dataset.
                ``"flag"`` — filters become row selectors; full dataset preserved,
                only matching rows get the custom space applied.

        Raises:
            ConfigError: If config is not provided.
        """
        if config is None:
            raise ConfigError("config must be provided (path or EntityConfig)")

        if isinstance(config, EntityConfig):
            self.config = config
            self.config_path: Path | None = getattr(config, "config_path", None)
        elif isinstance(config, (str, Path)):
            config_path = Path(config)
            if custom is not None:
                self.config = merge_configs(
                    primary=config_path, custom=custom,
                    custom_filter_mode=custom_filter_mode,
                )
            else:
                self.config = ConfigParser().parse(config_path)
            self.config_path = config_path
        else:
            raise ConfigError(
                f"config must be a path (str/Path) or EntityConfig, got {type(config).__name__}"
            )

        self.sub_entity_custom = sub_entity_custom or {}

        self.join_engine = JoinEngine()
        self.entity_builder = EntityBuilder()
        self._caster = CastTransformer()
        self._expression_namespace = build_expression_namespace(self.config.expression_packages)

    def explain(self) -> str:
        """Show the execution plan without loading any data.

        Returns a formatted string describing every stage the pipeline will
        execute based on the current config: sources, joins, filters, fields
        (source vs derived), validations, and sub-entities.

        Usage::

            pipeline = EntityPipeline("configs/facility.yaml")
            print(pipeline.explain())
        """
        lines: list[str] = []
        cfg = self.config
        lines.append(f"Entity: {cfg.entity.name} (v{cfg.entity.version})")
        if cfg.entity.description:
            lines.append(f"  {cfg.entity.description}")
        lines.append("")

        # Parameters
        if cfg.parameters:
            lines.append("Parameters:")
            for k, v in cfg.parameters.items():
                lines.append(f"  {k}: {v!r}")
            lines.append("")

        # Sources
        lines.append(f"Sources ({len(cfg.sources)}):")
        for s in cfg.sources:
            tag = " [PRIMARY]" if s.primary else ""
            lines.append(f"  {s.name} ({s.type}){tag}")
        lines.append("")

        # Joins
        if cfg.joins:
            lines.append(f"Joins ({len(cfg.joins)}):")
            for j in cfg.joins:
                keys = []
                for cond in j.on:
                    if cond.condition:
                        keys.append(f"expr: {cond.condition}")
                    else:
                        keys.append(f"{cond.left_col} = {cond.right_col}")
                lines.append(f"  {j.left} ↔ {j.right} ({j.how}, {j.relationship})")
                for k in keys:
                    lines.append(f"    on: {k}")
            lines.append("")

        # Pre-filters
        if cfg.pre_filters:
            lines.append(f"Pre-filters ({len(cfg.pre_filters)}):")
            for f in cfg.pre_filters:
                lines.append(f"  {f}")
            lines.append("")

        # Fields
        source_fields = [f for f in cfg.fields if f.source is not None]
        derived_fields = [f for f in cfg.fields if f.derived is not None]
        temp_fields = [f for f in cfg.fields if f.temp]
        pk_fields = [f for f in cfg.fields if f.primary_key]
        nested_fields = [f for f in cfg.fields if f.entity_ref]

        lines.append(f"Fields ({len(cfg.fields)}):")
        lines.append(f"  Pass 1 — source fields ({len(source_fields)}):")
        for f in source_fields:
            extras = []
            if f.primary_key:
                extras.append("PK")
            if f.temp:
                extras.append("temp")
            if f.fill_na is not None:
                extras.append(f"fill_na={f.fill_na!r}")
            tag = f" [{', '.join(extras)}]" if extras else ""
            lines.append(f"    {f.name} ({f.dtype}): {f.source}{tag}")

        lines.append(f"  Pass 2 — derived fields ({len(derived_fields)}):")
        for f in derived_fields:
            extras = []
            if f.temp:
                extras.append("temp")
            if f.fill_na is not None:
                extras.append(f"fill_na={f.fill_na!r}")
            tag = f" [{', '.join(extras)}]" if extras else ""
            if isinstance(f.derived, DerivedFunctionRef):
                origin = f"fn:{f.derived.function}"
                if f.derived.kwargs:
                    origin += f" kwargs={f.derived.kwargs}"
            else:
                origin = f.derived
            lines.append(f"    {f.name} ({f.dtype}): {origin}{tag}")
        lines.append("")

        # Post-filters
        if cfg.post_filters:
            lines.append(f"Post-filters ({len(cfg.post_filters)}):")
            for f in cfg.post_filters:
                lines.append(f"  {f}")
            lines.append("")

        # Validations
        val_count = sum(len(f.validation_type or []) for f in cfg.fields)
        global_count = len(cfg.validations or [])
        if val_count or global_count:
            lines.append(f"Validations ({val_count} field-level, {global_count} global):")
            for f in cfg.fields:
                for v in (f.validation_type or []):
                    sev = (f.validation_severity or {}).get(v, "warning")
                    lines.append(f"  {f.name}: {v} (severity={sev})")
            for v in (cfg.validations or []):
                lines.append(f"  [global] {v.type} (severity={v.severity})")
            lines.append("")

        # Sub-entities
        if nested_fields:
            lines.append(f"Sub-entities ({len(nested_fields)}):")
            for f in nested_fields:
                ref_cfg = f.entity_ref_config or "(auto-resolved)"
                lines.append(f"  {f.name} → {f.entity_ref} (config: {ref_cfg})")
            lines.append("")

        # Summary
        lines.append("Execution order:")
        steps = ["Load sources"]
        if cfg.joins:
            steps.append("Execute joins")
        if cfg.pre_filters:
            steps.append("Apply pre-filters")
        steps.append(f"Pass 1: {len(source_fields)} source fields")
        steps.append("Validate source fields")
        steps.append(f"Pass 2: {len(derived_fields)} derived fields")
        if cfg.post_filters:
            steps.append("Apply post-filters")
        steps.append("Validate derived + global")
        if nested_fields:
            steps.append(f"Process {len(nested_fields)} sub-entities")
        steps.append(f"Build {cfg.entity.name} class ({len(cfg.fields) - len(temp_fields)} fields)")
        for i, step in enumerate(steps, 1):
            lines.append(f"  {i}. {step}")

        return "\n".join(lines)

    def run(self, parameters: dict[str, Any] | None = None, debug: bool = False) -> PipelineResult:
        """Execute the full pipeline.

        Args:
            parameters: Runtime parameters injected by the orchestrator. Merged with
                the config's global ``parameters`` block using the priority:
                runtime > source-level > global config default.
            debug: When ``True``, saves DataFrame snapshots at each stage boundary
                on ``result.snapshots`` and attaches ``DebugContext`` to any exception.

        Returns:
            PipelineResult containing entity class, instances, DataFrame, and report.

        Raises:
            SourceError: If data loading fails.
            JoinError: If joining fails.
            TransformError: If a transformation fails (has ``.debug_context`` when debug=True).
            ValidationError: If validation fails with severity=error (has ``.debug_context`` when debug=True).
        """
        entity_name = self.config.entity.name
        logger.info("=" * 60)
        logger.info("Starting pipeline for entity: %s", entity_name)
        logger.info("=" * 60)

        snapshots: dict[str, pd.DataFrame] = {}

        def _snap(key: str, df: pd.DataFrame) -> None:
            """Save a DataFrame snapshot if debug mode is active."""
            if debug:
                snapshots[key] = df.copy()

        def _attach_debug(
            exc: TransformError | ValidationError,
            stage: str,
            df: pd.DataFrame,
        ) -> None:
            """Attach DebugContext to an exception if debug mode is active."""
            if not debug:
                return
            field = getattr(exc, "field", None)
            exc.debug_context = DebugContext(
                dataframe=df.copy(),
                field=field,
                expression=self._get_field_expression(field) if field else None,
                parameters=effective_params,
                stage=stage,
            )

        # Resolve effective parameters: global config defaults → runtime overrides
        runtime_params = parameters or {}
        effective_params: dict[str, Any] = {
            **self.config.parameters, **runtime_params,
        }
        # 1. Load all sources
        logger.info("Stage 1/5: Loading sources")
        frames = self._load_sources(runtime_params)

        # 2. Identify primary and execute joins
        logger.info("Stage 2/5: Executing joins")
        result_df = self._execute_joins(frames)
        _snap(SNAP_AFTER_JOIN, result_df)

        # 2b. Pre-filters — reduce dataset before field processing
        if self.config.pre_filters:
            logger.info("Applying pre-filters (before field processing)")
            try:
                result_df = self._apply_filters(result_df, effective_params, self.config.pre_filters)
            except TransformError as exc:
                _attach_debug(exc, STAGE_PRE_FILTERS, result_df)
                raise
            _snap(SNAP_AFTER_PRE_FILTERS, result_df)

        # 3. Field transforms — two ordered passes
        logger.info("Stage 3/5: Applying transformations")

        # Compute space selector masks (flag mode) after source columns are available
        space_selectors = getattr(self.config, "space_selectors", None) or {}
        space_masks: dict[str, pd.Series] = {}

        # Pass 1: source fields (rename/expr → cast → fill_na)
        try:
            result_df = self._apply_source_fields(result_df, effective_params, space_masks)
        except TransformError as exc:
            _attach_debug(exc, STAGE_SOURCE_FIELDS, result_df)
            raise
        _snap(SNAP_AFTER_SOURCE_FIELDS, result_df)

        # Compute selector masks after source fields are ready (selectors may reference them)
        if space_selectors:
            space_masks = self._compute_space_selector_masks(
                result_df, effective_params, space_selectors,
            )

        # Validate source fields before derived fields run
        report = self._run_field_validations(result_df, pass_filter="source", space_masks=space_masks)
        if report.has_errors:
            logger.error("Source field validation failed with %d errors", report.error_count)
            exc = ValidationError(report=report, message=report.format_detail())
            _attach_debug(exc, STAGE_SOURCE_VALIDATION, result_df)
            raise exc

        # Pass 2: derived fields (eval → cast → fill_na), then post-filters
        try:
            result_df = self._apply_derived_fields(result_df, effective_params, space_masks)
        except TransformError as exc:
            _attach_debug(exc, STAGE_DERIVED_FIELDS, result_df)
            raise
        _snap(SNAP_AFTER_DERIVED_FIELDS, result_df)

        if self.config.post_filters:
            try:
                result_df = self._apply_filters(result_df, effective_params, self.config.post_filters)
            except TransformError as exc:
                _attach_debug(exc, STAGE_POST_FILTERS, result_df)
                raise
            _snap(SNAP_AFTER_POST_FILTERS, result_df)
            # Recompute space masks — post-filters may have dropped rows, invalidating indices
            if space_selectors:
                space_masks = self._compute_space_selector_masks(
                    result_df, effective_params, space_selectors,
                )

        # 4. Validate derived fields + global validations
        logger.info("Stage 4/5: Running validations")
        derived_report = self._run_field_validations(result_df, pass_filter="derived", space_masks=space_masks)
        global_report = self._run_validations(result_df)
        report.results.extend(derived_report.results)
        report.results.extend(global_report.results)

        if report.has_errors:
            logger.error("Validation failed with %d errors", report.error_count)
            exc = ValidationError(report=report, message=report.format_detail())
            _attach_debug(exc, STAGE_VALIDATION, result_df)
            raise exc

        if report.has_warnings:
            logger.warning("Validation completed with %d warnings", report.warning_count)

        _snap(SNAP_AFTER_VALIDATION, result_df)

        # 4b. Process sub-entities (nested fields with entity_ref)
        sub_entity_classes = process_sub_entity_fields(
            self.config.fields, result_df, report, self.config_path,
            expression_namespace=self._expression_namespace,
            sub_entity_custom=self.sub_entity_custom,
        )

        # 5. Drop temp fields, then build entity class and instances
        logger.info("Stage 5/5: Building entity class and instances")
        entity_fields = [f for f in self.config.fields if not f.temp]
        entity_cols = [f.name for f in entity_fields if f.name in result_df.columns]
        entity_df = result_df[entity_cols]

        entity_cls = self.entity_builder.build_class(
            entity_name, entity_fields, sub_entity_classes=sub_entity_classes
        )
        entities = self.entity_builder.to_entities(entity_df, entity_cls)

        logger.info("=" * 60)
        logger.info(
            "Pipeline complete: %d %s entities created", len(entities), entity_name
        )
        logger.info("=" * 60)

        _snap(SNAP_FINAL, entity_df)

        return PipelineResult(
            entity_class=entity_cls,
            entities=entities,
            dataframe=result_df,
            validation_report=report,
            metadata={
                "entity_name": entity_name,
                "entity_version": self.config.entity.version,
                "source_count": len(self.config.sources),
                "record_count": len(result_df),
                "field_count": len(entity_fields),
                **({"space_selector_counts": {
                    space: int(mask.sum()) for space, mask in space_masks.items()
                }} if space_masks else {}),
            },
            sub_entity_classes=sub_entity_classes,
            snapshots=snapshots,
        )

    # ------------------------------------------------------------------
    # Stage implementations
    # ------------------------------------------------------------------

    def _load_sources(self, runtime_params: dict[str, Any]) -> dict[str, pd.DataFrame]:
        """Load all configured data sources.

        Each source's parameters are resolved with priority:
        global config defaults → source-level overrides → runtime parameters.

        Snowflake sources that share the same connection config reuse a single
        connection object, avoiding redundant authentication round-trips.
        """
        from impact.entity.source.snowflake import _connection_key, create_snowflake_connection

        frames: dict[str, pd.DataFrame] = {}

        # Build shared Snowflake connections keyed by connection config identity
        sf_connections: dict[tuple, Any] = {}

        try:
            for source_cfg in self.config.sources:
                # Priority: global defaults < source-level < runtime
                merged = {**self.config.parameters, **source_cfg.parameters, **runtime_params}
                effective_cfg = source_cfg.model_copy()
                effective_cfg.parameters = merged

                connector = ConnectorRegistry.get(effective_cfg.type)

                if effective_cfg.type == "snowflake" and isinstance(effective_cfg.connection, SnowflakeConnectionConfig):
                    key = _connection_key(effective_cfg.connection)
                    if key not in sf_connections:
                        sf_connections[key] = create_snowflake_connection(effective_cfg.connection)
                    df = connector.load(effective_cfg, connection=sf_connections[key])
                else:
                    df = connector.load(effective_cfg)

                frames[effective_cfg.name] = df
                logger.info(
                    "  Loaded source '%s': %d rows × %d columns",
                    effective_cfg.name,
                    len(df),
                    len(df.columns),
                )
        finally:
            # Close all shared Snowflake connections
            for conn in sf_connections.values():
                try:
                    conn.close()
                except Exception:
                    pass

        return frames

    def _execute_joins(self, frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Execute all configured joins in order.

        Joins are executed in config order. Each join uses ``join_cfg.left`` and
        ``join_cfg.right`` to look up the current state of that source. After a
        join, the left source is updated with the result. This supports pre-joins
        between non-primary sources (e.g. joining collateral into facility rows
        before nesting facilities under an obligor).
        """
        primary_name = next(s.name for s in self.config.sources if s.primary)
        resolved: dict[str, pd.DataFrame] = dict(frames)
        logger.info("  Primary source: '%s' (%d rows)", primary_name, len(resolved[primary_name]))

        for join_cfg in self.config.joins or []:
            left_df = resolved[join_cfg.left]
            right_df = resolved[join_cfg.right]
            joined = self.join_engine.execute(left_df, right_df, join_cfg)
            resolved[join_cfg.left] = joined
            logger.info(
                "  After join '%s' ↔ '%s': %d rows",
                join_cfg.left,
                join_cfg.right,
                len(joined),
            )

        return resolved[primary_name]

    def _apply_filters(
        self, df: pd.DataFrame, parameters: dict[str, Any],
        filters: list[str],
    ) -> pd.DataFrame:
        """Apply row-level filter expressions.

        Filters are pandas eval expressions evaluated in order. Use
        ``@param`` for external parameters. Packages declared in
        ``expression_packages`` are available directly (e.g. ``pd.isna(col)``).
        """
        result = df
        for condition in filters:
            before = len(result)
            try:
                mask = result.eval(condition, resolvers=[self._expression_namespace], local_dict=parameters)
                result = result.loc[mask].reset_index(drop=True)
            except Exception as exc:
                hint = enhance_expression_error(exc, self.config.expression_packages)
                raise TransformError(f"Filter '{condition}' failed: {exc}.{hint}") from exc
            logger.info("  Filter '%s': %d → %d rows", condition, before, len(result))
        return result

    def _compute_space_selector_masks(
        self, df: pd.DataFrame, parameters: dict[str, Any],
        space_selectors: dict[str, list[str]],
    ) -> dict[str, pd.Series]:
        """Compute boolean masks for flag-mode space selectors.

        Each space with selectors gets a boolean Series indicating which rows
        should receive that space's fields. Rows not matching remain unaffected
        (space fields get ``None``).
        """
        masks: dict[str, pd.Series] = {}
        for space_name, conditions in space_selectors.items():
            mask = pd.Series(True, index=df.index)
            for cond in conditions:
                try:
                    cond_mask = df.eval(
                        cond, resolvers=[self._expression_namespace], local_dict=parameters,
                    )
                    mask = mask & cond_mask
                except Exception as exc:
                    hint = enhance_expression_error(exc, self.config.expression_packages)
                    raise TransformError(
                        f"Space selector for '{space_name}': expression '{cond}' failed: {exc}.{hint}"
                    ) from exc
            matched = int(mask.sum())
            logger.info(
                "  Space '%s' selector: %d / %d rows matched",
                space_name, matched, len(df),
            )
            masks[space_name] = mask
        return masks

    def _apply_source_fields(
        self, df: pd.DataFrame, parameters: dict[str, Any] | None = None,
        space_masks: dict[str, pd.Series] | None = None,
    ) -> pd.DataFrame:
        """Pass 1 — process all source fields: copy/expr → cast → fill_na.

        ``source`` accepts two forms (source-name prefixes are stripped automatically):
        - Column reference: ``'col'`` or ``'src_name.col'`` — pass-through or copy to new name.
        - Expression: ``'src_name.col_a * 0.95'`` — evaluated via ``df.eval()``.

        Column references with a different name create a **copy** (not a rename),
        so the original column remains available for derived fields in Pass 2.
        Only fields declared in the config are kept in the final entity.

        ``pd`` and ``np`` are available directly (e.g. ``pd.isna(col)``).
        Use ``@param`` for external parameters.

        When ``space_masks`` is provided, fields belonging to a flagged space
        are only computed for matching rows; non-matching rows get ``None``.
        """
        params = parameters or {}
        masks = space_masks or {}
        result = df.copy()
        source_prefixes = {s.name for s in self.config.sources}

        # Pre-compute stripped sources (avoids re-sorting per field)
        stripped_map = {
            f.name: strip_source_prefixes(f.source, source_prefixes)
            for f in self.config.fields if f.source is not None
        }

        # Batch copy columns (original columns preserved for derived field access)
        for field in self.config.fields:
            if field.source is None:
                continue
            stripped = stripped_map[field.name]
            if stripped.isidentifier() and stripped != field.name:
                if stripped not in result.columns:
                    raise TransformError(
                        f"Field '{field.name}': source column '{stripped}' not found in DataFrame"
                    )
                mask = masks.get(field.space) if field.space else None
                if mask is not None:
                    result[field.name] = None
                    result.loc[mask, field.name] = result.loc[mask, stripped]
                else:
                    result[field.name] = result[stripped]
                logger.info("  Field '%s': copied from '%s'", field.name, stripped)

        # Per source field: expression eval → cast → fill_na
        for field in self.config.fields:
            if field.source is None:
                continue
            stripped = stripped_map[field.name]

            mask = masks.get(field.space) if field.space else None

            if not stripped.isidentifier():
                # Source is an expression — evaluate it
                try:
                    if mask is not None:
                        result[field.name] = None
                        result.loc[mask, field.name] = result.loc[mask].eval(
                            stripped, resolvers=[self._expression_namespace], local_dict=params,
                        )
                    else:
                        result[field.name] = result.eval(
                            stripped, resolvers=[self._expression_namespace], local_dict=params,
                        )
                except Exception as exc:
                    hint = enhance_expression_error(exc, self.config.expression_packages)
                    raise TransformError(
                        f"Field '{field.name}': failed to evaluate source expression "
                        f"'{field.source}': {exc}.{hint}"
                    ) from exc
                logger.info("  Field '%s': computed from source expression", field.name)

            result = cast_and_fill(result, field.name, field.dtype, field.fill_na, self._caster)
            # For flag-mode space fields, reset non-matching rows to None after cast
            if mask is not None:
                result.loc[~mask, field.name] = None

        return result

    def _apply_derived_fields(
        self, df: pd.DataFrame, parameters: dict[str, Any] | None = None,
        space_masks: dict[str, pd.Series] | None = None,
    ) -> pd.DataFrame:
        """Pass 2 — compute derived fields after all source fields are processed.

        ``derived`` runs after source fields are fully renamed, cast, filled, and
        validated. Use field names only — no ``src_name.`` prefix needed or expected.
        Supports pandas expressions and row-wise lambdas.

        Packages from ``expression_packages`` are available directly in both
        eval expressions and lambdas. Use ``@param`` for external parameters
        in both eval and lambda expressions.

        When ``space_masks`` is provided, derived fields belonging to a flagged
        space are only computed for matching rows; non-matching rows get ``None``.
        """
        params = parameters or {}
        masks = space_masks or {}
        lambda_ns = {"__builtins__": __builtins__, **self._expression_namespace, **params}
        result = df
        for field in self.config.fields:
            if field.derived is None:
                continue

            mask = masks.get(field.space) if field.space else None

            if isinstance(field.derived, DerivedFunctionRef):
                # Function reference — import and apply row-wise
                result = self._apply_derived_function(
                    result, field.name, field.derived, mask, params,
                )
            else:
                # Inline expression — eval or lambda
                expr = field.derived.strip()
                fn = None
                try:
                    if expr.startswith("lambda"):
                        expr = normalize_lambda_at_params(expr)
                        fn = eval(expr, lambda_ns)  # noqa: S307
                        if mask is not None:
                            result[field.name] = None
                            result.loc[mask, field.name] = result.loc[mask].apply(fn, axis=1)
                        else:
                            result[field.name] = result.apply(fn, axis=1)
                    else:
                        if mask is not None:
                            result[field.name] = None
                            result.loc[mask, field.name] = result.loc[mask].eval(
                                expr, resolvers=[self._expression_namespace], local_dict=params,
                            )
                        else:
                            result[field.name] = result.eval(expr, resolvers=[self._expression_namespace], local_dict=params)
                except Exception as exc:
                    diag, samples = "", []
                    if expr.startswith("lambda") and fn is not None:
                        diag, samples = format_lambda_diagnostic(result, fn)
                    hint = enhance_expression_error(exc, self.config.expression_packages)
                    raise TransformError(
                        f"Field '{field.name}': derived expression "
                        f"'{field.derived}' failed: {exc}.{diag}{hint}",
                        field=field.name,
                        failing_samples=samples,
                    ) from exc
            logger.info("  Field '%s': derived from expression", field.name)
            result = cast_and_fill(result, field.name, field.dtype, field.fill_na, self._caster)
            # For flag-mode space fields, reset non-matching rows to None after cast
            if mask is not None:
                result.loc[~mask, field.name] = None

        return result

    def _apply_derived_function(
        self, df: pd.DataFrame, field_name: str,
        ref: DerivedFunctionRef, mask: pd.Series | None,
        parameters: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Apply a derived function reference (row-wise).

        The function is imported from the dotted path and called per row via
        ``df.apply(fn, axis=1)``. Config ``kwargs`` are resolved and passed
        as keyword arguments. Values starting with ``@`` are resolved from
        runtime parameters (e.g. ``threshold: "@snapshot_date"`` passes the
        runtime value of ``snapshot_date`` as the ``threshold`` kwarg).
        """
        import functools

        try:
            fn = import_dotted_path(ref.function, error_class=TransformError)
        except TransformError:
            raise
        except Exception as exc:
            raise TransformError(
                f"Field '{field_name}': cannot import function '{ref.function}': {exc}"
            ) from exc

        # Resolve @param references in kwargs values
        params = parameters or {}
        resolved_kwargs: dict[str, Any] = {}
        for k, v in ref.kwargs.items():
            if isinstance(v, str) and v.startswith("@"):
                param_name = v[1:]
                if param_name in params:
                    resolved_kwargs[k] = params[param_name]
                else:
                    raise TransformError(
                        f"Field '{field_name}': kwarg '{k}' references parameter "
                        f"'@{param_name}' but it is not defined in parameters. "
                        f"Available: {sorted(params)}"
                    )
            else:
                resolved_kwargs[k] = v

        if resolved_kwargs:
            fn = functools.partial(fn, **resolved_kwargs)

        try:
            if mask is not None:
                df[field_name] = None
                df.loc[mask, field_name] = df.loc[mask].apply(fn, axis=1)
            else:
                df[field_name] = df.apply(fn, axis=1)
        except Exception as exc:
            diag, samples = format_lambda_diagnostic(df, fn)
            raise TransformError(
                f"Field '{field_name}': function '{ref.function}' failed: {exc}.{diag}",
                field=field_name,
                failing_samples=samples,
            ) from exc
        return df

    def _run_field_validations(
        self, df: pd.DataFrame, pass_filter: str | None = None,
        space_masks: dict[str, pd.Series] | None = None,
    ) -> ValidationReport:
        """Run inline validation rules for fields matching ``pass_filter``.

        Args:
            pass_filter: ``"source"`` — only source fields,
                         ``"derived"`` — only derived fields,
                         ``None`` — all fields.
            space_masks: When provided, validates flagged-space fields only
                against matching rows (non-matching rows are excluded).
        """
        masks = space_masks or {}
        report = ValidationReport()
        # Cache filtered DataFrames per space (avoid repeated loc + reset_index)
        space_df_cache: dict[str, pd.DataFrame] = {}

        for field in self.config.fields:
            # For flagged-space fields, validate only matching rows
            space = field.space
            if space and space in masks:
                if space not in space_df_cache:
                    space_df_cache[space] = df.loc[masks[space]].reset_index(drop=True)
                validate_df = space_df_cache[space]
            else:
                validate_df = df

            for v_cfg in field.build_validation_configs(pass_filter=pass_filter):
                validator = ValidatorRegistry.get(v_cfg.type)
                result = validator.validate(validate_df, v_cfg)
                result.field_name = result.field_name or field.name
                report.results.append(result)
                status = "PASS" if result.passed else result.severity.upper()
                logger.info(
                    "  Field '%s' [%s] %s: %s", field.name, status, v_cfg.type, result.message
                )
                if not result.passed and result.failing_samples:
                    for sample in result.failing_samples[:3]:
                        vals = ", ".join(f"{k}={v!r}" for k, v in sample.items())
                        logger.info("    ↳ sample: %s", vals)

        return report

    def _run_validations(self, df: pd.DataFrame) -> ValidationReport:
        """Run all configured validation rules."""
        report = ValidationReport()

        for v_cfg in self.config.validations or []:
            validator = ValidatorRegistry.get(v_cfg.type)
            result = validator.validate(df, v_cfg)
            report.results.append(result)

            status = "PASS" if result.passed else result.severity.upper()
            logger.info("  Validation [%s]: %s", status, result.message)

        return report

    def _get_field_expression(self, field_name: str | None) -> str | None:
        """Look up the expression/function path for a field by name."""
        if not field_name:
            return None
        for f in self.config.fields:
            if f.name == field_name:
                if isinstance(f.derived, DerivedFunctionRef):
                    return f"fn:{f.derived.function}"
                if isinstance(f.derived, str):
                    return f.derived
                if isinstance(f.source, str):
                    return f.source
        return None
