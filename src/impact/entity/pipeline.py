"""Pipeline orchestrator — ties all Entity Data Module components together.

This is the main entry point for running an entity processing pipeline.
It loads the YAML config, executes all stages (load → join → transform →
filter → validate → build), and returns a structured result.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from impact.common.exceptions import ConfigError, SourceError, TransformError, ValidationError
from impact.common.logging import get_logger
from impact.common.utils import cast_and_fill, format_lambda_diagnostic, strip_source_prefixes
from impact.entity.config.parser import ConfigParser
from impact.entity.config.schema import EntityConfig, SnowflakeConnectionConfig
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

    def __init__(self, config_path: str | Path | None = None, config: EntityConfig | None = None):
        """Initialize the pipeline.

        Args:
            config_path: Path to the YAML config file.
            config: Pre-parsed EntityConfig (alternative to config_path).

        Raises:
            ConfigError: If neither config_path nor config is provided.
        """
        if config is not None:
            self.config = config
            self.config_path: Path | None = None
        elif config_path is not None:
            self.config_path = Path(config_path)
            self.config = ConfigParser().parse(config_path)
        else:
            raise ConfigError("Either config_path or config must be provided")

        self.join_engine = JoinEngine()
        self.entity_builder = EntityBuilder()
        self._caster = CastTransformer()

    def run(self, parameters: dict[str, Any] | None = None) -> PipelineResult:
        """Execute the full pipeline.

        Args:
            parameters: Runtime parameters injected by the orchestrator. Merged with
                the config's global ``parameters`` block using the priority:
                runtime > source-level > global config default.

        Returns:
            PipelineResult containing entity class, instances, DataFrame, and report.

        Raises:
            SourceError: If data loading fails.
            JoinError: If joining fails.
            TransformError: If a transformation fails.
            ValidationError: If validation fails with severity=error.
        """
        entity_name = self.config.entity.name
        logger.info("=" * 60)
        logger.info("Starting pipeline for entity: %s", entity_name)
        logger.info("=" * 60)

        # Resolve effective parameters: global config defaults → runtime overrides
        runtime_params = parameters or {}
        effective_params: dict[str, Any] = {**self.config.parameters, **runtime_params}

        # 1. Load all sources
        logger.info("Stage 1/5: Loading sources")
        frames = self._load_sources(runtime_params)

        # 2. Identify primary and execute joins
        logger.info("Stage 2/5: Executing joins")
        result_df = self._execute_joins(frames)

        # 2b. Pre-filters — reduce dataset before field processing
        if self.config.pre_filters:
            logger.info("Applying pre-filters (before field processing)")
            result_df = self._apply_filters(result_df, effective_params, self.config.pre_filters)

        # 3. Field transforms — two ordered passes
        logger.info("Stage 3/5: Applying transformations")

        # Pass 1: source fields (rename/expr → cast → fill_na)
        result_df = self._apply_source_fields(result_df, effective_params)

        # Validate source fields before derived fields run
        report = self._run_field_validations(result_df, pass_filter="source")
        if report.has_errors:
            logger.error("Source field validation failed with %d errors", report.error_count)
            raise ValidationError(report=report, message=report.format_detail())

        # Pass 2: derived fields (eval → cast → fill_na), then post-filters
        result_df = self._apply_derived_fields(result_df, effective_params)
        if self.config.post_filters:
            result_df = self._apply_filters(result_df, effective_params, self.config.post_filters)

        # 4. Validate derived fields + global validations
        logger.info("Stage 4/5: Running validations")
        derived_report = self._run_field_validations(result_df, pass_filter="derived")
        global_report = self._run_validations(result_df)
        report.results.extend(derived_report.results)
        report.results.extend(global_report.results)

        if report.has_errors:
            logger.error("Validation failed with %d errors", report.error_count)
            raise ValidationError(report=report, message=report.format_detail())

        if report.has_warnings:
            logger.warning("Validation completed with %d warnings", report.warning_count)

        # 4b. Process sub-entities (nested fields with entity_ref)
        sub_entity_classes = process_sub_entity_fields(
            self.config.fields, result_df, report, self.config_path
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
            },
            sub_entity_classes=sub_entity_classes,
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

        Filters are pandas eval expressions evaluated in order. Use ``@param_name``
        syntax to reference values from ``parameters`` (e.g. ``status == @active_status``).
        """
        result = df
        for condition in filters:
            before = len(result)
            try:
                mask = result.eval(condition, local_dict=parameters)
                result = result.loc[mask].reset_index(drop=True)
            except Exception as exc:
                raise TransformError(f"Filter '{condition}' failed: {exc}") from exc
            logger.info("  Filter '%s': %d → %d rows", condition, before, len(result))
        return result

    def _apply_source_fields(
        self, df: pd.DataFrame, parameters: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Pass 1 — process all source fields: copy/expr → cast → fill_na.

        ``source`` accepts two forms (source-name prefixes are stripped automatically):
        - Column reference: ``'col'`` or ``'src_name.col'`` — pass-through or copy to new name.
        - Expression: ``'src_name.col_a * 0.95'`` — evaluated via ``df.eval()``.

        Column references with a different name create a **copy** (not a rename),
        so the original column remains available for derived fields in Pass 2.
        Only fields declared in the config are kept in the final entity.

        Parameters are available in expressions via ``@param_name`` syntax.
        """
        params = parameters or {}
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
                result[field.name] = result[stripped]
                logger.info("  Field '%s': copied from '%s'", field.name, stripped)

        # Per source field: expression eval → cast → fill_na
        for field in self.config.fields:
            if field.source is None:
                continue
            stripped = stripped_map[field.name]

            if not stripped.isidentifier():
                # Source is an expression — evaluate it
                try:
                    result[field.name] = result.eval(stripped, local_dict=params)
                except Exception as exc:
                    raise TransformError(
                        f"Field '{field.name}': failed to evaluate source expression "
                        f"'{field.source}': {exc}"
                    ) from exc
                logger.info("  Field '%s': computed from source expression", field.name)

            result = cast_and_fill(result, field.name, field.dtype, field.fill_na, self._caster)

        return result

    def _apply_derived_fields(
        self, df: pd.DataFrame, parameters: dict[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Pass 2 — compute derived fields after all source fields are processed.

        ``derived`` runs after source fields are fully renamed, cast, filled, and
        validated. Use field names only — no ``src_name.`` prefix needed or expected.
        Supports pandas expressions and row-wise lambdas.

        Parameters are available via:
        - ``@param_name`` in pandas eval expressions
        - Direct variable name in lambdas (e.g. ``lambda row: row['a'] * threshold``)
        """
        params = parameters or {}
        lambda_ns = {"__builtins__": __builtins__, **params}
        result = df
        for field in self.config.fields:
            if field.derived is None:
                continue
            expr = field.derived.strip()
            try:
                if expr.startswith("lambda"):
                    fn = eval(expr, lambda_ns)  # noqa: S307
                    result[field.name] = result.apply(fn, axis=1)
                else:
                    result[field.name] = result.eval(expr, local_dict=params)
            except Exception as exc:
                diag, samples = "", []
                if expr.startswith("lambda"):
                    diag, samples = format_lambda_diagnostic(result, fn)
                raise TransformError(
                    f"Field '{field.name}': derived expression "
                    f"'{field.derived}' failed: {exc}.{diag}",
                    field=field.name,
                    failing_samples=samples,
                ) from exc
            logger.info("  Field '%s': derived from expression", field.name)
            result = cast_and_fill(result, field.name, field.dtype, field.fill_na, self._caster)

        return result

    def _run_field_validations(
        self, df: pd.DataFrame, pass_filter: str | None = None
    ) -> ValidationReport:
        """Run inline validation rules for fields matching ``pass_filter``.

        Args:
            pass_filter: ``"source"`` — only source fields,
                         ``"derived"`` — only derived fields,
                         ``None`` — all fields.
        """
        report = ValidationReport()

        for field in self.config.fields:
            for v_cfg in field.build_validation_configs(pass_filter=pass_filter):
                validator = ValidatorRegistry.get(v_cfg.type)
                result = validator.validate(df, v_cfg)
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
