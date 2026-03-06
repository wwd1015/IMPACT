"""Pipeline orchestrator — ties all Entity Data Module components together.

This is the main entry point for running an entity processing pipeline.
It loads the YAML config, executes all stages (load → join → transform →
validate → build), and returns a structured result.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from impact.common.exceptions import ConfigError, SourceError, ValidationError
from impact.common.logging import get_logger
from impact.entity.config.parser import ConfigParser
from impact.entity.config.schema import EntityConfig, SourceConfig
from impact.entity.join.engine import JoinEngine
from impact.entity.model.builder import EntityBuilder
from impact.entity.validate.base import ValidationReport, ValidationResult

# Import builtins to trigger decorator registration
import impact.entity.source.csv_excel  # noqa: F401
import impact.entity.source.parquet  # noqa: F401
import impact.entity.source.snowflake  # noqa: F401
import impact.entity.transform.builtin  # noqa: F401
import impact.entity.validate.builtin  # noqa: F401

from impact.entity.source.registry import ConnectorRegistry
from impact.entity.transform.registry import TransformRegistry
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
        elif config_path is not None:
            self.config = ConfigParser().parse(config_path)
        else:
            raise ConfigError("Either config_path or config must be provided")

        self.join_engine = JoinEngine()
        self.entity_builder = EntityBuilder()

    def run(self, parameters: dict[str, Any] | None = None) -> PipelineResult:
        """Execute the full pipeline.

        Args:
            parameters: Optional runtime parameters that override config parameters.

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

        # 1. Load all sources
        logger.info("Stage 1/5: Loading sources")
        frames = self._load_sources(parameters)

        # 2. Identify primary and execute joins
        logger.info("Stage 2/5: Executing joins")
        result_df = self._execute_joins(frames)

        # 3. Apply transformations
        logger.info("Stage 3/5: Applying transformations")
        result_df = self._apply_transforms(result_df)

        # 4. Run validations
        logger.info("Stage 4/5: Running validations")
        report = self._run_validations(result_df)

        if report.has_errors:
            logger.error("Validation failed with %d errors", report.error_count)
            raise ValidationError(report=report, message=str(report))

        if report.has_warnings:
            logger.warning("Validation completed with %d warnings", report.warning_count)

        # 5. Build entity class and instances
        logger.info("Stage 5/5: Building entity class and instances")
        entity_cls = self.entity_builder.build_class(entity_name, self.config.fields)
        entities = self.entity_builder.to_entities(result_df, entity_cls)

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
                "field_count": len(self.config.fields),
            },
        )

    # ------------------------------------------------------------------
    # Stage implementations
    # ------------------------------------------------------------------

    def _load_sources(
        self, parameters: dict[str, Any] | None = None
    ) -> dict[str, pd.DataFrame]:
        """Load all configured data sources."""
        frames: dict[str, pd.DataFrame] = {}

        for source_cfg in self.config.sources:
            # Merge runtime parameters with config parameters
            if parameters:
                effective_cfg = source_cfg.model_copy()
                effective_cfg.parameters = {**source_cfg.parameters, **parameters}
            else:
                effective_cfg = source_cfg

            connector = ConnectorRegistry.get(effective_cfg.type)
            df = connector.load(effective_cfg)
            frames[effective_cfg.name] = df
            logger.info(
                "  Loaded source '%s': %d rows × %d columns",
                effective_cfg.name,
                len(df),
                len(df.columns),
            )

        return frames

    def _execute_joins(self, frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Execute all configured joins in order."""
        # Start with the primary source
        primary_name = next(s.name for s in self.config.sources if s.primary)
        result_df = frames[primary_name]
        logger.info("  Primary source: '%s' (%d rows)", primary_name, len(result_df))

        for join_cfg in self.config.joins or []:
            right_df = frames[join_cfg.right]
            result_df = self.join_engine.execute(result_df, right_df, join_cfg)
            logger.info(
                "  After join '%s' ↔ '%s': %d rows",
                join_cfg.left,
                join_cfg.right,
                len(result_df),
            )

        return result_df

    def _apply_transforms(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply all configured transformations in order."""
        result_df = df

        for i, t_cfg in enumerate(self.config.transforms or [], 1):
            transformer = TransformRegistry.get(t_cfg.type)
            result_df = transformer.apply(result_df, t_cfg)
            logger.info("  Transform %d/%d (%s): done", i, len(self.config.transforms or []), t_cfg.type)

        return result_df

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
