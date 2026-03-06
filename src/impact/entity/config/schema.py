"""Pydantic v2 models defining the YAML configuration schema.

The config schema is the single source of truth for the entire Entity Data Module
pipeline. It defines data sources, joins, transformations, validations, and the
entity field mapping (which doubles as the dynamic class definition).
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


# ---------------------------------------------------------------------------
# Entity metadata
# ---------------------------------------------------------------------------
class EntityMeta(BaseModel):
    """Top-level entity metadata."""

    name: str = Field(..., description="Entity class name, e.g. 'Facility'")
    description: str = Field("", description="Human-readable description")
    version: str = Field("1.0", description="Schema version string")


# ---------------------------------------------------------------------------
# Source configs
# ---------------------------------------------------------------------------
class SnowflakeConnectionConfig(BaseModel):
    """Snowflake-specific connection parameters."""

    account: str
    database: str
    schema_: str = Field(alias="schema")
    warehouse: str
    role: str | None = None
    user: str | None = None
    password: str | None = None
    authenticator: str | None = None

    model_config = {"populate_by_name": True}


class SourceConfig(BaseModel):
    """Configuration for a single data source."""

    name: str = Field(..., description="Unique source identifier")
    type: Literal["snowflake", "parquet", "csv", "excel"] = Field(
        ..., description="Source type"
    )
    primary: bool = Field(False, description="Whether this is the primary source")

    # Snowflake-specific
    connection: SnowflakeConnectionConfig | None = None
    query: str | None = None

    # File-based sources
    path: str | None = None
    options: dict[str, Any] = Field(default_factory=dict)

    # Parameterized placeholders
    parameters: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_source_fields(self) -> SourceConfig:
        if self.type == "snowflake":
            if not self.connection or not self.query:
                raise ValueError(
                    f"Source '{self.name}': snowflake type requires 'connection' and 'query'"
                )
        elif self.type in ("parquet", "csv", "excel"):
            if not self.path:
                raise ValueError(
                    f"Source '{self.name}': {self.type} type requires 'path'"
                )
        return self


# ---------------------------------------------------------------------------
# Join configs
# ---------------------------------------------------------------------------
class JoinKeyCondition(BaseModel):
    """A single join key pair or expression condition."""

    left_col: str | None = None
    right_col: str | None = None
    condition: str | None = None

    @model_validator(mode="after")
    def validate_condition(self) -> JoinKeyCondition:
        has_cols = self.left_col is not None and self.right_col is not None
        has_expr = self.condition is not None
        if not has_cols and not has_expr:
            raise ValueError("JoinKeyCondition requires either (left_col + right_col) or condition")
        return self


class JoinConfig(BaseModel):
    """Configuration for joining two sources."""

    left: str = Field(..., description="Left source name")
    right: str = Field(..., description="Right source name")
    how: Literal["left", "right", "inner"] = Field("left", description="Join type")
    on: list[JoinKeyCondition] = Field(..., description="Join conditions")
    relationship: Literal["one_to_one", "one_to_many"] = Field(
        "one_to_one", description="Expected cardinality"
    )
    nested_as: str | None = Field(
        None, description="Column name for nested DataFrame (one_to_many only)"
    )

    @model_validator(mode="after")
    def validate_nesting(self) -> JoinConfig:
        if self.relationship == "one_to_many" and not self.nested_as:
            raise ValueError(
                f"Join {self.left} ↔ {self.right}: one_to_many relationship requires 'nested_as'"
            )
        return self


# ---------------------------------------------------------------------------
# Transform configs
# ---------------------------------------------------------------------------
class TransformConfig(BaseModel):
    """Configuration for a single transformation step."""

    type: Literal["cast", "rename", "derive", "fill_na", "drop", "filter", "custom"] = Field(
        ..., description="Transform type"
    )

    # cast
    columns: dict[str, str] | None = None

    # rename
    mapping: dict[str, str] | None = None

    # derive
    name: str | None = None
    expression: str | None = None
    dtype: str | None = None

    # fill_na
    strategy: dict[str, Any] | None = None

    # drop
    drop_columns: list[str] | None = Field(None, alias="drop_columns")

    # filter
    condition: str | None = None

    # custom
    function: str | None = None
    kwargs: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Validation configs
# ---------------------------------------------------------------------------
class ValidationConfig(BaseModel):
    """Configuration for a single validation rule."""

    type: Literal["not_null", "unique", "range", "expression", "custom"] = Field(
        ..., description="Validation type"
    )
    severity: Literal["error", "warning"] = Field(
        "error", description="error = halt pipeline; warning = log and continue"
    )

    # not_null / unique
    columns: list[str] | None = None

    # range
    column: str | None = None
    min: float | None = None
    max: float | None = None

    # expression
    rule: str | None = None
    message: str | None = None

    # custom
    function: str | None = None
    kwargs: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Field configs (= entity class definition)
# ---------------------------------------------------------------------------
class FieldConfig(BaseModel):
    """Definition of a single field on the entity class."""

    name: str = Field(..., description="Field / column name")
    dtype: str = Field(..., description="Data type: str, int32, int64, float64, datetime, bool, nested")
    description: str = Field("", description="Human-readable description")
    primary_key: bool = Field(False, description="Whether this is part of the primary key")
    entity_ref: str | None = Field(
        None, description="Reference to another entity config (for nested fields)"
    )


# ---------------------------------------------------------------------------
# Top-level config
# ---------------------------------------------------------------------------
class EntityConfig(BaseModel):
    """Root configuration model for the Entity Data Module pipeline.

    This is the Pydantic representation of the entire YAML config file.
    """

    entity: EntityMeta
    sources: list[SourceConfig]
    joins: list[JoinConfig] | None = None
    transforms: list[TransformConfig] | None = None
    validations: list[ValidationConfig] | None = None
    fields: list[FieldConfig]

    @model_validator(mode="after")
    def validate_single_primary(self) -> EntityConfig:
        primaries = [s for s in self.sources if s.primary]
        if len(primaries) != 1:
            raise ValueError(
                f"Exactly one source must be marked as primary, found {len(primaries)}"
            )
        return self

    @model_validator(mode="after")
    def validate_join_references(self) -> EntityConfig:
        source_names = {s.name for s in self.sources}
        for join in self.joins or []:
            if join.left not in source_names:
                raise ValueError(f"Join references unknown left source: '{join.left}'")
            if join.right not in source_names:
                raise ValueError(f"Join references unknown right source: '{join.right}'")
        return self
