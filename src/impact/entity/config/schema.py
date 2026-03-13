"""Pydantic v2 models defining the YAML configuration schema.

The config schema is the single source of truth for the entire Entity Data Module
pipeline. It defines data sources, joins, transformations, validations, and the
entity field mapping (which doubles as the dynamic class definition).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

# Reusable type alias — kept in sync with ValidationConfig.type
ValidationTypeLiteral = Literal["not_null", "unique", "range", "expression", "custom"]

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
    """Configuration for a single data source.

    For Snowflake sources, ``connection`` can be either:
    - An inline ``SnowflakeConnectionConfig`` dict (account, database, schema, warehouse, ...)
    - A string referencing a named connection defined in the top-level ``connections`` section

    String references are resolved by ``EntityConfig.resolve_connection_refs()``
    during config validation.
    """

    # Names that should never be used as source names for security reasons
    _UNSAFE_NAMES: frozenset[str] = frozenset({"os", "sys", "subprocess", "shutil"})

    name: str = Field(..., description="Unique source identifier")
    type: Literal["snowflake", "sqlite", "parquet", "csv", "excel"] = Field(
        ..., description="Source type"
    )
    primary: bool = Field(False, description="Whether this is the primary source")

    # Snowflake-specific — inline config or string reference to connections section
    connection: SnowflakeConnectionConfig | str | None = None
    query: str | None = None

    # File-based sources
    path: str | None = None
    options: dict[str, Any] = Field(default_factory=dict)

    # Parameterized placeholders
    parameters: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_source_fields(self) -> SourceConfig:
        if self.name in self._UNSAFE_NAMES:
            raise ValueError(
                f"Source name '{self.name}' is reserved (unsafe). Choose a different name."
            )
        if self.type == "snowflake":
            # connection can be a string ref (resolved later by EntityConfig) or inline config
            if self.connection is None or not self.query:
                raise ValueError(
                    f"Source '{self.name}': snowflake type requires 'connection' and 'query'"
                )
        elif self.type == "sqlite":
            if not self.path or not self.query:
                raise ValueError(
                    f"Source '{self.name}': sqlite type requires 'path' and 'query'"
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

    type: ValidationTypeLiteral = Field(..., description="Validation type")
    severity: Literal["error", "warning"] = Field(
        "error", description="error = halt pipeline; warning = log and continue"
    )

    # not_null / unique
    columns: list[str] | None = None

    # range
    column: str | None = None
    min: float | None = None
    max: float | None = None
    min_exclusive: bool = False
    max_exclusive: bool = False

    # expression
    rule: str | None = None
    message: str | None = None

    # custom
    function: str | None = None
    kwargs: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Field configs (= entity class definition + inline transforms + validations)
# ---------------------------------------------------------------------------
class FieldConfig(BaseModel):
    """Definition of a single field on the entity class.

    A field can optionally declare its own data origin, transformation, and
    validation rules inline, consolidating what would otherwise be spread
    across the top-level ``transforms`` and ``validations`` sections.

    Data origin (mutually exclusive):
    - ``source``: map/rename an existing column (e.g. ``"product_type"`` or
      ``"facility_main.product_type"``).  The source-prefix notation is
      informational; only the column name after the last ``.`` is used.
    - ``derived``: compute this column from a pandas expression
      (e.g. ``"outstanding_balance / commitment_amount"``).

    Inline transforms:
    - ``fill_na``: scalar fill value applied after source/derived resolution.
      Omit (or set to ``null``) to skip.

    Inline validations (compact format):
    - ``validation_type``: list of types — ``not_null``, ``unique``, ``range``,
      ``expression``, ``custom``.
    - ``validation_rule``: dict of rules keyed by type (required for ``range``
      and ``expression``; omit for ``not_null`` and ``unique``).
    - ``validation_severity``: dict of severity overrides keyed by type.
      Default is ``"warning"``; use ``"error"`` to halt the pipeline.
    """

    name: str = Field(..., description="Field / column name")
    dtype: str = Field(..., description="Data type: str, int32, int64, float64, datetime, bool, nested")
    space: str | None = Field(
        None,
        description=(
            "Namespace for this field. None = primary space. "
            "Set automatically by merge_configs for custom config fields. "
            "Users should not set this directly in YAML."
        ),
    )
    description: str = Field("", description="Human-readable description")
    primary_key: bool = Field(False, description="Whether this is part of the primary key")
    entity_ref: str | None = Field(
        None, description="Reference to another entity config (for nested fields)"
    )
    entity_ref_config: str | None = Field(
        None,
        description=(
            "Explicit filename for the sub-entity config (e.g. 'collateral_demo.yaml'). "
            "Resolved relative to the parent config's directory. "
            "When omitted, the pipeline searches by convention: "
            "{snake_case(entity_ref)}.yaml or {snake_case(entity_ref)}_*.yaml."
        ),
    )

    # --- Data origin (mutually exclusive) ---
    source: str | None = Field(
        None,
        description=(
            "Processed in Pass 1 (with all other source fields). Two forms:\n"
            "  Column reference: 'col' or 'src_name.col' — pass-through or rename.\n"
            "  Expression: 'src_name.col_a + src_name.col_b' — evaluated via df.eval();\n"
            "    source-name prefixes are stripped automatically before evaluation."
        ),
    )
    derived: str | None = Field(
        None,
        description=(
            "Processed in Pass 2, after all source fields are fully processed "
            "(renamed, cast, filled, validated). Use field names only — no src_name prefix.\n"
            "  pandas expression: 'col_a / col_b'\n"
            "  row-wise lambda:   'lambda row: row[\"a\"] * 2'"
        ),
    )

    # --- Inline transforms ---
    fill_na: Any = Field(
        None,
        description="Scalar value to fill NAs after dtype cast. Omit to skip.",
    )

    # --- Temp flag ---
    temp: bool = Field(
        False,
        description=(
            "If true, the field is available throughout processing (transforms, validations) "
            "but is dropped before the entity class is built. Use for intermediate calculations."
        ),
    )

    # --- Inline validations (compact format) ---
    validation_type: list[ValidationTypeLiteral] | None = Field(
        None,
        description=(
            "List of validation types to apply to this field. "
            "Supported: not_null, unique, range, expression, custom."
        ),
    )
    validation_rule: dict[str, Any] | None = Field(
        None,
        description=(
            "Rules keyed by validation type. Required for 'range' ([min, max] or {min, max}) "
            "and 'expression' (rule string). Omit for not_null and unique."
        ),
    )
    validation_severity: dict[str, str] | None = Field(
        None,
        description=(
            "Severity overrides keyed by validation type. "
            "Default severity is 'warning'. Use 'error' to halt the pipeline."
        ),
    )

    @model_validator(mode="after")
    def validate_source_and_derived(self) -> FieldConfig:
        if self.source is not None and self.derived is not None:
            raise ValueError(
                f"Field '{self.name}': 'source' and 'derived' are mutually exclusive."
            )
        if self.source is None and self.derived is None:
            raise ValueError(
                f"Field '{self.name}': exactly one of 'source' or 'derived' is required."
            )
        return self

    def build_validation_configs(self, pass_filter: str | None = None) -> list[ValidationConfig]:
        """Expand compact field-level validation syntax into ValidationConfig objects.

        Args:
            pass_filter: ``"source"`` to return configs only for source fields,
                ``"derived"`` for derived fields, or ``None`` for all fields.

        Rule conventions by type:
        - ``not_null`` / ``unique``: no rule needed; column = this field.
        - ``range``: ``validation_rule.range`` is ``[min, max]`` or ``{min, max}``.
        - ``expression``: ``validation_rule.expression`` is a pandas-eval boolean string.
        - ``custom``: ``validation_rule.custom`` is a dotted function path.

        Default severity is ``"warning"`` unless overridden in ``validation_severity``.
        """
        if not self.validation_type:
            return []

        # Filter by field origin (source vs derived)
        if pass_filter == "source" and self.derived is not None:
            return []
        if pass_filter == "derived" and self.source is not None:
            return []

        rules = self.validation_rule or {}
        severities = self.validation_severity or {}
        configs: list[ValidationConfig] = []

        for vtype in self.validation_type:
            severity = severities.get(vtype, "warning")
            common: dict[str, Any] = {"type": vtype, "severity": severity}

            if vtype in ("not_null", "unique"):
                configs.append(ValidationConfig(**common, columns=[self.name]))

            elif vtype == "range":
                raw = rules.get("range")
                if raw is None:
                    raise ValueError(
                        f"Field '{self.name}': validation_type 'range' requires "
                        "'range' key in validation_rule (e.g. [0.0, 1.0] or (0.0, 1.0))"
                    )
                min_exclusive = False
                max_exclusive = False
                if isinstance(raw, str):
                    # Parse bracket notation: "[0, 1]", "(0, 1)", "[0, 1)", "(0, null]"
                    stripped = raw.strip()
                    if stripped[0] == "(":
                        min_exclusive = True
                    if stripped[-1] == ")":
                        max_exclusive = True
                    inner = stripped[1:-1]
                    parts = [p.strip() for p in inner.split(",")]
                    lo = None if parts[0] == "null" else float(parts[0])
                    hi = None if parts[1] == "null" else float(parts[1])
                elif isinstance(raw, (list, tuple)):
                    lo, hi = raw[0], raw[1]
                else:
                    lo, hi = raw.get("min"), raw.get("max")
                    min_exclusive = raw.get("min_exclusive", False)
                    max_exclusive = raw.get("max_exclusive", False)
                configs.append(ValidationConfig(
                    **common, column=self.name, min=lo, max=hi,
                    min_exclusive=min_exclusive, max_exclusive=max_exclusive,
                ))

            elif vtype == "expression":
                rule_str = rules.get("expression")
                if rule_str is None:
                    raise ValueError(
                        f"Field '{self.name}': validation_type 'expression' requires "
                        "'expression' key in validation_rule"
                    )
                configs.append(ValidationConfig(**common, rule=rule_str, message=rule_str))

            elif vtype == "custom":
                fn = rules.get("custom")
                if fn is None:
                    raise ValueError(
                        f"Field '{self.name}': validation_type 'custom' requires "
                        "'custom' key in validation_rule (dotted function path)"
                    )
                configs.append(ValidationConfig(**common, function=fn))

        return configs


# ---------------------------------------------------------------------------
# Top-level config
# ---------------------------------------------------------------------------
class EntityConfig(BaseModel):
    """Root configuration model for the Entity Data Module pipeline.

    This is the Pydantic representation of the entire YAML config file.
    """

    entity: EntityMeta
    expression_packages: dict[str, str] = Field(
        default_factory=lambda: {"pd": "pandas", "np": "numpy"},
        description=(
            "Packages available in expressions (eval and lambda). "
            "Keys are aliases used in expressions, values are importable module names. "
            "Default: {pd: pandas, np: numpy}. "
            "Example: {pd: pandas, np: numpy, math: math}"
        ),
    )
    parameters: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Global default parameters. Applied to all sources and filters. "
            "Source-level parameters override these; runtime parameters passed to "
            "pipeline.run() override both. Use for values like snapshot_date that "
            "are shared across sources."
        ),
    )
    connections: dict[str, SnowflakeConnectionConfig] = Field(
        default_factory=dict,
        description=(
            "Named connection configs. Define once, reference by name in sources. "
            "Currently supports Snowflake connections. Example: "
            "connections:\\n  lending_db:\\n    account: ...\\n"
            "sources:\\n  - connection: lending_db  # reference by name"
        ),
    )
    sources: list[SourceConfig] = Field(
        default_factory=list,
        description="Data sources. Omit for sub-entity configs (nested data comes from parent).",
    )
    joins: list[JoinConfig] | None = None
    pre_filters: list[str] | None = Field(
        None,
        description=(
            "Row-level filters applied BEFORE field processing (right after joins). "
            "Use raw source column names (before any renaming). "
            "Reduces the dataset early for better performance on large data. "
            "Each entry is a pandas eval expression. "
            "Use @param_name syntax to reference runtime parameters."
        ),
    )
    post_filters: list[str] | None = Field(
        None,
        description=(
            "Row-level filters applied AFTER all field processing (Pass 1 + Pass 2). "
            "Use processed field names (after renaming). "
            "Each entry is a pandas eval expression. "
            "Use @param_name syntax to reference runtime parameters."
        ),
    )
    validations: list[ValidationConfig] | None = None
    fields: list[FieldConfig]

    # Non-YAML metadata — set by ConfigParser.parse() or merge_configs()
    # Used by the pipeline to resolve sub-entity config files
    config_path: Path | None = Field(None, exclude=True)

    @model_validator(mode="after")
    def resolve_connection_refs(self) -> EntityConfig:
        """Resolve string connection references to actual SnowflakeConnectionConfig objects."""
        for source in self.sources:
            if isinstance(source.connection, str):
                ref_name = source.connection
                if ref_name not in self.connections:
                    available = ", ".join(sorted(self.connections)) or "(none)"
                    raise ValueError(
                        f"Source '{source.name}': connection '{ref_name}' not found "
                        f"in connections section. Available: {available}"
                    )
                source.connection = self.connections[ref_name]
        return self

    @model_validator(mode="after")
    def validate_single_primary(self) -> EntityConfig:
        if not self.sources:
            return self  # sub-entity configs have no sources
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

    @model_validator(mode="after")
    def validate_source_names_vs_packages(self) -> EntityConfig:
        """Ensure no source name conflicts with an expression_packages alias."""
        for source in self.sources:
            if source.name in self.expression_packages:
                raise ValueError(
                    f"Source name '{source.name}' conflicts with expression package alias "
                    f"'{source.name}' (maps to '{self.expression_packages[source.name]}'). "
                    f"Choose a different source name."
                )
        return self
