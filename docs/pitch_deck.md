# IMPACT — Entity Data Module
## Pitch Deck Content

---

## Slide 1: Title

**IMPACT**
*Entity Data Module*

A config-driven pipeline platform for financial lending credit modeling.
Define your entities in YAML. Let the engine handle the rest.

- Standardized Entity Pipeline
- Multi-Source Ingestion
- Built-In Validation & Debugging

---

## Slide 2: The Shared Goal

**Every entity pipeline has the same structure.** The core data treatment — loading sources, joining tables, casting types, validating constraints — is common across models. Only the custom transformation logic (derived fields, business rules, filters) is unique to each model developer (MD).

**Both IMPACT and the Python-based alternative share this architecture:**

| Layer | Who owns it | What it does |
|---|---|---|
| **Core engine** | Platform team | Data pulling, joining, casting, validation, entity building |
| **Custom logic** | Model developer (MD) | Derived fields, business rules, model-specific filters |

**The question is: how should MDs define their custom logic?**

- **Python-based protocol** — MDs write Python code on top of a standardized data pull
- **IMPACT (config-driven)** — MDs declare their custom logic in YAML config

---

## Slide 3: Two Approaches, Side by Side

**Same task: add utilization rate, risk score, and risk grade to a Facility entity.**

### Python-Based Protocol

```python
# Core engine provides standardized data pull
data = platform.load("facility", snapshot_date="2025-12-31")

# MD writes Python for custom transformations
data["utilization_rate"] = data["outstanding_balance"] / data["commitment_amount"]
data["risk_score"] = data.apply(
    lambda row: min(row["utilization_rate"] * 1.2, 1.0), axis=1
)
data["risk_grade"] = data["risk_score"].apply(
    lambda x: "HIGH" if x > 0.5 else "LOW"
)

# MD also handles type casting, null filling, validation, filtering...
data["utilization_rate"] = data["utilization_rate"].astype("float64")
assert (data["utilization_rate"] >= 0).all() & (data["utilization_rate"] <= 1).all()
```

### IMPACT Config

```yaml
fields:
  - name: utilization_rate
    derived: "outstanding_balance / commitment_amount"
    dtype: float64
    validation_type: [range]
    validation_rule:
      range: [0.0, 1.0]

  - name: risk_score
    derived: "lambda row: min(row['utilization_rate'] * 1.2, 1.0)"
    dtype: float64

  - name: risk_grade
    derived: "lambda row: 'HIGH' if row['risk_score'] > @risk_threshold else 'LOW'"
    dtype: str
```

**Same outcome.** But with config: type casting, validation, null handling, and error diagnostics are built in — not left to the MD to implement.

---

## Slide 4: Why Config Over Code

**Config doesn't remove flexibility — it removes boilerplate and adds guardrails.**

### What MDs no longer need to handle manually

- **Type casting** — declared as `dtype: float64`, engine handles it
- **Null filling** — declared as `fill_na: 0.0`, applied after cast automatically
- **Validation** — declared inline per field, with severity levels (error halts, warning logs)
- **Filtering** — `pre_filters` and `post_filters` with clear before/after semantics
- **Error diagnostics** — row-level failure details generated automatically by the engine

### What stays the same

- MDs still define their own derived fields and business rules
- MDs still control parameters, filters, and validation rules
- Complex logic can still be written in Python (function references — covered later)

### The pipeline behind every config

```
Load  →  Join  →  Pre-Filter  →  Transform  →  Post-Filter  →  Validate  →  Build
(multi-   (1:1 &    (raw          (2-pass       (processed     (error/      (typed
 source)   1:many)   columns)      fields)       fields)        warn)        entities)
```

Every stage is declarative, composable, and produces immutable outputs.

---

## Slide 5: What the Config System Gives You

**6 key capabilities**

### 1. Multi-Source Ingestion
Snowflake, SQLite, Parquet, CSV, Excel — mix freely. Parameterized queries with `${VAR}` and `{param}` syntax. Define connections once, reuse across sources.

### 2. Nested Entities
One-to-many joins produce sub-entities automatically. Collateral items become `list[Collateral]` on the parent — fully typed with their own config and validation.

### 3. Inline Validation
5 built-in types: `not_null`, `unique`, `range`, `expression`, `custom` — per-field, with severity levels. Errors halt the pipeline; warnings log and continue.

### 4. Expressions & Functions
Simple math stays in YAML. Row-wise lambdas for moderate complexity. Full Python function references for complex logic — with IDE support and unit testability.

### 5. Custom Spaces
Overlay model-specific fields onto a shared entity. Each team owns their config; the engine composes them at runtime. No copy-paste, no conflicts.

### 6. Typed Output
Dynamic dataclasses built from your field definitions. Primary keys, optional fields, nested types — all generated from YAML. Not raw DataFrames.

---

## Slide 6: Fail Fast — Before Data Ever Loads

**Parse-time validation catches mistakes in seconds, not after a 10-minute Snowflake query.**

In a Python-based approach, syntax errors and type mismatches surface only at runtime — after data has already been loaded. With IMPACT, config is fully validated before any data is touched.

### Caught at Config Parse Time

- **Schema violations** — missing required fields, bad types, unknown keys (Pydantic v2)
- **Expression syntax errors** — mismatched parentheses, bad lambdas compiled before any data loads
- **Field conflicts** — duplicate names, source/derived both set on one field
- **Unsafe packages** — `os`, `sys`, `subprocess` blocked as expression namespaces

### Example

```yaml
fields:
  - name: risk_score
    derived: "(balance / amount"   # ← missing closing parenthesis
    dtype: float64
```

```
ConfigError: Syntax error in derived expression for field 'risk_score':
  unexpected EOF while parsing (<field:risk_score:derived>, line 1)

# No data was loaded.
# No Snowflake query was executed.
# Feedback in under 1 second.
```

---

## Slide 7: Addressing the #1 Concern — Debuggability

**The biggest concern with config-driven systems: "How do I debug when something goes wrong?"**

With a Python-based approach, MDs can set breakpoints, print intermediate values, and step through code line by line. Fair point — and we took it seriously.

**We built three debug features specifically to match or exceed that experience:**

### Feature 1: `pipeline.explain()`
Preview the full execution plan — sources, joins, field processing order, validations — **without loading a single row of data**. Instant feedback on what will happen.

### Feature 2: `debug=True` Snapshots
Save a DataFrame snapshot at every stage boundary. Compare before/after at each step. Inspect interactively in notebooks — just like you would with intermediate variables in Python.

### Feature 3: `debug_context` on Exceptions
When errors occur, the exception carries the live DataFrame, failing field, expression, parameters, and stage — right on the exception object. No re-run needed.

---

## Slide 8: Debug Feature 1 — `pipeline.explain()`

**See your entire execution plan at a glance — before any data is touched.**

```python
pipeline = EntityPipeline("configs/demo/facility_demo.yaml")
print(pipeline.explain())
```

```
╭─ Execution Plan: Facility v1.0 ─────────────────────────────────╮
│                                                                  │
│  Parameters:  snapshot_date = 2025-12-31                         │
│               active_product = TERM_LOAN                         │
│                                                                  │
│  Sources:     facility_main  (sqlite) [PRIMARY]                  │
│               collateral     (parquet)                           │
│               rating_overrides (csv)                             │
│                                                                  │
│  Joins:       facility_main ← collateral  (left, 1:many)        │
│               facility_main ← rating_overrides  (left, 1:1)     │
│                                                                  │
│  Pre-filters: 1 filter  │  commitment_amount > 0                │
│                                                                  │
│  Fields:      Pass 1 (source): 10 fields                        │
│               Pass 2 (derived): 4 fields                        │
│               Temp fields: 1 (dropped before build)             │
│                                                                  │
│  Post-filters: 1 filter  │  product_category == @active_product │
│                                                                  │
│  Validations: 6 field-level  │  0 global                        │
│                                                                  │
│  Execution:   1. Load 3 sources                                  │
│               2. Execute 2 joins                                 │
│               3. Apply pre-filters                               │
│               4. Process source fields (Pass 1)                  │
│               5. Process derived fields (Pass 2)                 │
│               6. Apply post-filters                              │
│               7. Run validations                                 │
│               8. Build Facility entities                         │
╰──────────────────────────────────────────────────────────────────╯
```

---

## Slide 9: Debug Feature 2 — Stage-by-Stage Snapshots

**Run with `debug=True` and inspect the DataFrame at every stage boundary.**

Each snapshot is an independent copy — safe to modify, compare, and explore in notebooks.

| Snapshot Key | What It Captures |
|---|---|
| `after_join` | All sources combined |
| `after_pre_filters` | Rows filtered by raw columns |
| `after_source_fields` | Pass 1 complete — copied, cast, filled |
| `after_derived_fields` | Pass 2 complete — derived fields computed |
| `after_post_filters` | Final row selection applied |
| `after_validation` | All validations passed |
| `final` | Entity-ready — temp fields dropped |

```python
result = pipeline.run(debug=True)

# Inspect any stage interactively — just like intermediate variables in Python
result.snapshots["after_join"].shape
# → (1500, 12)  all sources merged

result.snapshots["after_pre_filters"].shape
# → (1200, 12)  300 rows filtered out

result.snapshots["final"].shape
# → (800, 13)  post-filtered, temp dropped

# Compare across stages
before = result.snapshots["after_source_fields"]
after = result.snapshots["after_derived_fields"]
new_cols = set(after.columns) - set(before.columns)
# → {'utilization_rate', 'days_to_maturity', ...}

# Find which rows were filtered
pre = result.snapshots["after_join"]
post = result.snapshots["after_pre_filters"]
dropped = pre[~pre.index.isin(post.index)]
```

---

## Slide 10: Debug Feature 3 — Debug Context on Exceptions

**When something fails, the exception carries everything you need to diagnose it — no re-runs required.**

```python
try:
    result = pipeline.run(debug=True)
except TransformError as e:
    ctx = e.debug_context

    ctx.dataframe     # The live DataFrame at point of failure (1200 rows x 14 cols)
    ctx.field         # 'utilization_rate'
    ctx.expression    # 'outstanding_balance / commitment_amount'
    ctx.parameters    # {'snapshot_date': '2025-12-31', ...}
    ctx.stage         # 'derived_fields'
```

### Row-Level Diagnostics

On top of debug context, errors always include the exact rows and values that caused the failure:

```
# Cast failure — shows un-castable values
TransformError: Field 'interest_rate': cannot cast to float64.
  Failing samples (3 of 47):
    row 12: 'N/A'
    row 89: 'pending'
    row 203: ''

# Lambda failure — shows failing row data
TransformError: Field 'days_to_maturity': lambda failed on row 42.
  Row data: {'maturity_date': NaT, 'origination_date': 2024-01-15}

# Sub-entity — includes parent context
ValidationError: Sub-entity 'Collateral' validation failed
  for parent row (facility_id='FAC-1042'):
  collateral_value: 3 null values
```

---

## Slide 11: Custom Spaces — Compose, Don't Copy

**Teams add their own fields without touching the primary config. The engine merges them into isolated "spaces" on the entity.**

In a Python-based approach, adding team-specific fields means modifying shared code or maintaining separate scripts. With IMPACT, each team owns their own config file.

### Custom config (owned by risk team):

```yaml
# custom_risk.yaml
parameters:
  risk_threshold: 0.5

fields:
  - name: risk_score
    derived: "lambda row: min(row['utilization_rate'] * 1.2, 1.0)"
    dtype: float64
  - name: risk_grade
    derived: "lambda row: 'HIGH' if row['risk_score'] > @risk_threshold else 'LOW'"
    dtype: str
```

### Usage:

```python
result = EntityPipeline(
    config="facility.yaml",
    custom={"risk": "custom_risk.yaml"},
).run()

entity.facility_id          # primary field — direct attribute
entity.risk_score           # space field — transparent access (unique name)
entity.spaces["risk"]       # all risk fields as a dict
entity.primary_only()       # entity without any custom spaces
```

### Two Filter Modes:

| Mode | Behavior | Use Case |
|---|---|---|
| `"filter"` (default) | Custom filters reduce the dataset | Single-model development |
| `"flag"` | Full dataset preserved; filters become row selectors | Composite engines — all models across full portfolio |

---

## Slide 12: When YAML Isn't Enough — Escape Hatches

**Config handles the majority of use cases. For complex logic, move to real Python functions with full IDE support — the best of both worlds.**

### Derived Function References

```yaml
# In YAML config — reference a Python function
fields:
  - name: risk_score
    derived:
      function: "myproject.risk.compute_score"
      kwargs:
        threshold: 0.5
        snapshot: "@snapshot_date"
    dtype: float64
```

```python
# myproject/risk.py — real Python with full IDE support
def compute_score(row: pd.Series, threshold: float = 0.5, snapshot: str = None) -> float:
    """Syntax highlighting, type checking, breakpoints — all work."""
    util = row["utilization_rate"]
    age = (pd.Timestamp(snapshot) - row["origination_date"]).days
    return min(util * (1 + age / 365), 1.0)
```

### What you get:
- **Syntax highlighting** and autocomplete in your IDE
- **Breakpoints** — step through row-by-row in a debugger
- **Type checking** — mypy, pyright, IDE type hints all work
- **Unit testable** — test the function independently with mock data
- **Linting** — ruff, flake8, pylint all run normally
- **Git diff** — meaningful diffs on logic changes, not string edits

---

## Slide 13: Summary — Config-Driven vs. Python-Based Protocol

| Concern | Python-Based Protocol | IMPACT Config |
|---|---|---|
| **How MDs define logic** | Write Python code | Declare in YAML (or reference Python functions) |
| **Type casting & null handling** | MD codes it manually | Declared per field — engine handles it |
| **Validation** | MD writes asserts or custom checks | Built-in: not_null, unique, range, expression, custom |
| **Error messages** | Standard Python tracebacks | Row-level diagnostics: exact values, parent keys |
| **Debugging** | Breakpoints and print statements | explain(), snapshots, debug_context — plus function refs for breakpoints |
| **Config errors** | Runtime exceptions after data loads | Caught at parse time — before data loads |
| **Multi-team composition** | Shared code or separate scripts | Custom spaces — isolated configs, merged at runtime |
| **Typed output** | MD builds data structures | Dynamic dataclasses generated from config |
| **Complex logic** | Native Python — full IDE support | Function references — same IDE support, declared in config |

---

## Slide 14: Closing

**Define Your Entity. Run the Pipeline. Trust the Output.**

IMPACT gives you the speed of configuration with the power and debuggability of code.

```python
from impact.entity.pipeline import EntityPipeline

result = EntityPipeline("facility.yaml").run()
result.entities    # → [Facility(...), Facility(...), ...]
```

```
pip install impact[all]
```
