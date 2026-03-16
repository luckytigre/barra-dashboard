# Module Inventory

Date: 2026-03-16
Status: Inventory snapshot
Owner: Codex

## Backend Top-Level Areas

| Area | Owns Today | Current Problems |
| --- | --- | --- |
| `backend/api` | FastAPI routes, auth, router registration, some response shaping | Route thinness is inconsistent; some routes still assemble domain truth inline |
| `backend/analytics` | Serving refresh pipeline, diagnostics, payload contracts, reusable risk/exposure views | `pipeline.py` is too broad; health diagnostics are mixed near lighter serving work |
| `backend/data` | SQLite/Neon adapters, query surfaces, durable model/serving/runtime state | `core_reads.py` and `model_outputs.py` are now thin facades, but they still expose internal composition helpers that deserve restraint |
| `backend/orchestration` | Profile-driven rebuild/refresh job orchestration | Much improved after decomposition, but `run_model_pipeline.py` is still the central integration shell |
| `backend/risk_model` | Factor-model math, regression, raw-history generation, covariance/specific risk | Some workflow/storage behavior still leaks into domain packages |
| `backend/services` | Application services, Neon sync/mirror, holdings mutations, refresh manager, route-facing assembly | `services/` mixes application surfaces with heavy infrastructure and workflow helpers |
| `backend/universe` | Universe bootstrap, ESTU, security master sync | Clearer than many areas, but still tied into orchestration and source-table workflows |
| `backend/portfolio` | Portfolio models and position storage helpers | Small and relatively clear |
| `backend/scripts` | Operator and migration CLI entrypoints | Some scripts directly import deep internals and duplicate workflow wiring |
| `backend/tests` | Behavioral and regression coverage | Strong test footprint, but some tests still monkeypatch route internals rather than stable service seams |

## Frontend Top-Level Areas

| Area | Owns Today | Current Problems |
| --- | --- | --- |
| `frontend/src/app` | Page entrypoints and API proxies | Some pages still perform local payload joining |
| `frontend/src/features` | Feature-specific UI modules | Generally good; some feature contracts still depend on broad shared types |
| `frontend/src/components` | Reusable UI pieces | Mostly fine |
| `frontend/src/lib` | API helpers, types, truth helpers, refresh logic | `types.ts` is too large; some page contracts are still implicit |
| `frontend/src/hooks` | Small client hooks | Fine |

## High-Value Modules

### `backend/orchestration/run_model_pipeline.py`

Appears to own:
- stage planning
- stage execution
- Neon readiness and workspace behavior
- artifact writing
- CLI entry semantics

Problems:
- still the main orchestration integration hub, though profile metadata, stage-date planning, stage-loop execution, stage implementation, post-run finalization, and runtime-policy helpers have been extracted
- hard to test components independently
- imports many layers directly

### `backend/analytics/pipeline.py`

Appears to own:
- refresh context loading
- payload reuse policy
- risk-engine truth resolution
- payload building
- publish/persist coordination

Problems:
- still the main refresh coordinator after extracting context, reuse, publish, and persistence helpers
- hard to isolate a single serving surface

### `backend/analytics/services/cache_publisher.py`

Appears to own:
- staged serving payload creation
- snapshot-local cache staging
- refresh metadata assembly
- health-diagnostics carry-forward and publication coordination

Problems:
- much improved after helper extraction, but still the main serving-stage coordinator
- remains a key integration surface where payload assembly and publication order meet

### `backend/data/core_reads.py`

Appears to own:
- Neon/local backend selection
- SQL translation
- source-date and exposure queries
- local latest-price cache management

Problems:
- now a stable facade over `core_read_backend.py`, `source_dates.py`, and `source_reads.py`
- now has callers and tests using the lower-level module seams rather than patching facade-private helpers

### `backend/data/model_outputs.py`

Appears to own:
- model-output schema
- factor-return extraction
- metadata shaping
- Neon-first and local mirror persistence

Problems:
- now a stable facade over `model_output_schema.py`, `model_output_state.py`, `model_output_payloads.py`, and `model_output_writers.py`
- now has tests driven by configuration and lower-level helper seams rather than facade-private monkeypatch hooks

### `backend/services/neon_mirror.py`

Appears to own:
- schema application
- broad mirror sync
- parity audit
- prune behavior
- bounded comparison helpers

Problems:
- very large infrastructure module
- multiple operational roles combined

### `backend/analytics/health.py`

Appears to own:
- deep model diagnostics and statistical health studies

Problems:
- heavy diagnostics engine in a flat package with lighter serving code
- operationally important but hard to reason about incrementally

### `frontend/src/lib/types.ts`

Appears to own:
- broad frontend contract types for many pages

Problems:
- now a barrel over domain-specific type modules
- still encourages broad import surfaces until callers gradually narrow their imports

## Route Surfaces

| Route | Current State | Notes |
| --- | --- | --- |
| `operator.py` | Improved; now delegates to service | Still carries temporary compatibility seam for older tests |
| `data.py` | Improved; now delegates to service | Good model for future route cleanup |
| `exposures.py` | Better | Serving-payload assembly is extracted; history resolution still lives here |
| `risk.py` | Better | Serving-payload assembly is extracted |
| `portfolio.py` | Better | Serving-payload assembly is extracted; what-if routes still belong here for now |
| `holdings.py` | Better | Uses holdings service but still route-heavy in spots |
| `refresh.py` | Operational entrypoint | Needs to stay thin and profile-aware |
| `health.py` | Thin | Good |
| `universe.py` | Mixed | Needs later review during route pass |

## Infrastructure / Adapter Surfaces

| Module | Role | Problem |
| --- | --- | --- |
| `backend/data/neon.py` | Neon connection helper | Fine |
| `backend/data/sqlite.py` | local cache helpers | Narrow, but many callers still depend on it directly |
| `backend/data/serving_outputs.py` | durable serving payload store | Good surface |
| `backend/data/runtime_state.py` | narrow runtime truth surface | Good direction; still needs parity/read-path hardening over time |
| `backend/data/job_runs.py` | job run store | Useful, but participates in overlapping runtime truth |

## Scripts / Entrypoints

Three distinct categories already exist but are not documented as separate categories:

1. App entrypoints
   - `backend/main.py`
   - frontend app pages

2. Workflow entrypoints
   - `backend/scripts/run_model_pipeline.py`
   - refresh API routes

3. Operational utilities
   - `backend/scripts/neon_*`
   - `backend/scripts/backfill_*`
   - `scripts/local_app/*`

The architecture should preserve this distinction more explicitly.
