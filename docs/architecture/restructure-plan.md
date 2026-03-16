# Repository Restructure Plan

Date: 2026-03-16
Status: Active master plan
Owner: Codex

## Overview

This is the living master plan for the repository restructuring program.

It tracks:
- current architectural goals
- agreed target state
- implementation sequence
- completed work
- in-progress work
- deferred work
- known risks
- next steps

This plan complements:
- [current-state.md](./current-state.md)
- [module-inventory.md](./module-inventory.md)
- [target-architecture.md](./target-architecture.md)
- [dependency-rules.md](./dependency-rules.md)
- [refactor-roadmap.md](./refactor-roadmap.md)

## Architectural Goals

1. Eliminate redundancy where it obscures ownership.
2. Centralize truth assembly behind explicit service modules.
3. Make entrypoints thin.
4. Separate workflows from reusable routines.
5. Reduce oversized mixed-responsibility modules.
6. Keep durable storage surfaces explicit.
7. Keep docs aligned with the code as changes land.
8. Avoid introducing generic framework layers.

## Agreed Target State

The agreed target state is:
- thin API routes
- explicit application services
- explicit orchestration/job modules
- data adapters split by durable surface
- domain/compute modules isolated from transport concerns
- one architecture documentation package
- one active restructuring tracker

See [target-architecture.md](./target-architecture.md) for the canonical target shape.

## Implementation Phases

### Phase 1: Architecture Docs And Dashboard Truth Extraction

Status: `completed`

Scope:
- create the `docs/architecture/` package
- document current state and target state
- extract dashboard-serving payload assembly out of remaining route files

### Phase 2: Orchestration Decomposition

Status: `completed`

Scope:
- split profile metadata, stage planning, execution, and post-run publication

### Phase 3: Refresh Pipeline Decomposition

Status: `completed`

Scope:
- split refresh context, reuse policy, payload builders, and publish coordinator

### Phase 4: Storage Surface Cleanup

Status: `completed`

Scope:
- split `core_reads.py`
- split `model_outputs.py`

### Phase 5: Frontend Contract Cleanup

Status: `completed`

Scope:
- reduce oversized shared type surfaces
- keep freshness semantics centralized

### Phase 6: Documentation Consolidation

Status: `completed`

Scope:
- reduce plan sprawl and clarify archival status

## Completed Items

- extracted `operator_status_service.py`
- extracted `data_diagnostics_service.py`
- thinned `backend/api/routes/operator.py`
- thinned `backend/api/routes/data.py`
- introduced this `docs/architecture/` documentation package
- extracted `dashboard_payload_service.py`
- thinned `backend/api/routes/exposures.py`
- thinned `backend/api/routes/risk.py`
- thinned `backend/api/routes/portfolio.py`
- extracted `backend/orchestration/profiles.py`
- moved profile and stage-selection metadata out of `run_model_pipeline.py`
- updated `refresh_manager` and `operator_status_service` to depend on orchestration profile metadata directly instead of the full orchestrator
- extracted `backend/orchestration/stage_planning.py`
- extracted `backend/orchestration/post_run_publish.py`
- reduced `run_model_pipeline.py` by moving as-of/session planning and Neon post-run publication helpers behind orchestration-local modules
- extracted `backend/orchestration/stage_execution.py`
- extracted `backend/orchestration/finalize_run.py`
- moved the stage loop and post-stage finalization flow out of `run_model_pipeline.py` while preserving `_run_stage` as the compatibility seam
- extracted `backend/orchestration/stage_runner.py`
- moved `_run_stage` implementation into an orchestration-local stage runner while preserving `run_model_pipeline._run_stage` as the test and compatibility seam
- extracted `backend/analytics/refresh_context.py`
- extracted `backend/analytics/reuse_policy.py`
- extracted `backend/analytics/publish_payloads.py`
- extracted `backend/analytics/refresh_persistence.py`
- moved refresh-context policy, universe-loadings reuse rules, publish-only payload stamping, and durable refresh persistence coordination out of `backend/analytics/pipeline.py`
- split `backend/data/core_reads.py` into a thin facade over:
  - `backend/data/core_read_backend.py`
  - `backend/data/source_dates.py`
  - `backend/data/source_reads.py`
- split `backend/data/model_outputs.py` into a thin facade over:
  - `backend/data/model_output_schema.py`
  - `backend/data/model_output_state.py`
  - `backend/data/model_output_payloads.py`
  - `backend/data/model_output_writers.py`
- split `frontend/src/lib/types.ts` into domain-specific contract modules behind a stable barrel surface
- extracted `backend/analytics/refresh_metadata.py`
- extracted `backend/analytics/health_payloads.py`
- reduced `backend/analytics/services/cache_publisher.py` by moving serving-source-date assembly, model-sanity report logic, eligibility-summary loading, and health carry-forward/reuse helpers into dedicated analytics modules
- removed truly unused compatibility-style wrappers from `backend/analytics/pipeline.py` and `backend/analytics/services/cache_publisher.py`
- extracted `backend/orchestration/runtime_support.py`
- moved serving-refresh risk-cache policy, broad Neon-mirror profile policy, temporary runtime path overrides, covariance serialization, and core-cache reset helpers out of `run_model_pipeline.py`
- migrated tests away from legacy monkeypatch seams in `run_model_pipeline.py`, `core_reads.py`, and `model_outputs.py` onto module-local helper seams and configuration-driven behavior
- clarified `docs/architecture/restructure-plan.md` as the active repository-structure tracker and demoted older top-level cleanup plans to completed or subordinate context

## In-Progress Items

- none for the original restructure scope

## Deferred Items

- full Neon-native rebuild engine
- distributed refresh locking
- broad frontend state-management changes
- generalized repository abstraction
- deeper decomposition of `backend/services/neon_mirror.py`, `backend/analytics/health.py`, and large risk-model workflow modules
- removal or formalization of the deliberate `_run_stage` orchestration test seam if stage-level tests are later rewritten around `backend/orchestration/stage_runner.py`

## Known Risks

1. Some modules are still large enough that even good refactors can have wide blast radius.
2. Several truth surfaces remain transitional because Neon migration is still in progress.
3. `run_model_pipeline.py` is much smaller, but it remains the central integration shell for the rebuild workflow.
4. Over-refactoring could make a hobby tool harder to navigate.

## Open Questions

1. How far should `services/` be subdivided before the package becomes harder to navigate than today?
2. Which parts of `analytics/health.py` should remain near serving code versus move behind a more explicit diagnostics package?
3. When should the deliberate `_run_stage` seam be retired versus kept as the stable stage-level test harness?

## Next Recommended Steps

1. Treat `docs/architecture/restructure-plan.md` as the active repository-structure tracker; use the remaining top-level plan docs as domain-specific execution history or focused operating notes.
2. Revisit `backend/services/neon_mirror.py`, `backend/analytics/health.py`, and large risk-model workflow modules only when there is a concrete behavior or ownership win.
3. Keep the current shape stable long enough for ordinary feature work before deciding whether any further package churn is justified.
