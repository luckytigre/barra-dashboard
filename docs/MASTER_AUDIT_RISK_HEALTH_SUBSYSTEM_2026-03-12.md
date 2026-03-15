# Master Audit: Risk, Health, Persistence, and Serving

Date: 2026-03-12

## Post-Implementation Status

This document captures the pre-remediation audit state of the subsystem. The implementation work described by this audit has now been applied in the codebase, so the findings below should be read as the original baseline, not the current live design.

Resolved in code:

- country is no longer forced into the industry sum-to-zero constraint
- the live style set no longer includes a standalone `Value` factor; the current runtime serves a 45-factor model
- `r_squared` is now objective-consistent and unclipped
- robust `t_stat` and `robust_se` fields are now computed and persisted from the estimator layer
- raw residuals and model residuals are now separated for specific-risk use
- the beta market proxy was replaced with a lagged-cap-weighted proxy with price-floor filtering
- Health diagnostics are now computed from staged inputs, persisted on every refresh, and served from durable current payloads first
- Health Section 4 now uses the full covariance payload from `risk_engine_cov`
- durable SQLite factor-return writes now replace stale date slices instead of only appending from the latest date forward
- Neon factor-return sync and parity now check inference fields and sampled value equality, not just counts/date bounds
- `/api/exposures` and the affected frontend surfaces are now hardened against the payload-shape mismatches called out in the audit

Partially resolved or still open:

- inference currently ships as HC1; HC2 / HC3 evaluation and leverage diagnostics remain follow-up work
- winsorization governance remains configurable and improved, but not fully instrumented with all of the diagnostic metadata proposed below
- durable-serving publish and cache-snapshot publish are still separate stores, so there is still no single atomic cross-store commit boundary
- route-by-route contract enforcement is still more fragmented than ideal
- process-local refresh locking remains a known architectural weakness

Interpretation note:

- the detailed findings below remain useful as the rationale for the remediation
- the remediation status above is the current source of truth for whether each issue is still outstanding

## Purpose

This document is a deep audit of the risk-model, health-diagnostics, persistence, serving, and frontend contract stack in this repository.

The goal was not to do a narrow “find one bug” review. The goal was to inspect the system from multiple angles:

- statistical correctness
- implementation correctness
- data lineage
- cache and snapshot semantics
- durable persistence
- Neon sync and parity
- API and frontend semantics
- code bloat and maintainability
- robustness under refreshes, backfills, schema drift, and stale data

## Audit Method

This audit combined:

- 5 independent primary audits
- 5 independent oversight audits that challenged or narrowed the primary claims
- direct runtime inspection of `backend/runtime/cache.db` and `backend/runtime/data.db`
- direct payload inspection from live cache rows and published snapshots
- targeted local test execution

Primary audit areas:

- estimator math and data preparation
- cache, persistence, and Neon sync
- health diagnostics and snapshot/publish pipeline
- API and frontend contracts
- architecture, modularity, and test coverage

Oversight audits then re-checked the strongest claims to separate true defects from overstatements.

## Scope

Most heavily audited files and subsystems:

- `backend/risk_model/wls_regression.py`
- `backend/risk_model/daily_factor_returns.py`
- `backend/risk_model/descriptors.py`
- `backend/analytics/health.py`
- `backend/analytics/services/cache_publisher.py`
- `backend/analytics/pipeline.py`
- `backend/orchestration/run_model_pipeline.py`
- `backend/data/sqlite.py`
- `backend/data/model_outputs.py`
- `backend/services/neon_mirror.py`
- `backend/services/neon_stage2.py`
- `backend/scripts/neon_parity_audit.py`
- `backend/api/routes/health.py`
- `backend/api/routes/exposures.py`
- `backend/data/history_queries.py`
- `frontend/src/features/health/*`
- `frontend/src/components/ExposureBarChart.tsx`
- `frontend/src/components/FactorDrilldown.tsx`
- `frontend/src/lib/types.ts`

## Direct Runtime Facts Verified

- `daily_factor_returns` in `cache.db` spans `2021-01-11` through `2026-03-03`, with `1291` dates and `59386` total rows.
- Latest cache dates currently have `46` factor rows per date.
- `model_factor_returns_daily` in `data.db` has `93` factor rows on `2026-03-03`.
- The active cache snapshot is `model_run_20260312T164919Z`.
- That active snapshot has `risk` and `refresh_meta` rows, but no `health_diagnostics` row.
- `cache_get("health_diagnostics")` therefore falls back to the older base-cache row.
- The served `health_diagnostics` payload says `as_of = 2026-03-03` and still contains the proxy-t-stat note.
- The current `health_diagnostics.section1.r2_series` starts at `2016-03-11`, even though `daily_factor_returns` currently starts at `2021-01-11`.
- The current snapshot `risk` payload has `46` factor details, `15` covariance factors in the frontend matrix, and risk shares of roughly `country=2.47`, `industry=33.04`, `style=16.85`, `idio=47.65`.
- The current `health_diagnostics` payload is materially stale versus the current serving risk payload.

## Tests Executed

I directly ran this local slice:

- `53 passed in 13.86s`

Command scope:

- `test_cache_snapshot_publish.py`
- `test_serving_output_route_preference.py`
- `test_cache_publisher_service.py`
- `test_cuse4_priority_efficiency.py`
- `test_audit_fixes.py`
- `test_history_queries.py`
- `test_universe_loadings_service.py`
- `test_operating_model_contract.py`
- `test_api_golden_snapshots.py`
- `test_operator_status_route.py`

Independent reviewer agents also reported passing targeted slices in the areas they audited:

- `21 passed`
- `33 passed`
- `41 passed`
- `76 passed`

The conclusion from the test evidence is not “the system is healthy.” The conclusion is: many important failure modes are semantic, lineage-related, or coverage gaps, not currently failing tests.

## Executive Summary

The subsystem has multiple real defects, not just technical debt.

The most serious findings are:

- The phase-A regression constraint is mathematically wrong when `Country: US` is present. Country is being forced into the same sum-to-zero block as industries, which shifts every industry return while leaving fit unchanged.
- `r_squared` is computed with a weight convention that does not match the solved objective, and then negative values are clipped away.
- Winsorization is censoring not only factor-return estimation but also the residual history later used to forecast specific risk.
- The `Beta` style descriptor is built from an equal-weight market proxy that can be materially contaminated by extreme low-price names.
- The live Health route is currently stale because `health_diagnostics` is not in the active published snapshot and falls back to an older base-cache payload.
- Even if health is recomputed on a full refresh, the current staging flow can still compute it against the previous live payloads instead of the just-staged ones.
- Health Section 4 forecast-vs-realized is not using full model covariance. It is using a style-only common-factor proxy plus specific risk.
- Durable SQLite and Neon can both preserve stale historical factor-return values after a cache recompute.
- Durable serving payload publish and cache-snapshot publish are split across stores with no atomic boundary.
- `/api/exposures` accepts incomplete payload shapes that the frontend can crash on.
- The frontend and backend semantics have drifted in several places, including weight explanations, chart labeling, and health payload typing.

In short:

- the math is not fully correct
- residual history used for specific-risk modeling is also being censored upstream
- the serving snapshot is only closed for keys actually staged into it
- persistence does not fully replace corrected history
- parity is too weak
- health diagnostics are currently untrustworthy

## Severity-Ranked Findings

## Critical Findings

### 1. Phase-A structural constraint is wrong when country is present

What is happening:

- `daily_factor_returns.py` builds the structural block as `Country: US + industry dummies`.
- `wls_regression.py` then applies one sum-to-zero transform to that entire block.

What this means:

- the implemented invariant is effectively:
  - `sum(industry returns) + country return = 0`
- not:
  - `sum(industry returns) = 0`

Why it matters:

- fitted values do not change
- residuals do not change
- but the stored factor-return series are semantically wrong
- covariance, attribution, and history built from those stored returns are therefore also wrong

Direct verification:

- on `2026-03-03`, `Country: US = 0.0171548`
- the sum of all non-style factor returns is effectively zero
- therefore industry-only returns sum to `-Country: US`

This contradicts the local spec in [cUSE4_engine_spec.md](/Users/shaun/Dropbox%20(Personal)/040%20-%20Creating/barra-dashboard/docs/specs/cUSE4_engine_spec.md#L311).

### 2. Published snapshot reads are porous for missing keys

What is happening:

- `cache_get()` prefers the active snapshot row
- but if that row is missing, it silently falls back to the raw base cache row

Why it matters:

- the published snapshot is only closed for keys that were restaged in that run
- any omitted key leaks mutable raw state into a supposedly published view

Direct verification:

- active snapshot: `model_run_20260312T164919Z`
- there is no `__snap__:model_run_20260312T164919Z:health_diagnostics`
- `cache_get("health_diagnostics")` therefore returns an older base-cache payload

The stronger corrected statement is:

- published snapshot reads are closed only for keys that actually exist under the active snapshot
- missing keys leak to raw rows

### 3. Health diagnostics are currently stale in live runtime

What is happening:

- light refresh usually skips recomputing `health_diagnostics`
- `/api/health/diagnostics` reads `cache_get("health_diagnostics")`
- the active March 12 snapshot does not contain a health row

Why it matters:

- the route is serving an older health document
- users are seeing stale diagnostics while other serving payloads are current

Direct verification:

- active snapshot is March 12
- current `health_diagnostics.as_of` is `2026-03-03`
- `health.section1.r2_series` starts in 2016
- current serving risk shares are roughly `country=2.47`, `industry=33.04`, `style=16.85`, `idio=47.65`
- current stored health variance split is effectively `idio=100%`, `industry=0%`, `style=0%`
- current stored health forecast vols are `0.0`

This is not a small freshness bug. It invalidates the current Health page as a source of truth.

### 4. Full refresh can still publish internally stale health

What is happening:

- `compute_health_diagnostics()` is called during staging
- but it reads `risk`, `portfolio`, and `universe_loadings` through `cache_get()`
- at that point the active snapshot pointer still references the previous live snapshot

Why it matters:

- even `health_refreshed = true` would not guarantee same-run parity with the newly staged payloads
- the problem is not only light-refresh reuse; the full-refresh path is also architecturally wrong today

This issue was added after the second-round independent reviews.

## High Findings

### 5. `r_squared` does not match the regression objective, and bad days are hidden

What is happening:

- regression solves on `sqrt(mcap)`-premultiplied data
- which means the minimized objective is effectively `mcap`-weighted SSE
- but `r_squared` is computed using `sqrt(mcap)` weights again
- then it is clipped into `[0,1]`

Why it matters:

- reported fit is not the fit of the solved objective
- negative-fit days are hidden as `0.0`

Direct verification:

- on `2026-03-03`, stored `R² = 0.362172`
- cap-weighted `R²` for the same fit is about `0.647911`
- on `2026-02-13`, unclipped current-weight `R² = -0.025085`
- cache stores that as `0.0`
- live cache contains `644` factor rows with `r_squared = 0.0`

### 6. Orthogonalization rules do not match the documented model

What is happening:

- code uses `industry_size` orthogonalization for `Momentum`, `Short-Term Reversal`, and `Residual Volatility`
- the spec expects:
  - `Residual Volatility ⟂ (Size, Beta)`
  - `Short-Term Reversal ⟂ Momentum`

Why it matters:

- canonicalized exposures are not the documented model exposures
- factor-return interpretation and factor covariance are affected

Direct verification on the latest local cross-section:

- `Residual Volatility` still has meaningful dependence on `Beta`
- `Short-Term Reversal` is not neutralized against `Momentum` as specified

### 7. Daily return winsorization is materially aggressive

What is happening:

- daily returns are winsorized at `5% / 95%`

Why it matters:

- roughly `10%` of the regression universe is clipped every day
- winsorization materially changes fit and therefore factor-return series

Direct verification:

- across the last `59` eligible dates, about `365.8` names/day were clipped out of about `3656`
- a `1% / 99%` rule would clip about `73.9` names/day
- on `2026-03-03`, fit moved from roughly:
  - no winsorization: `0.260757`
  - 1% winsorization: `0.332921`
  - current 5% winsorization: `0.362172`

The issue is not just that 5% is “unusual.” It is materially shaping the model.

### 8. Winsorization is also censoring residual history used for specific risk

What is happening:

- returns are winsorized before estimation
- residuals from the winsorized fit are then persisted
- specific risk is later built directly from that residual cache

Why it matters:

- the winsorization choice is not confined to factor-return estimation
- it also suppresses idiosyncratic shock history used by the specific-risk model

Direct verification:

- on `2025-12-24`, `YELLQ.PK` had raw return `299.0`
- it was winsorized to about `0.031992`
- its stored residual was about `0.032039`

That is a severe understatement of idiosyncratic shock history.

### 9. The `Beta` style descriptor is contaminated by the market proxy construction

What is happening:

- `beta_raw` is built against an equal-weight stock-universe mean return
- that return series is formed from raw close-to-close simple returns

Why it matters:

- extreme low-price names can contribute an outsized share of the market proxy
- this contaminates the `Beta` style descriptor before factor-return estimation even begins

Direct verification:

- on `2025-12-24`, `YELLQ.PK` alone contributed about `8.16` percentage points to the same-day market proxy
- on `2026-01-27`, `YELLQ.PK` and `INFIQ.PK` together contributed about `8.12` points

### 10. Health Section 4 uses a style-only covariance proxy, not full model covariance

What is happening:

- the serving `risk.cov_matrix` is style-only
- health reconstructs covariance from that display object
- specific risk is then added

Why it matters:

- forecast-vs-realized is not using full factor covariance
- country and industry common-factor covariance are missing

Corrected precise wording:

- Health Section 4 uses a style-only common-factor covariance proxy plus specific risk
- not the full factor model

Direct verification:

- current serving `cov_matrix.factors` length is `15`
- current `risk.factor_details` length is `46`
- style-proxy forecast for current portfolio is about `0.1223`
- full-factor forecast from full covariance is about `0.1549`

### 11. Durable SQLite persistence leaves stale corrected history

What is happening:

- `persist_model_outputs()` only loads factor returns from `>= MAX(date)` already in durable storage
- then it uses upserts

Why it matters:

- latest date slice refreshes
- older corrected history stays stale
- shrinkage in factor sets or specific-risk rows is not deleted cleanly

This was validated both by code inspection and by the current live mismatch:

- cache latest date has `46` factor rows
- `data.db` latest date has `93`

### 12. Neon factor-return sync can succeed while older values remain stale

What is happening:

- unchanged-count drift outside the overlap window is not corrected
- parity checks only count rows and compare date bounds for factor returns

Why it matters:

- Neon can report `ok`
- parity can report `ok`
- yet older factor-return values can still be wrong

### 13. Durable serving payload publish and cache-snapshot publish are not atomic

What is happening:

- pipeline writes `serving_payload_current` first
- then separately flips the cache snapshot pointer

Why it matters:

- a crash or interruption in between can leave durable serving payloads on a new run while cache snapshot state, operator-facing snapshot pointers, and health diagnostics still reflect the prior run

This issue was added after the second-round independent reviews.

### 14. `/api/exposures` can accept shapes that crash the exposures page

What is happening:

- backend route accepts very incomplete factor payloads
- frontend assumes `drilldown` exists and is iterable

Why it matters:

- this is not just a misleading empty state
- it is a real backend/frontend contract break

## Medium Findings

### 15. `residual_vol` is a shared unweighted residual-dispersion scalar, not a factor statistic

Why it matters:

- it is stored once per date and copied onto every factor row
- it is not factor-specific
- it is not a coefficient SE
- Health currently uses it as the denominator for a fake t-stat

Direct verification:

- there is exactly one distinct `residual_vol` per date in cache

### 16. `residualize_styles=False` branch is mathematically inconsistent

Why it matters:

- if styles are estimated on raw `y`
- but residuals are still defined as `phase_a_residual - style_fit`
- final residuals are not the residuals of the solved model

Default behavior avoids this, but the branch is misleading and unsafe.

### 17. Style canonicalization is conditioned on the regression subset, not full structural ESTU

Why it matters:

- missing returns can shift means, scales, and orthogonalization
- even when exposures themselves are unchanged

### 18. Important estimator diagnostics are not persisted

Missing or not persisted:

- phase-specific ranks
- singular values
- winsorized-count metrics
- post-orthogonality audit metrics
- weighted residual moments
- leverage diagnostics

### 19. `/api/health` freshness is not serving freshness

Why it matters:

- it reports `MAX(updated_at)` over the entire cache
- not active snapshot freshness
- not current published serving freshness

### 20. Frontend contract drift is real

Examples:

- health variance split omits country fields in types and UI
- coverage UI drops `scope_note` and `expected_ticker_count`
- factor drilldown explanation says `w_i = MV_i / sum(MV)` even though backend uses gross-normalized signed weights
- exposure bar chart labels “Long/Short” even though bars are sign buckets, not position-side buckets

### 21. API contract enforcement is fragmented route-by-route

What is happening:

- `risk` validates completeness aggressively
- `portfolio` normalizes aliases and has a different route discipline
- `exposures` trusts payload shape
- `health` bypasses durable serving payloads entirely
- frontend uses a mix of global rewrites and special proxy handlers

Why it matters:

- there is no single authoritative API contract boundary
- behavior varies by route family
- this increases drift risk and makes failures harder to reason about

This issue was added after the second-round independent reviews.

### 22. Factor-history logic is duplicated

What exists:

- `/api/exposures/history`
- Health Section 3 history assembly
- drilldown consumer

This is not yet a proven user-visible semantic contradiction, but it is a drift vector.

### 23. Skipped or impossible dates are retried forever

Why it matters:

- “complete” dates are defined as intersection across factor rows, residual rows, and eligibility rows
- many skip paths only write eligibility or nothing
- these dates never become terminal

There is no skip ledger or backoff for permanently impossible dates.

### 24. Refresh invalidation is mostly date-based, not content-based

Why it matters:

- corrected historical rows with unchanged max dates can leave derived outputs stale
- this affects source-dates logic, universe reuse, factor-return cache reuse, and some backfill scripts

### 25. Process-local refresh locking is fragile

What is happening:

- refresh coordination relies on module memory and a `threading.Lock`

Why it matters:

- multi-worker or multi-process serving cannot rely on it for true exclusion or status correctness

### 26. Raw-close return construction may be injecting avoidable pathologies

What is happening:

- factor-return construction uses raw `close`, not adjusted prices
- daily returns are simple `pct_change`
- recent local data includes extreme one-day moves in low-price names

Why it matters:

- winsorization is currently acting as a major containment mechanism for raw-return pathologies
- the currently verified live problem is not split/dividend handling in the checked recent window
- the currently verified live problem is extreme simple returns from distressed OTC names that the eligibility screen admits

## Findings That Were Narrowed By Oversight

The oversight reviewers corrected several first-wave claims. These corrections matter.

- Snapshot reads are not universally broken.
  The correct statement is: snapshot reads are porous for keys missing from the active snapshot.

- The drilldown weight issue is primarily a semantic/documentation issue, not a current arithmetic mismatch in shipped values.

- The “destructive resets everywhere” framing was too broad.
  The real architectural risk is stale reuse detection and selective destructive invalidation in some critical paths.

- Health Section 4 is not purely “style-only vol.”
  It is style-only common-factor covariance plus specific risk.

- The architecture overlap is mainly between `run_model_pipeline` and `pipeline.run_refresh`, not every refresh-related module.

- The stale-health mechanism is real, but the user-facing contradiction should be stated carefully.
  The shipped Health page already frames diagnostics as a local maintenance surface that can lag serving truth.
  That does not make the architecture acceptable, but it does narrow the “frontend lie” framing.

- Durable-serving truth surfaces are not equally broken.
  `risk`, `portfolio`, `exposures`, and `universe` are on the durable-serving discipline.
  `health/diagnostics` is the outlier mutable-cache surface.

## Additional Serious Issue Found In Oversight

One oversight reviewer found another materially important issue not stressed enough in the primary pass:

- factor returns are built from raw `close`, not adjusted prices
- daily returns use simple `pct_change`
- recent local data contains extreme penny-stock moves
- winsorization is therefore doing significant damage control for raw pathologies

The replacement third re-audit added two severe points:

- winsorized residual history is feeding specific-risk forecasting, so winsorization is censoring idiosyncratic shock history as well as factor-return estimation
- the `Beta` style descriptor is itself vulnerable to contamination from the equal-weight market proxy when extreme low-price names are admitted

This may be acceptable by design, but if not, it is another source of distorted factor-return estimation.

## What Is Trustworthy Today

These pieces appear directionally trustworthy:

- the core linear algebra solve itself is stable
- decomposition totals and current serving risk shares appear internally coherent
- the pipeline can produce and publish current `risk`, `portfolio`, `exposures`, and `universe` payloads on the durable-serving path
- many orchestration and route branches are covered by tests

These pieces are not trustworthy enough today:

- Health page diagnostics
- factor-return semantics when country is present
- reported `R²`
- historical durability of factor returns in `data.db` and Neon
- parity confidence for factor returns

## Code Bloat, Modularity, and Organization Findings

Main maintainability problems:

- `health.py` is monolithic and mixes raw reads, calculations, and payload shaping
- `compute_daily_factor_returns()` is too large and mixes extraction, eligibility, canonicalization, estimation, batching, and persistence
- orchestration policy is duplicated across `run_model_pipeline` and `pipeline.run_refresh`
- portfolio-view assembly is duplicated above the shared computational kernels
- frontend types are hand-maintained and behind backend payload reality
- API contract enforcement is fragmented route-by-route
- there is no realistic frontend or end-to-end test harness covering these contracts

Main refactor direction:

- do not rewrite everything
- split by high-value seams:
  - estimator
  - diagnostics persistence
  - health section providers
  - lifecycle/recompute coordinator
  - portfolio-view assembly
  - shared contract/schema layer

## Implementation Plan

## Phase 0: Contain Current User-Facing Incorrectness

- Mark Health diagnostics as stale or local-only until the snapshot/publish issue is fixed.
- Fix `/api/exposures` contract validation or make the frontend defensive against missing `drilldown`.
- Relabel the exposure bar chart away from “Long/Short” unless it is actually bucketed by position side.
- Fix drilldown explanatory wording so weight semantics match the backend.

## Phase 1: Fix Statistical Correctness In The Core Model

- Separate country from the constrained industry block in phase A.
- Keep only industries under the sum-to-zero restriction.
- Recompute factor-return history after this fix.
- Decouple residual-history persistence for specific-risk forecasting from any winsorization policy used for factor-return estimation, or persist both raw-fit and winsorized-fit residual paths.
- Redesign the market proxy used for `beta_raw` so extreme low-price names cannot dominate the `Beta` descriptor.
- Align orthogonalization rules in `descriptors.py` with the documented model.
- Decide whether returns should be based on raw closes or adjusted closes.
- Revisit the daily winsorization level. At minimum, make it configurable and audited.

## Phase 2: Fix Diagnostics So They Match The Solved Objective

- Compute `R²` with the same effective weighting as the solved WLS objective.
- Stop clipping negative `R²`.
- Persist:
  - reported objective-consistent `R²`
  - unclipped `R²`
  - winsorized counts
  - phase-specific condition numbers
  - rank diagnostics
  - leverage diagnostics
  - weighted and unweighted residual dispersion

## Phase 3: Replace Proxy T-Stats With Real Estimator-Layer Inference

- Implement true per-factor robust coefficient SEs and t-stats in the estimator layer.
- Use the full transformed sequential estimator, not a naive stage-B-only approximation.
- Prefer `HC2` or `HC3` unless validation clearly supports `HC1`.
- If `HC1` is used first, shadow `HC2/HC3` and persist leverage/support diagnostics.

## Phase 4: Fix Snapshot And Health Serving Semantics

- Make published snapshots closed-world for route-consumed keys, or remove fallback for those keys.
- Stage `health_diagnostics` into every published snapshot, or compute it after publish against an explicit snapshot id.
- Better: persist `health_diagnostics` durably with `run_id`, `snapshot_id`, `source_dates`, and `updated_at`.
- Make Health routing use the same durable-serving discipline as `risk` and `portfolio`, or explicitly document it as local mutable diagnostics.
- Fix the full-refresh path so recomputed health reads the just-staged payloads, not the previous live snapshot.
- Add an explicit atomicity or crash-recovery contract for durable-serving publish vs cache-snapshot publish.

## Phase 5: Fix Durable SQLite And Neon History Semantics

- Give `persist_model_outputs()` slice-replacement behavior, not latest-date upsert behavior.
- On factor-return methodology change, support true bounded replacement or full durable rebuild.
- Add durable schema migration/version handling for all model-output tables, not only one table.
- Teach Neon sync to detect content drift, not only row-count drift.
- Extend parity to include:
  - content checksums
  - per-date factor-set parity
  - null-rate checks
  - factor-return table coverage in both automatic and manual parity tools

## Phase 6: Fix Reuse And Invalidation

- Track provenance by more than max dates.
- Add per-surface fingerprints using at least:
  - max `updated_at`
  - latest `job_run_id`
  - earliest changed date when available
- Thread those fingerprints through:
  - source-date loading
  - universe-loadings reuse
  - factor-return invalidation
  - health invalidation

## Phase 7: Refactor High-Value Seams

- Extract one `RiskEngineLifecycle` or equivalent service used by both orchestration and pipeline refresh.
- Split `health.py` into section providers with typed inputs.
- Extract `assemble_portfolio_view(...)` so pipeline and what-if share the same assembly.
- Move payload typing to a shared schema layer so frontend types are derived, not hand-maintained.
- Standardize route contract enforcement so `risk`, `portfolio`, `exposures`, and `health` do not all have different validation behavior.

## Phase 8: Test The Real Failure Modes

Add direct tests for:

- constrained two-phase regression math
- country-not-constrained invariant
- orthogonalization invariants
- objective-consistent `R²`
- winsorized-count diagnostics
- snapshot closed-world behavior
- health restaging on light and full refresh
- stale base-cache fallback prevention
- durable slice replacement
- Neon content parity
- incomplete exposures payload rejection
- coverage UI contract fields
- frontend route/proxy behavior
- at least one realistic frontend or end-to-end contract test harness

## Priority Order

Recommended implementation order:

1. Fix country constraint
2. Fix winsorized residual contamination and Beta proxy construction
3. Fix Health snapshot/staleness behavior
4. Fix exposures contract crash path
5. Fix `R²` semantics and diagnostics persistence
6. Implement true t-stats and robust SEs
7. Fix durable/Neon history replacement and parity
8. Refactor lifecycle and health modules

## Suggested Reduced First Release

If the team needs a smaller safe first cut, ship only:

- country-constraint fix
- health snapshot/staleness fix
- exposures contract fix
- `R²` fix
- relabeling/factual frontend wording fixes

Then do:

- real t-stats
- richer diagnostics
- persistence/parity hardening
- structural refactors

## Final Assessment

This subsystem is not robust enough today to treat the Health page or factor-return history as authoritative.

The biggest issue is not one isolated bug. It is the combination of:

- one real math defect in factor-return construction
- several diagnostic-definition mismatches
- porous snapshot semantics
- stale health serving
- weak historical replacement semantics
- weak parity coverage
- and drift between backend truth and frontend explanation

The good news is that the system is fixable without a rewrite.

The highest-value path is:

- correct the model math
- make serving snapshots actually authoritative for the surfaces that use them
- make historical persistence replace corrected slices
- then add proper estimator-level inference and shared contracts

Until then, the safest operating assumption is:

- current risk payloads are more trustworthy than current Health diagnostics
- current factor-return history is not trustworthy enough for governance or statistical interpretation without repair
