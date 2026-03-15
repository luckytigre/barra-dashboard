# T-Stat Revision Plan And Implementation Record

Date: 2026-03-12

## Status

Implemented in the codebase on 2026-03-12.

What shipped:

- true persisted per-factor `robust_se` and `t_stat` fields from the estimator layer
- heteroskedasticity-robust HC1 linear-map inference in `backend/risk_model/wls_regression.py`
- widened cache, durable SQLite, and Neon factor-return schemas
- the current runtime factor set is 45 total factors with no standalone `Value` factor
- Health diagnostics migrated to stored t-stats with explicit staged-input computation
- Health serving moved to durable current payloads first, with cache fallback only as a safety net
- Neon factor-return parity strengthened beyond counts/date bounds to include column presence, non-null coverage, per-date factor counts, and sampled value checks

What remains open after implementation:

- evaluate `HC2` / `HC3` against the shipped HC1 method on leverage-heavy and sparse structural buckets
- add explicit leverage/support diagnostics to Health so t-stat interpretation is less fragile
- further reduce temporary proxy fallback dependence in Health once all runtimes are fully rebuilt
- continue governance work around winsorization diagnostics and factor-support metadata

This document now serves two purposes:

- it records the original migration plan
- it documents what was actually implemented and what still remains

## Goal

Replace the Health page's proxy t-stat with true per-factor regression t-stats computed from the existing two-phase weighted cross-sectional estimator, using heteroskedasticity-robust standard errors. Carry the new statistics through cache rebuild, durable persistence, Neon sync, health diagnostics, and frontend semantics.

## Original Problem State

Before this revision, the system did not compute coefficient standard errors in the regression layer. The estimator in `backend/risk_model/wls_regression.py` returned factor returns, residuals, `r_squared`, condition numbers, and `residual_vol`, but no coefficient covariance, no standard errors, and no t-stats.

Before this revision, the factor-return cache in `backend/risk_model/daily_factor_returns.py` persisted:

- `factor_return`
- `r_squared`
- `residual_vol`
- `cross_section_n`
- `eligible_n`
- `coverage`

Before this revision, the Health page in `backend/analytics/health.py` derived a proxy t-stat as:

`factor_return / residual_vol`

The frontend in `frontend/src/features/health/SectionRegression.tsx` explicitly labeled that quantity as approximate.

## Implemented State

The estimator now returns:

- `factor_returns`
- `robust_se`
- `t_stats`
- `residuals`
- `raw_residuals`
- `r_squared`
- condition numbers
- `residual_vol`

The factor-return cache now persists:

- `factor_return`
- `robust_se`
- `t_stat`
- `r_squared`
- `residual_vol`
- `cross_section_n`
- `eligible_n`
- `coverage`

The factor-return residual cache now persists both:

- `model_residual`
- `raw_residual`

Health now prefers stored `t_stat` values and only falls back to the old proxy path when historical rows do not yet contain usable t-stats.

## Why This Needs Revision

The current proxy is not a regression coefficient t-stat. It does not use a factor-specific coefficient standard error, does not reflect uncertainty induced by the constrained phase-A design, and does not properly account for the sequential two-phase estimator. It is acceptable only as a rough signal-to-noise heuristic.

If the Health page is meant to communicate whether factors are estimated precisely, the system needs true coefficient t-stats:

`t_j = beta_j / SE(beta_j)`

## Scope Audit

The change affects more than the regression function. The following paths are in scope:

- Estimator output contract: `backend/risk_model/wls_regression.py`
- Cache schema, cache signature, writes, reads: `backend/risk_model/daily_factor_returns.py`
- Risk-engine method version and recompute reuse: `backend/analytics/pipeline.py`
- Health diagnostics generation: `backend/analytics/health.py`
- Health payload staging and cache reuse: `backend/analytics/services/cache_publisher.py`
- Durable SQLite model outputs: `backend/data/model_outputs.py`
- Neon schema and factor-return sync: `docs/migrations/neon/NEON_CANONICAL_SCHEMA.sql`, `backend/services/neon_mirror.py`
- Health API/UI contract: `backend/api/routes/health.py`, `frontend/src/lib/types.ts`, `frontend/src/features/health/SectionRegression.tsx`
- Tests that create or assume the old `daily_factor_returns` shape

## Recommended Statistical Approach

### Point Estimates

Keep the existing point-estimate workflow unchanged:

- Phase A: intercept plus structural block using the existing constraint transform
- Phase B: style regression on phase-A residuals
- Final residual: raw return minus phase-A fit minus phase-B fit

### Standard Errors

Use heteroskedasticity-robust standard errors computed from the full sequential estimator, not from a naive standalone regression on phase-A residuals.

The key requirement is that phase-B inference must propagate uncertainty from phase-A residualization. Computing a separate robust regression on `residual_a ~ X_style` would miss that dependency and can misstate style-factor standard errors.

Implementation shape:

- Compute the constrained phase-A influence map in transformed space, then map it back through the existing constraint matrix.
- Compute the phase-B influence map against the residualized target while propagating phase-A uncertainty.
- Stack the persisted factor rows, excluding the intercept.
- Form a sandwich covariance for the full two-phase coefficient vector.
- Persist only diagonal outputs for now:
  - `robust_se`
  - `t_stat`

### Design Notes

- The runtime estimator was also corrected during this work so `Country: US` is now estimated as a separate unconstrained phase-A block, while only industries remain under the sum-to-zero constraint.
- `residual_vol` should remain a separate regression diagnostic. It should not be reused as a coefficient SE surrogate.
- The weighting convention should be stated precisely: the implementation premultiplies by `sqrt(market_cap)`, which means the solved objective is effectively `market_cap`-weighted WLS.
- Inference must target the actual transformed design. Style exposures are canonicalized, residualized, and re-standardized before entering the regression, so the covariance must be computed on the persisted estimator, not on raw descriptors.
- Returns are winsorized before estimation, so the robust covariance is conditional on the winsorized response, not the raw-return process.
- Robust SEs here are heteroskedasticity-robust, not fully dependence-robust. They still rely on cross-sectional independence conditional on design, which is imperfect in the presence of share-class duplication, common omitted shocks, and industry dependence.
- `HC1` was shipped as the first production implementation.
- The quant-review concern remains valid: leverage-heavy and sparse structural buckets are the main reason `HC2` or `HC3` should still be evaluated.
- Health should still gain leverage/support diagnostics or sampled HC2/HC3 comparison before the inference layer is treated as fully hardened.

## Persistence Plan

### Cache DB

Implemented:

Add nullable columns to `daily_factor_returns`:

- `robust_se`
- `t_stat`

Persist inference metadata in `daily_factor_returns_meta`:

- inference method
- HC variant
- weighting convention

Bump the cache method version and include the inference method in the cache signature so old rows are treated as stale.

### Durable SQLite

Implemented:

Add the same columns to `model_factor_returns_daily` in `backend/data/model_outputs.py`.

Important: the current persistence path is incremental from the latest date forward. That is not sufficient for this rollout. The migration needs a full factor-return backfill into `data.db`, not just an additive schema change.

### Neon

Implemented:

Add the same columns to `model_factor_returns_daily` in `docs/migrations/neon/NEON_CANONICAL_SCHEMA.sql` and update `backend/services/neon_mirror.py` to sync them.

Neon parity has now been extended to verify:

- new-column presence
- non-null coverage
- per-date factor-count parity on sampled dates
- sampled value equality for factor-return rows

## Health Diagnostics Migration

Replace the proxy t-stat path in `backend/analytics/health.py` with stored `t_stat` values for:

- histogram
- `% days |t| > 2`
- bucket breadth time series
- bucket breadth summary

Keep a temporary fallback path only for legacy caches, and label it explicitly when used.

Update Health page semantics:

- remove approximate-t wording
- update notes to say the diagnostics now use coefficient t-stats with heteroskedasticity-robust standard errors

The following governance diagnostics remain desirable follow-up work alongside the shipped t-stat migration:

- unclipped daily weighted `R²`
- count and share of negative-`R²` days
- `R²` distribution bands such as p10, median, and p90
- daily regression breadth and support metadata:
  - `cross_section_n`
  - `eligible_n`
  - coverage
  - factor count
  - method-version and factor-universe digest
- per-factor support diagnostics:
  - average constituent count or effective N
  - concentration or top-name share
  - `% days t-stat unavailable`
- clearer hit-rate summaries:
  - `% t > 2`
  - `% t < -2`
  - median `|t|`
  - rolling share of significant factors
- lineage metadata:
  - factor-return method version
  - inference method
  - cache rebuild date
  - fallback mode indicator

Breadth interpretation also needs revision:

- do not present structural and style breadth as directly comparable without qualification
- sparse structural buckets can produce high `|t|` because of leverage or concentration, not because the model is broadly healthy
- consider median or trimmed breadth in addition to mean `|t|`
- provide null reference points for interpretation:
  - `E|Z| ≈ 0.8`
  - `%|t| > 2 ≈ 4.6%`

## Rollout Plan

Completed:

1. Implement robust SE and t-stat computation in `wls_regression.py`.
2. Add estimator-level validation tests before widening persistence.
3. Widen the `daily_factor_returns` cache schema and update cache reads/writes.
4. Bump cache and risk-engine method versions.
5. Widen `model_factor_returns_daily` in `data.db`.
6. Apply Neon schema changes and sync logic changes.
7. Extend Neon parity checks for the new inference fields and sampled value checks.
8. Invalidate and regenerate `health_diagnostics` during refresh staging.
9. Update frontend semantics and notes.

Operational note:

- the code paths are in place for full-history replacement semantics in cache, durable SQLite, and Neon
- whether a given runtime has already been fully rebuilt still depends on actually running the recompute/sync workflows after deployment

## Validation Plan

### Estimator Tests

Add direct tests for:

- phase A only
- phase B only
- full two-phase constrained estimator
- equal-weights case collapsing to OLS HC1 behavior
- singular and near-singular designs returning safe nulls instead of invalid numeric output

### Persistence and Migration Tests

Add or update tests to cover:

- widened `daily_factor_returns` schema
- cache invalidation on method-version change
- full `data.db` backfill behavior
- Neon sync and parity for `robust_se` and `t_stat`
- health payload invalidation after inference-method change
- factor-universe parity between cache, durable SQLite, Neon, and health payloads
- lineage invalidation when method version or inference method changes

### Health Contract Tests

Implemented tests now prove:

- histogram uses stored `t_stat`
- hit-rate table uses stored `t_stat`
- breadth series and summary use stored `t_stat`
- notes and semantics no longer describe the proxy path after migration

### Shadow Validation

Recommended next validation, now that the cutover has shipped:

- current proxy t-stats
- exact robust t-stats under the candidate production method

Inspect at least:

- style-factor hit-rate changes
- broad structural bucket changes
- factor-universe coverage
- null-rate of computed t-stats
- leverage concentration and thin-factor behavior

Large changes are expected. Large unexplained changes in broad structural buckets should be treated as a red flag.

## Main Risks

- Incorrect sandwich covariance for the constrained two-phase estimator
- Partial backfill into `data.db` or Neon
- False confidence from parity checks that only compare row counts
- Stale `health_diagnostics` payloads surviving after deployment
- Semantic mismatch in the frontend if the backend has switched to real t-stats but the UI still describes an approximation
- Overinterpreting breadth or hit-rate for sparse structural factors without support and leverage context
- Treating robust t-stats as exact hypothesis-test objects despite residual cross-sectional dependence
- Shipping `HC1` without visibility into leverage, effective sample size, and thin-bucket behavior

## Current Quant Review Findings

Independent quant-style review of the current local repo and data surfaced additional issues that should be treated as part of this work:

- The current proxy t-stat is especially weak because every factor on a given date shares the same `residual_vol` denominator.
- The current local Health history appears stale in at least one surface, which points to lineage invalidation gaps in `health_diagnostics`.
- There is evidence of factor-universe mismatch between cache-backed factor returns and durable model outputs, so parity checks need to validate factor names and column completeness, not just row counts and date windows.
- Current daily fit levels appear broadly plausible for a large daily cross-sectional stock model, but the current clipped `R²` implementation can hide truly bad days.
- The local `2026-03-03` cross-section appears to include extremely high fitted leverage and sparse structural buckets, which is the main reason `HC2` or `HC3` should remain under active consideration.

## Second-Opinion Findings

Two independent reviews aligned on the same core conclusions:

- Robust inference needs to be computed at the estimator layer, not fabricated downstream from cached factor returns.
- The rollout must be treated as a full-history migration, not a small additive enhancement.
- The biggest operational risk is incomplete backfill and stale health payload reuse, not raw compute cost.

## Recommendation

Proceed with:

- true per-factor t-stats
- heteroskedasticity-robust standard errors, with `HC2` or `HC3` preferred if validation confirms the current leverage concerns
- full cache rebuild
- full durable backfill
- full Neon sync
- explicit health-cache invalidation
- support, leverage, lineage, and factor-universe diagnostics added to Health alongside the t-stat migration

Do not ship this as a frontend-only Health page change.
