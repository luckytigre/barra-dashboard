# Universe Registry And Model Gating

Date: 2026-08-23
Status: Current architecture and maintainer guidance
Decision owner: Shaun

The filename is retained for stable inbound links. The completed design,
implementation, remediation, and review record is archived unchanged in
[UNIVERSE_REGISTRY_AND_MODEL_GATING_IMPLEMENTATION_RECORD_2026-03-29.md](../archive/implementation-trackers/UNIVERSE_REGISTRY_AND_MODEL_GATING_IMPLEMENTATION_RECORD_2026-03-29.md).

## Purpose

This document defines the current boundary between tracked identity, taxonomy,
operator policy, observed source readiness, model membership, and serving
labels. These concepts must stay separate so that adding or diagnosing a
security does not depend on overloaded legacy flags.

## Current Contract

The universe has five ordered layers:

1. `security_registry` records tracked identity and lifecycle state.
2. `security_taxonomy_current` records what the security is.
3. `security_policy_current` records which ingest and model paths are allowed.
4. `security_source_observation_daily` records what source data was observed as
   of a date.
5. Model-owned membership and package records describe what actually happened
   in a cUSE build or cPAR package.

Consumer fields such as `model_status` are derived presentation rollups. They
must not become the source of truth for registry, policy, or source readiness.

## Authority And Publication

| Concern | Authoring or first-writer surface | Runtime authority |
| --- | --- | --- |
| Curated registry and policy | `data/reference/security_registry_seed.csv` | Neon after source publication when `DATA_BACKEND=neon` |
| Vendor taxonomy and raw source facts | Local SQLite ingest | Neon retained operating copy after source sync |
| Source readiness observations | Ingest/source-observation jobs | Neon after source publication |
| cUSE membership and serving rollups | cUSE pipeline | Published model/serving tables in Neon |
| cPAR coverage and fit state | cPAR package pipeline | Active published cPAR package in Neon |

Local SQLite remains the LSEG landing zone, deep archive, and rebuild workspace.
It is not the normal application-read authority after Neon cutover.

## Tracking, Taxonomy, Policy, And Observation

Tracking answers whether the project intentionally tracks a RIC. Active
selectors admit only rows whose `tracking_status` is `active`; disabled rows are
excluded unless a diagnostic caller explicitly requests them.

Taxonomy describes instrument structure and geography. Important fields include
`instrument_kind`, `vehicle_structure`, `issuer_country_code`,
`model_home_market_scope`, `is_single_name_equity`, and
`classification_ready`.

Policy records allowed capabilities:

- `price_ingest_enabled`
- `pit_fundamentals_enabled`
- `pit_classification_enabled`
- `allow_cuse_native_core`
- `allow_cuse_fundamental_projection`
- `allow_cuse_returns_projection`
- `allow_cpar_core_target`
- `allow_cpar_extended_target`

Policy describes intent; it does not prove that the required source data or
model output exists. Source-observation rows separately record price,
fundamentals, and classification availability as of a date.

## Named Selectors

`backend/universe/selectors.py` owns the supported source and model scopes:

- `load_registry_active_rows`
- `load_price_ingest_scope_rows`
- `load_pit_ingest_scope_rows`
- `load_identifier_refresh_scope_rows`
- `load_cuse_structural_candidate_scope_rows`
- `load_cuse_returns_projection_scope_rows`
- `load_cpar_build_scope_rows`
- `load_cpar_factor_basis_scope_rows`

New callers should use these named selectors or the public runtime-row owner.
They should not reconstruct scope with local combinations of legacy flags.

## Runtime Rows And Compatibility

`backend/universe/runtime_rows.py` is the public owner for merged runtime
universe rows. It composes current registry, policy, taxonomy, and observation
state through `backend/universe/runtime_authority.py`.

Compatibility surfaces remain for migration and historical reads:

- `security_master_compat_current`
- `security_master`
- legacy fields such as `coverage_role`, `classification_ok`, and
  `is_equity_eligible`

When the registry table exists, its tracked scope anchors normal runtime rows.
An empty registry does not silently repopulate the universe from legacy tables
unless a caller explicitly opts into `allow_empty_registry_fallback`. Requested
legacy compatibility rows may supplement scoped reads where the public runtime
owner permits them, but compatibility state must not displace registry rows.

Destructive retirement of the remaining compatibility surfaces is a separate
migration. Do not remove them as incidental cleanup.

## cUSE Membership

cUSE policy routes are:

- native core for admitted US single-name candidates
- fundamental projection for admitted non-US single-name candidates
- returns projection for admitted vehicles or other returns-projection names
- unavailable/ineligible when no permitted path produces a usable output

The build persists model-stage detail through cUSE membership and stage records.
Those records distinguish policy path, realized role, output status, reason,
quality, source snapshot, and projection-basis state. The UI compatibility
rollup remains:

- `core_estimated`
- `projected_only`
- `ineligible`

`model_status` is a late rollup. Gating must use registry policy, source facts,
and model membership rather than reverse-engineering that label.

## cPAR Coverage

cPAR is package-oriented and does not borrow cUSE membership semantics.
Registry policy determines whether a security may enter the core or extended
target scope. The active package then owns fit presence, fit quality, portfolio
use, ticker-detail use, hedge use, reason codes, and warnings.

A registry-admitted security that is absent from the active package is not
equivalent to an active-package fit. Search and ticker surfaces must preserve
that distinction.

Factor-basis assignments are cPAR package/model concerns and remain separate
from general tracked-universe identity.

## Failure And Fallback Rules

- Missing or incomplete registry companions must not be presented as valid
  registry-first authority.
- A present but empty registry fails closed by default rather than silently
  widening to all compatibility rows.
- Expected policy coverage and observed source coverage stay separate.
- A projection candidate without an available projection basis is surfaced as
  unavailable, not silently relabeled as native or eligible.
- An active-package cPAR read must not be synthesized from registry admission.
- Serving labels and warnings must describe the published result without
  mutating upstream policy.

## Adding Or Changing A Security

1. Update `data/reference/security_registry_seed.csv` with normalized identity,
   tracking, and any explicit policy override.
2. Run the registry/bootstrap and source-ingest workflow documented in
   [UNIVERSE_ADD_RUNBOOK.md](../reference/protocols/UNIVERSE_ADD_RUNBOOK.md).
3. Confirm taxonomy and source observations match the intended instrument.
4. Confirm the named selectors admit only the intended ingest and model paths.
5. Publish source state to Neon through the approved source-sync workflow.
6. Rebuild the affected cUSE lane or cPAR package through its normal operator
   path; do not trigger rebuilds from public read routes.
7. Verify model membership, serving status, and portfolio behavior.

## Implementation Anchors

| Current claim | Code owner | Focused verification |
| --- | --- | --- |
| Registry, taxonomy, policy, observation schemas | `backend/universe/schema.py` | `backend/tests/test_universe_migration_scaffolding.py` |
| Seed and explicit policy reconciliation | `backend/universe/registry_sync.py` | `backend/tests/test_security_registry_sync.py` |
| Merged runtime authority and fail-closed scope | `backend/universe/runtime_rows.py` | `backend/tests/test_universe_runtime_authority_boundaries.py` |
| Registry-first source reads | `backend/data/source_reads.py` | `backend/tests/test_registry_source_history_canonicalization.py` |
| Named source/model selectors | `backend/universe/selectors.py` | `backend/tests/test_universe_selector_parity.py` |
| Registry-backed explore/quote reads | `backend/data/registry_quote_reads.py` | `backend/tests/test_registry_quote_reads.py` |
| cUSE membership and stage records | `backend/risk_model/cuse_membership.py` | `backend/tests/test_cuse_membership_contract.py` |
| cUSE compatibility rollup | `backend/risk_model/model_status.py` | `backend/tests/test_model_status.py` |
| cPAR registry/build scope | `backend/data/cpar_source_reads.py` | `backend/tests/test_cpar_source_reads.py` |
| Registry-first Neon publication | `backend/services/neon_stage2.py` | `backend/tests/test_neon_registry_first_cutover.py` |

## Maintainer Rules

- Add new capabilities to policy rather than overloading identity or taxonomy.
- Add new source facts to observation/audit state rather than policy.
- Keep cUSE and cPAR membership contracts model-owned.
- Keep display defaults and warnings in serving rollups.
- Preserve effective-date behavior for historical reads.
- Add or change a named selector in one owner module and cover it with focused
  tests before migrating callers.
- Update this document when authority, selector ownership, gating, or fallback
  behavior changes. Put dated investigations and rollout narratives in the
  archive instead of appending them here.
