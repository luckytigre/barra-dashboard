# Docs Index

The active docs surface is grouped into four buckets:

## Architecture
- `architecture/ARCHITECTURE_AND_OPERATING_MODEL.md`: canonical project architecture, data flow, runtime roles, operating model, and known open limitations.
  - system identity is `cUSE` / `cUSE4`; any `Barra` wording in active docs should be treated as lineage context only
- `architecture/`: active architecture package.
  - start with `architecture/architecture-invariants.md`, `architecture/dependency-rules.md`, and `architecture/maintainer-guide.md`
  - deeper historical diagnosis, target-shape, inventory, audits, plans, investigations, and pass summaries live under `architecture/archive/`

## Operations
- `operations/OPERATIONS_PLAYBOOK.md`: runbook for refresh, retention, validation, and recovery.
  - canonical reference for the three horizons:
    - active cUSE model history
    - risk-model lookback
    - local/Neon source retention
  - current live model is a 45-factor set with 14 style factors and no standalone `Value` factor
- `operations/OPERATIONS_HARDENING_CHECKLIST.md`: pre-run hygiene, smoke checks, and rollback guardrails.
- Health page in the app is the live operator/runtime cockpit, including refresh warnings and top-level model quality.
- Data page is the maintenance surface for source-table lineage, coverage, cache surfaces, and integrity diagnostics.

## Reference
- `reference/specs/cUSE4_engine_spec.md`: cUSE4 model/foundation spec.
- `reference/specs/USE4_US_CORE_MARKET_ADR_2026-03-15.md`: accepted ADR for the US-core `Market` factor migration, one-stage WLS target, and post-cutover cleanup end state.
- `reference/protocols/TRBC_CLASSIFICATION_PIT_PROTOCOL.md`: canonical TRBC PIT classification protocol (`security_classification_pit` based).
- `reference/protocols/UNIVERSE_ADD_RUNBOOK.md`: approved onboarding workflow for adding new RICs to the universe and backfilling canonical source tables.
- `reference/migrations/`: active schema, holdings, and migration reference material.
- `../data/reference/security_master_seed.csv`: versioned registry-only seed artifact for the canonical universe registry.

## Archive
- `archive/legacy-plans/`: historical root-level plans and execution notes kept for context only; do not run commands from these files as active operational guidance.
- `archive/migrations/`: historical migration execution notes and retired operator runbooks.
