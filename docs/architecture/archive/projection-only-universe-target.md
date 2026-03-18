# Projection-Only Universe: Target Design

## Goal

Support ETFs and other non-equity instruments as **projection-only** participants in cUSE4. These instruments:

- get price history via LSEG ingest
- stay outside native cUSE estimation
- derive exposures from the durable cUSE core package
- are frozen on the same cadence as the stable core package
- are persisted once per active core package date
- are read by serving as a primary durable surface

## Data Model

### `security_master.coverage_role`

New column: `coverage_role TEXT NOT NULL DEFAULT 'native_equity'`

Values:
- `'native_equity'` — participates in core cross-sectional estimation (default for all existing rows)
- `'projection_only'` — prices ingested, exposures projected via returns regression

Relationship to existing flags:
- Projection-only rows will have `classification_ok=0, is_equity_eligible=0` (set by LSEG classification or seed defaults)
- The `coverage_role` column is the semantic marker; the existing flags are the enforcement mechanism

### `projected_instrument_loadings` (in data.db)

```sql
CREATE TABLE IF NOT EXISTS projected_instrument_loadings (
    ric TEXT NOT NULL,
    ticker TEXT NOT NULL,
    as_of_date TEXT NOT NULL,
    factor_name TEXT NOT NULL,
    exposure REAL NOT NULL,
    PRIMARY KEY (ric, as_of_date, factor_name)
);
```

### `projected_instrument_meta` (in data.db)

```sql
CREATE TABLE IF NOT EXISTS projected_instrument_meta (
    ric TEXT NOT NULL,
    as_of_date TEXT NOT NULL,
    projection_method TEXT NOT NULL DEFAULT 'ols_returns_regression',
    lookback_days INTEGER NOT NULL,
    obs_count INTEGER NOT NULL,
    r_squared REAL NOT NULL,
    projected_specific_var REAL,
    projected_specific_vol REAL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (ric, as_of_date)
);
```

## Projection Algorithm

For each projection-only instrument:

1. Load daily close prices from `security_prices_eod`
2. Compute arithmetic returns: `r_t = (close_t / close_{t-1}) - 1`
3. Load durable factor returns from `model_factor_returns_daily`, filtered through the active `core_state_through_date`
4. Inner-join instrument returns with factor returns on date and require coverage through the active core package date
5. Trim to trailing 252 observations (configurable `lookback_days`)
6. Skip if `obs_count < 60` (configurable `min_obs`)
7. OLS: `r_instrument(t) = Σ_k β_k · f_k(t) + ε(t)` via `numpy.linalg.lstsq`
8. Projected specific risk: `specific_var = var(ε) × 252` (annualized)
9. Persist results with `as_of_date = core_state_through_date`

Statistical refinements such as intercepts, EW weighting, ridge shrinkage, and outlier handling remain intentionally deferred unless current outputs show a real defect.

## Ingest Scope

| Instrument type | Prices | Fundamentals | Classification | Factor exposures |
|-----------------|--------|-------------|----------------|------------------|
| Native equity   | LSEG   | LSEG        | LSEG           | Cross-sectional WLS |
| Projection-only | LSEG   | —           | —              | Time-series OLS |

## Serving Integration

Projected instruments are injected into `build_universe_ticker_loadings()` after the native equity loop:

- `model_status = "projected_only"`
- `model_status_reason = "returns_projection"`
- `exposure_origin = "projected"`
- TRBC fields empty (ETFs don't have TRBC classification)
- Specific risk from projected residual variance
- Sensitivities computed same as native path
- `projection_asof = core_state_through_date`

If persisted projected outputs for the active core package are missing:

- serving does **not** recompute them opportunistically
- the instrument is surfaced explicitly as projection-unavailable/degraded
- the instrument remains marked as projection-derived, not native

New fields on `UniverseTickerPayload`:
- `exposure_origin: str` — `"native"` or `"projected"`
- `projection_method: str | None`
- `projection_r_squared: float | None`
- `projection_obs_count: int | None`
- `projection_asof: str | None`

## Pipeline Integration

Projection refresh is core-bound:

1. load projection-only RICs from `security_master`
2. compute/persist projected outputs only when the active workflow is a core lane or has just recomputed the core package
3. read persisted projected outputs for `core_state_through_date`
4. pass persisted results into `build_universe_ticker_loadings(...)`

Ordinary `serve-refresh` does not compute projection-only outputs. It reads the persisted projection surface or marks the instrument as projection-unavailable for the active core package.

## Neon Sync

`projected_instrument_loadings` and `projected_instrument_meta` are registered in `neon_stage2.TABLE_CONFIGS` for automatic sync.

## Initial ETF Universe

| RIC | Ticker | Description |
|-----|--------|-------------|
| SPY.P | SPY | SPDR S&P 500 ETF |
| XLE.P | XLE | Energy Select Sector |
| XLF.P | XLF | Financial Select Sector |
| XLI.P | XLI | Industrial Select Sector |
| XLK.P | XLK | Technology Select Sector |
| XLB.P | XLB | Materials Select Sector |
| XLC.P | XLC | Communication Services |
| XLV.P | XLV | Health Care Select Sector |
| XLP.P | XLP | Consumer Staples |
| XLU.P | XLU | Utilities Select Sector |
| XLY.P | XLY | Consumer Discretionary |
| XLRE.P | XLRE | Real Estate Select Sector |

## Verification Criteria

1. ETF rows in security_master with `coverage_role='projection_only'`
2. ETFs excluded from core model tables
3. ETF prices ingested via LSEG
4. projection outputs are sourced from durable `model_factor_returns_daily`
5. projection outputs only refresh on core-package cadence
6. ordinary `serve-refresh` reads persisted outputs and does not recompute them
7. `projection_asof` matches `core_state_through_date`
8. missing projection outputs are surfaced explicitly, not silently dropped
