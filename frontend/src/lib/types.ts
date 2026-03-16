export type ModelStatus = "core_estimated" | "projected_only" | "ineligible";
export type FactorFamily = "market" | "industry" | "style";

export interface FactorCatalogEntry {
  factor_id: string;
  factor_name: string;
  short_label: string;
  family: FactorFamily;
  block: string;
  source_column?: string | null;
  display_order?: number;
  covariance_display?: boolean;
  exposure_publish?: boolean;
  active?: boolean;
  method_version?: string;
}

export interface Position {
  ticker: string;
  name: string;
  long_short: string;
  trbc_economic_sector_short: string;
  trbc_economic_sector_short_abbr: string;
  shares: number;
  price: number;
  market_value: number;
  weight: number;
  account: string;
  sleeve: string;
  source: string;
  trbc_industry_group: string;
  exposures: Record<string, number>;
  risk_contrib_pct: number;
  model_status?: ModelStatus;
  eligibility_reason?: string;
  risk_mix?: {
    market: number;
    industry: number;
    style: number;
    idio: number;
  };
}

export interface SourceDates {
  fundamentals_asof?: string | null;
  exposures_asof?: string | null;
  exposures_latest_available_asof?: string | null;
  exposures_served_asof?: string | null;
  prices_asof?: string | null;
  classification_asof?: string | null;
}

export interface ServingSnapshotMeta {
  run_id?: string | null;
  snapshot_id?: string | null;
  refresh_started_at?: string | null;
}

export interface PortfolioData extends ServingSnapshotMeta {
  positions: Position[];
  total_value: number;
  position_count: number;
  source_dates?: SourceDates;
  _cached: boolean;
}

export interface WhatIfScenarioRow {
  account_id: string;
  ticker: string;
  ric?: string | null;
  quantity: number;
  source?: string | null;
}

export interface WhatIfHoldingDelta {
  account_id: string;
  ticker: string;
  ric: string;
  current_quantity: number;
  hypothetical_quantity: number;
  delta_quantity: number;
}

export interface WhatIfPreviewSide {
  positions: Position[];
  total_value: number;
  position_count: number;
  risk_shares: RiskShares;
  component_shares: Omit<RiskShares, "idio">;
  factor_details: FactorDetail[];
  exposure_modes: {
    raw: FactorExposure[];
    sensitivity: FactorExposure[];
    risk_contribution: FactorExposure[];
  };
  factor_catalog?: FactorCatalogEntry[];
}

export interface WhatIfFactorDeltaRow {
  factor_id: string;
  current: number;
  hypothetical: number;
  delta: number;
}

export interface WhatIfPreviewData {
  scenario_rows: WhatIfScenarioRow[];
  holding_deltas: WhatIfHoldingDelta[];
  current: WhatIfPreviewSide;
  hypothetical: WhatIfPreviewSide;
  diff: {
    total_value: number;
    position_count: number;
    risk_shares: RiskShares;
    factor_deltas: {
      raw: WhatIfFactorDeltaRow[];
      sensitivity: WhatIfFactorDeltaRow[];
      risk_contribution: WhatIfFactorDeltaRow[];
    };
  };
  source_dates?: SourceDates;
  serving_snapshot?: ServingSnapshotMeta;
  truth_surface?: string;
  _preview_only: boolean;
}

export interface WhatIfApplyRowResult {
  account_id: string;
  ticker: string;
  ric: string;
  current_quantity: number;
  applied_quantity: number;
  delta_quantity?: number;
  action: string;
}

export interface WhatIfApplyRejectedRow {
  row_number: number;
  reason_code: string;
  message: string;
}

export interface WhatIfApplyResponse {
  status: "ok" | "dry_run" | "rejected";
  accepted_rows: number;
  rejected_rows: number;
  rejection_counts: Record<string, number>;
  warnings: string[];
  applied_upserts: number;
  applied_deletes: number;
  row_results: WhatIfApplyRowResult[];
  rejected: WhatIfApplyRejectedRow[];
  import_batch_ids?: Record<string, string>;
}

export type HoldingsImportMode = "replace_account" | "upsert_absolute" | "increment_delta";

export interface HoldingsModeData {
  modes: HoldingsImportMode[];
  default: HoldingsImportMode;
}

export interface HoldingsAccount {
  account_id: string;
  account_name: string;
  is_active: boolean;
  positions_count: number;
  gross_quantity: number;
  last_position_updated_at: string | null;
}

export interface HoldingsAccountsData {
  accounts: HoldingsAccount[];
}

export interface HoldingsPosition {
  account_id: string;
  ric: string;
  ticker: string;
  quantity: number;
  source: string;
  updated_at: string | null;
}

export interface HoldingsPositionsData {
  positions: HoldingsPosition[];
  account_id: string | null;
  count: number;
}

export interface HoldingsImportRowPayload {
  account_id?: string;
  ric?: string;
  ticker?: string;
  quantity: number;
  source?: string;
}

export interface HoldingsImportResponse {
  status: string;
  mode: HoldingsImportMode;
  account_id: string;
  import_batch_id: string;
  accepted_rows: number;
  rejected_rows: number;
  rejection_counts: Record<string, number>;
  warnings: string[];
  applied_upserts: number;
  applied_deletes: number;
  refresh?: {
    started: boolean;
    state: Record<string, unknown>;
  } | null;
  preview_rejections?: Array<Record<string, unknown>>;
}

export interface HoldingsPositionEditResponse {
  status: string;
  action: string;
  account_id: string;
  ric: string;
  ticker: string | null;
  quantity: number;
  import_batch_id: string;
  refresh?: {
    started: boolean;
    state: Record<string, unknown>;
  } | null;
}

export interface FactorDrilldownItem {
  ticker: string;
  weight: number;
  exposure: number;
  sensitivity?: number;
  contribution: number;
}

export interface FactorExposure {
  factor_id: string;
  value: number;
  factor_vol?: number;
  coverage_pct?: number;
  cross_section_n?: number;
  eligible_n?: number;
  coverage_date?: string | null;
  drilldown: FactorDrilldownItem[];
}

export interface ExposuresData extends ServingSnapshotMeta {
  mode: string;
  factors: FactorExposure[];
  source_dates?: SourceDates;
  _cached: boolean;
}

export interface FactorHistoryPoint {
  date: string;
  factor_return: number;
  cum_return: number;
}

export interface FactorHistoryData {
  factor_id: string;
  factor_name: string;
  years: number;
  points: FactorHistoryPoint[];
  _cached: boolean;
}

export interface FactorDetail {
  factor_id: string;
  category: FactorFamily;
  exposure: number;
  factor_vol: number;
  sensitivity: number;
  marginal_var_contrib: number;
  pct_of_total: number;
  pct_of_systematic?: number;
}

export interface RiskShares {
  market: number;
  industry: number;
  style: number;
  idio: number;
}

export interface CovMatrix {
  factors: string[];
  correlation?: number[][];
  matrix?: number[][];
}

export interface RiskData extends ServingSnapshotMeta {
  risk_shares: RiskShares;
  component_shares: Omit<RiskShares, "idio">;
  factor_details: FactorDetail[];
  factor_catalog?: FactorCatalogEntry[];
  cov_matrix: CovMatrix;
  r_squared: number;
  source_dates?: SourceDates;
  risk_engine?: {
    status?: string;
    method_version?: string;
    last_recompute_date?: string;
    factor_returns_latest_date?: string;
    cross_section_min_age_days?: number;
    recompute_interval_days?: number;
    lookback_days?: number;
    specific_risk_ticker_count?: number;
    recomputed_this_refresh?: boolean;
    recompute_reason?: string;
  };
  model_sanity?: {
    status?: string;
    warnings?: string[];
    checks?: Record<string, number>;
    coverage_date?: string | null;
    latest_available_date?: string | null;
    selection_mode?: string;
    update_available?: boolean;
  };
  _cached: boolean;
}

export interface UniverseTickerItem {
  ticker: string;
  name: string;
  trbc_economic_sector_short: string;
  trbc_economic_sector_short_abbr: string;
  trbc_industry_group: string;
  market_cap: number | null;
  price: number;
  exposures: Record<string, number>;
  sensitivities: Record<string, number>;
  risk_loading: number | null;
  specific_var?: number | null;
  specific_vol?: number | null;
  model_status?: ModelStatus;
  eligibility_reason?: string;
  model_warning?: string;
  as_of_date?: string;
}

export interface UniverseTickerData {
  item: UniverseTickerItem;
  _cached: boolean;
}

export interface WeeklyPricePoint {
  date: string;
  close: number;
}

export interface UniverseTickerHistoryData {
  ticker: string;
  ric: string;
  years: number;
  points: WeeklyPricePoint[];
  _cached: boolean;
}

export interface UniverseSearchItem {
  ticker: string;
  ric?: string | null;
  name: string;
  trbc_economic_sector_short: string;
  trbc_economic_sector_short_abbr: string;
  trbc_industry_group?: string;
  risk_loading: number | null;
  specific_vol?: number | null;
  model_status?: ModelStatus;
  eligibility_reason?: string;
}

export interface UniverseSearchData {
  query: string;
  results: UniverseSearchItem[];
  total: number;
  _cached: boolean;
}

export interface UniverseFactorsData {
  factors: string[];
  factor_vols: Record<string, number>;
  factor_catalog?: FactorCatalogEntry[];
  r_squared?: number;
  ticker_count?: number;
  eligible_ticker_count?: number;
  core_estimated_ticker_count?: number;
  projected_only_ticker_count?: number;
  ineligible_ticker_count?: number;
  _cached: boolean;
}

export interface SeriesPoint {
  date: string;
  value: number;
}

export interface HealthHistogram {
  centers: number[];
  counts: number[];
}

export interface HealthCorrelationMatrix {
  factors: string[];
  correlation: number[][];
}

export interface HealthR2Point {
  date: string;
  r2: number;
  roll60: number;
  roll252: number;
}

export interface HealthFactorPctRow {
  factor_id: string;
  value: number;
}

export interface HealthIncrementalBlockR2Point {
  date: string;
  r2_full: number;
  r2_structural: number;
  r2_style_incremental: number;
  roll60_full: number;
  roll60_structural: number;
  roll60_style_incremental: number;
}

export interface HealthBucketBreadthPoint {
  date: string;
  industry_mean_abs_t: number;
  style_mean_abs_t: number;
}

export interface HealthBucketBreadthSummary {
  industry_mean_abs_t: number;
  style_mean_abs_t: number;
}

export interface HealthPortfolioVarianceSplit {
  market_pct_total: number;
  industry_pct_total: number;
  style_pct_total: number;
  idio_pct_total: number;
  market_pct_factor_only: number;
  industry_pct_factor_only: number;
  style_pct_factor_only: number;
}

export interface HealthExposureStats {
  factor_id: string;
  mean: number;
  std: number;
  p1: number;
  p99: number;
  max_abs: number;
}

export interface HealthTurnoverPoint {
  date: string;
  turnover: number;
  roll60: number;
}

export interface HealthForecastRealizedRow {
  name: string;
  forecast_vol: number;
  realized_vol_60d: number;
}

export interface HealthCoverageFieldRow {
  field: string;
  data_type: string;
  non_null_rows: number;
  total_rows: number;
  row_coverage_pct: number;
  avg_date_coverage_pct: number;
  worst_date: string | null;
  worst_date_coverage_pct: number;
  dates_below_80_pct_count: number;
  avg_ticker_lifecycle_coverage_pct: number;
  p10_ticker_lifecycle_coverage_pct: number;
  tickers_below_80_pct_count: number;
  coverage_score_pct: number;
}

export interface HealthCoverageTable {
  label: string;
  table: string;
  row_count: number;
  date_count: number;
  ticker_count: number;
  field_count: number;
  low_coverage_field_count: number;
  fields: HealthCoverageFieldRow[];
}

export interface HealthDiagnosticsData {
  status: string;
  as_of: string | null;
  notes: string[];
  factor_catalog?: FactorCatalogEntry[];
  section1: {
    sampling?: string;
    r2_series: HealthR2Point[];
    incremental_block_r2_series: HealthIncrementalBlockR2Point[];
    t_stat_hist: HealthHistogram;
    pct_days_abs_t_gt_2: HealthFactorPctRow[];
    bucket_breadth_series: HealthBucketBreadthPoint[];
    bucket_breadth_summary: HealthBucketBreadthSummary;
    portfolio_variance_split: HealthPortfolioVarianceSplit;
  };
  section2: {
    as_of: string | null;
    factor_stats: HealthExposureStats[];
    factor_histograms: Record<string, HealthHistogram>;
    exposure_corr: HealthCorrelationMatrix;
    turnover_series: HealthTurnoverPoint[];
  };
  section3: {
    factors: string[];
    cumulative_returns: Record<string, SeriesPoint[]>;
    rolling_vol_60d: Record<string, SeriesPoint[]>;
    return_corr: HealthCorrelationMatrix;
    return_dist: Record<string, HealthHistogram>;
  };
  section4: {
    eigenvalues: number[];
    forecast_vs_realized: HealthForecastRealizedRow[];
    rolling_avg_factor_vol: SeriesPoint[];
  };
  section5: {
    fundamentals: HealthCoverageTable;
    trbc_history: HealthCoverageTable;
  };
  _cached: boolean;
}

export interface DataTableStats {
  table: string;
  exists: boolean;
  row_count?: number;
  row_count_mode?: string | null;
  ticker_count?: number | null;
  date_column?: string | null;
  min_date?: string | null;
  max_date?: string | null;
  last_updated_at?: string | null;
  last_job_run_id?: string | null;
}

export interface DataDiagnosticsData {
  status: string;
  database_path: string;
  cache_db_path: string;
  diagnostic_scope?: {
    source?: string;
    plain_english?: string;
  };
  truth_surfaces?: {
    dashboard_serving?: {
      source?: string;
      plain_english?: string;
    };
    operator_status?: {
      source?: string;
      plain_english?: string;
    };
    local_diagnostics?: {
      source?: string;
      plain_english?: string;
    };
  };
  exposure_source_table: string;
  exposure_source?: {
    table: string;
    selection_mode: string;
    is_dynamic: boolean;
    latest_asof?: string | null;
    plain_english?: string | null;
  };
  source_tables: {
    security_master: DataTableStats | null;
    security_fundamentals_pit: DataTableStats | null;
    security_classification_pit: DataTableStats | null;
    security_prices_eod: DataTableStats | null;
    estu_membership_daily: DataTableStats | null;
    barra_raw_cross_section_history: DataTableStats | null;
    universe_cross_section_snapshot: DataTableStats | null;
  };
  exposure_duplicates: {
    active_exposure_source: {
      table: string;
      exists: boolean;
      duplicate_groups: number | null;
      duplicate_extra_rows: number | null;
      computed?: boolean;
    };
  };
  cross_section_usage: {
    eligibility_summary: {
      available: boolean;
      latest?: {
        date: string;
        exp_date: string | null;
        exposure_n: number;
        structural_eligible_n: number;
        core_structural_eligible_n: number;
        regression_member_n: number;
        projectable_n: number;
        projected_only_n: number;
        structural_coverage_pct: number;
        regression_coverage_pct: number;
        projectable_coverage_pct: number;
        alert_level: string;
      } | null;
      min_structural_eligible_n?: number | null;
      max_structural_eligible_n?: number | null;
      min_core_structural_eligible_n?: number | null;
      max_core_structural_eligible_n?: number | null;
      min_regression_member_n?: number | null;
      max_regression_member_n?: number | null;
      min_projectable_n?: number | null;
      max_projectable_n?: number | null;
      min_projected_only_n?: number | null;
      max_projected_only_n?: number | null;
    };
    factor_cross_section: {
      available: boolean;
      latest?: {
        date: string | null;
        cross_section_n_min: number;
        cross_section_n_max: number;
        eligible_n_min: number;
        eligible_n_max: number;
      } | null;
      min_cross_section_n?: number | null;
      max_cross_section_n?: number | null;
      min_eligible_n?: number | null;
      max_eligible_n?: number | null;
    };
  };
  risk_engine_meta: Record<string, unknown>;
  cache_outputs: Array<{
    key: string;
    updated_at_unix: number | null;
    updated_at_utc: string | null;
  }>;
}

export interface RefreshStatusState {
  status: string;
  job_id: string | null;
  pipeline_run_id: string | null;
  profile: string | null;
  requested_profile: string | null;
  mode: string | null;
  as_of_date: string | null;
  resume_run_id: string | null;
  from_stage: string | null;
  to_stage: string | null;
  force_core: boolean;
  force_risk_recompute: boolean;
  requested_at: string | null;
  started_at: string | null;
  finished_at: string | null;
  current_stage?: string | null;
  stage_index?: number | null;
  stage_count?: number | null;
  stage_started_at?: string | null;
  current_stage_message?: string | null;
  current_stage_progress_pct?: number | null;
  current_stage_items_processed?: number | null;
  current_stage_items_total?: number | null;
  current_stage_unit?: string | null;
  current_stage_heartbeat_at?: string | null;
  result: Record<string, unknown> | null;
  error: {
    type?: string;
    message?: string;
    traceback?: string;
  } | null;
}

export interface RefreshStatusData {
  status: string;
  refresh: RefreshStatusState;
}

export interface OperatorLaneLatestRun {
  run_id: string | null;
  profile: string | null;
  status: string;
  started_at: string | null;
  finished_at: string | null;
  updated_at: string | null;
  duration_seconds?: number | null;
  stage_count: number;
  completed_stage_count: number;
  failed_stage_count: number;
  running_stage_count: number;
  stage_duration_seconds_total?: number;
  current_stage?: OperatorLaneStage | null;
  stages: OperatorLaneStage[];
}

export interface OperatorLaneStage {
  stage_name: string;
  stage_order: number;
  status: string;
  started_at: string | null;
  completed_at: string | null;
  duration_seconds?: number | null;
  heartbeat_at?: string | null;
  details?: {
    message?: string | null;
    progress_kind?: string | null;
    progress_pct?: number | null;
    items_processed?: number | null;
    items_total?: number | null;
    unit?: string | null;
    current_date?: string | null;
    current_as_of?: string | null;
    dates_per_second?: number | null;
    computed_dates?: number | null;
    cached_dates?: number | null;
    skip_counts?: Record<string, number>;
    [key: string]: unknown;
  };
  error_type: string | null;
  error_message: string | null;
}

export interface OperatorLaneStatus {
  profile: string;
  label: string;
  description: string;
  core_policy: string;
  serving_mode: string;
  raw_history_policy: string;
  reset_core_cache: boolean;
  default_stages: string[];
  enable_ingest: boolean;
  ingest_policy?: string;
  rebuild_backend?: string;
  requires_neon_sync_before_core?: boolean;
  source_sync_required?: boolean;
  neon_readiness_required?: boolean;
  latest_run: OperatorLaneLatestRun;
}

export interface OperatorStatusData {
  status: string;
  generated_at: string;
  lanes: OperatorLaneStatus[];
  source_dates: SourceDates;
  local_archive_source_dates?: SourceDates | null;
  risk_engine: {
    status?: string;
    method_version?: string;
    last_recompute_date?: string;
    factor_returns_latest_date?: string;
    lookback_days?: number;
    cross_section_min_age_days?: number;
    recompute_interval_days?: number;
  };
  core_due: {
    due: boolean;
    reason: string;
  };
  refresh: RefreshStatusState;
  holdings_sync?: {
    pending?: boolean;
    pending_count?: number;
    dirty_revision?: number;
    dirty_since?: string | null;
    last_mutation_at?: string | null;
    last_mutation_kind?: string | null;
    last_mutation_summary?: string | null;
    last_mutation_account_id?: string | null;
    last_import_batch_id?: string | null;
    last_refresh_started_at?: string | null;
    last_refresh_finished_at?: string | null;
    last_refresh_status?: string | null;
    last_refresh_profile?: string | null;
    last_refresh_run_id?: string | null;
    last_refresh_message?: string | null;
    last_refresh_started_dirty_revision?: number | null;
  } | null;
  neon_sync_health?: {
    status?: string;
    message?: string;
    updated_at?: string;
    artifact_path?: string | null;
    mirror_status?: string | null;
    sync_status?: string | null;
    parity_status?: string | null;
    parity_issue_count?: number;
  } | null;
  active_snapshot?: {
    snapshot_id?: string;
    published_at?: number;
  } | null;
  latest_parity_artifact?: string | null;
  runtime?: {
    app_runtime_role?: string;
    allowed_profiles?: string[];
    local_only_profiles?: string[];
    canonical_serving_profile?: string;
    dashboard_truth_surface?: string;
    dashboard_truth_plain_english?: string;
    storage_contract_plain_english?: string;
    source_authority?: string;
    source_authority_plain_english?: string;
    local_archive_enabled?: boolean;
    local_archive_plain_english?: string;
    rebuild_authority?: string;
    rebuild_authority_plain_english?: string;
    diagnostics_scope?: string;
    diagnostics_scope_plain_english?: string;
    data_backend?: string;
    neon_database_configured?: boolean;
    neon_auto_sync_enabled?: boolean;
    neon_auto_sync_enabled_effective?: boolean;
    neon_auto_parity_enabled?: boolean;
    neon_auto_parity_enabled_effective?: boolean;
    neon_auto_prune_enabled?: boolean;
    neon_auto_prune_enabled_effective?: boolean;
    neon_authoritative_rebuilds?: boolean;
    neon_read_surfaces?: string[];
    serving_outputs_primary_reads?: boolean;
    serving_outputs_primary_reads_effective?: boolean;
    warnings?: string[];
  } | null;
}
