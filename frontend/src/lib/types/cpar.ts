export type CparFactorGroup = "market" | "sector" | "style";
export type CparFitStatus = "ok" | "limited_history" | "insufficient_history";
export type CparWarning = "continuity_gap" | "ex_us_caution";
export type CparHedgeStatus = "hedge_ok" | "hedge_degraded" | "hedge_unavailable";
export type CparHedgeMode = "factor_neutral" | "market_neutral";

export interface CparPackageMeta {
  package_run_id: string;
  package_date: string;
  profile: string;
  method_version: string;
  factor_registry_version: string;
  data_authority: string;
  lookback_weeks: number;
  half_life_weeks: number;
  min_observations: number;
  source_prices_asof?: string | null;
  classification_asof?: string | null;
  universe_count: number;
  fit_ok_count: number;
  fit_limited_count: number;
  fit_insufficient_count: number;
}

export interface CparFactorSpec {
  factor_id: string;
  ticker: string;
  label: string;
  group: CparFactorGroup;
  display_order: number;
  method_version: string;
  factor_registry_version: string;
}

export interface CparSearchItem {
  ticker: string | null;
  ric: string;
  display_name: string | null;
  fit_status: CparFitStatus;
  warnings: CparWarning[];
  hq_country_code?: string | null;
}

export interface CparSearchData extends CparPackageMeta {
  query: string;
  limit: number;
  total: number;
  results: CparSearchItem[];
}

export interface CparLoading {
  factor_id: string;
  label: string;
  group: CparFactorGroup;
  display_order: number;
  beta: number;
}

export interface CparTickerDetailData extends CparPackageMeta {
  ticker: string | null;
  ric: string;
  display_name: string | null;
  fit_status: CparFitStatus;
  warnings: CparWarning[];
  observed_weeks: number;
  lookback_weeks: number;
  longest_gap_weeks: number;
  price_field_used?: string | null;
  hq_country_code?: string | null;
  market_step_alpha?: number | null;
  beta_market_step1?: number | null;
  block_alpha?: number | null;
  beta_spy_trade?: number | null;
  raw_loadings: CparLoading[];
  thresholded_loadings: CparLoading[];
  pre_hedge_factor_variance_proxy?: number | null;
  pre_hedge_factor_volatility_proxy?: number | null;
}

export interface CparHedgeLeg {
  factor_id: string;
  label?: string | null;
  group?: CparFactorGroup | null;
  display_order?: number | null;
  weight: number;
}

export interface CparPostHedgeExposure {
  factor_id: string;
  label?: string | null;
  group?: CparFactorGroup | null;
  display_order?: number | null;
  pre_beta: number;
  hedge_leg: number;
  post_beta: number;
}

export interface CparHedgePreviewData extends CparPackageMeta {
  ticker: string | null;
  ric: string;
  display_name: string | null;
  fit_status: CparFitStatus;
  warnings: CparWarning[];
  mode: CparHedgeMode;
  hedge_status: CparHedgeStatus;
  hedge_reason?: string | null;
  hedge_legs: CparHedgeLeg[];
  post_hedge_exposures: CparPostHedgeExposure[];
  pre_hedge_factor_variance_proxy: number;
  post_hedge_factor_variance_proxy: number;
  gross_hedge_notional: number;
  net_hedge_notional: number;
  non_market_reduction_ratio?: number | null;
  stability: {
    leg_overlap_ratio?: number | null;
    gross_hedge_notional_change?: number | null;
    net_hedge_notional_change?: number | null;
  };
}

export interface CparMetaData extends CparPackageMeta {
  factor_count: number;
  factors: CparFactorSpec[];
}
