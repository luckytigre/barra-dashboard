"use client";

// Transitional mixed-family compatibility barrel.
// New cUSE4-owned frontend code should import from `@/hooks/useCuse4Api`.
// New cPAR-owned frontend code should import from `@/hooks/useCparApi`.

export { ApiError } from "@/lib/apiTransport";
export {
  applyPortfolioWhatIf,
  removeHoldingPosition,
  triggerHoldingsImport,
  upsertHoldingPosition,
  useHoldingsAccounts,
  useHoldingsModes,
  useHoldingsPositions,
} from "@/hooks/useHoldingsApi";
export {
  previewPortfolioWhatIf,
  triggerDailyMaintenanceRefresh,
  triggerRefreshProfile,
  triggerServeRefresh,
  useDataDiagnostics,
  useExposures,
  useFactorHistory,
  useHealthDiagnostics,
  useOperatorStatus,
  usePortfolio,
  useRefreshStatus,
  useRisk,
  useUniverseFactors,
  useUniverseSearch,
  useUniverseTicker,
  useUniverseTickerHistory,
} from "@/hooks/useCuse4Api";
export {
  previewCparExploreWhatIf,
  useCparFactorHistory,
  useCparMeta,
  useCparPortfolioHedge,
  useCparPortfolioWhatIf,
  useCparRisk,
  useCparSearch,
  useCparTicker,
  useCparTickerHistory,
} from "@/hooks/useCparApi";
