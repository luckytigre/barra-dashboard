"use client";

// cUSE4-only hook and mutation barrel for the default frontend surfaces.
// Prefer this over `@/hooks/useApi` in cUSE4-owned frontend code.

export {
  ApiError,
  applyPortfolioWhatIf,
  previewPortfolioWhatIf,
  removeHoldingPosition,
  triggerDailyMaintenanceRefresh,
  triggerHoldingsImport,
  triggerRefreshProfile,
  triggerServeRefresh,
  upsertHoldingPosition,
  useDataDiagnostics,
  useExposures,
  useFactorHistory,
  useHealthDiagnostics,
  useHoldingsAccounts,
  useHoldingsModes,
  useHoldingsPositions,
  useOperatorStatus,
  usePortfolio,
  useRefreshStatus,
  useRisk,
  useUniverseFactors,
  useUniverseSearch,
  useUniverseTicker,
  useUniverseTickerHistory,
} from "@/hooks/useApi";
