"use client";

// cPAR-only hook barrel for the namespaced cPAR frontend surfaces.
// Prefer this over `@/hooks/useApi` in cPAR-owned frontend code.

export {
  ApiError,
  useCparFactorHistory,
  useCparMeta,
  useCparRisk,
  useCparPortfolioHedge,
  useCparPortfolioWhatIf,
  useCparSearch,
  useHoldingsAccounts,
} from "@/hooks/useApi";
