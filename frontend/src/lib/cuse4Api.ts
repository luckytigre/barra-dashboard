// cUSE4-only frontend API helpers for the default route family.
// Prefer this over `@/lib/api` in cUSE4-owned frontend code.

import { ApiError, apiFetch, apiPath as legacyApiPath } from "@/lib/api";

export { ApiError, apiFetch };

export const cuse4ApiPath = {
  portfolio: legacyApiPath.portfolio,
  portfolioWhatIf: legacyApiPath.portfolioWhatIf,
  portfolioWhatIfApply: legacyApiPath.portfolioWhatIfApply,
  holdingsModes: legacyApiPath.holdingsModes,
  holdingsAccounts: legacyApiPath.holdingsAccounts,
  holdingsPositions: legacyApiPath.holdingsPositions,
  holdingsImport: legacyApiPath.holdingsImport,
  holdingsPosition: legacyApiPath.holdingsPosition,
  holdingsPositionRemove: legacyApiPath.holdingsPositionRemove,
  exposures: legacyApiPath.exposures,
  exposureHistory: legacyApiPath.exposureHistory,
  risk: legacyApiPath.risk,
  universeTicker: legacyApiPath.universeTicker,
  universeTickerHistory: legacyApiPath.universeTickerHistory,
  universeSearch: legacyApiPath.universeSearch,
  universeFactors: legacyApiPath.universeFactors,
  healthDiagnostics: legacyApiPath.healthDiagnostics,
  dataDiagnostics: legacyApiPath.dataDiagnostics,
  operatorStatus: legacyApiPath.operatorStatus,
  refreshProfile: legacyApiPath.refreshProfile,
  refreshStatus: legacyApiPath.refreshStatus,
} as const;

export const apiPath = cuse4ApiPath;
