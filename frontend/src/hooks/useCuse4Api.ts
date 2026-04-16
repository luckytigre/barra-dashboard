"use client";

// cUSE4-only hook and mutation barrel for the default frontend surfaces.
// Prefer this over `@/hooks/useApi` in cUSE4-owned frontend code.

import useSWR from "swr";
import { ApiError, apiFetch } from "@/lib/apiTransport";
import { cuse4ApiPath } from "@/lib/cuse4Api";
import type {
  DataDiagnosticsData,
  ExposuresData,
  FactorHistoryData,
  HealthDiagnosticsData,
  OperatorStatusData,
  PortfolioData,
  RefreshStatusData,
  RiskData,
  UniverseFactorsData,
  UniverseSearchData,
  UniverseTickerData,
  UniverseTickerHistoryData,
  WhatIfPreviewData,
  WhatIfScenarioRow,
} from "@/lib/types/cuse4";

export {
  applyPortfolioWhatIf,
  removeHoldingPosition,
  triggerHoldingsImport,
  upsertHoldingPosition,
  useHoldingsAccounts,
  useHoldingsModes,
  useHoldingsPositions,
} from "@/hooks/useHoldingsApi";
export { ApiError };

const SWR_OPTS = {
  revalidateOnFocus: false,
  revalidateOnReconnect: false,
  shouldRetryOnError: false,
  errorRetryCount: 0,
  refreshInterval: 0,
};

const HEAVY_DIAGNOSTICS_OPTS = {
  ...SWR_OPTS,
  refreshInterval: 0,
};

function refreshStatusRefreshInterval(data?: RefreshStatusData): number {
  return String(data?.refresh?.status || "").toLowerCase() === "running" ? 3000 : 0;
}

function operatorStatusRefreshInterval(data?: OperatorStatusData): number {
  const refreshRunning = String(data?.refresh?.status || "").toLowerCase() === "running";
  const laneRunning = (data?.lanes ?? []).some((lane) => String(lane.latest_run?.status || "").toLowerCase() === "running");
  return refreshRunning || laneRunning ? 3000 : 0;
}

export function usePortfolio() {
  return useSWR<PortfolioData>(cuse4ApiPath.portfolio(), apiFetch, SWR_OPTS);
}

export function useExposures(mode: string) {
  return useSWR<ExposuresData>(cuse4ApiPath.exposures(mode), apiFetch, SWR_OPTS);
}

export function useFactorHistory(factorId: string | null, years = 5) {
  const key = factorId ? cuse4ApiPath.exposureHistory(factorId, years) : null;
  return useSWR<FactorHistoryData>(key, apiFetch, SWR_OPTS);
}

export function useRisk() {
  return useSWR<RiskData>(cuse4ApiPath.risk(), apiFetch, SWR_OPTS);
}

export function useUniverseTicker(ticker: string | null) {
  const clean = ticker?.trim().toUpperCase() || null;
  const key = clean ? cuse4ApiPath.universeTicker(clean) : null;
  return useSWR<UniverseTickerData>(key, apiFetch, SWR_OPTS);
}

export function useUniverseTickerHistory(ticker: string | null, years = 5) {
  const clean = ticker?.trim().toUpperCase() || null;
  const key = clean ? cuse4ApiPath.universeTickerHistory(clean, years) : null;
  return useSWR<UniverseTickerHistoryData>(key, apiFetch, SWR_OPTS);
}

export function useUniverseSearch(query: string, limit = 8) {
  const q = query.trim();
  const key = q.length > 0 ? cuse4ApiPath.universeSearch(q, limit) : null;
  return useSWR<UniverseSearchData>(key, apiFetch, {
    ...SWR_OPTS,
    keepPreviousData: true,
  });
}

export function useUniverseFactors() {
  return useSWR<UniverseFactorsData>(cuse4ApiPath.universeFactors(), apiFetch, SWR_OPTS);
}

export function useHealthDiagnostics(enabled = true) {
  return useSWR<HealthDiagnosticsData>(enabled ? cuse4ApiPath.healthDiagnostics() : null, apiFetch, HEAVY_DIAGNOSTICS_OPTS);
}

export function useDataDiagnostics(opts?: { includeExactRowCounts?: boolean; includeExpensiveChecks?: boolean }) {
  return useSWR<DataDiagnosticsData>(cuse4ApiPath.dataDiagnostics(opts), apiFetch, HEAVY_DIAGNOSTICS_OPTS);
}

export function useOperatorStatus(enabled = true) {
  return useSWR<OperatorStatusData>(enabled ? cuse4ApiPath.operatorStatus() : null, apiFetch, {
    ...SWR_OPTS,
    refreshInterval: operatorStatusRefreshInterval,
  });
}

export function useRefreshStatus(enabled = true) {
  return useSWR<RefreshStatusData>(enabled ? cuse4ApiPath.refreshStatus() : null, apiFetch, {
    ...SWR_OPTS,
    refreshInterval: refreshStatusRefreshInterval,
  });
}

export async function triggerRefreshProfile(profile: string): Promise<{
  status: string;
  message?: string;
  refresh?: RefreshStatusData["refresh"];
}> {
  return apiFetch(cuse4ApiPath.refreshProfile(profile), { method: "POST" });
}

export async function triggerServeRefresh(): Promise<{
  status: string;
  message?: string;
  refresh?: RefreshStatusData["refresh"];
}> {
  return triggerRefreshProfile("serve-refresh");
}

export async function previewPortfolioWhatIf(payload: {
  scenario_rows: WhatIfScenarioRow[];
}): Promise<WhatIfPreviewData> {
  return apiFetch<WhatIfPreviewData>(cuse4ApiPath.portfolioWhatIf(), {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export async function triggerDailyMaintenanceRefresh(): Promise<{ status: string }> {
  return triggerRefreshProfile("source-daily-plus-core-if-due");
}
