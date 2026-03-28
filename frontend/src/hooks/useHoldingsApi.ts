"use client";

import useSWR from "swr";
import { ApiError, apiFetch } from "@/lib/apiTransport";
import { holdingsApiPath } from "@/lib/holdingsApi";
import type {
  HoldingsAccountsData,
  HoldingsImportMode,
  HoldingsImportResponse,
  HoldingsModeData,
  HoldingsPositionEditResponse,
  HoldingsPositionsData,
  WhatIfApplyResponse,
  WhatIfScenarioRow,
} from "@/lib/types/holdings";

const SWR_OPTS = {
  revalidateOnFocus: false,
  revalidateOnReconnect: false,
  shouldRetryOnError: false,
  errorRetryCount: 0,
  refreshInterval: 0,
};

export { ApiError };

export function useHoldingsModes() {
  return useSWR<HoldingsModeData>(holdingsApiPath.holdingsModes(), apiFetch, SWR_OPTS);
}

export function useHoldingsAccounts() {
  return useSWR<HoldingsAccountsData>(holdingsApiPath.holdingsAccounts(), apiFetch, SWR_OPTS);
}

export function useHoldingsPositions(accountId?: string | null) {
  const key = holdingsApiPath.holdingsPositions(accountId);
  return useSWR<HoldingsPositionsData>(key, apiFetch, SWR_OPTS);
}

export async function applyPortfolioWhatIf(payload: {
  scenario_rows: WhatIfScenarioRow[];
  requested_by?: string;
  default_source?: string;
}): Promise<WhatIfApplyResponse> {
  return apiFetch<WhatIfApplyResponse>(holdingsApiPath.portfolioWhatIfApply(), {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export async function triggerHoldingsImport(payload: {
  account_id: string;
  mode: HoldingsImportMode;
  rows: Array<{
    account_id?: string;
    ric?: string;
    ticker?: string;
    quantity: number;
    source?: string;
  }>;
  filename?: string;
  requested_by?: string;
  notes?: string;
  default_source?: string;
  dry_run?: boolean;
  trigger_refresh?: boolean;
}): Promise<HoldingsImportResponse> {
  return apiFetch<HoldingsImportResponse>(holdingsApiPath.holdingsImport(), {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export async function upsertHoldingPosition(payload: {
  account_id: string;
  quantity: number;
  ric?: string;
  ticker?: string;
  source?: string;
  requested_by?: string;
  notes?: string;
  dry_run?: boolean;
  trigger_refresh?: boolean;
}): Promise<HoldingsPositionEditResponse> {
  return apiFetch<HoldingsPositionEditResponse>(holdingsApiPath.holdingsPosition(), {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload),
  });
}

export async function removeHoldingPosition(payload: {
  account_id: string;
  ric?: string;
  ticker?: string;
  requested_by?: string;
  notes?: string;
  dry_run?: boolean;
  trigger_refresh?: boolean;
}): Promise<HoldingsPositionEditResponse> {
  return apiFetch<HoldingsPositionEditResponse>(holdingsApiPath.holdingsPositionRemove(), {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload),
  });
}
