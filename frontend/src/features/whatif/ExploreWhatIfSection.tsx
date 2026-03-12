"use client";

import { useEffect, useMemo, useState } from "react";
import { mutate } from "swr";
import ExposureBarChart from "@/components/ExposureBarChart";
import InlineShareDraftEditor from "@/features/holdings/components/InlineShareDraftEditor";
import ApiErrorState from "@/components/ApiErrorState";
import {
  previewPortfolioWhatIf,
  triggerHoldingsImport,
  triggerServeRefresh,
  useHoldingsAccounts,
  useHoldingsPositions,
} from "@/hooks/useApi";
import { ApiError, apiPath } from "@/lib/api";
import type {
  HoldingsPosition,
  UniverseTickerItem,
  WhatIfPreviewData,
  WhatIfScenarioRow,
} from "@/lib/types";
import { shortFactorLabel } from "@/lib/factorLabels";

type WhatIfMode = "raw" | "sensitivity" | "risk_contribution";

interface ScenarioDraftRow {
  key: string;
  account_id: string;
  ticker: string;
  quantity_text: string;
  source: string;
}

const MODES: Array<{ key: WhatIfMode; label: string }> = [
  { key: "raw", label: "Exposure" },
  { key: "sensitivity", label: "Sensitivity" },
  { key: "risk_contribution", label: "Risk Contrib" },
];

function normalizeAccountId(raw: string | null | undefined): string {
  return String(raw || "").trim().toLowerCase();
}

function normalizeTicker(raw: string | null | undefined): string {
  return String(raw || "").trim().toUpperCase();
}

function scenarioKey(accountId: string, ticker: string): string {
  return `${normalizeAccountId(accountId)}::${normalizeTicker(ticker)}`;
}

function parseQty(raw: string): number | null {
  const clean = String(raw || "").trim().replaceAll(",", "");
  if (!clean) return null;
  const out = Number.parseFloat(clean);
  return Number.isFinite(out) ? out : null;
}

function fmtQty(n: number): string {
  if (!Number.isFinite(n)) return "—";
  const rounded = Number(n.toFixed(6));
  if (Number.isInteger(rounded)) return `${rounded}`;
  return `${rounded}`;
}

function fmtMarketValue(n: number): string {
  if (!Number.isFinite(n)) return "—";
  if (Math.abs(n) >= 1e6) return `${(n / 1e6).toFixed(2)}M`;
  if (Math.abs(n) >= 1e3) return `${(n / 1e3).toFixed(1)}K`;
  return n.toFixed(2);
}

function marketValueFromRow(row: HoldingsPosition, priceMap: Map<string, number>): number {
  const px = Number(priceMap.get(normalizeTicker(row.ticker)) || 0);
  return Number(row.quantity || 0) * px;
}

export default function ExploreWhatIfSection({
  item,
  priceMap,
}: {
  item: UniverseTickerItem;
  priceMap: Map<string, number>;
}) {
  const { data: accountsData } = useHoldingsAccounts();
  const { data: holdingsData, error: holdingsError } = useHoldingsPositions(null);

  const [mode, setMode] = useState<WhatIfMode>("raw");
  const [accountId, setAccountId] = useState("");
  const [quantityText, setQuantityText] = useState("");
  const [source, setSource] = useState("what_if");
  const [busy, setBusy] = useState(false);
  const [previewData, setPreviewData] = useState<WhatIfPreviewData | null>(null);
  const [errorMessage, setErrorMessage] = useState("");
  const [resultMessage, setResultMessage] = useState("");
  const [scenarioDrafts, setScenarioDrafts] = useState<Record<string, ScenarioDraftRow>>({});

  const accountOptions = accountsData?.accounts ?? [];
  const holdingsRows = holdingsData?.positions ?? [];
  const selectedTicker = normalizeTicker(item.ticker);

  useEffect(() => {
    if (!accountId && accountOptions.length > 0) {
      setAccountId(accountOptions[0].account_id);
    }
  }, [accountId, accountOptions]);

  useEffect(() => {
    setPreviewData(null);
    setErrorMessage("");
    setResultMessage("");
  }, [selectedTicker]);

  const selectedTickerRows = useMemo(
    () =>
      holdingsRows
        .filter((row) => normalizeTicker(row.ticker) === selectedTicker)
        .sort((a, b) => normalizeAccountId(a.account_id).localeCompare(normalizeAccountId(b.account_id))),
    [holdingsRows, selectedTicker],
  );

  useEffect(() => {
    if (!accountId) return;
    const key = scenarioKey(accountId, selectedTicker);
    const staged = scenarioDrafts[key];
    if (staged) {
      setQuantityText(staged.quantity_text);
      return;
    }
    const liveRow = selectedTickerRows.find((row) => normalizeAccountId(row.account_id) === normalizeAccountId(accountId));
    if (liveRow) {
      setQuantityText(fmtQty(Number(liveRow.quantity || 0)));
      return;
    }
    setQuantityText("0");
  }, [accountId, selectedTicker, selectedTickerRows, scenarioDrafts]);

  const scenarioRows = useMemo(
    () =>
      Object.values(scenarioDrafts).sort((a, b) => {
        const byTicker = normalizeTicker(a.ticker).localeCompare(normalizeTicker(b.ticker));
        if (byTicker !== 0) return byTicker;
        return normalizeAccountId(a.account_id).localeCompare(normalizeAccountId(b.account_id));
      }),
    [scenarioDrafts],
  );

  const liveMarketValueSorted = useMemo(
    () =>
      [...holdingsRows]
        .sort((a, b) => Math.abs(marketValueFromRow(b, priceMap)) - Math.abs(marketValueFromRow(a, priceMap)))
        .slice(0, 10),
    [holdingsRows, priceMap],
  );

  function clearMessages() {
    setErrorMessage("");
    setResultMessage("");
  }

  function stageSelectedTicker() {
    clearMessages();
    setPreviewData(null);
    const account = normalizeAccountId(accountId);
    const qty = parseQty(quantityText);
    if (!account) {
      setErrorMessage("Select an account for the what-if row.");
      return;
    }
    if (qty === null) {
      setErrorMessage("Quantity must be numeric.");
      return;
    }
    const key = scenarioKey(account, selectedTicker);
    setScenarioDrafts((prev) => ({
      ...prev,
      [key]: {
        key,
        account_id: account,
        ticker: selectedTicker,
        quantity_text: quantityText.trim(),
        source: source.trim() || "what_if",
      },
    }));
    setResultMessage(`Scenario row staged for ${selectedTicker} in ${account}. Preview to compare impacts.`);
  }

  function updateScenarioRow(key: string, quantity_value: string) {
    setPreviewData(null);
    setScenarioDrafts((prev) => {
      const existing = prev[key];
      if (!existing) return prev;
      return {
        ...prev,
        [key]: {
          ...existing,
          quantity_text: quantity_value,
        },
      };
    });
  }

  function adjustScenarioRow(key: string, delta: number) {
    const existing = scenarioDrafts[key];
    if (!existing) return;
    const currentQty = parseQty(existing.quantity_text);
    if (currentQty === null) {
      setErrorMessage(`Fix quantity for ${existing.ticker} before stepping it.`);
      return;
    }
    updateScenarioRow(key, fmtQty(currentQty + delta));
  }

  function removeScenarioRow(key: string) {
    setPreviewData(null);
    clearMessages();
    setScenarioDrafts((prev) => {
      const next = { ...prev };
      delete next[key];
      return next;
    });
  }

  async function runPreview() {
    clearMessages();
    const payloadRows: WhatIfScenarioRow[] = [];
    for (const row of scenarioRows) {
      const qty = parseQty(row.quantity_text);
      if (qty === null) {
        setErrorMessage(`Fix quantity for ${row.ticker} before previewing.`);
        return;
      }
      payloadRows.push({
        account_id: row.account_id,
        ticker: row.ticker,
        quantity: qty,
        source: row.source,
      });
    }
    try {
      setBusy(true);
      const out = await previewPortfolioWhatIf({ scenario_rows: payloadRows });
      setPreviewData(out);
      setResultMessage(`Preview refreshed for ${payloadRows.length} scenario row${payloadRows.length === 1 ? "" : "s"}.`);
    } catch (err) {
      if (err instanceof ApiError) {
        setErrorMessage(typeof err.detail === "string" ? err.detail : err.message);
      } else if (err instanceof Error) {
        setErrorMessage(err.message);
      } else {
        setErrorMessage("What-if preview failed.");
      }
    } finally {
      setBusy(false);
    }
  }

  async function applyScenario() {
    clearMessages();
    if (scenarioRows.length === 0) {
      setErrorMessage("Stage at least one scenario row first.");
      return;
    }
    const byAccount = new Map<string, Array<{ ticker: string; quantity: number; source: string }>>();
    for (const row of scenarioRows) {
      const qty = parseQty(row.quantity_text);
      if (qty === null) {
        setErrorMessage(`Fix quantity for ${row.ticker} before applying.`);
        return;
      }
      const rows = byAccount.get(row.account_id) ?? [];
      rows.push({
        ticker: row.ticker,
        quantity: qty,
        source: row.source || "what_if",
      });
      byAccount.set(row.account_id, rows);
    }

    try {
      setBusy(true);
      for (const [account, rows] of byAccount.entries()) {
        await triggerHoldingsImport({
          account_id: account,
          mode: "upsert_absolute",
          rows,
          default_source: "what_if",
          trigger_refresh: false,
        });
      }
      await triggerServeRefresh();
      await Promise.all([
        mutate(apiPath.holdingsAccounts()),
        mutate(apiPath.holdingsPositions(null)),
        mutate(apiPath.portfolio()),
        mutate(apiPath.risk()),
        mutate(apiPath.exposures("raw")),
        mutate(apiPath.exposures("sensitivity")),
        mutate(apiPath.exposures("risk_contribution")),
        mutate(apiPath.operatorStatus()),
      ]);
      setScenarioDrafts({});
      setPreviewData(null);
      setResultMessage(`Applied ${scenarioRows.length} what-if row${scenarioRows.length === 1 ? "" : "s"} and started RECALC.`);
    } catch (err) {
      if (err instanceof ApiError) {
        setErrorMessage(typeof err.detail === "string" ? err.detail : err.message);
      } else if (err instanceof Error) {
        setErrorMessage(err.message);
      } else {
        setErrorMessage("Could not apply what-if scenario.");
      }
    } finally {
      setBusy(false);
    }
  }

  function discardScenario() {
    setScenarioDrafts({});
    setPreviewData(null);
    clearMessages();
    setResultMessage("Discarded what-if scenario rows.");
  }

  return (
    <div className="chart-card mb-4">
      <div className="explore-whatif-header">
        <div>
          <h3 style={{ margin: 0 }}>Portfolio What-If</h3>
          <div className="section-subtitle" style={{ marginTop: 6 }}>
            Preview account-aware holdings changes for {selectedTicker} against the current live holdings ledger. Preview is read-only; nothing writes to Neon until you apply and RECALC.
          </div>
        </div>
      </div>

      {holdingsError && (
        <div style={{ marginTop: 12 }}>
          <ApiErrorState title="What-If Holdings Not Ready" error={holdingsError} />
        </div>
      )}

      <div className="explore-whatif-builder">
        <div className="holdings-form-block">
          <label htmlFor="explore-whatif-account">Account</label>
          <input
            id="explore-whatif-account"
            className="explore-input holdings-compact-input"
            list="explore-whatif-account-options"
            value={accountId}
            onChange={(e) => setAccountId(e.target.value.toLowerCase())}
            placeholder="ibkr_multistrat"
          />
          <datalist id="explore-whatif-account-options">
            {accountOptions.map((account) => (
              <option key={account.account_id} value={account.account_id}>
                {account.account_name}
              </option>
            ))}
          </datalist>
        </div>
        <div className="holdings-form-block">
          <label htmlFor="explore-whatif-ticker">Ticker</label>
          <input
            id="explore-whatif-ticker"
            className="explore-input holdings-compact-input"
            value={selectedTicker}
            disabled
          />
        </div>
        <div className="holdings-form-block">
          <label htmlFor="explore-whatif-qty">Scenario Qty</label>
          <input
            id="explore-whatif-qty"
            className="explore-input holdings-compact-input"
            value={quantityText}
            onChange={(e) => setQuantityText(e.target.value)}
            inputMode="decimal"
            placeholder="0"
          />
        </div>
        <div className="holdings-form-block">
          <label htmlFor="explore-whatif-source">Source</label>
          <input
            id="explore-whatif-source"
            className="explore-input holdings-compact-input"
            value={source}
            onChange={(e) => setSource(e.target.value)}
            placeholder="what_if"
          />
        </div>
        <div className="holdings-form-actions explore-whatif-actions">
          <button className="btn-action" onClick={stageSelectedTicker} disabled={busy}>
            Stage Scenario Row
          </button>
          <button className="btn-action" onClick={() => void runPreview()} disabled={busy || scenarioRows.length === 0}>
            {busy ? "Working..." : "Preview What-If"}
          </button>
          <button className="btn-action" onClick={() => void applyScenario()} disabled={busy || scenarioRows.length === 0}>
            Apply + RECALC
          </button>
          <button className="btn-action" onClick={discardScenario} disabled={busy || (scenarioRows.length === 0 && !previewData)}>
            Discard
          </button>
        </div>
      </div>

      {(resultMessage || errorMessage) && (
        <div className={`explore-whatif-message${errorMessage ? " error" : ""}`}>
          {errorMessage || resultMessage}
        </div>
      )}

      <div className="explore-whatif-grid">
        <div className="dash-table">
          <h4 style={{ marginTop: 0 }}>Current Holdings For {selectedTicker}</h4>
          <table>
            <thead>
              <tr>
                <th>Account</th>
                <th className="text-right">Qty</th>
                <th className="text-right">Mkt Val</th>
                <th className="text-right">Source</th>
              </tr>
            </thead>
            <tbody>
              {selectedTickerRows.length > 0 ? selectedTickerRows.map((row) => (
                <tr key={`${row.account_id}:${row.ticker}`}>
                  <td>{row.account_id}</td>
                  <td className="text-right">{fmtQty(row.quantity)}</td>
                  <td className={`text-right ${marketValueFromRow(row, priceMap) >= 0 ? "positive" : "negative"}`.trim()}>
                    {fmtMarketValue(marketValueFromRow(row, priceMap))}
                  </td>
                  <td className="text-right">{row.source || "—"}</td>
                </tr>
              )) : (
                <tr>
                  <td colSpan={4} className="holdings-empty-row">No current holding for {selectedTicker}.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>

        <div className="dash-table">
          <h4 style={{ marginTop: 0 }}>Scenario Rows [{scenarioRows.length}]</h4>
          <table>
            <thead>
              <tr>
                <th>Account</th>
                <th>Ticker</th>
                <th className="text-right">Qty</th>
                <th className="text-right">Action</th>
              </tr>
            </thead>
            <tbody>
              {scenarioRows.length > 0 ? scenarioRows.map((row) => (
                <tr key={row.key}>
                  <td>{row.account_id}</td>
                  <td>{row.ticker}</td>
                  <td className="text-right">
                    <InlineShareDraftEditor
                      quantityText={row.quantity_text}
                      draftActive
                      invalid={parseQty(row.quantity_text) === null}
                      titleBase={`${row.ticker} ${row.account_id}`}
                      onQuantityTextChange={(value) => updateScenarioRow(row.key, value)}
                      onStep={(delta) => adjustScenarioRow(row.key, delta)}
                    />
                  </td>
                  <td className="text-right">
                    <button className="btn-action subtle" onClick={() => removeScenarioRow(row.key)} type="button">
                      Remove
                    </button>
                  </td>
                </tr>
              )) : (
                <tr>
                  <td colSpan={4} className="holdings-empty-row">Stage one or more scenario rows to preview portfolio impact.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>

      <div className="dash-table" style={{ marginTop: 14 }}>
        <h4 style={{ marginTop: 0 }}>Top Live Holdings</h4>
        <table>
          <thead>
            <tr>
              <th>Ticker</th>
              <th className="text-right">Qty</th>
              <th className="text-right">Mkt Val</th>
              <th className="text-right">Account</th>
            </tr>
          </thead>
          <tbody>
            {liveMarketValueSorted.map((row) => (
              <tr key={`${row.account_id}:${row.ticker}`}>
                <td>{row.ticker}</td>
                <td className="text-right">{fmtQty(row.quantity)}</td>
                <td className={`text-right ${marketValueFromRow(row, priceMap) >= 0 ? "positive" : "negative"}`.trim()}>
                  {fmtMarketValue(marketValueFromRow(row, priceMap))}
                </td>
                <td className="text-right">{row.account_id}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {previewData && (
        <>
          <div className="explore-mode-toggle" style={{ marginTop: 16 }}>
            {MODES.map((entry) => (
              <button
                key={entry.key}
                type="button"
                className={`explore-mode-btn${mode === entry.key ? " active" : ""}`}
                onClick={() => setMode(entry.key)}
              >
                {entry.label}
              </button>
            ))}
          </div>

          <div className="explore-detail-grid" style={{ marginTop: 12 }}>
            <div className="chart-card">
              <h4>Current Portfolio</h4>
              <ExposureBarChart factors={previewData.current.exposure_modes[mode]} mode={mode} />
            </div>
            <div className="chart-card">
              <h4>Hypothetical Portfolio</h4>
              <ExposureBarChart factors={previewData.hypothetical.exposure_modes[mode]} mode={mode} />
            </div>
          </div>

          <div className="explore-whatif-grid" style={{ marginTop: 12 }}>
            <div className="dash-table">
              <h4 style={{ marginTop: 0 }}>Risk Share Delta</h4>
              <table>
                <thead>
                  <tr>
                    <th>Bucket</th>
                    <th className="text-right">Current</th>
                    <th className="text-right">Hypothetical</th>
                    <th className="text-right">Delta</th>
                  </tr>
                </thead>
                <tbody>
                  {(["country", "industry", "style", "idio"] as const).map((bucket) => (
                    <tr key={bucket}>
                      <td>{bucket}</td>
                      <td className="text-right">{previewData.current.risk_shares[bucket].toFixed(2)}%</td>
                      <td className="text-right">{previewData.hypothetical.risk_shares[bucket].toFixed(2)}%</td>
                      <td className={`text-right ${previewData.diff.risk_shares[bucket] >= 0 ? "positive" : "negative"}`.trim()}>
                        {previewData.diff.risk_shares[bucket] >= 0 ? "+" : ""}
                        {previewData.diff.risk_shares[bucket].toFixed(2)}%
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            <div className="dash-table">
              <h4 style={{ marginTop: 0 }}>Holding Delta</h4>
              <table>
                <thead>
                  <tr>
                    <th>Account</th>
                    <th>Ticker</th>
                    <th className="text-right">Current</th>
                    <th className="text-right">Hypothetical</th>
                    <th className="text-right">Delta</th>
                  </tr>
                </thead>
                <tbody>
                  {previewData.holding_deltas.length > 0 ? previewData.holding_deltas.map((row) => (
                    <tr key={`${row.account_id}:${row.ticker}`}>
                      <td>{row.account_id}</td>
                      <td>{row.ticker}</td>
                      <td className="text-right">{fmtQty(row.current_quantity)}</td>
                      <td className="text-right">{fmtQty(row.hypothetical_quantity)}</td>
                      <td className={`text-right ${row.delta_quantity >= 0 ? "positive" : "negative"}`.trim()}>
                        {row.delta_quantity >= 0 ? "+" : ""}
                        {fmtQty(row.delta_quantity)}
                      </td>
                    </tr>
                  )) : (
                    <tr>
                      <td colSpan={5} className="holdings-empty-row">No holding delta.</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>

          <div className="dash-table" style={{ marginTop: 12 }}>
            <h4 style={{ marginTop: 0 }}>Key Factor Differences</h4>
            <table>
              <thead>
                <tr>
                  <th>Factor</th>
                  <th className="text-right">Current</th>
                  <th className="text-right">Hypothetical</th>
                  <th className="text-right">Delta</th>
                </tr>
              </thead>
              <tbody>
                {previewData.diff.factor_deltas[mode].map((row) => (
                  <tr key={row.factor}>
                    <td>{shortFactorLabel(row.factor)}</td>
                    <td className="text-right">{row.current.toFixed(mode === "risk_contribution" ? 2 : 4)}{mode === "risk_contribution" ? "%" : ""}</td>
                    <td className="text-right">{row.hypothetical.toFixed(mode === "risk_contribution" ? 2 : 4)}{mode === "risk_contribution" ? "%" : ""}</td>
                    <td className={`text-right ${row.delta >= 0 ? "positive" : "negative"}`.trim()}>
                      {row.delta >= 0 ? "+" : ""}
                      {row.delta.toFixed(mode === "risk_contribution" ? 2 : 4)}
                      {mode === "risk_contribution" ? "%" : ""}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}
