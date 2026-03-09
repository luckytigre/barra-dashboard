"use client";

import ApiErrorState from "@/components/ApiErrorState";
import type { HoldingsPosition } from "@/lib/types";
import { fmtQty } from "../lib/csv";

interface HoldingsLedgerSectionProps {
  selectedAccount: string;
  holdingsRows: HoldingsPosition[];
  holdingsError?: unknown;
  busy: boolean;
  onAdjust: (row: HoldingsPosition, delta: number) => Promise<void>;
  onRemove: (row: HoldingsPosition) => void;
}

export default function HoldingsLedgerSection({
  selectedAccount,
  holdingsRows,
  holdingsError,
  busy,
  onAdjust,
  onRemove,
}: HoldingsLedgerSectionProps) {
  return (
    <div className="chart-card mb-4">
      <h3>
        Current Holdings
        {selectedAccount ? ` (${selectedAccount})` : ""}
        {" "}
        [{holdingsRows.length}]
      </h3>
      <div className="detail-history-empty" style={{ marginBottom: 10 }}>
        This table is the live holdings ledger. The model portfolio table below refreshes after a serving update, so temporary differences are expected until `RECALC` runs.
      </div>
      {holdingsError ? (
        <ApiErrorState title="Holdings Not Ready" error={holdingsError} />
      ) : (
        <div className="dash-table" style={{ overflowX: "auto" }}>
          <table>
            <thead>
              <tr>
                <th>Account</th>
                <th>Ticker</th>
                <th>RIC</th>
                <th className="text-right">Quantity</th>
                <th>Source</th>
                <th>Updated</th>
                <th>Action</th>
              </tr>
            </thead>
            <tbody>
              {holdingsRows.map((row) => (
                <tr key={`${row.account_id}:${row.ric}`}>
                  <td>{row.account_id}</td>
                  <td>{row.ticker || "—"}</td>
                  <td>{row.ric}</td>
                  <td className="text-right">{fmtQty(row.quantity)}</td>
                  <td>{row.source || "—"}</td>
                  <td>{row.updated_at || "—"}</td>
                  <td>
                    <span style={{ display: "inline-flex", gap: 8, alignItems: "center" }}>
                      <button
                        className="explore-search-btn"
                        onClick={() => void onAdjust(row, 5)}
                        disabled={busy}
                        style={{ padding: 0 }}
                        title={`Increase ${row.ticker} by 5 shares`}
                      >
                        ↑5
                      </button>
                      <button
                        className="explore-search-btn"
                        onClick={() => void onAdjust(row, -5)}
                        disabled={busy}
                        style={{ padding: 0 }}
                        title={`Decrease ${row.ticker} by 5 shares`}
                      >
                        ↓5
                      </button>
                      <button
                        className="explore-search-btn"
                        onClick={() => onRemove(row)}
                        disabled={busy}
                        style={{ padding: 0 }}
                      >
                        Remove
                      </button>
                    </span>
                  </td>
                </tr>
              ))}
              {holdingsRows.length === 0 && (
                <tr>
                  <td colSpan={7} style={{ color: "rgba(169,182,210,0.75)" }}>
                    No positions for this account yet.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
