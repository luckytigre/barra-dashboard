"use client";

import type { HoldingsAccount, HoldingsImportMode } from "@/lib/types";

interface HoldingsImportPanelProps {
  selectedAccount: string;
  accountOptions: HoldingsAccount[];
  mode: HoldingsImportMode;
  csvSource: string;
  busy: boolean;
  modeOptions: HoldingsImportMode[];
  onAccountChange: (value: string) => void;
  onModeChange: (value: HoldingsImportMode) => void;
  onSourceChange: (value: string) => void;
  onFileChange: (file: File | null) => void;
  onRunImport: () => void;
  modeLabel: (mode: HoldingsImportMode) => string;
  modeHelp: (mode: HoldingsImportMode) => string;
}

export default function HoldingsImportPanel({
  selectedAccount,
  accountOptions,
  mode,
  csvSource,
  busy,
  modeOptions,
  onAccountChange,
  onModeChange,
  onSourceChange,
  onFileChange,
  onRunImport,
  modeLabel,
  modeHelp,
}: HoldingsImportPanelProps) {
  return (
    <div className="holdings-grid">
      <div className="holdings-form-block">
        <label htmlFor="account-id">Account ID</label>
        <input
          id="account-id"
          className="explore-input holdings-compact-input"
          list="account-id-options"
          placeholder="ACCT-CORE"
          value={selectedAccount}
          onChange={(e) => onAccountChange(e.target.value.toUpperCase())}
        />
        <datalist id="account-id-options">
          {accountOptions.map((a) => (
            <option key={a.account_id} value={a.account_id}>
              {a.account_name}
            </option>
          ))}
        </datalist>
      </div>

      <div className="holdings-form-block">
        <label htmlFor="import-mode">CSV Mode</label>
        <select
          id="import-mode"
          className="health-select"
          value={mode}
          onChange={(e) => onModeChange(e.target.value as HoldingsImportMode)}
        >
          {modeOptions.map((m) => (
            <option key={m} value={m}>{modeLabel(m)}</option>
          ))}
        </select>
        <div style={{ color: "rgba(169,182,210,0.8)", fontSize: 11 }}>{modeHelp(mode)}</div>
      </div>

      <div className="holdings-form-block">
        <label htmlFor="csv-source">Source Tag</label>
        <input
          id="csv-source"
          className="explore-input holdings-compact-input"
          value={csvSource}
          onChange={(e) => onSourceChange(e.target.value)}
          placeholder="csv_upload"
        />
      </div>

      <div className="holdings-form-block">
        <label htmlFor="csv-file">Import CSV</label>
        <input
          id="csv-file"
          className="holdings-file-input"
          type="file"
          accept=".csv,text/csv"
          onChange={(e) => onFileChange(e.target.files?.[0] ?? null)}
        />
      </div>

      <div className="holdings-form-block">
        <button
          className="explore-search-btn"
          onClick={onRunImport}
          disabled={busy}
          style={{ width: "fit-content", paddingLeft: 0 }}
        >
          {busy ? "Running..." : "Run CSV Import"}
        </button>
      </div>
    </div>
  );
}
