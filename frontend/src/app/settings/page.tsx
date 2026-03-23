"use client";

import { useAppSettings } from "@/components/AppSettingsContext";
import { useBackground } from "@/components/BackgroundContext";

const BACKGROUND_OPTIONS = [
  {
    value: "topo",
    label: "Topographic",
    description: "Layered linework background.",
  },
  {
    value: "flow",
    label: "Flow",
    description: "Animated field background.",
  },
  {
    value: "none",
    label: "None",
    description: "No decorative background.",
  },
] as const;

export default function SettingsPage() {
  const { cparFactorHistoryMode, setCparFactorHistoryMode } = useAppSettings();
  const { mode: backgroundMode, setMode: setBackgroundMode } = useBackground();
  const useMarketAdjustedHistory = cparFactorHistoryMode === "market_adjusted";
  const selectedBackground = BACKGROUND_OPTIONS.find((option) => option.value === backgroundMode) ?? BACKGROUND_OPTIONS[0];

  return (
    <div className="settings-page">
      <div className="settings-shell chart-card">
        <div className="settings-header">
          <div className="settings-kicker">Settings</div>
        </div>

        <section className="settings-section">
          <div className="settings-section-header settings-section-header-global">
            <h3>Global</h3>
          </div>
          <div className="settings-inline-row">
            <div className="settings-inline-copy">
              <div className="settings-option-label">Background</div>
              <div className="settings-option-help">{selectedBackground.description}</div>
            </div>
            <div className="settings-segmented-control" role="tablist" aria-label="Background mode">
              {BACKGROUND_OPTIONS.map((option) => (
                <button
                  key={option.value}
                  type="button"
                  role="tab"
                  aria-selected={backgroundMode === option.value}
                  className={`settings-segmented-option${backgroundMode === option.value ? " active" : ""}`}
                  onClick={() => setBackgroundMode(option.value)}
                >
                  {option.label}
                </button>
              ))}
            </div>
          </div>
        </section>

        <section className="settings-section">
          <div className="settings-section-header settings-section-header-cpar">
            <h3>cPAR</h3>
          </div>
          <div className="settings-inline-row">
            <div className="settings-inline-copy">
              <div className="settings-option-label">Include Regression Intercept In Residualized Factor Returns</div>
              <div className="settings-option-help">
                {useMarketAdjustedHistory
                  ? "On: non-market cPAR drilldowns show market-adjusted return, including the fitted intercept."
                  : "Off: non-market cPAR drilldowns show the pure zero-mean residual after intercept and market are removed."}
              </div>
            </div>
            <button
              type="button"
              className={`toggle-switch settings-toggle toggle-switch-positive${useMarketAdjustedHistory ? " active" : ""}`}
              onClick={() => setCparFactorHistoryMode(useMarketAdjustedHistory ? "residual" : "market_adjusted")}
              aria-pressed={useMarketAdjustedHistory}
              aria-label="Toggle regression intercept in residualized factor returns"
              title={
                useMarketAdjustedHistory
                  ? "Turn off regression intercept in residualized factor returns"
                  : "Turn on regression intercept in residualized factor returns"
              }
            >
              <span className="toggle-switch-track" />
            </button>
          </div>
        </section>

        <section className="settings-section">
          <div className="settings-section-header settings-section-header-cuse">
            <h3>cUSE</h3>
          </div>
          <div className="settings-empty-row">No cUSE-specific settings yet.</div>
        </section>
      </div>
    </div>
  );
}
