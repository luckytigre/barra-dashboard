"use client";

import { formatCparNumber } from "@/lib/cparTruth";
import type { CparLoading } from "@/lib/types/cpar";

const GROUP_LABELS: Record<string, string> = {
  market: "Market",
  sector: "Sector",
  style: "Style",
};

export default function CparExploreLoadingsChart({
  rows,
  selectedFactorId,
  onSelectFactor,
}: {
  rows: CparLoading[];
  selectedFactorId: string | null;
  onSelectFactor: (factorId: string) => void;
}) {
  const maxMagnitude = rows.reduce((maxValue, row) => Math.max(maxValue, Math.abs(row.beta)), 0) || 1;

  return (
    <div className="cpar-factor-chart-shell cpar-explore-factor-chart" data-testid="cpar-explore-loadings-chart">
      <div className="cpar-factor-chart-legend">
        <span className="cpar-detail-chip">Left: negative beta</span>
        <span className="cpar-detail-chip">Right: positive beta</span>
        <span className="cpar-detail-chip">Thresholded loadings only</span>
      </div>

      <div className="cpar-factor-chart-grid" role="list" aria-label="cPAR thresholded loadings chart">
        {rows.map((row, index) => {
          const previousGroup = index > 0 ? rows[index - 1]?.group : null;
          const showGroupLabel = index === 0 || previousGroup !== row.group;
          const width = Math.min(50, (Math.abs(row.beta) / maxMagnitude) * 50);
          const left = row.beta >= 0 ? 50 : 50 - width;
          const toneClass = row.beta >= 0 ? "positive" : "negative";

          return (
            <div key={row.factor_id} role="listitem">
              {showGroupLabel ? (
                <div className="cpar-factor-chart-group-label">{GROUP_LABELS[row.group] || row.group}</div>
              ) : null}
              <button
                type="button"
                className={`cpar-factor-chart-row ${selectedFactorId === row.factor_id ? "selected" : ""}`}
                onClick={() => onSelectFactor(row.factor_id)}
                aria-pressed={selectedFactorId === row.factor_id}
              >
                <div className="cpar-factor-chart-meta">
                  <div>
                    <div className="cpar-factor-chart-label">{row.label}</div>
                    <div className="cpar-table-sub">{row.factor_id}</div>
                  </div>
                  <div className="cpar-factor-chart-values">
                    <span>{formatCparNumber(row.beta, 3)} beta</span>
                  </div>
                </div>
                <div className="cpar-factor-chart-track">
                  <span className="cpar-factor-chart-axis" aria-hidden="true" />
                  <span
                    className={`cpar-factor-chart-bar ${toneClass}`}
                    aria-hidden="true"
                    style={{
                      left: `${left}%`,
                      width: `${width}%`,
                    }}
                  />
                </div>
              </button>
            </div>
          );
        })}
      </div>
    </div>
  );
}
