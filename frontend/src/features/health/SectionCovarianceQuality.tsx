"use client";

import HelpLabel from "@/components/HelpLabel";
import type { HealthDiagnosticsData } from "@/lib/types/cuse4";
import { Bar, Line } from "./charts";
import { commonLineOptions, fmtPct, seriesData } from "./utils";

export default function SectionCovarianceQuality({ data }: { data: HealthDiagnosticsData }) {
  const eigenvalues = data.section4.eigenvalues ?? [];
  const eigenData = {
    labels: eigenvalues.map((_v, i) => `λ${i + 1}`),
    datasets: [
      {
        label: "Eigenvalue",
        data: eigenvalues,
        backgroundColor: "rgba(169, 182, 210, 0.55)",
        borderWidth: 0,
      },
    ],
  };

  return (
    <div className="chart-card">
      <h3>
        <HelpLabel
          label="Section 4 — Covariance Quality"
          plain="Checks whether covariance forecasts are stable and close to what actually happened."
          math="Uses eigenvalues and forecast-vs-realized volatility"
        />
      </h3>
      <div className="health-grid-2-half">
        <div>
          <h4>Eigenvalue Spectrum</h4>
          <div className="health-chart-sm">
            <Bar data={eigenData} options={{ responsive: true, maintainAspectRatio: false }} />
          </div>
        </div>
        <div>
          <h4>Rolling Average Factor Vol</h4>
          <div className="health-chart-sm">
            <Line
              data={seriesData(data.section4.rolling_avg_factor_vol, "Avg Factor Vol", "rgba(169, 182, 210, 0.85)", 100)}
              options={{
                ...commonLineOptions,
                scales: {
                  ...commonLineOptions.scales,
                  y: {
                    ...(commonLineOptions.scales?.y || {}),
                    ticks: {
                      color: "rgba(169, 182, 210, 0.5)",
                      callback: (v) => `${Number(v).toFixed(1)}%`,
                      font: { size: 9 },
                    },
                  },
                },
              }}
            />
          </div>
        </div>
      </div>
      <div className="dash-table health-table">
        <table>
          <thead>
            <tr>
              <th>Portfolio Sample</th>
              <th className="text-right">Forecast Vol</th>
              <th className="text-right">Realized Vol (60d)</th>
              <th className="text-right">Gap</th>
            </tr>
          </thead>
          <tbody>
            {(data.section4.forecast_vs_realized || []).map((row) => {
              const gap = (Number(row.realized_vol_60d) || 0) - (Number(row.forecast_vol) || 0);
              return (
                <tr key={row.name}>
                  <td>{row.name}</td>
                  <td className="text-right">{fmtPct(row.forecast_vol, 2)}</td>
                  <td className="text-right">{fmtPct(row.realized_vol_60d, 2)}</td>
                  <td className={`text-right ${gap >= 0 ? "positive" : "negative"}`}>{fmtPct(gap, 2)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
