"use client";

import { useEffect, useMemo, useState } from "react";
import { formatCparNumber } from "@/lib/cparTruth";
import type { CparTickerDetailData } from "@/lib/types/cpar";
import CparExploreLoadingsChart from "./CparExploreLoadingsChart";

export default function CparExploreLoadingsSection({
  detail,
}: {
  detail: CparTickerDetailData;
}) {
  const thresholdedRows = useMemo(() => (
    [...detail.thresholded_loadings].sort((left, right) => (
      left.display_order - right.display_order
      || Math.abs(right.beta) - Math.abs(left.beta)
      || left.factor_id.localeCompare(right.factor_id)
    ))
  ), [detail.thresholded_loadings]);
  const rawRows = useMemo(() => (
    [...detail.raw_loadings].sort((left, right) => (
      left.display_order - right.display_order
      || Math.abs(right.beta) - Math.abs(left.beta)
      || left.factor_id.localeCompare(right.factor_id)
    ))
  ), [detail.raw_loadings]);
  const [selectedFactorId, setSelectedFactorId] = useState<string | null>(thresholdedRows[0]?.factor_id || null);

  useEffect(() => {
    setSelectedFactorId((current) => (
      current && thresholdedRows.some((row) => row.factor_id === current)
        ? current
        : thresholdedRows[0]?.factor_id || null
    ));
  }, [thresholdedRows]);

  const selectedThresholded = thresholdedRows.find((row) => row.factor_id === selectedFactorId) || thresholdedRows[0] || null;
  const selectedRaw = rawRows.find((row) => row.factor_id === selectedThresholded?.factor_id) || null;
  const rawOnlyRows = rawRows.filter((row) => !thresholdedRows.some((item) => item.factor_id === row.factor_id));

  return (
    <section className="chart-card cpar-explore-loadings-module" data-testid="cpar-loadings-panel">
      <div className="cpar-explore-module-header">
        <div>
          <div className="cpar-explore-kicker">Loadings</div>
          <h3 className="cpar-explore-module-title">Thresholded Factor Interpretation</h3>
          <div className="cpar-explore-module-subtitle">
            `/cpar/explore` stays on the persisted fit row. Thresholded loadings lead because they match the hedge
            trade space; raw ETF loadings stay visible as secondary context below.
          </div>
        </div>
        <div className="cpar-explore-module-status">{thresholdedRows.length} thresholded</div>
      </div>

      <div className="explore-hero-stats cpar-explore-hero-stats cpar-explore-loadings-stats">
        <div className="explore-hero-stat">
          <span className="label">Thresholded</span>
          <span className="value">{thresholdedRows.length}</span>
        </div>
        <div className="explore-hero-stat">
          <span className="label">Raw Factors</span>
          <span className="value">{rawRows.length}</span>
        </div>
        <div className="explore-hero-stat">
          <span className="label">Market Step</span>
          <span className="value">{formatCparNumber(detail.beta_market_step1, 3)}</span>
        </div>
        <div className="explore-hero-stat">
          <span className="label">Trade Beta</span>
          <span className="value">{formatCparNumber(detail.beta_spy_trade, 3)}</span>
        </div>
      </div>

      {detail.fit_status === "limited_history" ? (
        <div className="cpar-inline-message warning">
          <strong>Limited history stays visible, but deserves more caution.</strong>
          <span>Use the persisted loadings, then confirm hedge stability on `/cpar/hedge` before treating the package row as operational.</span>
        </div>
      ) : null}

      {thresholdedRows.length === 0 ? (
        <div className="detail-history-empty compact">
          Thresholding zeroed every non-market leg in the persisted trade-space payload.
        </div>
      ) : (
        <>
          <CparExploreLoadingsChart
            rows={thresholdedRows}
            selectedFactorId={selectedThresholded?.factor_id || null}
            onSelectFactor={setSelectedFactorId}
          />

          {selectedThresholded ? (
            <div className="cpar-explore-selected-loading">
              <div>
                <div className="cpar-explore-panel-title">{selectedThresholded.label}</div>
                <div className="cpar-explore-panel-subtitle">
                  Thresholded loadings are the cPAR-native interpretation surface for this page. Raw ETF loadings remain visible below for comparison.
                </div>
              </div>
              <div className="cpar-badge-row compact">
                <span className="cpar-detail-chip">{selectedThresholded.factor_id}</span>
                <span className="cpar-detail-chip">Thresholded {formatCparNumber(selectedThresholded.beta, 3)}</span>
                {selectedRaw ? <span className="cpar-detail-chip">Raw {formatCparNumber(selectedRaw.beta, 3)}</span> : null}
              </div>
            </div>
          ) : null}
        </>
      )}

      {rawOnlyRows.length > 0 ? (
        <div className="cpar-badge-row compact">
          <span className="cpar-detail-chip">{rawOnlyRows.length} raw-only factor{rawOnlyRows.length === 1 ? "" : "s"}</span>
          {rawOnlyRows.slice(0, 3).map((row) => (
            <span key={row.factor_id} className="cpar-detail-chip">
              {row.factor_id} {formatCparNumber(row.beta, 3)}
            </span>
          ))}
        </div>
      ) : null}

      <details className="cpar-explore-raw-details">
        <summary>Raw ETF loadings</summary>
        {rawRows.length === 0 ? (
          <div className="detail-history-empty compact">
            No persisted raw ETF loadings were available for this cPAR fit.
          </div>
        ) : (
          <div className="dash-table">
            <table>
              <thead>
                <tr>
                  <th>Factor</th>
                  <th>Group</th>
                  <th className="text-right">Raw Beta</th>
                </tr>
              </thead>
              <tbody>
                {rawRows.map((row) => (
                  <tr key={row.factor_id}>
                    <td>
                      <strong>{row.label}</strong>
                      <span className="cpar-table-sub">{row.factor_id}</span>
                    </td>
                    <td>{row.group}</td>
                    <td className="text-right cpar-number-cell">{formatCparNumber(row.beta, 3)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </details>
    </section>
  );
}
