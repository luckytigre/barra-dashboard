"use client";

import { useEffect, useMemo, useState } from "react";
import { formatCparNumber } from "@/lib/cparTruth";
import type { CparPortfolioHedgeData } from "@/lib/types/cpar";
import CparRiskFactorDrilldown from "./CparRiskFactorDrilldown";
import CparRiskFactorLoadingsChart from "./CparRiskFactorLoadingsChart";

export default function CparRiskFactorSummaryCard({
  portfolio,
}: {
  portfolio: CparPortfolioHedgeData;
}) {
  const factorRows = useMemo(() => (
    [...portfolio.factor_chart].sort((left, right) => (
      left.display_order - right.display_order
      || Math.abs(right.aggregate_beta) - Math.abs(left.aggregate_beta)
      || left.factor_id.localeCompare(right.factor_id)
    ))
  ), [portfolio.factor_chart]);
  const [selectedFactorId, setSelectedFactorId] = useState<string | null>(factorRows[0]?.factor_id || null);

  useEffect(() => {
    setSelectedFactorId((current) => (
      current && factorRows.some((row) => row.factor_id === current)
        ? current
        : factorRows[0]?.factor_id || null
    ));
  }, [factorRows]);

  const selectedFactor = factorRows.find((row) => row.factor_id === selectedFactorId) || factorRows[0] || null;

  return (
    <section className="chart-card" data-testid="cpar-risk-factor-summary">
      <h3>Factor Loadings Profile</h3>
      <div className="section-subtitle">
        This stays cPAR-native. The chart decomposes each factor into negative and positive covered-row contributions,
        keeps the net aggregate beta explicit, and uses the same package-scoped snapshot that drives the hedge preview.
      </div>

      <div className="cpar-badge-row compact">
        <span className="cpar-detail-chip">{factorRows.length} active factors</span>
        <span className="cpar-detail-chip">
          Pre Var {formatCparNumber(portfolio.pre_hedge_factor_variance_proxy, 3)}
        </span>
      </div>

      {factorRows.length === 0 ? (
        <div className="detail-history-empty compact">
          No covered holdings rows contributed to the aggregate thresholded portfolio vector.
        </div>
      ) : (
        <>
          <CparRiskFactorLoadingsChart
            rows={factorRows}
            selectedFactorId={selectedFactor?.factor_id || null}
            onSelectFactor={setSelectedFactorId}
          />

          {selectedFactor ? <CparRiskFactorDrilldown factor={selectedFactor} /> : null}
        </>
      )}
    </section>
  );
}
