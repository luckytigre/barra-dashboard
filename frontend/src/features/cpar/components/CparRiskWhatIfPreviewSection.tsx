"use client";

import { formatCparNumber } from "@/lib/cparTruth";
import type { CparPortfolioWhatIfData } from "@/lib/types/cpar";

export default function CparRiskWhatIfPreviewSection({
  whatIf,
}: {
  whatIf: CparPortfolioWhatIfData;
}) {
  return (
    <section className="chart-card" data-testid="cpar-portfolio-whatif-scenarios">
      <h3>Scenario Preview Rows</h3>
      <div className="section-subtitle">
        Each row is previewed against the active package only. Coverage and fit warnings remain explicit, and no
        holdings mutation occurs.
      </div>
      <div className="dash-table">
        <table>
          <thead>
            <tr>
              <th>Instrument</th>
              <th className="text-right">Current Qty</th>
              <th className="text-right">Delta</th>
              <th className="text-right">Hyp Qty</th>
              <th className="text-right">MV Delta</th>
              <th>Coverage</th>
            </tr>
          </thead>
          <tbody>
            {whatIf.scenario_rows.map((row) => (
              <tr key={row.ric}>
                <td>
                  <strong>{row.ticker || row.ric}</strong>
                  <span className="cpar-table-sub">{row.display_name || row.ric}</span>
                </td>
                <td className="text-right cpar-number-cell">{formatCparNumber(row.current_quantity, 2)}</td>
                <td className="text-right cpar-number-cell">{formatCparNumber(row.quantity_delta, 2)}</td>
                <td className="text-right cpar-number-cell">{formatCparNumber(row.hypothetical_quantity, 2)}</td>
                <td className="text-right cpar-number-cell">{formatCparNumber(row.market_value_delta, 2)}</td>
                <td>{row.coverage_reason || row.coverage}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
