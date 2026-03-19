"use client";

import { useMemo, useState } from "react";
import HelpLabel from "@/components/HelpLabel";
import TableRowToggle from "@/components/TableRowToggle";
import type { HealthDiagnosticsData } from "@/lib/types/cuse4";
import {
  COLLAPSED_ROWS,
  type CoverageSortKey,
  fmtInt,
  fmtNum,
  sortCoverageRows,
} from "./utils";

export default function SectionCoverage({ data }: { data: HealthDiagnosticsData }) {
  const [showAllFundCoverageRows, setShowAllFundCoverageRows] = useState(false);
  const [showAllTrbcCoverageRows, setShowAllTrbcCoverageRows] = useState(false);
  const [fundCovSortKey, setFundCovSortKey] = useState<CoverageSortKey>("coverage_score_pct");
  const [fundCovSortAsc, setFundCovSortAsc] = useState(false);
  const [trbcCovSortKey, setTrbcCovSortKey] = useState<CoverageSortKey>("coverage_score_pct");
  const [trbcCovSortAsc, setTrbcCovSortAsc] = useState(false);

  const sortedFundCoverageRows = useMemo(() => {
    const rows = data.section5?.fundamentals?.fields ?? [];
    return sortCoverageRows(rows, fundCovSortKey, fundCovSortAsc);
  }, [data, fundCovSortKey, fundCovSortAsc]);

  const sortedTrbcCoverageRows = useMemo(() => {
    const rows = data.section5?.trbc_history?.fields ?? [];
    return sortCoverageRows(rows, trbcCovSortKey, trbcCovSortAsc);
  }, [data, trbcCovSortKey, trbcCovSortAsc]);

  const showFundCoverageRows = showAllFundCoverageRows ? sortedFundCoverageRows : sortedFundCoverageRows.slice(0, COLLAPSED_ROWS);
  const showTrbcCoverageRows = showAllTrbcCoverageRows ? sortedTrbcCoverageRows : sortedTrbcCoverageRows.slice(0, COLLAPSED_ROWS);
  const fundCoverage = data.section5?.fundamentals ?? { row_count: 0, date_count: 0, low_coverage_field_count: 0 };
  const trbcCoverage = data.section5?.trbc_history ?? { row_count: 0, date_count: 0, low_coverage_field_count: 0 };

  const renderCoverageTable = (
    prefix: string,
    rows: typeof showFundCoverageRows,
    totalRows: number,
    expanded: boolean,
    onToggle: () => void,
    sortKey: CoverageSortKey,
    setSortKey: (value: CoverageSortKey) => void,
    sortAsc: boolean,
    setSortAsc: (value: boolean | ((prev: boolean) => boolean)) => void,
  ) => (
    <div className="dash-table health-table">
      <table>
        <thead>
          <tr>
            <th onClick={() => {
              if (sortKey === "field") setSortAsc((s) => !s);
              else { setSortKey("field"); setSortAsc(true); }
            }}>Field{sortKey === "field" ? (sortAsc ? " ↑" : " ↓") : ""}</th>
            <th className="text-right" onClick={() => {
              if (sortKey === "coverage_score_pct") setSortAsc((s) => !s);
              else { setSortKey("coverage_score_pct"); setSortAsc(true); }
            }}>Coverage Score{sortKey === "coverage_score_pct" ? (sortAsc ? " ↑" : " ↓") : ""}</th>
            <th className="text-right" onClick={() => {
              if (sortKey === "row_coverage_pct") setSortAsc((s) => !s);
              else { setSortKey("row_coverage_pct"); setSortAsc(false); }
            }}>Row %{sortKey === "row_coverage_pct" ? (sortAsc ? " ↑" : " ↓") : ""}</th>
            <th className="text-right" onClick={() => {
              if (sortKey === "avg_ticker_lifecycle_coverage_pct") setSortAsc((s) => !s);
              else { setSortKey("avg_ticker_lifecycle_coverage_pct"); setSortAsc(false); }
            }}>Avg Lifecycle %{sortKey === "avg_ticker_lifecycle_coverage_pct" ? (sortAsc ? " ↑" : " ↓") : ""}</th>
            <th className="text-right" onClick={() => {
              if (sortKey === "p10_ticker_lifecycle_coverage_pct") setSortAsc((s) => !s);
              else { setSortKey("p10_ticker_lifecycle_coverage_pct"); setSortAsc(true); }
            }}>P10 Lifecycle %{sortKey === "p10_ticker_lifecycle_coverage_pct" ? (sortAsc ? " ↑" : " ↓") : ""}</th>
            <th className="text-right">Worst Date %</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={`${prefix}-${row.field}`}>
              <td>{row.field}</td>
              <td className="text-right">{fmtNum(row.coverage_score_pct, 1)}%</td>
              <td className="text-right">{fmtNum(row.row_coverage_pct, 1)}%</td>
              <td className="text-right">{fmtNum(row.avg_ticker_lifecycle_coverage_pct, 1)}%</td>
              <td className="text-right">{fmtNum(row.p10_ticker_lifecycle_coverage_pct, 1)}%</td>
              <td className="text-right">{fmtNum(row.worst_date_coverage_pct, 1)}%</td>
            </tr>
          ))}
        </tbody>
      </table>
      <TableRowToggle
        totalRows={totalRows}
        collapsedRows={COLLAPSED_ROWS}
        expanded={expanded}
        onToggle={onToggle}
        label="fields"
      />
    </div>
  );

  return (
    <div className="chart-card">
      <h3>
        <HelpLabel
          label="Section 5 — Source Data Coverage"
          plain="Coverage audit for source-of-truth historical fundamentals and historical TRBC fields."
          math="Coverage score = 0.4×row coverage + 0.4×avg ticker lifecycle coverage + 0.2×p10 ticker lifecycle coverage"
        />
      </h3>

      <div className="health-kpi-strip" style={{ marginBottom: 10 }}>
        <div className="health-kpi">
          <div className="health-kpi-label">Historical Fundamentals</div>
          <div className="health-kpi-subrow"><span>Rows</span><strong>{fmtInt(fundCoverage.row_count)}</strong></div>
          <div className="health-kpi-subrow"><span>Dates</span><strong>{fmtInt(fundCoverage.date_count)}</strong></div>
          <div className="health-kpi-subrow"><span>Low-Coverage Fields (&lt;80)</span><strong>{fmtInt(fundCoverage.low_coverage_field_count)}</strong></div>
        </div>
        <div className="health-kpi">
          <div className="health-kpi-label">Historical TRBC</div>
          <div className="health-kpi-subrow"><span>Rows</span><strong>{fmtInt(trbcCoverage.row_count)}</strong></div>
          <div className="health-kpi-subrow"><span>Dates</span><strong>{fmtInt(trbcCoverage.date_count)}</strong></div>
          <div className="health-kpi-subrow"><span>Low-Coverage Fields (&lt;80)</span><strong>{fmtInt(trbcCoverage.low_coverage_field_count)}</strong></div>
        </div>
      </div>

      <h4 style={{ marginTop: 0 }}>Historical Fundamentals Coverage</h4>
      {renderCoverageTable(
        "fund",
        showFundCoverageRows,
        sortedFundCoverageRows.length,
        showAllFundCoverageRows,
        () => setShowAllFundCoverageRows((p) => !p),
        fundCovSortKey,
        setFundCovSortKey,
        fundCovSortAsc,
        setFundCovSortAsc,
      )}

      <h4 style={{ marginTop: 12 }}>Historical TRBC Coverage</h4>
      {renderCoverageTable(
        "trbc",
        showTrbcCoverageRows,
        sortedTrbcCoverageRows.length,
        showAllTrbcCoverageRows,
        () => setShowAllTrbcCoverageRows((p) => !p),
        trbcCovSortKey,
        setTrbcCovSortKey,
        trbcCovSortAsc,
        setTrbcCovSortAsc,
      )}
    </div>
  );
}
