"use client";

import { useMemo } from "react";
import { compareNumber, compareText, useSortableRows } from "@/hooks/useSortableRows";
import { formatCparNumber } from "@/lib/cparTruth";
import type { CparPostHedgeExposure } from "@/lib/types/cpar";

type SortKey = "factor" | "group" | "pre" | "hedge" | "post";

export default function CparPostHedgeTable({
  rows,
}: {
  rows: CparPostHedgeExposure[];
}) {
  const comparators = useMemo<Record<SortKey, (left: CparPostHedgeExposure, right: CparPostHedgeExposure) => number>>(
    () => ({
      factor: (left, right) => compareText(left.label || left.factor_id, right.label || right.factor_id),
      group: (left, right) => compareText(left.group, right.group),
      pre: (left, right) => compareNumber(left.pre_beta, right.pre_beta),
      hedge: (left, right) => compareNumber(left.hedge_leg, right.hedge_leg),
      post: (left, right) => compareNumber(left.post_beta, right.post_beta),
    }),
    [],
  );
  const { sortedRows, handleSort, arrow } = useSortableRows<CparPostHedgeExposure, SortKey>({
    rows,
    comparators,
  });

  return (
    <section className="chart-card" data-testid="cpar-post-hedge-table">
      <h3>Post-Hedge Exposures</h3>
      {rows.length === 0 ? (
        <div className="detail-history-empty compact">
          No post-hedge exposure rows were returned for this package.
        </div>
      ) : (
        <div className="dash-table">
          <table>
            <thead>
              <tr>
                <th onClick={() => handleSort("factor")}>Factor{arrow("factor")}</th>
                <th onClick={() => handleSort("group")}>Group{arrow("group")}</th>
                <th className="text-right" onClick={() => handleSort("pre")}>Pre{arrow("pre")}</th>
                <th className="text-right" onClick={() => handleSort("hedge")}>Hedge{arrow("hedge")}</th>
                <th className="text-right" onClick={() => handleSort("post")}>Post{arrow("post")}</th>
              </tr>
            </thead>
            <tbody>
              {sortedRows.map((row) => (
                <tr key={row.factor_id}>
                  <td>
                    <strong>{row.label || row.factor_id}</strong>
                    <span className="cpar-table-sub">{row.factor_id}</span>
                  </td>
                  <td>{row.group || "—"}</td>
                  <td className="text-right cpar-number-cell">{formatCparNumber(row.pre_beta, 3)}</td>
                  <td className="text-right cpar-number-cell">{formatCparNumber(row.hedge_leg, 3)}</td>
                  <td className="text-right cpar-number-cell">{formatCparNumber(row.post_beta, 3)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
