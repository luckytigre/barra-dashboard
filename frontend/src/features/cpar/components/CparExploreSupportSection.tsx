"use client";

import Link from "next/link";
import { formatCparPackageDate } from "@/lib/cparTruth";
import type { CparTickerDetailData } from "@/lib/types/cpar";
import CparExploreSourceContextCard from "./CparExploreSourceContextCard";

function row(label: string, value: string) {
  return (
    <div className="explore-quote-row">
      <span className="explore-quote-row-label">{label}</span>
      <span className="explore-quote-row-value">{value}</span>
    </div>
  );
}

export default function CparExploreSupportSection({
  detail,
  hedgeHref,
}: {
  detail: CparTickerDetailData;
  hedgeHref: string;
}) {
  return (
    <section className="chart-card cpar-explore-support-module" data-testid="cpar-explore-support">
      <div className="cpar-explore-module-header">
        <div>
          <div className="cpar-explore-kicker">Support Context</div>
          <h3 className="cpar-explore-module-title">Persisted Facts And Package-Date Source Context</h3>
          <div className="cpar-explore-module-subtitle">
            These values support the selected package row after the primary thresholded-loadings read above.
          </div>
        </div>
      </div>

      <div className="explore-detail-grid cpar-explore-detail-grid">
        <div className="cpar-explore-facts-panel">
          <div className="cpar-explore-panel-title">Persisted Fit Facts</div>
          <div className="cpar-explore-panel-subtitle">
            These values come from the active-package fit row and stay authoritative even when supplemental source
            context is partial or unavailable.
          </div>
          <div className="explore-quote-data-grid compact cpar-explore-facts-grid">
            {row("Ticker", detail.ticker || "—")}
            {row("RIC", detail.ric)}
            {row("Package Date", formatCparPackageDate(detail.package_date))}
            {row("Price Field", detail.price_field_used || "—")}
            {row("Lookback", `${detail.lookback_weeks}w`)}
            {row("Observed", `${detail.observed_weeks}w`)}
            {row("Longest Gap", `${detail.longest_gap_weeks}w`)}
            {row("Classification As Of", formatCparPackageDate(detail.classification_asof))}
          </div>
        </div>

        <CparExploreSourceContextCard detail={detail} embedded />
      </div>

      <div className="cpar-inline-message neutral">
        <strong>Continue to hedge after reading the support context.</strong>
        <span>
          The hedge page reuses the same active-package ticker and RIC selection, then applies persisted hedge logic
          without any request-time refit or build behavior.
        </span>
        <div className="cpar-badge-row compact">
          <Link href={hedgeHref} className="cpar-detail-chip" prefetch={false}>
            Continue To /cpar/hedge
          </Link>
        </div>
      </div>
    </section>
  );
}
