"use client";

import { useEffect, useMemo, useState } from "react";
import { formatCparNumber, formatCparPackageDate } from "@/lib/cparTruth";
import type { CparTickerDetailData } from "@/lib/types/cpar";
import CparExploreLoadingsChart from "./CparExploreLoadingsChart";
import CparWarningsBar from "./CparWarningsBar";

function formatPrice(value: number | null | undefined, currency?: string | null): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "—";
  try {
    return new Intl.NumberFormat("en-US", {
      style: "currency",
      currency: currency || "USD",
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(value);
  } catch {
    return `${currency || "PX"} ${value.toFixed(2)}`;
  }
}

function detailRow(label: string, value: string) {
  return (
    <div className="explore-quote-row">
      <span className="explore-quote-row-label">{label}</span>
      <span className="explore-quote-row-value">{value}</span>
    </div>
  );
}

export default function CparExploreDetailModule({
  detail,
}: {
  detail: CparTickerDetailData;
}) {
  const [expanded, setExpanded] = useState(false);
  const [spotlight, setSpotlight] = useState(false);
  const thresholdedRows = useMemo(() => (
    [...detail.thresholded_loadings].sort((left, right) => (
      left.display_order - right.display_order
      || Math.abs(right.beta) - Math.abs(left.beta)
      || left.factor_id.localeCompare(right.factor_id)
    ))
  ), [detail.thresholded_loadings]);
  const [selectedFactorId, setSelectedFactorId] = useState<string | null>(thresholdedRows[0]?.factor_id || null);
  const latestPrice = detail.source_context.latest_price_context;
  const classification = detail.source_context.classification_snapshot;
  const commonName = detail.source_context.latest_common_name;
  const classificationValue = [
    classification?.trbc_economic_sector,
    classification?.trbc_industry_group,
    classification?.trbc_activity,
  ].filter(Boolean).join(" · ") || "—";
  const hasLoadings = thresholdedRows.length > 0 && detail.fit_status !== "insufficient_history";
  const sourceContextStatus = detail.source_context.status;

  useEffect(() => {
    setExpanded(false);
    setSpotlight(true);
    setSelectedFactorId(thresholdedRows[0]?.factor_id || null);
    const timer = window.setTimeout(() => setSpotlight(false), 2400);
    return () => window.clearTimeout(timer);
  }, [detail.ric, thresholdedRows]);

  return (
    <section
      className={`explore-quote-module${expanded ? " open" : ""}${spotlight && !expanded ? " fresh" : ""}`}
      data-testid="cpar-detail-panel"
    >
      <button
        type="button"
        className={`explore-quote-trigger${expanded ? " open" : ""}`}
        onClick={() => setExpanded((prev) => !prev)}
        aria-expanded={expanded}
      >
        <span className="explore-quote-trigger-copy">
          <span className="explore-quote-trigger-kicker">Quote</span>
          <span className="explore-quote-trigger-title">
            <span className="ticker">{detail.ticker || detail.ric}</span>
            <span className="name">{detail.display_name || detail.ric}</span>
          </span>
          <span className="explore-quote-trigger-meta">
            {classification?.trbc_economic_sector || "Unclassified"}
            {classification?.trbc_industry_group ? ` • ${classification.trbc_industry_group}` : ""}
          </span>
        </span>

        <span className="explore-quote-trigger-strip">
          <span className="explore-quote-trigger-metric">
            <span className="explore-quote-trigger-metric-label">Price</span>
            <span className="explore-quote-trigger-metric-value strong">
              {latestPrice ? formatPrice(latestPrice.price, latestPrice.currency) : "—"}
            </span>
          </span>
          <span className="explore-quote-trigger-metric">
            <span className="explore-quote-trigger-metric-label">Trade Beta</span>
            <span className="explore-quote-trigger-metric-value">{formatCparNumber(detail.beta_spy_trade, 3)}</span>
          </span>
          <span className="explore-quote-trigger-metric">
            <span className="explore-quote-trigger-metric-label">Observed</span>
            <span className="explore-quote-trigger-metric-value">{detail.observed_weeks}w</span>
          </span>
        </span>

        <span className="explore-quote-trigger-action">
          <span className="explore-quote-trigger-action-copy">
            <span className="explore-quote-trigger-action-label">
              {expanded ? "Hide quote" : "Show quote"}
            </span>
            <span className="explore-quote-trigger-action-hint">
              {expanded ? "Collapse overlay" : "Open overlay"}
            </span>
          </span>
          <span className="explore-quote-trigger-glyph" aria-hidden="true">+</span>
        </span>
      </button>

      <div className="explore-quote-overlay" aria-hidden={!expanded}>
        <div className="explore-quote-overlay-card">
          <div className="explore-quote-overlay-body">
            <div className="explore-quote-overlay-left">
              <div className="explore-quote-spark-panel">
                <div className="explore-quote-spark-summary">
                  <span className="explore-quote-spark-latest">
                    Package {formatCparPackageDate(detail.package_date)}
                  </span>
                  <span className="explore-quote-spark-range">
                    Source prices {formatCparPackageDate(detail.source_prices_asof)}
                  </span>
                </div>
                <div className="explore-quote-spark-empty">
                  cPAR explore stays on one persisted package row. This surface does not expose a separate ticker history chart.
                </div>
              </div>

              <div className="explore-quote-data-panel">
                <div className="explore-quote-data-grid compact">
                  {detailRow("RIC", detail.ric)}
                  {detailRow("Fit Status", detail.fit_status.replaceAll("_", " "))}
                  {detailRow("Observed", `${detail.observed_weeks}w`)}
                  {detailRow("Longest Gap", `${detail.longest_gap_weeks}w`)}
                  {detailRow("Latest Source Price", latestPrice ? formatPrice(latestPrice.price, latestPrice.currency) : "—")}
                  {detailRow("Price Field", latestPrice?.price_field_used || detail.price_field_used || "—")}
                  {detailRow("Common Name", commonName?.value || "—")}
                  {detailRow("Classification", classificationValue)}
                  {detailRow("Trade Beta", formatCparNumber(detail.beta_spy_trade, 3))}
                  {detailRow("Market Step", formatCparNumber(detail.beta_market_step1, 3))}
                  {detailRow("HQ", detail.hq_country_code || "—")}
                  {detailRow("Classification As Of", formatCparPackageDate(detail.classification_asof))}
                </div>
              </div>
            </div>

            <div className="explore-quote-chart-panel">
              <div className="explore-quote-chart-head">
                <span>Factor Exposures</span>
                <span>{hasLoadings ? `${thresholdedRows.length} thresholded` : "Unavailable"}</span>
              </div>
              {hasLoadings ? (
                <div className="explore-quote-chart-scroll">
                  <CparExploreLoadingsChart
                    rows={thresholdedRows}
                    selectedFactorId={selectedFactorId}
                    onSelectFactor={setSelectedFactorId}
                  />
                </div>
              ) : (
                <div className="explore-quote-chart-empty">
                  {detail.fit_status === "insufficient_history"
                    ? "Factor exposures are unavailable because this persisted fit row is insufficient_history."
                    : "Factor exposures are unavailable for this instrument."}
                </div>
              )}
            </div>
          </div>

          {(detail.warnings.length > 0 || detail.fit_status !== "ok" || sourceContextStatus !== "ok") ? (
            <div className="explore-quote-note">
              <CparWarningsBar fitStatus={detail.fit_status} warnings={detail.warnings} />
              {sourceContextStatus !== "ok" ? (
                <div style={{ marginTop: 8 }}>
                  Supplemental package-date source context is {sourceContextStatus.replaceAll("_", " ")}. The persisted cPAR fit row remains authoritative.
                </div>
              ) : null}
            </div>
          ) : null}
        </div>
      </div>
    </section>
  );
}
