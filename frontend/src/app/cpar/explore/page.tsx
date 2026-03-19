"use client";

import { Suspense } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import AnalyticsLoadingViz from "@/components/AnalyticsLoadingViz";
import { useCparMeta, useCparTicker } from "@/hooks/useApi";
import { canNavigateCparSearchResult, formatCparPackageDate, readCparError } from "@/lib/cparTruth";
import type { CparSearchItem } from "@/lib/types";
import CparHedgePanel from "@/features/cpar/components/CparHedgePanel";
import CparLoadingsTable from "@/features/cpar/components/CparLoadingsTable";
import CparPackageBanner from "@/features/cpar/components/CparPackageBanner";
import CparSearchPanel from "@/features/cpar/components/CparSearchPanel";
import CparWarningsBar from "@/features/cpar/components/CparWarningsBar";

function buildExploreHref(item: CparSearchItem): string {
  const params = new URLSearchParams();
  if (item.ticker) params.set("ticker", item.ticker);
  params.set("ric", item.ric);
  return `/cpar/explore?${params.toString()}`;
}

function CparExplorePageInner() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const ticker = searchParams?.get("ticker")?.trim().toUpperCase() || null;
  const ric = searchParams?.get("ric")?.trim() || null;
  const querySeed = ric || ticker || "";

  const { data: meta, error: metaError, isLoading: metaLoading } = useCparMeta();
  const {
    data: detail,
    error: detailError,
    isLoading: detailLoading,
  } = useCparTicker(ticker, ric);

  if (metaLoading && !meta) {
    return <AnalyticsLoadingViz message="Loading cPAR explore..." />;
  }

  const metaState = metaError ? readCparError(metaError) : null;
  const detailState = detailError ? readCparError(detailError) : null;
  const detailBlocked = detail?.fit_status === "insufficient_history";

  return (
    <div className="cpar-page">
      <section className="cpar-page-header">
        <div className="cpar-section-kicker">cPAR / Explore</div>
        <h1>Active Package Detail</h1>
        <p className="cpar-page-copy">
          Search one persisted instrument, inspect raw and thresholded loadings, then derive the hedge preview from
          the package already on disk.
        </p>
      </section>

      {meta ? <CparPackageBanner meta={meta} factors={meta.factors} title="Current Explore Package" /> : null}

      {metaState ? (
        <section className="chart-card cpar-alert-card" data-testid="cpar-explore-not-ready">
          <h3>{metaState.kind === "not_ready" ? "cPAR Explore Not Ready" : "cPAR Explore Unavailable"}</h3>
          <div className="section-subtitle">{metaState.message}</div>
        </section>
      ) : null}

      <div className="cpar-two-column">
        <CparSearchPanel
          initialQuery={querySeed}
          selectedRic={ric}
          title="Search The Active Package"
          helperText="The explore page resolves to one active-package row. Use RIC selection when a ticker is ambiguous."
          onSelectResult={(item) => {
            if (!canNavigateCparSearchResult(item)) return;
            router.push(buildExploreHref(item));
          }}
        />

        <section className="chart-card" data-testid="cpar-detail-panel">
          <h3>Selected Instrument</h3>
          {!ticker && ric ? (
            <div className="cpar-inline-message warning">
              <strong>RIC result cannot open detail directly.</strong>
              <span>
                This active-package search hit has no ticker symbol, and the current cPAR detail route is ticker-keyed.
              </span>
              <span>Use another search result with a ticker, or extend the backend route contract in a later slice.</span>
            </div>
          ) : !ticker ? (
            <div className="detail-history-empty">
              Select a search result to load one cPAR package row and its persisted hedge preview.
            </div>
          ) : detailLoading && !detail ? (
            <AnalyticsLoadingViz message={`Loading cPAR detail for ${ric || ticker}...`} />
          ) : detailState ? (
            <div className={`cpar-inline-message ${detailState.kind === "ambiguous" ? "warning" : "error"}`}>
              <strong>
                {detailState.kind === "ambiguous"
                  ? "Ticker is ambiguous."
                  : detailState.kind === "missing"
                    ? "Ticker not found."
                    : "Detail unavailable."}
              </strong>
              <span>{detailState.message}</span>
              {detailState.kind === "ambiguous" ? (
                <span>Choose a specific RIC from the search results on the left.</span>
              ) : null}
            </div>
          ) : detail ? (
            <>
              <div className="cpar-detail-header">
                <div>
                  <div className="cpar-detail-title">{detail.display_name || detail.ticker || detail.ric}</div>
                  <div className="cpar-detail-subtitle">
                    {detail.ticker || "—"} · {detail.ric} · HQ {detail.hq_country_code || "—"}
                  </div>
                </div>
                <CparWarningsBar fitStatus={detail.fit_status} warnings={detail.warnings} />
              </div>
              <div className="cpar-package-grid compact">
                <div className="cpar-package-metric">
                  <div className="cpar-package-label">Observed</div>
                  <div className="cpar-package-value">{detail.observed_weeks}w</div>
                  <div className="cpar-package-detail">Longest gap {detail.longest_gap_weeks}w</div>
                </div>
                <div className="cpar-package-metric">
                  <div className="cpar-package-label">Price Field</div>
                  <div className="cpar-package-value">{detail.price_field_used || "—"}</div>
                  <div className="cpar-package-detail">Package date {formatCparPackageDate(detail.package_date)}</div>
                </div>
                <div className="cpar-package-metric">
                  <div className="cpar-package-label">Pre-Hedge Vol</div>
                  <div className="cpar-package-value">{detail.pre_hedge_factor_volatility_proxy?.toFixed(3) || "—"}</div>
                  <div className="cpar-package-detail">
                    Variance {detail.pre_hedge_factor_variance_proxy?.toFixed(3) || "—"}
                  </div>
                </div>
                <div className="cpar-package-metric">
                  <div className="cpar-package-label">SPY Trade Beta</div>
                  <div className="cpar-package-value">{detail.beta_spy_trade?.toFixed(3) || "—"}</div>
                  <div className="cpar-package-detail">
                    Market step {detail.beta_market_step1?.toFixed(3) || "—"}
                  </div>
                </div>
              </div>
              {detailBlocked ? (
                <div className="cpar-inline-message warning" data-testid="cpar-insufficient-history">
                  <strong>Loadings and hedge output are blocked.</strong>
                  <span>
                    This row is persisted as `insufficient_history`, so the frontend only renders identity, package
                    metadata, and warnings.
                  </span>
                </div>
              ) : (
                <div className="cpar-inline-message neutral">
                  <strong>Package-only semantics.</strong>
                  <span>
                    This detail row and the hedge preview below are both derived from the same active package date,
                    without any request-time refit.
                  </span>
                </div>
              )}
            </>
          ) : null}
        </section>
      </div>

      {detail && !detailBlocked ? (
        <>
          <div className="cpar-two-column">
            <CparLoadingsTable title="Raw ETF Loadings" rows={detail.raw_loadings} />
            <CparLoadingsTable
              title="Thresholded ETF Loadings"
              rows={detail.thresholded_loadings}
              emptyText="Thresholding zeroed every non-market leg in the persisted trade-space payload."
            />
          </div>

          <CparHedgePanel ticker={detail.ticker || ticker || detail.ric} ric={detail.ric} fitStatus={detail.fit_status} />
        </>
      ) : null}

      {detail && detail.fit_status === "limited_history" ? (
        <section className="chart-card">
          <h3>Interpretation Note</h3>
          <div className="section-subtitle">
            `limited_history` still renders loadings and hedge output, but adjacent package comparisons deserve more
            caution than a full-history `ok` row.
          </div>
          <div className="detail-history-empty compact">
            If the hedge is available, compare the stability and non-market reduction metrics before using it.
          </div>
          {detail.pre_hedge_factor_variance_proxy !== null ? (
            <div className="cpar-detail-chip">
              Current pre-hedge variance proxy: {detail.pre_hedge_factor_variance_proxy?.toFixed(3)}
            </div>
          ) : null}
        </section>
      ) : null}
    </div>
  );
}

export default function CparExplorePage() {
  return (
    <Suspense fallback={<AnalyticsLoadingViz message="Loading cPAR explore..." />}>
      <CparExplorePageInner />
    </Suspense>
  );
}
