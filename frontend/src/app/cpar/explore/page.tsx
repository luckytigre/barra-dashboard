"use client";

import { Suspense } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { useCparMeta, useCparTicker } from "@/hooks/useCparApi";
import { canNavigateCparSearchResult, readCparError, sameCparPackageIdentity } from "@/lib/cparTruth";
import type { CparSearchItem } from "@/lib/types/cpar";
import { CparInlineLoadingState, CparPageLoadingState } from "@/features/cpar/components/CparLoadingState";
import CparExploreDetailModule from "@/features/cpar/components/CparExploreDetailModule";
import CparExploreSearchModule from "@/features/cpar/components/CparExploreSearchModule";
import CparHedgePanel from "@/features/cpar/components/CparHedgePanel";

function buildExploreHref(item: CparSearchItem): string {
  const params = new URLSearchParams();
  if (item.ticker) params.set("ticker", item.ticker);
  params.set("ric", item.ric);
  return `/cpar/explore?${params.toString()}`;
}

function ExploreSelectionState({
  title,
  message,
  tone = "neutral",
  testId = "cpar-detail-panel",
}: {
  title: string;
  message: string;
  tone?: "neutral" | "warning" | "error";
  testId?: string;
}) {
  return (
    <section className="chart-card cpar-explore-selection-state" data-testid={testId}>
      <div className="cpar-explore-module-header">
        <div>
          <div className="cpar-explore-kicker">Selected Instrument</div>
          <h3 className="cpar-explore-module-title">{title}</h3>
        </div>
      </div>
      <div className={`cpar-inline-message ${tone}`}>
        <strong>{title}</strong>
        <span>{message}</span>
      </div>
    </section>
  );
}

function CparExplorePageInner() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const ticker = searchParams?.get("ticker")?.trim().toUpperCase() || null;
  const ric = searchParams?.get("ric")?.trim() || null;
  const querySeed = ric || ticker || "";

  const { data: meta, error: metaError, isLoading: metaLoading } = useCparMeta();
  const metaState = metaError ? readCparError(metaError) : null;
  const metaReady = Boolean(meta) && !metaState;
  const {
    data: detail,
    error: detailError,
    isLoading: detailLoading,
  } = useCparTicker(metaReady ? ticker : null, ric);

  if (metaLoading && !meta) {
    return <CparPageLoadingState message="Loading cPAR explore..." />;
  }

  const detailState = detailError ? readCparError(detailError) : null;
  const detailPackageMismatch = Boolean(meta && detail && !sameCparPackageIdentity(meta, detail));
  const detailBlocked = detail?.fit_status === "insufficient_history" || detailPackageMismatch;

  return (
    <div className="cpar-page explore-page-stack">
      {metaState ? (
        <section className="chart-card cpar-alert-card" data-testid="cpar-explore-not-ready">
          <h3>{metaState.kind === "not_ready" ? "cPAR Explore Not Ready" : "cPAR Explore Unavailable"}</h3>
          <div className="section-subtitle">{metaState.message}</div>
        </section>
      ) : null}

      {!ticker && ric ? (
        <ExploreSelectionState
          title="Ticker Required For Detail"
          message="This active-package search hit has no ticker symbol, and the current cPAR detail route is ticker-keyed. Use another search result with a ticker, or extend the backend route contract in a later slice."
          tone="warning"
        />
      ) : metaState ? (
        <ExploreSelectionState
          title="Active Package Metadata Unavailable"
          message="Reload after the active cPAR package is readable again before opening detail or the hedge workflow."
          tone="warning"
        />
      ) : detailLoading && !detail ? (
        <section className="chart-card cpar-explore-selection-state" data-testid="cpar-detail-panel">
          <div className="cpar-explore-module-header">
            <div>
              <div className="cpar-explore-kicker">Selected Instrument</div>
              <h3 className="cpar-explore-module-title">Loading Persisted Fit Detail</h3>
            </div>
          </div>
          <CparInlineLoadingState message={`Loading cPAR detail for ${ric || ticker}...`} />
        </section>
      ) : detailState ? (
        <section className="chart-card cpar-explore-selection-state" data-testid="cpar-detail-panel">
          <div className="cpar-explore-module-header">
            <div>
              <div className="cpar-explore-kicker">Selected Instrument</div>
              <h3 className="cpar-explore-module-title">
                {detailState.kind === "ambiguous"
                  ? "Resolve Ticker Ambiguity"
                  : detailState.kind === "missing"
                    ? "Ticker Not Found"
                    : "Detail Read Unavailable"}
              </h3>
            </div>
          </div>
          <div className={`cpar-inline-message ${detailState.kind === "ambiguous" ? "warning" : "error"}`}>
            <strong>
              {detailState.kind === "ambiguous"
                ? "Ticker is ambiguous."
                : detailState.kind === "missing"
                  ? "Ticker not found."
                  : "Detail unavailable."}
            </strong>
            <span>{detailState.message}</span>
            {detailState.kind === "ambiguous" ? <span>Choose a specific RIC from the search results below.</span> : null}
          </div>
        </section>
      ) : detailPackageMismatch ? (
        <section className="chart-card cpar-explore-selection-state" data-testid="cpar-detail-panel">
          <div className="cpar-explore-module-header">
            <div>
              <div className="cpar-explore-kicker">Selected Instrument</div>
              <h3 className="cpar-explore-module-title">Reload To Pin One Package</h3>
            </div>
          </div>
          <div className="cpar-inline-message error" data-testid="cpar-package-mismatch">
            <strong>Active package changed during read.</strong>
            <span>The banner package no longer matches the persisted detail row.</span>
            <span>Reload the page to pin one cPAR package before reading loadings or hedge output.</span>
          </div>
        </section>
      ) : detail ? (
        <CparExploreDetailModule detail={detail} />
      ) : null}

      <div className="explore-detail-grid">
        <CparExploreSearchModule
          initialQuery={querySeed}
          selectedRic={ric}
          onSelectResult={(item) => {
            if (!canNavigateCparSearchResult(item)) return;
            router.push(buildExploreHref(item));
          }}
        />

        {detail && !detailBlocked && !metaState && !detailState && !detailPackageMismatch ? (
          <CparHedgePanel
            ticker={detail.ticker || ticker || detail.ric}
            ric={detail.ric}
            fitStatus={detail.fit_status}
            expectedPackageRunId={detail.package_run_id}
            expectedPackageDate={detail.package_date}
          />
        ) : detail?.fit_status === "insufficient_history" ? (
          <section className="chart-card" data-testid="cpar-explore-workflow-panel">
            <h3>Hedge Preview</h3>
            <div className="cpar-inline-message warning" data-testid="cpar-insufficient-history">
              <strong>Hedge workflow is blocked.</strong>
              <span>
                This row is persisted as `insufficient_history`, so cPAR explore stays on the selected quote detail only.
              </span>
            </div>
          </section>
        ) : (
          <section className="chart-card" data-testid="cpar-explore-workflow-panel">
            <h3>Hedge Preview</h3>
            <div className="detail-history-empty compact">
              Select one active-package ticker to open the cPAR quote card and its persisted hedge workflow.
            </div>
          </section>
        )}
      </div>
    </div>
  );
}

export default function CparExplorePage() {
  return (
    <Suspense fallback={<CparPageLoadingState message="Loading cPAR explore..." />}>
      <CparExplorePageInner />
    </Suspense>
  );
}
