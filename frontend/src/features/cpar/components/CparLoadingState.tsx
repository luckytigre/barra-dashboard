"use client";

import AnalyticsLoadingViz from "@/components/AnalyticsLoadingViz";

export function CparPageLoadingState({ message }: { message: string }) {
  return (
    <div className="cpar-page-loading-shell" data-testid="cpar-page-loading">
      <AnalyticsLoadingViz message={message} />
    </div>
  );
}

export function CparInlineLoadingState({ message }: { message: string }) {
  return (
    <div className="detail-history-empty compact cpar-inline-loading" role="status" aria-live="polite">
      {message}
    </div>
  );
}
