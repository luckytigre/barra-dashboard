"use client";

import { usePortfolio } from "@/hooks/useApi";
import PositionTable from "@/components/PositionTable";
import AnalyticsLoadingViz from "@/components/AnalyticsLoadingViz";
import ApiErrorState from "@/components/ApiErrorState";

export default function PositionsPage() {
  const { data, isLoading, error } = usePortfolio();

  if (isLoading) {
    return <AnalyticsLoadingViz message="Loading positions..." />;
  }
  if (error) {
    return <ApiErrorState title="Positions Not Ready" error={error} />;
  }

  const positions = data?.positions ?? [];

  return (
    <div>
      <div className="chart-card mb-4">
        <h3>All Positions ({positions.length})</h3>
        <PositionTable positions={positions} />
      </div>
    </div>
  );
}
