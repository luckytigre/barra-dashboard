"use client";

export default function CparSurfaceResetCard({
  title,
  description,
  testId,
}: {
  title: string;
  description: string;
  testId: string;
}) {
  return (
    <div className="cpar-page">
      <section className="chart-card" data-testid={testId}>
        <div className="cpar-explore-module-header">
          <div>
            <div className="cpar-explore-kicker">Under Reconstruction</div>
            <h3 className="cpar-explore-module-title">{title}</h3>
          </div>
        </div>
        <div className="section-subtitle">
          This cPAR surface has been intentionally cleared and will be rebuilt from the ground up.
        </div>
        <div className="detail-history-empty compact">{description}</div>
      </section>
    </div>
  );
}
