const MODEL_FAMILIES = [
  {
    title: "cUSE",
    subtitle: "Default structural risk workflow",
    body:
      "Use cUSE when you want descriptor-native risk decomposition, stable structural attribution, and the incumbent app workflow for portfolio diagnosis.",
  },
  {
    title: "cPAR",
    subtitle: "Parallel ETF-proxy and hedge workflow",
    body:
      "Use cPAR when you want package-pinned ETF-proxy factor reads, single-name explore detail, and hedge-specific ETF recommendation surfaces.",
  },
];

const WORKFLOW = [
  {
    step: "1. Risk",
    detail:
      "Start on cUSE for the default structural risk read. Use cPAR Risk for aggregate ETF-proxy factor reads, coverage buckets, covariance context, and row-level hedge popovers on covered tickers.",
  },
  {
    step: "2. Explore",
    detail:
      "Use cPAR Explore for single-name fit detail and preview-only scenario analysis before you touch holdings. This is the right place to inspect one name in isolation.",
  },
  {
    step: "3. Positions",
    detail:
      "Use Positions to review live holdings, stage edits, and run RECALC when you want staged changes written and the read surfaces refreshed against the current model/package. Until RECALC finishes, live holdings and modeled analytics can differ.",
  },
  {
    step: "4. Hedge",
    detail:
      "Use the cPAR hedge workspace for the scoped factor-neutral ETF recommendation across all permitted accounts or one account, then inspect the modeled post-hedge exposure change.",
  },
];

const PAGE_MAP = [
  {
    page: "/cuse/exposures",
    purpose: "Default cUSE exposures and structural risk surface",
    notes: "Structural risk decomposition and the incumbent portfolio diagnosis workflow.",
  },
  {
    page: "/cpar/risk",
    purpose: "Aggregate cPAR risk surface",
    notes: "Display-space factor chart, coverage summary, positions mix, covariance view, and row-level hedge popovers for covered names.",
  },
  {
    page: "/cpar/explore",
    purpose: "Single-name cPAR detail",
    notes: "One-name factor detail plus preview-only scenario analysis.",
  },
  {
    page: "/positions",
    purpose: "Shared live holdings control surface",
    notes: "Stage holdings edits here, then RECALC to refresh holdings-backed analytics from the current model/package.",
  },
  {
    page: "/cpar/hedge",
    purpose: "Portfolio hedge workspace",
    notes: "Current cPAR exposure chart plus the scoped factor-neutral ETF recommendation and projected post-hedge exposures.",
  },
];

const OPERATING_RULES = [
  "cUSE remains the default app-facing family; cPAR is explicitly parallel and namespaced.",
  "cPAR frontend pages are package-pinned and read-only unless they intentionally reuse the shared holdings owner.",
  "cPAR Risk stays display-first, while cPAR Hedge combines the current exposure view with backend-owned ETF recommendation logic and projected post-hedge effects.",
  "RECALC writes staged holdings changes and refreshes holdings-backed read surfaces against the current model/package; it does not imply request-time model rebuilding on these pages.",
];

export default function TutorialPage() {
  return (
    <div className="cpar-page">
      <section className="chart-card">
        <h3>Tutorial</h3>
        <div className="section-subtitle">
          In-app orientation for the model families, the main pages, and the normal path from diagnosis to positions to
          hedge work.
        </div>
      </section>

      <section className="chart-card">
        <h3>Model Families</h3>
        <div style={{ display: "grid", gap: 14, gridTemplateColumns: "repeat(auto-fit, minmax(260px, 1fr))" }}>
          {MODEL_FAMILIES.map((family) => (
            <div key={family.title} style={{ display: "grid", gap: 6 }}>
              <strong>{family.title}</strong>
              <div className="cpar-table-sub">{family.subtitle}</div>
              <div className="cpar-table-sub">{family.body}</div>
            </div>
          ))}
        </div>
      </section>

      <section className="chart-card">
        <h3>Recommended Workflow</h3>
        <div style={{ display: "grid", gap: 14 }}>
          {WORKFLOW.map((item) => (
            <div key={item.step} style={{ display: "grid", gap: 4 }}>
              <strong>{item.step}</strong>
              <div className="cpar-table-sub">{item.detail}</div>
            </div>
          ))}
        </div>
      </section>

      <section className="chart-card">
        <h3>Page Map</h3>
        <div className="dash-table">
          <table>
            <thead>
              <tr>
                <th>Page</th>
                <th>Purpose</th>
                <th>What To Expect</th>
              </tr>
            </thead>
            <tbody>
              {PAGE_MAP.map((row) => (
                <tr key={row.page}>
                  <td><strong>{row.page}</strong></td>
                  <td>{row.purpose}</td>
                  <td>{row.notes}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="chart-card">
        <h3>Important Rules</h3>
        <div style={{ display: "grid", gap: 10 }}>
          {OPERATING_RULES.map((rule) => (
            <div key={rule} className="cpar-table-sub">
              {rule}
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}
