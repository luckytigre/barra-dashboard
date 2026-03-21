export default function LandingSummary() {
  return (
    <section className="landing-summary" aria-label="Model overview">
      <div className="landing-summary-card landing-summary-card-cuse">
        <h3 className="landing-summary-card-title landing-summary-card-title-cuse">
          <span className="landing-summary-card-prefix">c</span>USE
        </h3>
        <p className="landing-summary-card-subtitle">Cross-Sectional Equity Risk Model</p>
        <p className="landing-summary-card-body">
          Descriptor-based factor model inspired by USE4 methodology.
          Estimates exposures from fundamental characteristics — size, value,
          momentum, volatility, industry membership — via constrained
          weighted least-squares regression across the investable universe.
        </p>
        <dl className="landing-summary-traits">
          <div className="landing-summary-trait">
            <dt>Method</dt>
            <dd>Single-stage constrained WLS with weighted sum-to-zero industry constraints, solved jointly across market, industry, and style blocks</dd>
          </div>
          <div className="landing-summary-trait">
            <dt>Factors</dt>
            <dd>Market + 11 TRBC industries + 12 style factors (Size, B/P, E/Y, Momentum, Beta, Leverage, Growth, Profitability, Liquidity, Residual Vol, Reversal, Nonlinear Size)</dd>
          </div>
          <div className="landing-summary-trait">
            <dt>Factor hierarchy</dt>
            <dd>Style descriptors are orthogonalized in dependency order via WLS — Momentum is residualized to industry and Size, Residual Vol to Size and Beta, Reversal to Momentum, Liquidity and Nonlinear Size to Size. Fundamental value factors are neutralized to industry only. This keeps factor returns interpretable and prevents correlated descriptors from inflating exposures.</dd>
          </div>
          <div className="landing-summary-trait">
            <dt>Exposures</dt>
            <dd>Forward-looking, derived from cross-sectional descriptor ranks — not from return history</dd>
          </div>
          <div className="landing-summary-trait">
            <dt>Strengths</dt>
            <dd>Rich risk decomposition with clean factor attribution, stable structure, interpretable even for names with limited return history</dd>
          </div>
        </dl>
      </div>

      <div className="landing-summary-card landing-summary-card-cpar">
        <h3 className="landing-summary-card-title landing-summary-card-title-cpar">
          <span className="landing-summary-card-prefix">c</span>PAR
        </h3>
        <p className="landing-summary-card-subtitle">Parsimonious and Actionable Regression</p>
        <p className="landing-summary-card-body">
          Returns-based regression using real, tradable ETF proxies.
          Every factor is something you can buy or sell — SPY for market,
          sector SPDRs for industries, iShares style ETFs for momentum,
          value, quality, low-vol, and size.
        </p>
        <dl className="landing-summary-traits">
          <div className="landing-summary-trait">
            <dt>Method</dt>
            <dd>Three-step time-series regression on weekly returns — market fit first, then ridge regression on the residual using orthogonalized sector and style proxies jointly</dd>
          </div>
          <div className="landing-summary-trait">
            <dt>Factors</dt>
            <dd>SPY + 11 sector SPDRs (XLB, XLC, XLE, XLF, XLI, XLK, XLP, XLRE, XLU, XLV, XLY) + 5 style ETFs (MTUM, VLUE, QUAL, USMV, IWM)</dd>
          </div>
          <div className="landing-summary-trait">
            <dt>Factor hierarchy</dt>
            <dd>Market beta is estimated first and stripped. Sector and style ETF returns are then orthogonalized to the market — the post-market block regresses on these residualized series jointly. This ensures sector and style betas measure incremental exposure beyond market, and final coefficients are back-transformed into raw ETF space for direct tradability.</dd>
          </div>
          <div className="landing-summary-trait">
            <dt>Exposures</dt>
            <dd>Regression betas in raw ETF units — directly interpretable as hedge ratios, with ridge regularization and post-fit thresholding for sparsity</dd>
          </div>
          <div className="landing-summary-trait">
            <dt>Strengths</dt>
            <dd>Every loading maps to a tradable instrument, hedge construction is deterministic, outputs are sparse and stable across rebalances</dd>
          </div>
        </dl>
      </div>
    </section>
  );
}
