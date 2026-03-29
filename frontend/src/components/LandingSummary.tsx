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
          Estimates exposures from fundamental characteristics, industry
          structure, and orthogonalized style descriptors via constrained
          weighted least-squares regression across the investable universe.
        </p>
        <dl className="landing-summary-traits">
          <div className="landing-summary-trait">
            <dt>Method</dt>
            <dd>Single-stage constrained WLS with weighted sum-to-zero industry constraints, solved jointly across market, industry, and style blocks</dd>
          </div>
          <div className="landing-summary-trait">
            <dt>Factors</dt>
            <dd>Market + industry factors + 14 style factors. The live style set includes Size, Nonlinear Size, Beta, Momentum, Short-Term Reversal, Residual Volatility, Liquidity, Book-to-Price, Earnings Yield, Leverage, Growth, Profitability, Investment, and Dividend Yield.</dd>
          </div>
          <div className="landing-summary-trait">
            <dt>Factor hierarchy</dt>
            <dd>Style descriptors are orthogonalized in dependency order via WLS. Momentum is residualized to industry and Size, Residual Volatility to Size and Beta, Short-Term Reversal to Momentum, and Liquidity and Nonlinear Size to Size. Fundamental value and balance-sheet descriptors are neutralized to industry only.</dd>
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
          Returns-based regression built on real ETF proxies, but expressed in
          residualized factor space for risk reading. Market stays explicit;
          sector and style sleeves are residualized to market, then fit jointly
          so the cPAR surface emphasizes incremental structure instead of raw ETF
          overlap.
        </p>
        <dl className="landing-summary-traits">
          <div className="landing-summary-trait">
            <dt>Method</dt>
            <dd>One-shot weighted ridge on weekly returns with market unpenalized and a market-residualized sector/style block fit jointly</dd>
          </div>
          <div className="landing-summary-trait">
            <dt>Factors</dt>
            <dd>SPY + 11 sector SPDRs (XLB, XLC, XLE, XLF, XLI, XLK, XLP, XLRE, XLU, XLV, XLY) + 5 style ETFs (MTUM, VLUE, QUAL, USMV, IWM)</dd>
          </div>
          <div className="landing-summary-trait">
            <dt>Factor hierarchy</dt>
            <dd>Sector and style ETF returns are residualized to market at the package level. cPAR then fits market plus that residual block together, so non-market loadings describe incremental exposure beyond market without requiring a separate second-stage regression.</dd>
          </div>
          <div className="landing-summary-trait">
            <dt>Exposures</dt>
            <dd>Risk pages show residualized factor-space loadings. Hedge workflows can still translate that fit back into actionable raw ETF hedge space when needed.</dd>
          </div>
          <div className="landing-summary-trait">
            <dt>Strengths</dt>
            <dd>Compact tradable proxy set, clearer incremental risk reading than raw ETF betas, and a direct bridge back to raw ETF hedges when actionability matters</dd>
          </div>
        </dl>
      </div>
    </section>
  );
}
