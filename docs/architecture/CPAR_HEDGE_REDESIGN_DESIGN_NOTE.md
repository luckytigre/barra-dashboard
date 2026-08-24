# cPAR Hedge Redesign — Design Note

- Status: **decided and partially implemented** — see §9
- Date: 2026-08-23
- Scope: `backend/cpar/hedge_engine.py` and the services that consume it
- Decision owner: Shaun

§§1-2 (status quo and defects) stand as written. §§3-6 record the options that
were considered; the design actually chosen is simpler than P2 and is recorded
in §9, which supersedes the Phase 0 / Phase 1 split in §6.

This note records why the cPAR hedge packages are being reworked, what is
proposed, and what was validated before committing to the change. It does not
change behaviour on its own.

## 1. Status quo

### 1.1 How a hedge is produced today

The model fits, per instrument:

```
r = α_m + β_m·m + Σᵢ bᵢ·fᵢ + ε        fᵢ = pᵢ − αᵢ − γᵢ·m
```

where `m` is SPY, `pᵢ` are the raw proxy ETF returns, and `fᵢ` are those
proxies orthogonalized to the market (`backend/cpar/orthogonalization.py`,
which explicitly refuses to admit the market factor into the panel).

Substituting `fᵢ` back gives the tradable form used by the hedge engine:

```
r = c + spy_trade·m + Σᵢ bᵢ·pᵢ + ε     spy_trade = β_m − Σᵢ bᵢγᵢ
```

Two facts follow, and both matter later:

- The coefficient on each **raw** proxy is numerically identical to its
  residual-space coefficient `bᵢ`. Orthogonalization changes only the market
  coefficient (`backend/cpar/backtransform.py:25-47`).
- `β_m` is *exactly* the univariate beta of the asset against SPY. Because the
  `fᵢ` are WLS residuals on `[1, m]`, the normal matrix is block-diagonal
  between the market term and the factor block, so the ridge penalty applied to
  the block (`sector_lambda=1.0`, `style_lambda=2.0`,
  `backend/orchestration/cpar_stages.py:325-326`) cannot perturb `β_m` at all.

Both quantities are persisted per instrument: `market_step_beta` (= `β_m`) and
`spy_trade_beta_raw` (= `spy_trade`), at `backend/data/cpar_schema.py:302-304`,
read back at `backend/data/cpar_queries.py:76-78`.

`hedge_trade_loadings_from_fit` (`backend/services/cpar_display_loadings.py:55-70`)
assembles the vector the engine sees: the stored thresholded loadings, with the
market entry overwritten by `spy_trade_beta_raw`.

### 1.2 The two packages

**Market-neutral** (`hedge_engine.py:159-210`): if `|spy_trade| < 0.10`
(`MARKET_MATERIALITY_THRESHOLD`, line 13) it returns no leg and status
`hedge_ok` with reason `below_market_materiality_threshold`. Otherwise the sole
leg is `−spy_trade` on SPY (line 206).

**Factor-neutral** (`hedge_engine.py:282-356`):

1. `_candidate_factor_ids` (213) — non-market factors with `|b| >= 0.05`, plus
   SPY if `|spy_trade| >= 0.10`.
2. `_prune_correlated_substitutes` (224) — for any pair with `|corr| > 0.90` on
   the **raw** covariance, drop whichever has the smaller `|loading|`.
3. `_apply_leg_cap_with_limit` (259) — cap at `MAX_HEDGE_LEGS = 5`, reserving
   the first slot for SPY *if it survived pruning*.
4. Weights by exact negation: `{f: −loading[f]}` (328).
5. `TINY_POSITION_THRESHOLD = 0.05` (17) — any leg whose weight is below 0.05 is
   then dropped **after** the cap, with no backfill and no resizing (329-333,
   and 408-411 on the recommendation path). Codified at
   `CPAR1_MATH_KERNEL.md:185`. This matters for P2: a re-solved weight vector
   will routinely contain sub-0.05 entries, and this filter would reintroduce
   exactly the "drop remainder unresized" pathology P2 exists to remove.

Quality is scored by `non_market_reduction_ratio` (334-344), which **excludes
the market factor from both numerator and denominator**; below 0.50 the status
degrades (345).

Sizing is `quantity = weight × base_notional / price`
(`backend/services/cpar_hedge_trade_sizing.py:102-103`), identical for both
packages.

A third path, `build_factor_neutral_recommendation` (359), powers the
recommendation panel. It does **not** prune, caps at 10 legs, and computes its
ratio over **all** loadings including market (413-419).

### 1.3 Why both packages report the same SPY quantity

This was the question that started the review. The answer is that the SPY
quantity is computed once, from the instrument, and nothing about package
selection can change it:

- Both packages size the SPY leg as `−spy_trade`.
- `spy_trade` is already net of the market exposure carried by the proxy legs.
- Therefore the market leg is invariant to which non-market legs are included.

Given the current construction this is correct, not a copy-paste defect: if the
two differed, market exposure would be double-counted. The registry is
`SPY + 11 SPDR sectors + 5 style ETFs` (`backend/cpar/factor_registry.py:12-30`).

## 2. The problem

### 2.1 "Market-neutral" does not neutralize market beta

`spy_trade` is deliberately shrunk on the assumption that the proxy legs will
deliver their share of the market. The market-neutral package never trades
those legs, so that exposure is simply left in the book.

Worked example — a name with `β_m = 1.10` loading `0.80` on XLK (`γ_XLK ≈ 1.05`):

| quantity | value |
| --- | --- |
| asset total market beta | 1.10 |
| shorted by market-neutral (`spy_trade`) | 0.26 |
| **market beta remaining in book** | **0.84** |
| remaining under factor-neutral (all legs traded) | 0.00 |

Aggravating factor: the `0.10` materiality gate tests `|spy_trade|`, so a name
with ~1.1 actual beta whose sector loading absorbs enough of it returns
`below_market_materiality_threshold` — no hedge at all, status `hedge_ok`.

**This is not spec-faithful — it is a divergence from the spec.** An earlier
draft of this note claimed the implementation matched the spec and the spec was
wrong. That was a misreading. `cPAR_full_spec.md:610` says
`hedge weight for SPY = -beta_market`, and `beta_market` has exactly one
definition, at line 454: the Step 1 market-fit coefficient
`y = alpha_market + beta_market·m + eps_market` — i.e. **β_m**. The token
`beta_spy_trade` does not appear anywhere in the spec; it is introduced
downstream in `CPAR1_MATH_KERNEL.md:179`.

The spec is not wholly unambiguous — §"Factor-Neutral Hedge" asserts the
engine works from thresholded raw ETF-space loadings, in which basis the market
coefficient would be `spy_trade` — but the market-neutral rule names a term with
a single, explicit definition, and the implementation substituted a different
quantity for it.

This materially changes P1's status: it is **restoring the spec's most natural
reading**, not a product decision that overrides the spec.

### 2.2 The pruner systematically deletes the market leg

Nothing protects SPY in `_prune_correlated_substitutes`. Two structural facts
make SPY the default victim rather than an occasional one:

- SPY is the raw-correlation hub of this registry — every member is a
  long-only, cap-weighted US large-cap fund (SPY vs QUAL ≈ 0.97, vs XLI ≈ 0.93,
  vs XLK/MTUM ≈ 0.92). Correlations above the `0.90` threshold are the norm,
  not the exception.
- `spy_trade = β_m − Σ bᵢγᵢ` is *structurally* the smallest coefficient in the
  vector for any name with positive sector/style loadings — so SPY
  systematically loses the "keep the larger exposure" contest.

When SPY is dropped it is never restored: `_apply_leg_cap_with_limit` reserves a
slot only `if MARKET_FACTOR_ID in candidate_ids`, and that list is the
already-pruned one. And because `non_market_reduction_ratio` excludes market,
the loss cannot degrade the status.

Consequences, verified numerically:

- **Same-sign case** (`SPY 0.155 / XLK 0.90`, corr 0.92): SPY pruned, hedge is
  `−0.90 XLK`, ratio reports 1.000 and `hedge_ok`. Variance reduction is
  genuinely good (~98.7%) but 0.155 of directional beta is carried silently —
  $1.55M on a $10M position. A disclosure defect.
- **Opposite-sign case** (`SPY +0.30 / VLUE −0.40`, corr 0.92): SPY pruned,
  hedge is `+0.40 VLUE`, and the "hedge" **increases** variance while swinging
  total market beta from −0.08 to +0.68. Reported ratio 1.000, status
  `hedge_ok`. A risk defect.

  Magnitude is assumption-dependent and this note does not pin it down. At
  weekly vols `σ_SPY = 2.2%`, `σ_VLUE = 3.0%` the increase is +4.1%
  (vol 0.00647 → 0.00660); at a wider vol spread an independent check obtained
  roughly 2x. The *direction* is robust to the assumption; the magnitude is not,
  and the figure above should not be quoted without its vols.

### 2.3 Contributing weaknesses

- **The leftover is invisible where it matters.** The service returns
  `post_hedge_exposures`, and `CparPostHedgeTable` renders it on the portfolio
  and recommendation panels — but `CparPositionHedgePopover.tsx` never renders
  it. On the single-name sizing screen the only quality signal shown is
  `Reduction {non_market_reduction_ratio}`, i.e. "100.0%".
- **Tests cannot catch it.** `test_cpar_hedge_engine.py:32` does exercise
  pruning, but `_identity_covariance` gives SPY zero correlation with
  everything, so no test in the file *can* prune SPY.
- **Two surfaces disagree.** The recommendation path does not prune and does
  count market in its ratio, so it and the compact preview can emit
  contradictory hedges for the same portfolio.
- **The pruner has no mathematical justification.** Weights are exact negation —
  nothing solves a linear system — so collinearity causes zero numerical harm.
  Pruning is purely a ticket-count heuristic, and the leg cap already bounds
  ticket count. Every prune trades a real unhedged exposure for one fewer trade,
  decided on raw correlations where nearly every pair looks like a substitute.

## 3. Proposed design

### P1 — Market-neutral becomes a beta-weighted dollar-delta hedge

Size the SPY leg at `−β_m` (`market_step_beta`) rather than `−spy_trade`.
Because `β_m` is exactly the univariate beta, `−β_m × notional / price` is the
textbook beta-weighted dollar hedge and genuinely nulls total market beta. The
materiality gate tests `|β_m|`.

What it does **not** do: it leaves sector and style exposure untouched, and it
is a *beta-neutrality* guarantee rather than a variance optimum — the
variance-minimizing single-SPY leg is `−(Σ_SPY,· β)/σ²_SPY`, which differs
whenever the position has non-market loadings correlated with SPY. That is a
defensible product choice, but it should be named accurately.

**P1 must be scoped to the market-neutral package only.** Validation surfaced a
trap that the original framing missed: `−spy_trade` is the *correct marginal*
SPY leg conditional on also holding the factor legs. Applying `−β_m` inside
factor-neutral would over-hedge by exactly `Σbᵢγᵢ`:

| package | SPY leg | residual market beta |
| --- | --- | --- |
| market-neutral, today | `−spy_trade` = −0.26 | **+0.84** (under-hedged) |
| market-neutral, P1 | `−β_m` = −1.10 | **0.00** — correct |
| factor-neutral, today | `−spy_trade` = −0.26 | **≈0.00** — correct in principle |
| factor-neutral, if P1 applied | `−β_m` = −1.10 | **−0.84** (over-hedged) |

The third row is idealized. Because `spy_trade_beta_raw` is computed from
*unthresholded* betas while the factor legs come from the *thresholded* vector
(§4.4), today's factor-neutral actually leaves `Σ_{j zeroed} bⱼγⱼ` of market
beta, and that residue **compounds across the book** under aggregation. So the
status quo is slightly worse than the row suggests.

So the two packages should deliberately use *different* market bases. Under P2
this trap disappears on its own, because SPY becomes one tradable among many in
a joint solve.

**Implementation is larger than a flag.** `thresholded_loadings["SPY"]` *already
is* `β_m` (`cpar_stages.py:333`; market is exempt from thresholding at
`backtransform.py:62-63`), and the only thing converting it to `spy_trade` is the
three-line overwrite in `hedge_trade_loadings_from_fit`
(`cpar_display_loadings.py:67-69`). But a flag on that function yields **one**
vector, and the services build a single aggregate loading vector that is shared
by both packages — `cpar_position_hedge_service.py:125-131` passes the same
object to `build_market_neutral_hedge` (150) and `build_factor_neutral_hedge`
(156). A naive flag would therefore apply the total-beta basis to *both*
packages, which is precisely the error this section warns against.

P1 requires, across three files:

1. the `market_basis` flag on `hedge_trade_loadings_from_fit` (~4 lines);
2. a **second** `_aggregate_loadings` pass in `cpar_position_hedge_service.py`,
   plumbed to the market-neutral call only (~8 lines);
3. the same treatment in **`cpar_portfolio_account_snapshot_service.py:151-157`**,
   which feeds `build_hedge_preview(mode=...)` at :239 and dispatches to
   `build_market_neutral_hedge` at `hedge_engine.py:447`. This is the portfolio
   market-neutral panel. **If P1 skips it, the popover and the portfolio panel
   will report different SPY quantities for the same package** — a new
   two-surfaces-disagree defect of exactly the kind §2.3 complains about.

Realistic size: ~35–40 lines plus mode-conditional dispatch, not ~20. The extra
aggregation pass is cheap — `cpar_portfolio_account_snapshot_service.py` already
performs three (147, 151, 158) and each is O(N·17) — so the "at most 2 hedge
builds per request" property is unaffected.

Also fix, while in this function: `dict(clean_fit.get("thresholded_loadings") if
thresholded else clean_fit.get("raw_loadings") or {})` binds `or {}` to the
`else` branch only, so a null `thresholded_loadings` would raise. Not reachable
today (`cpar_queries.py:80` defaults to `{}`), but P1 edits this exact line.

### P2 — Factor-neutral becomes a cardinality-constrained variance-minimizing hedge

Replace *(heuristic selection) → (exact negation) → (drop remainder unresized)*
with: choose a subset `S` of at most `X` tradable ETFs **and solve jointly for
their weights** to minimize residual factor variance of the hedged book.

Rationale: exact negation is optimal only in the unconstrained case. Once
cardinality is capped, surviving legs must be resized to stand in for dropped
ones — a leg correlated `ρ` with a dropped leg can absorb roughly `ρ²` of its
variance. Today it absorbs none.

This is a strict generalization: with `X >= |candidates|` the optimum is exactly
`w = −β`, i.e. current behaviour. It also dissolves the problems in §2.2 — there
is no leg-deletion contest, so the market leg cannot silently vanish — and it
makes the quality score an honest residual-variance ratio rather than a gross
magnitude ratio that excludes market by fiat.

## 4. Validation

Independent quantitative review. **No return data exists in-repo**
(`backend/runtime/data.db` is a 16KB stub), so all covariance figures below come
from an *assumed* synthetic covariance calibrated to reproduce the qualitative
facts that matter (`corr(SPY,QUAL)=0.982`, `corr(SPY,XLK)=0.933`,
`corr(SPY,XLU)=0.616`, annualized vols 15–28%). They are illustrative, not
measured.

### 4.1 P1 is correct — confirmed

Block-diagonality verified structurally and numerically: the `[1,m] × F`
cross-block of the weighted normal matrix measures `1.22e-18` against a max
diagonal of 1.0, and `market_beta` is invariant to ridge across nine orders of
magnitude (`λ = 0 … 1e9` all give `0.926767772111`, identical to the univariate
WLS beta). Shorting `−β_m` moves residual market beta `+0.9268 → 0.0000`.

**This is exact only for full-history instruments.** `cpar_stages.py:312-316`
masks to `valid_mask` and renormalizes the weights, but
`orth_result.residual_matrix` was orthogonalized on the *full* 52-week sample,
and orthogonality under full-sample weights does not survive subsetting. With
`DEFAULT_MIN_OBSERVATIONS = 39` (`status_rules.py:18`), up to 13 weeks can be
dropped and still fit. Measured over 2000 random 40-of-52 masks: relative
`|β_m − univariate β|` median 0.9%, p95 2.8%, worst 6.1%. P1 is unaffected in
substance — β_m remains vastly closer to total beta than `spy_trade` — but the
"ridge cannot perturb β_m at all" phrasing holds only for complete histories.

The gate change is safe. `|β_m| >= 0.10` passes for essentially every equity,
and that is correct rather than noise — `β_m ≈ 1` is a genuine, unpenalized,
precisely-estimated exposure and usually the largest single variance source. The
*current* gate is the dangerous one: because `spy_trade = β_m − Σbᵢγᵢ` and proxy
`γ` run 0.58–1.15, `spy_trade` for a fully-loaded equity lands in the 0.1–0.3
band, straddling the threshold. A name with `β_m = 1.05` can be gated out of its
market hedge because its factor loadings happen to reconstruct market beta.
Keep 0.10 as a guard for genuinely low-beta instruments; do not raise it.

### 4.2 P2 is mathematically correct — confirmed

Objective, with `A` = all loaded factors, `S` ⊆ registry the tradable subset,
`Σ` the raw proxy covariance:

```
minimize_{w supported on S}   (β + w)ᵀ Σ (β + w)
```

This is a Gram-matrix least-squares problem — a weighted regression of the
position onto the hedge legs. Verified:

- Unconstrained over all legs recovers `w = −β` exactly
  (`max|wᵢ + βᵢ| = 8.4e-15`). P2 strictly generalizes current behaviour.
- Closed form: `w_S = −(Σ_SS)⁻¹ Σ_{S,A} β_A`.
- The `ρ²` recovery intuition is exact: XLK/QUAL (ρ=0.919) recovers 0.8442 of
  the dropped leg's variance, SPY/XLU (ρ=0.616) recovers 0.3800.

On a representative tech-ish loading vector with a 5-leg cap:

| variant | post/pre variance |
| --- | --- |
| today (prune + cap + exact negation) | 0.1463 |
| **today's own legs, weights re-solved** | **0.0077** (19x better) |
| full P2 (re-select + re-solve) | 0.0020 (73x better) |

**~95% of the gain comes from re-solving the weights, not from re-selecting the
subset.** That decomposition drives the phasing in §6.

### 4.3 The flagged collinearity risk is largely misplaced

This was the main concern held against P2. It does not survive testing.

**Conditioning.** Across *all* 6188 five-subsets: median `cond(Σ_SS)` = 50.7,
p95 = 310, worst = 414. Zero subsets exceed 1e3. Forcing `corr(SPY,QUAL)=0.995`
raises the worst case only to ~1352 — against float64's ~1e16, that is 13 digits
of headroom. A plain Cholesky with a `1e-12·trace` jitter suffices. **The
collinearity that motivated pruning does not return as numerical instability.**

**Estimation error.** The real risk is statistical: `N_eff = 45` effective
observations for a 17-asset covariance, giving ~15% relative error in `(1−ρ²)`,
which min-variance weights inherit. Tested by simulating 120 independent 52-week
samples, building each hedge on `Σ̂`, and scoring realized variance under the
true `Σ`:

| variant | median ratio | p90 | max gross |
| --- | --- | --- | --- |
| today: prune + cap + negation | 0.1633 | 0.2728 | 1.48 |
| **P2, no regularization** | **0.0024** | **0.0030** | 2.00 |
| P2, ridge 0.10 | 0.0037 | 0.0050 | 1.75 |
| P2, ridge 0.25 | 0.0071 | 0.0100 | 1.78 |
| P2, ridge 0.02 + gross cap 1.5 | 0.0190 | 0.0220 | 1.50 |

Oracle (true `Σ` known) = 0.0020. Unregularized P2 sits at 0.0024 median with
p90 = 0.0030 — a 20% degradation from oracle, against a 68x degradation for the
current engine. **Every regularizer tested made it monotonically worse.**

**The min-variance blow-up premise was imported from the wrong problem.** That
pathology belongs to the *budget-constrained* portfolio `min wᵀΣw s.t. 1ᵀw = 1`,
where near-identical assets force exploding offsetting weights. P2 has no budget
constraint — it is a regression against a bounded target `β`. Demonstrated on a
deliberately near-degenerate subset over 200 estimations: individual weights on
collinear legs wander (`w[SPY]` spread 0.44), but **their sum and the resulting
variance do not** (cluster sum spread 0.22, realized variance p95 within 25% of
oracle). Splitting −0.93 across three 0.95-correlated instruments in a different
proportion is economically a no-op. Worst gross notional with no cap: 2.00x,
against the position's own `Σ|β|` of 1.92. There is no blow-up to prevent.

**Conclusion: do not add ridge, L1, or a gross-notional constraint to the
objective.** They cost 3x–8x and buy nothing.

Two supports for this, confirmed on review:

- **`Σ` cannot be rank-deficient.** It is `XᵀWX` over 52 weekly observations of
  17 assets (`DEFAULT_LOOKBACK_WEEKS = 52`, `DEFAULT_HALF_LIFE_WEEKS = 26`), so
  rank ≤ 17 and full. `_build_factor_return_series` *raises* on any incomplete
  proxy history (`cpar_stages.py:139-143`), so there is no partial-panel path,
  and `covariance_matrix_for_factors` raises on any missing pair rather than
  zero-filling (`hedge_engine.py:54-61`). The rank-deficiency hypothesis that
  would have collapsed this section is refuted.
- **The target `β` is provably bounded.** `_aggregate_loadings` divides signed
  `market_value` by *gross* market value, so `Σᵢ|wᵢ| = 1` exactly and the
  aggregate loading vector is a convex combination of per-instrument loadings.
  It cannot scale arbitrarily, which is what removes the amplification channel.

**Scope caveat.** All conditioning evidence covers `|S| = 5`, and only the 5×5
`Σ_SS` is ever inverted. If Phase 0.5's stop condition fires and the leg cap is
raised, `|S|` grows toward 17, where `N_eff/|S| → 45/17 ≈ 2.6` and both
conditioning and estimation error degrade sharply. **The simulation gives zero
coverage of the configuration Phase 0.5 recommends.** Either bound `|S| ≤ 5` for
the solve regardless of the cap, or re-run this analysis at the larger size.

### 4.4 What validation changed in the proposal

- **P1 must not be applied to factor-neutral** (§3). This was a real error in
  the original framing.
- **Cost is a non-issue.** Hedges are built on a single *aggregate* loading
  vector — `_aggregate_loadings` collapses the book first — so there are at most
  2 hedge builds per HTTP request regardless of book size. Full `C(17,5)` search
  is ~101 ms in pure Python, ~10 ms with NumPy. The k=10 recommendation path
  mostly short-circuits: when `#loaded ≤ k`, the P2 optimum *is* `w = −β`.
- **`previous_hedge_weights` and the stability diagnostics are dead code** — no
  caller supplies them, so `leg_overlap_ratio` and both notional-change fields
  are always null. Verified. P2 introduces day-to-day weight churn on collinear
  legs (economically a no-op but visible), so this needs wiring up or accepting.
- **A pre-existing defect P1 also fixes:** `spy_trade_beta_raw` is computed from
  *unthresholded* `residualized_betas` but injected into a *thresholded* vector,
  so today's market leg carries a `Σbᵢγᵢ` adjustment for proxies whose `bᵢ` was
  subsequently zeroed. `β_m` is threshold-free and has no such problem.

## 5. Blast radius

**Schema / persistence: nothing.** Both `market_step_beta` and
`spy_trade_beta_raw` are already persisted (`cpar_schema.py:302,304`,
`cpar_queries.py:76,78`, Neon mirror `NEON_CPAR_SCHEMA.sql:79,81`). **Hedges are
not persisted anywhere** — no table, no writer. There is no state to migrate.

**API payload.** `post_hedge_loadings` / `post_hedge_exposures`, `hedge_legs`,
`gross_hedge_notional`, `net_hedge_notional` and the variance proxies keep their
semantics. The one that must change is `non_market_reduction_ratio` — a gross-|β|
L1 ratio that excludes market, which is precisely why a dropped SPY leg cannot
degrade status. Replacing it with `1 − post_var/pre_var` (including market)
preserves the wire type, range and rendering, so **the contract does not break —
only the meaning does.** Rename to `residual_variance_reduction_ratio` and
dual-write for one release rather than silently redefining it.

Consumers: `frontend/src/lib/types/cpar.ts` (239, 284, 463, 478),
`CparPortfolioHedgePanel.tsx:97`, `CparPortfolioHedgeRecommendationPanel.tsx:62-63`,
`CparPositionHedgePopover.tsx:57`, `frontend/scripts/cpar_hedge_page_smoke.mjs`.
All render it under a label literally called "Reduction", which is *more* honest
under a variance definition. `_display_space_reduction_ratio`
(`cpar_portfolio_hedge_recommendation_service.py:165-178`) computes a second,
display-space definition and must be consolidated.

**The 0.50 `hedge_degraded` threshold is calibrated against an L1 ratio and must
be recalibrated against a variance ratio, or a large fraction of the book flips
to `hedge_degraded` on day one.** This is the most likely operational surprise.

**Tests to rewrite:** `test_cpar_hedge_engine.py` — `..._deterministic_after_pruning_and_leg_cap`,
`..._marks_degraded_when_leg_cap_leaves_too_much_residual`,
`..._respects_explicit_leg_cap`, `..._recommendation_uses_top_magnitude_candidates_up_to_ten`
all encode exact negation + pruning; `test_market_neutral_hedge_skips_small_market_beta`
encodes the `|spy_trade|` gate. Plus `test_cpar_routes.py`,
`test_cpar_portfolio_hedge_recommendation_service.py`,
`test_cpar_position_hedge_service.py`, `test_cpar_portfolio_hedge_service.py`.

**Docs:** `CPAR1_MATH_KERNEL.md:176-197` is a near-verbatim spec of the current
rules and must be rewritten. `CPAR_BACKEND_READ_SURFACES.md:301` describes
`spy_trade_beta_raw` as the per-name hedge-trade-space escape hatch, which P1
changes. `docs/archive/legacy-plans/cPAR_full_spec.md:629,645,683` is the origin
of the 0.90 / 5-leg / 50% constants (archived, no edit required).

**Explainability regression — the genuine cost of full P2.** The joint solve will
select legs with *zero* underlying loading (a substitute such as QUAL for a
position that holds none). `post_hedge_loadings` then honestly shows a new
negative QUAL exposure on a position that had none. Today's engine can never do
that.

The problem is worse than an earlier draft of this note stated. That draft said
the popover renders `pre_beta / hedge_leg / post_beta` per factor — it does not,
contradicting §2.3 of this same note. `CparPositionHedgePopover.tsx:62-87`
renders **ETF / Quantity / Value only**; the per-factor columns live in
`CparPostHedgeTable.tsx:66-68`, which the popover never mounts. So a user would
see a naked QUAL trade with *no* explanatory context whatsoever.

The mitigation is cheap, because the payload already carries
`post_hedge_exposures` (`cpar_position_hedge_service.py:82-86`): render the
existing `CparPostHedgeTable` in the popover. Nothing new needs building.
P2-lite avoids the issue entirely — legs remain the user's own exposures and
only the sizes change.

**Unaddressed by either design:** `β` is a ridge estimate, deliberately shrunk
(λ=1 sectors, λ=2 styles on standardized columns). Exact negation of a shrunk
coefficient systematically under-hedges. P2's variance objective partially
self-corrects, but the `β` entering `Σ_{S,A} β_A` is still shrunk. Worth a
documented caveat.

## 6. Recommendation

**Phase 0 — delete `_prune_correlated_substitutes`. Ship first, ship today.**

One line: `hedge_engine.py:326`, `pruned = _prune_correlated_substitutes(...)`
becomes `pruned = candidates`. Rewrite
`test_..._deterministic_after_pruning_and_leg_cap` and
`test_..._respects_explicit_leg_cap`.

This is the correct first move by this note's own argument. The pruner produces
the only *risk* defect in the document (§2.2 — a hedge that increases variance);
§2.3 already concedes it has no mathematical justification, since weights are
exact negation and the leg cap already bounds ticket count. With the pruner gone
and exact negation retained, the opposite-sign case resolves completely: both
legs survive the cap, both are exactly negated, both post-hedge loadings go to
zero, variance strictly falls. **No re-solve is required to fix it.** Under the
existing L1 metric, removing the pruner is weakly improving for every input — it
only ever adds correctly-signed legs up to the cap.

No metric change, no threshold recalibration, no wire change, no data
dependency. Cost is a slightly higher ticket count, bounded by the existing
5-leg cap. An earlier draft of this note bundled this into Phase 2, behind a
Phase 0.5 measurement that **cannot currently be run** (§6) — leaving the most
dangerous live defect gated on an action nobody can take.

**Phase 1 — P1, standalone.** Add a `market_basis` flag to
`hedge_trade_loadings_from_fit`: `"total_beta"` leaves the SPY entry as `β_m`,
`"marginal"` keeps the current `spy_trade_beta_raw` overwrite. Market-neutral
uses `"total_beta"` and gates on `|β_m|`; **factor-neutral keeps `"marginal"`**
(§3). Resolve the unthresholded-`spy_trade_beta_raw` inconsistency. Update
`CPAR1_MATH_KERNEL.md:179` and the gate test.

**Phase 0.5 — measure before committing to P2. BLOCKING DEPENDENCY.** Histogram
`len([f for f in thresholded_loadings if |β| >= 0.05])` per instrument and for
the aggregate portfolio vector. **If the median is ≤ 5, raise the leg cap and
stop** — that alone fully resolves the cardinality problem with zero math risk.
P2's entire value proposition is conditional on positions routinely exceeding
the cap, and that number is currently unknown.

**This cannot be run from the repo.** `backend/runtime/data.db` holds one table
(`serving_payload_current`) with zero rows, `cache.db` is empty, and there is no
`cpar_instrument_fits` table, persisted package, or fixture carrying
`thresholded_loadings` anywhere in the tree. The measurement requires live Neon
credentials or a full package build against a price source the repo does not
hold. Everything after Phase 1 is therefore gated on an action no one working
from the repo alone can take. **Needs a named owner and a named data source
before Phase 2 can be scheduled.** If the cap is raised as a result, see the
`|S| ≤ 5` scope caveat in §4.3.

**Phase 2 — P2-lite, if the measurement justifies it.** Keep
`_candidate_factor_ids`, **delete `_prune_correlated_substitutes`** (the source
of the §2.2 defect), keep the leg cap, and re-solve the weights:

```
w_S = −(Σ_SS + 1e-12·tr(Σ_SS)/|S| · I)⁻¹ Σ_{S,A} β_A     # jitter for safety, NOT ridge
```

Replace the reduction ratio with a variance ratio including market, dual-written
for one release, and recalibrate the degraded threshold against real data. This
alone kills the risk-increasing case in §2.2: an opposite-sign "hedge" now
reports a negative ratio and degrades correctly.

**Phase 3 — full subset search, optional.** Exhaustive `C(17,5)`, SPY *chosen
rather than forced*, short-circuiting to exact negation when `#loaded ≤ k`. Needs
a "substitute leg" affordance in the popover first.

**Explicitly do not build:** ridge on `w`, L1/lasso selection, or a
gross-notional cap in the objective (§4.3). Add a gross-notional *warning*
threshold instead, and wire up the dead stability diagnostics to manage churn.

## 7. Open questions for the decision owner

- **Phase 0.5 is a blocking dependency, not a task.** It cannot be run from the
  repo (§6). Who runs it, against which package, with what credentials?
- Is the market-neutral surface intended as beta-neutrality or as a variance
  optimum? P1 delivers the former. Note this is no longer a spec question —
  §2.1 establishes the spec already asks for `−beta_market`.
- Is the explainability cost of full P2 (substitute legs) acceptable, or is
  P2-lite the terminal state?
- Does `TINY_POSITION_THRESHOLD` survive into P2 (§1.2 step 5)? Keeping it
  reintroduces the drop-unresized pathology; removing it changes ticket counts
  and contradicts `CPAR1_MATH_KERNEL.md:185`.

## 8. Review history

- Drafted from a code investigation prompted by the question "why do both hedge
  packages report the same SPY quantity?"
- Validation pass: confirmed the P1 algebra and the P2 formulation, refuted the
  collinearity/regularization concern, and surfaced the P1 scoping trap (§3).
- Adversarial pass: corrected the spec reading (§2.1 — P1 is a bug fix, not a
  spec override), found the shared-aggregate-vector problem and the omitted
  third surface (§3), the `TINY_POSITION_THRESHOLD` omission (§1.2), the
  partial-history limit on block-diagonality (§4.1), an internal contradiction
  about what the popover renders (§5), and the phasing error now corrected by
  Phase 0 (§6). It refuted the rank-deficiency hypothesis (§4.3).

Claims in this note are sourced to `file:line`; citations were audited in the
adversarial pass. Covariance figures are from an assumed synthetic matrix — no
return data exists in-repo.

## 9. Decision and implementation

**Decision: keep exact negation. No re-optimization, no covariance in the
sizing.** The loadings are used as they already exist; converting to trade space
and negating is the whole of it. A six-instrument factor package — the five
largest factor legs plus a market leg — plus a separate pure beta hedge.

This supersedes the Phase 0 / Phase 1 split in §6 and defers P2 (§3) and P2-lite
(§4.2) indefinitely. They remain on record as refinements, not commitments.

### What shipped

1. **Market-neutral sizes on total market beta.** `build_market_neutral_hedge`
   takes an optional `market_total_beta`; when supplied it replaces the market
   entry of the loading vector. Both call sites pass the market entry of the
   *residual-space* aggregate, which is already `β_m`
   (`cpar_stages.py:333`) — so no new field, no schema change, and no second
   trade-space aggregation. `build_factor_neutral_hedge` does not receive it and
   keeps the trade-space coefficient, which is correct there (§3).
2. **The correlated-substitute pruner is deleted**, along with
   `_correlation_matrix` and `CORRELATION_PRUNE_THRESHOLD`, which had no other
   users. Weights are exact negation, so collinearity causes no numerical harm
   and the leg cap already bounds ticket count.
3. **The leg cap now bounds non-market legs only.** The market leg rides
   alongside rather than competing for a slot, so a full package is
   `MAX_HEDGE_LEGS + 1` = 6 instruments. This is what makes the market leg
   structurally un-droppable.
4. **The recommendation path uses the same selection helper**, so the two
   surfaces can no longer recommend different instrument sets for one vector.

### Deliberately not done

The market leg uses the **stored** `spy_trade_beta`, which nets out the market
content of *all* factor loadings including untraded ones. Recomputing it over
only the traded legs would zero total market beta exactly; as shipped, a small
residue remains (`+0.0277` of beta on a representative 8-factor vector, ~$277k
on a $10M position). Accepted as the minimal-change option. Revisit if that
residue proves material.

`non_market_reduction_ratio` is unchanged. It still excludes the market factor,
but the defect that made this dangerous — a silently dropped market leg — is
structurally impossible now, so replacing it with a variance ratio (§5) is no
longer urgent and would force a threshold recalibration against data that
cannot currently be read (§6).

### Verification

`backend/tests/test_cpar_hedge_engine.py` rewritten against the new rules, with
regression coverage added for the two defects in §2.2: the market leg surviving
a 0.98-correlated substitute, and a fix-agnostic assertion that a hedge never
increases `post_hedge_variance_proxy` above `pre_hedge_variance_proxy`.

Full backend suite run before and after on Python 3.12: identical failure sets
(26 pre-existing, unrelated), pass count 963 → 967. **No regressions
introduced.**

### Pre-existing issue found during verification

`backend/services/neon_authority.py` uses a backslash inside an f-string
expression, which is a `SyntaxError` before Python 3.12 (PEP 701). The project
declares `requires-python = ">=3.11"` and `.github/workflows/ci.yml` pins
`python-version: "3.11"`, so **13 test files fail to collect on CI today**,
independent of this change. Either raise the CI pin and `requires-python` to
3.12, or rewrite that f-string. Not addressed here.
