"""Fixed cPAR1 factor registry."""

from __future__ import annotations

from backend.cpar.contracts import FactorSpec

CPAR1_METHOD_VERSION = "cPAR1"
CPAR1_FACTOR_REGISTRY_VERSION = "cPAR1_registry_v1"

MARKET_FACTOR_ID = "SPY"

_REGISTRY: tuple[FactorSpec, ...] = (
    FactorSpec(factor_id="SPY", ticker="SPY", label="Market", group="market", display_order=0),
    FactorSpec(factor_id="XLB", ticker="XLB", label="Materials", group="sector", display_order=10),
    FactorSpec(factor_id="XLC", ticker="XLC", label="Communication Services", group="sector", display_order=11),
    FactorSpec(factor_id="XLE", ticker="XLE", label="Energy", group="sector", display_order=12),
    FactorSpec(factor_id="XLF", ticker="XLF", label="Financials", group="sector", display_order=13),
    FactorSpec(factor_id="XLI", ticker="XLI", label="Industrials", group="sector", display_order=14),
    FactorSpec(factor_id="XLK", ticker="XLK", label="Technology", group="sector", display_order=15),
    FactorSpec(factor_id="XLP", ticker="XLP", label="Consumer Staples", group="sector", display_order=16),
    FactorSpec(factor_id="XLRE", ticker="XLRE", label="Real Estate", group="sector", display_order=17),
    FactorSpec(factor_id="XLU", ticker="XLU", label="Utilities", group="sector", display_order=18),
    FactorSpec(factor_id="XLV", ticker="XLV", label="Health Care", group="sector", display_order=19),
    FactorSpec(factor_id="XLY", ticker="XLY", label="Consumer Discretionary", group="sector", display_order=20),
    FactorSpec(factor_id="MTUM", ticker="MTUM", label="Momentum", group="style", display_order=30),
    FactorSpec(factor_id="VLUE", ticker="VLUE", label="Value", group="style", display_order=31),
    FactorSpec(factor_id="QUAL", ticker="QUAL", label="Quality", group="style", display_order=32),
    FactorSpec(factor_id="USMV", ticker="USMV", label="Low Volatility", group="style", display_order=33),
    FactorSpec(factor_id="IWM", ticker="IWM", label="Size", group="style", display_order=34),
)

_SPEC_BY_ID = {spec.factor_id: spec for spec in _REGISTRY}


def build_cpar1_factor_registry() -> tuple[FactorSpec, ...]:
    return _REGISTRY


def factor_spec_by_id(factor_id: str) -> FactorSpec:
    clean = str(factor_id or "").strip().upper()
    if clean not in _SPEC_BY_ID:
        raise KeyError(f"Unknown cPAR1 factor_id: {factor_id}")
    return _SPEC_BY_ID[clean]


def factor_group_for_id(factor_id: str) -> str:
    return factor_spec_by_id(factor_id).group


def factor_ids_for_group(group: str) -> tuple[str, ...]:
    clean = str(group or "").strip().lower()
    return tuple(spec.factor_id for spec in _REGISTRY if spec.group == clean)


def ordered_factor_ids(*, include_market: bool = True) -> tuple[str, ...]:
    if include_market:
        return tuple(spec.factor_id for spec in _REGISTRY)
    return tuple(spec.factor_id for spec in _REGISTRY if spec.group != "market")


def serialize_factor_registry() -> list[dict[str, object]]:
    return [
        {
            "factor_id": spec.factor_id,
            "ticker": spec.ticker,
            "label": spec.label,
            "group": spec.group,
            "display_order": spec.display_order,
            "method_version": CPAR1_METHOD_VERSION,
            "factor_registry_version": CPAR1_FACTOR_REGISTRY_VERSION,
        }
        for spec in _REGISTRY
    ]
