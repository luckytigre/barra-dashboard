from backend.cpar.factor_registry import (
    CPAR1_FACTOR_REGISTRY_VERSION,
    CPAR1_METHOD_VERSION,
    MARKET_FACTOR_ID,
    build_cpar1_factor_registry,
    factor_group_for_id,
    factor_ids_for_group,
    serialize_factor_registry,
)


def test_cpar1_factor_registry_is_stable_and_ordered() -> None:
    registry = build_cpar1_factor_registry()

    assert registry[0].factor_id == MARKET_FACTOR_ID
    assert registry[1].factor_id == "XLB"
    assert registry[-1].factor_id == "IWM"
    assert len(registry) == 17

    serialized = serialize_factor_registry()
    assert serialized[0]["method_version"] == CPAR1_METHOD_VERSION
    assert serialized[0]["factor_registry_version"] == CPAR1_FACTOR_REGISTRY_VERSION
    assert [row["factor_id"] for row in serialized[:3]] == ["SPY", "XLB", "XLC"]


def test_cpar1_factor_groups_match_expected_registry_splits() -> None:
    assert factor_group_for_id("SPY") == "market"
    assert factor_group_for_id("XLK") == "sector"
    assert factor_group_for_id("MTUM") == "style"

    assert factor_ids_for_group("market") == ("SPY",)
    assert factor_ids_for_group("sector") == (
        "XLB",
        "XLC",
        "XLE",
        "XLF",
        "XLI",
        "XLK",
        "XLP",
        "XLRE",
        "XLU",
        "XLV",
        "XLY",
    )
    assert factor_ids_for_group("style") == ("MTUM", "VLUE", "QUAL", "USMV", "IWM")
