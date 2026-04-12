from backend.risk_model import cuse_membership


def test_derive_realized_role_keeps_returns_projection_when_origin_already_normalized() -> None:
    role = cuse_membership._derive_realized_role(
        model_status="projected_only",
        exposure_origin="projected_fundamental",
        reason_code="returns_projection",
        projection_method="ols_returns_regression",
    )
    assert role == "projected_returns"
