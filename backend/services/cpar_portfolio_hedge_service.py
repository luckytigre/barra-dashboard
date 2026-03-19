"""Read-only account-scoped cPAR portfolio hedge payload service."""

from __future__ import annotations

from backend.services import cpar_portfolio_snapshot_service

CparPortfolioAccountNotFound = cpar_portfolio_snapshot_service.CparPortfolioAccountNotFound


def load_cpar_portfolio_hedge_payload(
    *,
    account_id: str,
    mode: str,
    data_db=None,
) -> dict[str, object]:
    return cpar_portfolio_snapshot_service.load_cpar_portfolio_hedge_payload(
        account_id=account_id,
        mode=mode,
        data_db=data_db,
    )
