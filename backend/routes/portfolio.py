"""GET /api/portfolio — positions, total_value, position_count."""

from fastapi import APIRouter

from analytics.trbc_economic_sector_short import abbreviate_trbc_economic_sector_short
from db.sqlite import cache_get
from routes.readiness import raise_cache_not_ready

router = APIRouter()


@router.get("/portfolio")
async def get_portfolio():
    data = cache_get("portfolio")
    if data is None:
        raise_cache_not_ready(
            cache_key="portfolio",
            message="Portfolio cache is empty. Run refresh to build positions.",
            refresh_mode="light",
        )
    positions = []
    for raw in data.get("positions", []):
        trbc_economic_sector_short = str(
            raw.get("trbc_economic_sector_short")
            or raw.get("trbc_sector")
            or raw.get("sector")
            or ""
        )
        positions.append(
            {
                **raw,
                "trbc_economic_sector_short": trbc_economic_sector_short,
                "trbc_economic_sector_short_abbr": str(
                    raw.get("trbc_economic_sector_short_abbr")
                    or raw.get("trbc_sector_abbr")
                    or abbreviate_trbc_economic_sector_short(trbc_economic_sector_short)
                ),
            }
        )
    return {**data, "positions": positions, "_cached": True}
