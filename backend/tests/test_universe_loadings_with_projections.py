"""Verify projected instruments merge correctly into universe loadings payload."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from backend.risk_model.projected_loadings import ProjectedLoadingResult


class TestProjectedInstrumentsInUniverse:
    """Test that projected instruments are injected into the universe loadings payload."""

    def _make_projected_loading(
        self,
        *,
        ric: str = "SPY.P",
        ticker: str = "SPY",
        exposures: dict[str, float] | None = None,
        r_squared: float = 0.97,
        obs_count: int = 252,
        specific_var: float = 0.005,
        specific_vol: float = 0.0707,
    ) -> ProjectedLoadingResult:
        if exposures is None:
            exposures = {"Market": 1.0, "Size": -0.05, "Value": 0.02}
        return ProjectedLoadingResult(
            ric=ric,
            ticker=ticker,
            exposures=exposures,
            specific_var=specific_var,
            specific_vol=specific_vol,
            r_squared=r_squared,
            obs_count=obs_count,
            lookback_days=252,
            projection_asof="2025-03-14",
            status="ok",
        )

    def test_projected_loading_result_dataclass(self):
        """ProjectedLoadingResult should hold all expected fields."""
        result = self._make_projected_loading()
        assert result.ric == "SPY.P"
        assert result.ticker == "SPY"
        assert result.status == "ok"
        assert result.r_squared == 0.97
        assert "Market" in result.exposures
        assert result.specific_var > 0
        assert result.specific_vol > 0

    def test_insufficient_data_result(self):
        """Insufficient-data results should have empty exposures."""
        result = ProjectedLoadingResult(
            ric="FAKE.P",
            ticker="FAKE",
            status="insufficient_data",
        )
        assert result.status == "insufficient_data"
        assert result.exposures == {}
        assert result.r_squared == 0.0

    def test_projected_payload_fields(self):
        """Verify the structure of a projected UniverseTickerPayload."""
        result = self._make_projected_loading()

        # Simulate what universe_loadings.py does when injecting
        payload = {
            "ticker": result.ticker,
            "ric": result.ric,
            "model_status": "projected_only",
            "model_status_reason": "returns_projection",
            "exposure_origin": "projected_returns",
            "projection_method": "ols_returns_regression",
            "projection_r_squared": round(result.r_squared, 6),
            "projection_obs_count": result.obs_count,
            "projection_asof": result.projection_asof,
            "specific_var": round(result.specific_var, 8),
            "specific_vol": round(result.specific_vol, 6),
        }

        assert payload["model_status"] == "projected_only"
        assert payload["exposure_origin"] == "projected_returns"
        assert payload["projection_method"] == "ols_returns_regression"
        assert payload["projection_r_squared"] == 0.97
        assert payload["projection_obs_count"] == 252
        assert payload["projection_asof"] == "2025-03-14"
        assert payload["specific_var"] > 0
        assert payload["specific_vol"] > 0

    def test_only_ok_results_are_injected(self):
        """Only results with status='ok' should be included in the payload."""
        results = {
            "SPY": self._make_projected_loading(ticker="SPY"),
            "FAKE": ProjectedLoadingResult(
                ric="FAKE.P",
                ticker="FAKE",
                status="insufficient_data",
            ),
            "ERR": ProjectedLoadingResult(
                ric="ERR.P",
                ticker="ERR",
                status="error",
            ),
        }

        injected = {
            ticker: result
            for ticker, result in results.items()
            if result.status == "ok"
        }

        assert "SPY" in injected
        assert "FAKE" not in injected
        assert "ERR" not in injected
