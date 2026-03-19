"""Pure cPAR1 math-kernel package.

This package intentionally owns only model/domain logic. It does not own
routes, services, orchestration entrypoints, or persistence adapters.
"""

from backend.cpar.factor_registry import (
    CPAR1_FACTOR_REGISTRY_VERSION,
    CPAR1_METHOD_VERSION,
    MARKET_FACTOR_ID,
    build_cpar1_factor_registry,
    factor_group_for_id,
    factor_spec_by_id,
)

__all__ = [
    "CPAR1_FACTOR_REGISTRY_VERSION",
    "CPAR1_METHOD_VERSION",
    "MARKET_FACTOR_ID",
    "build_cpar1_factor_registry",
    "factor_group_for_id",
    "factor_spec_by_id",
]
