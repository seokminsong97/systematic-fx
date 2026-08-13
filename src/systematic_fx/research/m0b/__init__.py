"""Fail-closed staged real-data adapter for a bounded CME 6E M0b slice."""

from systematic_fx.research.m0b.adapter import build_real_slice, verify_real_slice
from systematic_fx.research.m0b.config import RealSliceConfig, load_real_slice_config
from systematic_fx.research.m0b.materialize import (
    load_materialized_real_slice,
    materialize_real_slice,
)
from systematic_fx.research.m0b.model import RealSliceBuild, RealSliceError

__all__ = [
    "RealSliceBuild",
    "RealSliceConfig",
    "RealSliceError",
    "build_real_slice",
    "load_materialized_real_slice",
    "load_real_slice_config",
    "materialize_real_slice",
    "verify_real_slice",
]
