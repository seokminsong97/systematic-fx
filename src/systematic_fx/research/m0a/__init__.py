"""M0a deterministic research-data walking skeleton."""

from systematic_fx.research.m0a.config import EpochConfig, compute_code_snapshot_sha256, load_epoch
from systematic_fx.research.m0a.family import PullbackContinuationSearchSpace
from systematic_fx.research.m0a.features import build_features
from systematic_fx.research.m0a.fixture import build_fixture
from systematic_fx.research.m0a.labels import build_labels
from systematic_fx.research.m0a.model import (
    BarrierSpec,
    Direction,
    EventFeature,
    FeatureRow,
    FirstTouchType,
    InstrumentMetadata,
    LabelRow,
    M0aConfigError,
    M0aDataError,
    M0aError,
    MarketFixture,
    PreviousDayVolume,
    QuoteAwareLabel,
    QuoteEvent,
    RollGuard,
    SessionWindow,
)

__all__ = [
    "BarrierSpec",
    "Direction",
    "EpochConfig",
    "EventFeature",
    "FeatureRow",
    "FirstTouchType",
    "InstrumentMetadata",
    "LabelRow",
    "M0aConfigError",
    "M0aDataError",
    "M0aError",
    "MarketFixture",
    "PreviousDayVolume",
    "PullbackContinuationSearchSpace",
    "QuoteAwareLabel",
    "QuoteEvent",
    "RollGuard",
    "SessionWindow",
    "build_features",
    "build_fixture",
    "build_labels",
    "compute_code_snapshot_sha256",
    "load_epoch",
]
