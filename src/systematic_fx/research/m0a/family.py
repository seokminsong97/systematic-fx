"""Finite deterministic strategy-family search for the M0a research skeleton.

The family layer deliberately knows nothing about PostgreSQL, workers, or
admission.  It turns one precommitted seed and budget into unique canonical
candidate configurations, and turns one point-in-time feature row into a
boolean signal.  That keeps candidate generation replayable without putting an
LLM or an adaptive optimiser in the control loop.
"""

from __future__ import annotations

import random
from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Final

from systematic_fx.research.hypotheses import canonical_sha256
from systematic_fx.research.m0a.model import BarrierSpec, Direction, EventFeature, M0aDataError

PULLBACK_CONTINUATION_FAMILY_ID: Final = "pullback_continuation_v1"


class StrategyFamilyError(M0aDataError):
    """A family request is unbounded, unsupported, or internally inconsistent."""


@dataclass(frozen=True, slots=True)
class PullbackContinuationSearchSpace:
    """Finite, ordered axes committed by an epoch before candidate generation."""

    trend_1h_min_ticks: tuple[int, ...]
    pullback_length_min: tuple[int, ...]
    pullback_length_max: tuple[int, ...]
    close_location_threshold_ppm: tuple[int, ...]
    volatility_quantile_min_ppm: tuple[int, ...]
    volatility_quantile_max_ppm: tuple[int, ...]
    imbalance_threshold_ppm: tuple[int | None, ...]
    directions: tuple[Direction, ...]
    feature_tier: str
    max_generation_attempts_per_candidate: int
    min_generation_attempts: int

    def __post_init__(self) -> None:
        axes: tuple[tuple[object, ...], ...] = (
            self.trend_1h_min_ticks,
            self.pullback_length_min,
            self.pullback_length_max,
            self.close_location_threshold_ppm,
            self.volatility_quantile_min_ppm,
            self.volatility_quantile_max_ppm,
            self.imbalance_threshold_ppm,
            self.directions,
        )
        if any(not axis or len(set(axis)) != len(axis) for axis in axes):
            raise StrategyFamilyError("family search axes must be non-empty and duplicate-free")
        if any(value <= 0 for value in self.trend_1h_min_ticks):
            raise StrategyFamilyError("trend thresholds must be positive")
        if any(value <= 0 for value in (*self.pullback_length_min, *self.pullback_length_max)):
            raise StrategyFamilyError("pullback axes must be positive")
        if any(
            not any(maximum >= minimum for maximum in self.pullback_length_max)
            for minimum in self.pullback_length_min
        ):
            raise StrategyFamilyError("a pullback minimum has no valid maximum")
        if any(not 500_000 <= value <= 900_000 for value in self.close_location_threshold_ppm):
            raise StrategyFamilyError("close-location search axis is outside M0a bounds")
        if any(not 0 <= value < 1_000_000 for value in self.volatility_quantile_min_ppm):
            raise StrategyFamilyError("minimum volatility-quantile axis is invalid")
        if any(not 0 < value <= 1_000_000 for value in self.volatility_quantile_max_ppm):
            raise StrategyFamilyError("maximum volatility-quantile axis is invalid")
        if any(
            not any(maximum > minimum for maximum in self.volatility_quantile_max_ppm)
            for minimum in self.volatility_quantile_min_ppm
        ):
            raise StrategyFamilyError("a volatility minimum has no valid maximum")
        if any(
            value is not None and not 0 <= value <= 500_000
            for value in self.imbalance_threshold_ppm
        ):
            raise StrategyFamilyError("imbalance threshold axis is invalid")
        if set(self.directions) != {Direction.LONG, Direction.SHORT}:
            raise StrategyFamilyError("M0a family search must precommit long and short directions")
        if not self.feature_tier:
            raise StrategyFamilyError("feature_tier must not be empty")
        if self.max_generation_attempts_per_candidate <= 0 or self.min_generation_attempts <= 0:
            raise StrategyFamilyError("candidate generation attempt bounds must be positive")

    def as_dict(self) -> dict[str, object]:
        return {
            "trend_1h_min_ticks": list(self.trend_1h_min_ticks),
            "pullback_length_min": list(self.pullback_length_min),
            "pullback_length_max": list(self.pullback_length_max),
            "close_location_threshold_ppm": list(self.close_location_threshold_ppm),
            "volatility_quantile_min_ppm": list(self.volatility_quantile_min_ppm),
            "volatility_quantile_max_ppm": list(self.volatility_quantile_max_ppm),
            "imbalance_threshold_ppm": list(self.imbalance_threshold_ppm),
            "directions": [direction.value for direction in self.directions],
            "feature_tier": self.feature_tier,
            "max_generation_attempts_per_candidate": self.max_generation_attempts_per_candidate,
            "min_generation_attempts": self.min_generation_attempts,
        }


DEFAULT_PULLBACK_CONTINUATION_SEARCH_SPACE: Final = PullbackContinuationSearchSpace(
    trend_1h_min_ticks=(1, 2, 3, 4, 6),
    pullback_length_min=(1, 2, 3),
    pullback_length_max=(3, 4, 6, 8),
    close_location_threshold_ppm=(550_000, 650_000, 750_000, 850_000),
    volatility_quantile_min_ppm=(0, 100_000, 200_000, 300_000),
    volatility_quantile_max_ppm=(700_000, 800_000, 900_000, 1_000_000),
    imbalance_threshold_ppm=(None, 0, 50_000, 150_000, 250_000),
    directions=(Direction.LONG, Direction.SHORT),
    feature_tier="M0A_MINIMAL",
    max_generation_attempts_per_candidate=200,
    min_generation_attempts=1_000,
)


@dataclass(frozen=True, slots=True)
class PullbackContinuationParameters:
    """Integer-only thresholds for one pullback-continuation rule.

    Quantile fields are parts per million.  For a short candidate,
    ``close_location_threshold_ppm`` is mirrored around one million and the
    imbalance threshold is sign-reversed.
    """

    trend_1h_min_ticks: int
    pullback_length_min: int
    pullback_length_max: int
    close_location_threshold_ppm: int
    volatility_quantile_min_ppm: int
    volatility_quantile_max_ppm: int
    imbalance_threshold_ppm: int | None

    def __post_init__(self) -> None:
        if self.trend_1h_min_ticks <= 0:
            raise StrategyFamilyError("trend_1h_min_ticks must be positive")
        if not 1 <= self.pullback_length_min <= self.pullback_length_max:
            raise StrategyFamilyError("pullback length interval is invalid")
        if not 500_000 <= self.close_location_threshold_ppm <= 900_000:
            raise StrategyFamilyError("close-location threshold must be in [500000, 900000]")
        if not (
            0 <= self.volatility_quantile_min_ppm < self.volatility_quantile_max_ppm <= 1_000_000
        ):
            raise StrategyFamilyError("volatility quantile interval is invalid")
        if self.imbalance_threshold_ppm is not None and not (
            0 <= self.imbalance_threshold_ppm <= 500_000
        ):
            raise StrategyFamilyError("imbalance threshold must be null or in [0, 500000]")

    def as_dict(self) -> dict[str, int | None]:
        return {
            "close_location_threshold_ppm": self.close_location_threshold_ppm,
            "imbalance_threshold_ppm": self.imbalance_threshold_ppm,
            "pullback_length_max": self.pullback_length_max,
            "pullback_length_min": self.pullback_length_min,
            "trend_1h_min_ticks": self.trend_1h_min_ticks,
            "volatility_quantile_max_ppm": self.volatility_quantile_max_ppm,
            "volatility_quantile_min_ppm": self.volatility_quantile_min_ppm,
        }


@dataclass(frozen=True, slots=True)
class StrategyCandidate:
    """One immutable rule and barrier selection.

    Generation seed and draw index are provenance, not configuration identity.
    Consequently, discovering the same rule from two seeds produces the same
    ``candidate_hash`` and the ledger can reject the duplicate.
    """

    family_id: str
    direction: Direction
    barrier: BarrierSpec
    parameters: PullbackContinuationParameters
    generation_seed: int
    generation_index: int
    feature_tier: str = "M0A_MINIMAL"

    def __post_init__(self) -> None:
        if self.family_id != PULLBACK_CONTINUATION_FAMILY_ID:
            raise StrategyFamilyError("unsupported M0a strategy family")
        if isinstance(self.generation_seed, bool) or not 0 <= self.generation_seed < 2**64:
            raise StrategyFamilyError("generation_seed must be an unsigned 64-bit integer")
        if isinstance(self.generation_index, bool) or self.generation_index < 0:
            raise StrategyFamilyError("generation_index must be non-negative")
        if not self.feature_tier:
            raise StrategyFamilyError("feature_tier must not be empty")

    def identity_payload(self) -> dict[str, object]:
        """Return the canonical configuration used for duplicate prevention."""

        return {
            "barrier": self.barrier.as_dict(),
            "direction": self.direction.value,
            "family_id": self.family_id,
            "feature_tier": self.feature_tier,
            "parameters": self.parameters.as_dict(),
        }

    @property
    def candidate_hash(self) -> str:
        return canonical_sha256(self.identity_payload())

    def as_dict(self) -> dict[str, object]:
        return {
            "candidate_hash": self.candidate_hash,
            "generation_index": self.generation_index,
            "generation_seed": self.generation_seed,
            **self.identity_payload(),
        }


class StrategyFamily(ABC):
    """Extensible, deterministic family interface used by the epoch runner."""

    family_id: str

    @abstractmethod
    def generate(
        self,
        *,
        budget: int,
        seed: int,
        barriers: Sequence[BarrierSpec],
        search_space: PullbackContinuationSearchSpace,
    ) -> tuple[StrategyCandidate, ...]:
        """Generate no more and no fewer than the precommitted unique budget."""

    @abstractmethod
    def signal(self, candidate: StrategyCandidate, feature: EventFeature) -> bool:
        """Return a point-in-time boolean signal for one feature row."""


class PullbackContinuationFamily(StrategyFamily):
    """One-hour trend, five-minute pullback and reclaim/rollover rule."""

    family_id = PULLBACK_CONTINUATION_FAMILY_ID

    def generate(
        self,
        *,
        budget: int,
        seed: int,
        barriers: Sequence[BarrierSpec],
        search_space: PullbackContinuationSearchSpace,
    ) -> tuple[StrategyCandidate, ...]:
        if isinstance(budget, bool) or not isinstance(budget, int) or budget <= 0:
            raise StrategyFamilyError("candidate budget must be a positive integer")
        if isinstance(seed, bool) or not isinstance(seed, int) or not 0 <= seed < 2**64:
            raise StrategyFamilyError("seed must be an unsigned 64-bit integer")
        barrier_values = tuple(barriers)
        if not barrier_values or len({item.barrier_id for item in barrier_values}) != len(
            barrier_values
        ):
            raise StrategyFamilyError("barriers must be a non-empty sequence with unique ids")
        if any(not isinstance(item, BarrierSpec) for item in barrier_values):
            raise StrategyFamilyError("barriers must contain only BarrierSpec values")
        if not isinstance(search_space, PullbackContinuationSearchSpace):
            raise StrategyFamilyError("search_space must be prevalidated epoch family axes")

        # Draw directly from each frozen axis.  We intentionally do not build or
        # traverse the Cartesian product: the epoch pays only for its real draws.
        generator = random.Random(seed)
        candidates: list[StrategyCandidate] = []
        seen_hashes: set[str] = set()
        attempts = 0
        maximum_attempts = max(
            search_space.min_generation_attempts,
            budget * search_space.max_generation_attempts_per_candidate,
        )
        while len(candidates) < budget and attempts < maximum_attempts:
            attempts += 1
            minimum = generator.choice(search_space.pullback_length_min)
            maximum_choices = tuple(
                value for value in search_space.pullback_length_max if value >= minimum
            )
            trend_minimum = generator.choice(search_space.trend_1h_min_ticks)
            pullback_maximum = generator.choice(maximum_choices)
            close_location = generator.choice(search_space.close_location_threshold_ppm)
            volatility_minimum = generator.choice(search_space.volatility_quantile_min_ppm)
            volatility_maximum_choices = tuple(
                value
                for value in search_space.volatility_quantile_max_ppm
                if value > volatility_minimum
            )
            volatility_maximum = generator.choice(volatility_maximum_choices)
            imbalance_threshold = generator.choice(search_space.imbalance_threshold_ppm)
            parameters = PullbackContinuationParameters(
                trend_1h_min_ticks=trend_minimum,
                pullback_length_min=minimum,
                pullback_length_max=pullback_maximum,
                close_location_threshold_ppm=close_location,
                volatility_quantile_min_ppm=volatility_minimum,
                volatility_quantile_max_ppm=volatility_maximum,
                imbalance_threshold_ppm=imbalance_threshold,
            )
            candidate = StrategyCandidate(
                family_id=self.family_id,
                direction=generator.choice(search_space.directions),
                barrier=generator.choice(barrier_values),
                parameters=parameters,
                generation_seed=seed,
                generation_index=len(candidates),
                feature_tier=search_space.feature_tier,
            )
            if candidate.candidate_hash in seen_hashes:
                continue
            seen_hashes.add(candidate.candidate_hash)
            candidates.append(candidate)
        if len(candidates) != budget:
            raise StrategyFamilyError(
                "candidate budget exceeds the unique finite family space available to random draws"
            )
        return tuple(candidates)

    def signal(self, candidate: StrategyCandidate, feature: EventFeature) -> bool:
        if candidate.family_id != self.family_id:
            raise StrategyFamilyError("candidate belongs to a different family")
        if not isinstance(feature, EventFeature):
            raise StrategyFamilyError("feature must be an EventFeature")
        if (
            not feature.feature_valid
            or feature.roll_cross
            or feature.inside_roll_guard
            or feature.trend_1h_ticks is None
            or feature.context_1h_end_ns is None
            or feature.context_1h_end_ns > feature.event_ts_ns
            or feature.volatility_quantile_ppm is None
        ):
            return False

        rule = candidate.parameters
        if not rule.pullback_length_min <= feature.pullback_length <= rule.pullback_length_max:
            return False
        if not (
            rule.volatility_quantile_min_ppm
            <= feature.volatility_quantile_ppm
            <= rule.volatility_quantile_max_ppm
        ):
            return False

        sign = 1 if candidate.direction is Direction.LONG else -1
        if sign * feature.trend_1h_ticks < rule.trend_1h_min_ticks:
            return False
        close_location = (
            feature.close_location_ppm if sign == 1 else 1_000_000 - feature.close_location_ppm
        )
        if close_location < rule.close_location_threshold_ppm:
            return False
        return rule.imbalance_threshold_ppm is None or (
            sign * feature.depth_imbalance_ppm >= rule.imbalance_threshold_ppm
        )


FAMILIES: Final[Mapping[str, StrategyFamily]] = {
    PULLBACK_CONTINUATION_FAMILY_ID: PullbackContinuationFamily(),
}


def get_family(family_id: str) -> StrategyFamily:
    """Resolve one explicitly registered family; automatic family creation is forbidden."""

    try:
        return FAMILIES[family_id]
    except KeyError as error:
        raise StrategyFamilyError(f"unknown strategy family {family_id!r}") from error


def generate_candidates(
    *,
    budget: int,
    seed: int,
    barriers: Sequence[BarrierSpec],
    family_id: str = PULLBACK_CONTINUATION_FAMILY_ID,
    search_space: PullbackContinuationSearchSpace = DEFAULT_PULLBACK_CONTINUATION_SEARCH_SPACE,
) -> tuple[StrategyCandidate, ...]:
    """Convenience API used by the epoch pipeline."""

    return get_family(family_id).generate(
        budget=budget,
        seed=seed,
        barriers=barriers,
        search_space=search_space,
    )


def candidate_signal(candidate: StrategyCandidate, feature: EventFeature) -> bool:
    """Evaluate the candidate through its registered family implementation."""

    return get_family(candidate.family_id).signal(candidate, feature)
