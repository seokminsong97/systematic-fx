"""Deterministic null/control selection primitives for M0a.

These functions select control event timestamps only.  Economic evaluation is
performed by :mod:`systematic_fx.research.m0a.evaluate` through exactly the same
label lookup and sequential simulator used for the real candidate.
"""

from __future__ import annotations

import random
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType

from systematic_fx.research.hypotheses import canonical_sha256
from systematic_fx.research.m0a.family import StrategyCandidate
from systematic_fx.research.m0a.model import EventFeature, M0aDataError


class ControlError(M0aDataError):
    """A null/control request cannot be reproduced safely."""


@dataclass(frozen=True, slots=True)
class NullControlCandidate:
    """One explicit null experiment owned by a real parent candidate."""

    control_id: str
    method: str
    parent_candidate_hash: str
    direction: str
    barrier: Mapping[str, object]
    random_seed: int
    generation_index: int
    parameters: Mapping[str, object]
    family_id: str
    null_family_id: str = "m0a_null_control_v1"
    feature_tier: str = "M0A_MINIMAL_NULL"

    def __post_init__(self) -> None:
        if self.control_id not in {"circular_block_shift_v1", "matched_random_entry_v1"}:
            raise ControlError("unknown null control id")
        if not self.family_id or self.null_family_id != "m0a_null_control_v1":
            raise ControlError("null candidate family identity is invalid")
        object.__setattr__(self, "barrier", MappingProxyType(dict(self.barrier)))
        object.__setattr__(self, "parameters", MappingProxyType(dict(self.parameters)))
        expected_method = {
            "circular_block_shift_v1": "CIRCULAR_BLOCK_TIME_SHIFT",
            "matched_random_entry_v1": "MATCHED_RANDOM_ENTRY",
        }[self.control_id]
        if self.method != expected_method:
            raise ControlError("null control id and method disagree")
        if len(self.parent_candidate_hash) != 64:
            raise ControlError("parent_candidate_hash must be a SHA-256 digest")
        if self.direction not in {"long", "short"}:
            raise ControlError("null direction must be long or short")
        if not 0 <= self.random_seed < 2**64 or self.generation_index < 0:
            raise ControlError("null seed/index is invalid")

    def identity_payload(self) -> dict[str, object]:
        return {
            "barrier": dict(self.barrier),
            "control_id": self.control_id,
            "direction": self.direction,
            "family_id": self.family_id,
            "feature_tier": self.feature_tier,
            "method": self.method,
            "null_family_id": self.null_family_id,
            "parameters": dict(self.parameters),
            "parent_candidate_hash": self.parent_candidate_hash,
            "random_seed": self.random_seed,
        }

    @property
    def candidate_hash(self) -> str:
        return canonical_sha256(self.identity_payload())

    def as_dict(self) -> dict[str, object]:
        return {
            "candidate_hash": self.candidate_hash,
            "generation_index": self.generation_index,
            **self.identity_payload(),
        }


def generate_null_candidates(
    real_candidates: Sequence[StrategyCandidate],
    *,
    seed: int,
    circular_block_size: int = 4,
) -> tuple[NullControlCandidate, ...]:
    """Precommit exactly two explicit null experiments per real candidate.

    Output order is stable: real candidate generation order, then circular
    shift followed by matched random entry.  No observed performance influences
    generation or allocation.
    """

    candidates = tuple(real_candidates)
    if len({item.candidate_hash for item in candidates}) != len(candidates):
        raise ControlError("real candidates contain duplicate identities")
    if circular_block_size <= 0:
        raise ControlError("circular_block_size must be positive")
    if not 0 <= seed < 2**64:
        raise ControlError("seed must be an unsigned 64-bit integer")
    result: list[NullControlCandidate] = []
    for parent in candidates:
        parent_seed = seed ^ int(parent.candidate_hash[:16], 16)
        shared = {
            "parent_candidate_hash": parent.candidate_hash,
            "direction": parent.direction.value,
            "barrier": parent.barrier.as_dict(),
            "family_id": parent.family_id,
        }
        result.append(
            NullControlCandidate(
                control_id="circular_block_shift_v1",
                method="CIRCULAR_BLOCK_TIME_SHIFT",
                random_seed=parent_seed,
                generation_index=len(result),
                parameters={"block_size": circular_block_size},
                **shared,
            )
        )
        result.append(
            NullControlCandidate(
                control_id="matched_random_entry_v1",
                method="MATCHED_RANDOM_ENTRY",
                random_seed=parent_seed ^ 0x9E3779B97F4A7C15,
                generation_index=len(result),
                parameters={
                    "holding_horizon_seconds": parent.barrier.max_hold_seconds,
                    "matching_axes": ("month", "session", "volatility_regime"),
                },
                **shared,
            )
        )
    return tuple(result)


@dataclass(frozen=True, slots=True)
class CircularShiftSelection:
    block_size: int
    shift_blocks: int
    original_signal_count: int
    shifted_signal_count: int
    shifted_mask: tuple[bool, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "block_size": self.block_size,
            "original_signal_count": self.original_signal_count,
            "shift_blocks": self.shift_blocks,
            "shifted_signal_count": self.shifted_signal_count,
        }


@dataclass(frozen=True, slots=True)
class MatchedRandomSelection:
    """Mapping from each real signal index to one non-signal control index."""

    pairs: tuple[tuple[int, int], ...]
    exact_match_count: int
    same_month_match_count: int
    relaxed_match_count: int
    unmatched_count: int
    seed: int

    @property
    def selected_indices(self) -> tuple[int, ...]:
        return tuple(control for _, control in self.pairs)

    def as_dict(self) -> dict[str, object]:
        return {
            "exact_match_count": self.exact_match_count,
            "matched_count": len(self.pairs),
            "relaxed_match_count": self.relaxed_match_count,
            "same_month_match_count": self.same_month_match_count,
            "seed": self.seed,
            "unmatched_count": self.unmatched_count,
        }


def circular_block_shift(
    signal_mask: Sequence[bool],
    *,
    block_size: int,
    seed: int,
) -> CircularShiftSelection:
    """Rotate complete chronological mask blocks by a seeded non-zero offset.

    The boolean sequence, including its within-block signal clustering, is left
    intact.  Only its alignment to future labels changes.  The final short block
    is rotated as a unit like every other block.
    """

    mask = tuple(signal_mask)
    if len(mask) < 2:
        raise ControlError("circular shift requires at least two chronological events")
    if any(not isinstance(item, bool) for item in mask):
        raise ControlError("signal_mask must contain only booleans")
    if isinstance(block_size, bool) or not isinstance(block_size, int) or block_size <= 0:
        raise ControlError("block_size must be a positive integer")
    if isinstance(seed, bool) or not isinstance(seed, int) or not 0 <= seed < 2**64:
        raise ControlError("seed must be an unsigned 64-bit integer")
    blocks = tuple(mask[index : index + block_size] for index in range(0, len(mask), block_size))
    if len(blocks) < 2:
        # A one-event block is the smallest deterministic fallback that can
        # actually break alignment on a very small walking-skeleton fixture.
        block_size = 1
        blocks = tuple((item,) for item in mask)
    generator = random.Random(seed)
    shift = generator.randrange(1, len(blocks))
    rotated = blocks[-shift:] + blocks[:-shift]
    shifted = tuple(item for block in rotated for item in block)
    if len(shifted) != len(mask) or sum(shifted) != sum(mask):
        raise AssertionError("circular block rotation changed mask cardinality")
    return CircularShiftSelection(
        block_size=block_size,
        shift_blocks=shift,
        original_signal_count=sum(mask),
        shifted_signal_count=sum(shifted),
        shifted_mask=shifted,
    )


def _month(feature: EventFeature) -> str:
    return feature.trading_date.isoformat()[:7]


def _volatility_regime(feature: EventFeature) -> int:
    quantile = feature.volatility_quantile_ppm
    if quantile is None:
        return -1
    return min(4, max(0, quantile * 5 // 1_000_001))


def _matches_level(
    row: EventFeature,
    *,
    level: int,
    month: str,
    session: str,
    regime: int,
) -> bool:
    row_month = _month(row)
    row_regime = _volatility_regime(row)
    if level == 0:
        return row_month == month and row.session_id == session and row_regime == regime
    if level == 1:
        return row_month == month and row_regime == regime
    if level == 2:
        return row_month == month and row.session_id == session
    if level == 3:
        return row_month == month
    if level == 4:
        return row.session_id == session and row_regime == regime
    if level == 5:
        return row_regime == regime
    if level == 6:
        return True
    raise AssertionError("unknown matched-control relaxation level")


def matched_random_entries(
    features: Sequence[EventFeature],
    *,
    signal_indices: Sequence[int],
    eligible_indices: Sequence[int],
    seed: int,
) -> MatchedRandomSelection:
    """Select non-signal entries matched on month, session and volatility regime.

    The caller supplies an eligible pool already restricted to the real
    candidate's direction, barrier and holding horizon; that makes direction
    ratio and horizon exact.  Selection is without replacement.  Relaxation is
    deterministic and explicitly counted instead of silently claiming an exact
    match when a tiny fixture lacks one.
    """

    rows = tuple(features)
    if not rows:
        raise ControlError("matched random control requires feature rows")
    if isinstance(seed, bool) or not isinstance(seed, int) or not 0 <= seed < 2**64:
        raise ControlError("seed must be an unsigned 64-bit integer")
    signals = tuple(signal_indices)
    eligible = tuple(eligible_indices)
    if len(set(signals)) != len(signals) or len(set(eligible)) != len(eligible):
        raise ControlError("control indices must not contain duplicates")
    if any(index < 0 or index >= len(rows) for index in (*signals, *eligible)):
        raise ControlError("control index is outside the feature sequence")

    signal_set = set(signals)
    available = set(eligible) - signal_set
    generator = random.Random(seed)
    pairs: list[tuple[int, int]] = []
    exact = 0
    same_month = 0
    relaxed = 0

    ordered_signals = sorted(signals, key=lambda index: (rows[index].event_ts_ns, index))
    for signal_index in ordered_signals:
        target = rows[signal_index]
        target_month = _month(target)
        target_session = target.session_id
        target_regime = _volatility_regime(target)

        # The level order protects monthly frequency before relaxing to other
        # sessions/months.  Only the first non-empty level is sampled.
        selected: int | None = None
        selected_level = -1
        for level in range(7):
            choices = sorted(
                index
                for index in available
                if _matches_level(
                    rows[index],
                    level=level,
                    month=target_month,
                    session=target_session,
                    regime=target_regime,
                )
            )
            if choices:
                selected = choices[generator.randrange(len(choices))]
                selected_level = level
                break
        if selected is None:
            continue
        available.remove(selected)
        pairs.append((signal_index, selected))
        exact += int(selected_level == 0)
        same_month += int(_month(rows[selected]) == target_month)
        relaxed += int(selected_level > 0)

    return MatchedRandomSelection(
        pairs=tuple(pairs),
        exact_match_count=exact,
        same_month_match_count=same_month,
        relaxed_match_count=relaxed,
        unmatched_count=len(signals) - len(pairs),
        seed=seed,
    )
