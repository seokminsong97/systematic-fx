"""Pure deterministic evaluation primitives for frozen AI bar patterns.

This module intentionally has no filesystem, database, network, CLI, or clock
access.  Callers must supply already verified five-minute and one-second
``TradeBar`` values together with their immutable manifest outcome-span ids.

The execution convention is the preregistered ``MODERATE_COMBINED`` diagnostic:
the next contiguous five-minute bar supplies the entry reference, its first
observed one-second bar proves the entry, TP/SL are resolved from one-second
trade OHLC, simultaneous hits stop first, and all results use integer ticks and
exact :class:`fractions.Fraction` values.
"""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import date
from fractions import Fraction
from itertools import pairwise
from math import comb
from typing import Final, Literal, Protocol

from systematic_fx.features.bars import ONE_SECOND_NS, TradeBar
from systematic_fx.research.ai_pattern_discovery import AndRule
from systematic_fx.research.ai_pattern_discovery_v2 import DirectionalProposalBatch
from systematic_fx.research.hypotheses import canonical_sha256

FIVE_MINUTE_SECONDS: Final = 300
HALF_HOUR_NS: Final = 1_800 * ONE_SECOND_NS
RATIO_SCALE: Final = 1_000_000
DEFAULT_NULL_SEED: Final = "ai-pattern-holdout-v1"
MISSING_PRIOR_20_HISTORY: Final = "MISSING_PRIOR_20_HISTORY"
MATCHED_RELAXATION_LEVELS: Final = (
    "SAME_DATE_CONTRACT_OUTCOME_SPAN_SIGNAL_SEGMENT_30M_UTC_BUCKET_CAUSAL_STRATUM",
    (
        "SAME_DATE_CONTRACT_OUTCOME_SPAN_SIGNAL_SEGMENT_"
        "ADJACENT_PLUS_MINUS_1_30M_UTC_BUCKET_RETAIN_CAUSAL_STRATUM"
    ),
    "SAME_DATE_CONTRACT_OUTCOME_SPAN_SIGNAL_SEGMENT_DROP_BUCKET_AND_CAUSAL_STRATUM",
)

MaskKind = Literal["REAL", "CIRCULAR_SHIFT", "MATCHED_RANDOM"]
Direction = Literal["LONG", "SHORT"]
TradeDisposition = Literal["TP_FIRST", "STOP_FIRST", "TIMEOUT"]


class HoldoutEvaluationError(ValueError):
    """A supplied bar view, frozen pattern, mask, or result is invalid."""


def _integer(value: object, *, label: str, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise HoldoutEvaluationError(f"{label} must be an integer")
    if minimum is not None and value < minimum:
        raise HoldoutEvaluationError(f"{label} must be >= {minimum}")
    return value


def _nonempty(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise HoldoutEvaluationError(f"{label} must be a non-empty string")
    return value


def _sha256(value: object, *, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise HoldoutEvaluationError(f"{label} must be a lowercase SHA-256")
    return value


def _fraction_payload(value: Fraction | None) -> dict[str, int] | None:
    if value is None:
        return None
    return {"denominator": value.denominator, "numerator": value.numerator}


@dataclass(frozen=True, slots=True)
class BarWithOutcomeSpan:
    """One verified trade bar plus the manifest-assigned outcome span."""

    bar: TradeBar
    outcome_span_id: int

    def __post_init__(self) -> None:
        if not isinstance(self.bar, TradeBar):
            raise HoldoutEvaluationError("bar must be a TradeBar")
        _integer(self.outcome_span_id, label="outcome_span_id", minimum=1)

    @property
    def identity(self) -> tuple[int, str, int, int]:
        return self.outcome_span_id, self.bar.contract, self.bar.segment_id, self.bar.start_ns


@dataclass(frozen=True, slots=True)
class ExecutionSpec:
    """Frozen one-hour, 32/24-tick ``MODERATE_COMBINED`` execution contract."""

    take_profit_ticks: int = 32
    stop_loss_ticks: int = 24
    horizon_seconds: int = 3_600
    entry_adverse_ticks: int = 2
    take_profit_trade_through_ticks: int = 1
    stop_total_minimum_adverse_ticks: int = 4
    terminal_exit_adverse_ticks: int = 2
    variable_debit_ticks: int = 5
    allocated_fixed_cost_ticks: int = 5

    def __post_init__(self) -> None:
        expected = {
            "take_profit_ticks": 32,
            "stop_loss_ticks": 24,
            "horizon_seconds": 3_600,
            "entry_adverse_ticks": 2,
            "take_profit_trade_through_ticks": 1,
            "stop_total_minimum_adverse_ticks": 4,
            "terminal_exit_adverse_ticks": 2,
            "variable_debit_ticks": 5,
            "allocated_fixed_cost_ticks": 5,
        }
        for name, required in expected.items():
            if _integer(getattr(self, name), label=name, minimum=0) != required:
                raise HoldoutEvaluationError(
                    f"{name} differs from the frozen MODERATE_COMBINED contract"
                )

    @property
    def fully_loaded_cost_ticks(self) -> int:
        return self.variable_debit_ticks + self.allocated_fixed_cost_ticks

    def as_dict(self) -> dict[str, int | str]:
        return {
            "allocated_fixed_cost_ticks": self.allocated_fixed_cost_ticks,
            "entry_adverse_ticks": self.entry_adverse_ticks,
            "horizon_seconds": self.horizon_seconds,
            "scenario_id": "MODERATE_COMBINED",
            "stop_loss_ticks": self.stop_loss_ticks,
            "stop_total_minimum_adverse_ticks": self.stop_total_minimum_adverse_ticks,
            "take_profit_ticks": self.take_profit_ticks,
            "take_profit_trade_through_ticks": self.take_profit_trade_through_ticks,
            "terminal_exit_adverse_ticks": self.terminal_exit_adverse_ticks,
            "variable_debit_ticks": self.variable_debit_ticks,
        }


DEFAULT_EXECUTION_SPEC: Final = ExecutionSpec()


@dataclass(frozen=True, slots=True)
class MorphologyFeatures:
    range_ticks: int
    signed_body_ppm: int
    absolute_body_ppm: int
    close_location_ppm: int
    upper_wick_ppm: int
    lower_wick_ppm: int

    @property
    def rule_values(self) -> tuple[int, ...]:
        return (
            self.range_ticks,
            self.signed_body_ppm,
            self.absolute_body_ppm,
            self.close_location_ppm,
            self.upper_wick_ppm,
            self.lower_wick_ppm,
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "absolute_body_ppm": self.absolute_body_ppm,
            "close_location_ppm": self.close_location_ppm,
            "lower_wick_ppm": self.lower_wick_ppm,
            "range_ticks": self.range_ticks,
            "signed_body_ppm": self.signed_body_ppm,
            "upper_wick_ppm": self.upper_wick_ppm,
        }


def _ratio_ppm(numerator: int, denominator: int) -> int:
    magnitude = abs(numerator) * RATIO_SCALE // denominator
    return -magnitude if numerator < 0 else magnitude


def morphology_features(bar: TradeBar) -> MorphologyFeatures:
    """Reproduce ``ai_discovery_context`` completed-5m morphology exactly."""

    if not isinstance(bar, TradeBar) or bar.timeframe_seconds != FIVE_MINUTE_SECONDS:
        raise HoldoutEvaluationError("morphology requires one five-minute TradeBar")
    spread = bar.high_ticks - bar.low_ticks
    if spread == 0:
        signed_body = 0
        close_location = RATIO_SCALE // 2
        upper_wick = 0
        lower_wick = 0
    else:
        signed_body = _ratio_ppm(bar.close_ticks - bar.open_ticks, spread)
        close_location = _ratio_ppm(bar.close_ticks - bar.low_ticks, spread)
        upper_wick = _ratio_ppm(bar.high_ticks - max(bar.open_ticks, bar.close_ticks), spread)
        lower_wick = _ratio_ppm(min(bar.open_ticks, bar.close_ticks) - bar.low_ticks, spread)
    return MorphologyFeatures(
        range_ticks=spread,
        signed_body_ppm=signed_body,
        absolute_body_ppm=abs(signed_body),
        close_location_ppm=close_location,
        upper_wick_ppm=upper_wick,
        lower_wick_ppm=lower_wick,
    )


@dataclass(frozen=True, slots=True)
class FrozenProposal:
    selection_rank: int
    proposal_sha256: str
    direction: Direction
    rule: AndRule

    def __post_init__(self) -> None:
        _integer(self.selection_rank, label="selection_rank", minimum=1)
        _sha256(self.proposal_sha256, label="proposal_sha256")
        if self.direction not in ("LONG", "SHORT"):
            raise HoldoutEvaluationError("proposal direction must be LONG or SHORT")
        if not isinstance(self.rule, AndRule):
            raise HoldoutEvaluationError("proposal rule must be a canonical AndRule")


class _FrozenProposalLike(Protocol):
    selection_rank: int
    proposal_sha256: str
    direction: Direction
    rule: AndRule


def freeze_proposals(
    value: DirectionalProposalBatch | Sequence[_FrozenProposalLike],
) -> tuple[FrozenProposal, ...]:
    """Normalize an immutable Batch3 wrapper or its artifact-reconstructed rows."""

    raw: object
    if isinstance(value, DirectionalProposalBatch) or hasattr(value, "proposals"):
        raw = value.proposals
    else:
        raw = value
    if not isinstance(raw, Sequence) or not raw:
        raise HoldoutEvaluationError("frozen proposal collection must be non-empty")
    frozen: list[FrozenProposal] = []
    for item in raw:
        if hasattr(item, "pattern"):
            pattern = item.pattern
            frozen.append(
                FrozenProposal(
                    selection_rank=item.selection_rank,
                    proposal_sha256=item.sha256,
                    direction=pattern.direction,
                    rule=pattern.rule,
                )
            )
        else:
            frozen.append(
                FrozenProposal(
                    selection_rank=item.selection_rank,
                    proposal_sha256=item.proposal_sha256,
                    direction=item.direction,
                    rule=item.rule,
                )
            )
    ordered = tuple(sorted(frozen, key=lambda item: item.selection_rank))
    if len({item.selection_rank for item in ordered}) != len(ordered):
        raise HoldoutEvaluationError("frozen proposals contain duplicate selection ranks")
    if len({item.proposal_sha256 for item in ordered}) != len(ordered):
        raise HoldoutEvaluationError("frozen proposals contain duplicate SHA identities")
    return ordered


@dataclass(frozen=True, slots=True)
class SignalMask:
    key: str
    proposal_sha256: str
    kind: MaskKind
    values: tuple[bool, ...]
    null_seed_sha256s: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _nonempty(self.key, label="mask key")
        _sha256(self.proposal_sha256, label="proposal_sha256")
        if self.kind not in ("REAL", "CIRCULAR_SHIFT", "MATCHED_RANDOM"):
            raise HoldoutEvaluationError("unknown signal-mask kind")
        if not isinstance(self.values, tuple) or not self.values:
            raise HoldoutEvaluationError("signal mask must be a non-empty tuple")
        if any(not isinstance(item, bool) for item in self.values):
            raise HoldoutEvaluationError("signal mask values must be booleans")
        if tuple(sorted(set(self.null_seed_sha256s))) != self.null_seed_sha256s:
            raise HoldoutEvaluationError("null seed SHA values must be sorted and unique")
        for value in self.null_seed_sha256s:
            _sha256(value, label="null_seed_sha256")
        if self.kind == "REAL" and self.null_seed_sha256s:
            raise HoldoutEvaluationError("REAL mask cannot carry null seed identities")

    @property
    def signal_count(self) -> int:
        return sum(self.values)

    def as_dict(self) -> dict[str, object]:
        selected_indexes = [index for index, selected in enumerate(self.values) if selected]
        return {
            "key": self.key,
            "kind": self.kind,
            "mask_values_sha256": canonical_sha256(list(self.values)),
            "proposal_sha256": self.proposal_sha256,
            "selected_index_sha256": canonical_sha256(selected_indexes),
            "selected_indexes": selected_indexes,
            "signal_count": self.signal_count,
            "null_seed_sha256s": list(self.null_seed_sha256s),
            "value_count": len(self.values),
        }


def _validated_bar_tuple(
    values: Sequence[BarWithOutcomeSpan], *, timeframe_seconds: int, label: str
) -> tuple[BarWithOutcomeSpan, ...]:
    if not isinstance(values, Sequence) or not values:
        raise HoldoutEvaluationError(f"{label} must be a non-empty sequence")
    bars = tuple(values)
    prior: tuple[int, str, int] | None = None
    seen: set[tuple[int, str, int]] = set()
    for wrapped in bars:
        if not isinstance(wrapped, BarWithOutcomeSpan):
            raise HoldoutEvaluationError(f"{label} must contain BarWithOutcomeSpan values")
        if wrapped.bar.timeframe_seconds != timeframe_seconds:
            raise HoldoutEvaluationError(f"{label} contains the wrong timeframe")
        identity = wrapped.bar.start_ns, wrapped.bar.contract, wrapped.outcome_span_id
        if identity in seen:
            raise HoldoutEvaluationError(f"{label} contains a duplicate bar identity")
        seen.add(identity)
        if prior is not None and identity <= prior:
            raise HoldoutEvaluationError(f"{label} must use deterministic chronological order")
        prior = identity
    return bars


def five_minute_eligible_positions(
    bars: Sequence[BarWithOutcomeSpan],
    *,
    decision_dates: Iterable[date],
    allowed_stage_tail_end_ns: int,
    spec: ExecutionSpec = DEFAULT_EXECUTION_SPEC,
) -> tuple[bool, ...]:
    """Return positions with a proven contiguous next 5m bucket and full tail."""

    values = _validated_bar_tuple(bars, timeframe_seconds=FIVE_MINUTE_SECONDS, label="5m bars")
    dates = frozenset(decision_dates)
    if not dates or any(not isinstance(item, date) for item in dates):
        raise HoldoutEvaluationError("decision_dates must be non-empty dates")
    tail_end = _integer(allowed_stage_tail_end_ns, label="allowed stage tail end", minimum=1)
    eligible = [False] * len(values)
    for index, signal in enumerate(values[:-1]):
        entry = values[index + 1]
        left = signal.bar
        right = entry.bar
        entry_second_start = right.first_trade_ns // ONE_SECOND_NS * ONE_SECOND_NS
        eligible[index] = (
            left.source_date in dates
            and left.end_ns == right.start_ns
            and left.contract == right.contract
            and left.segment_id == right.segment_id
            and signal.outcome_span_id == entry.outcome_span_id
            and entry_second_start < tail_end
        )
    return tuple(eligible)


def proposal_signal_mask(
    proposals: DirectionalProposalBatch | Sequence[_FrozenProposalLike],
    proposal_sha256: str,
    bars: Sequence[BarWithOutcomeSpan],
    eligible_positions: Sequence[bool],
) -> SignalMask:
    """Evaluate one frozen selected rule on completed five-minute morphology."""

    identity = _sha256(proposal_sha256, label="proposal_sha256")
    frozen = freeze_proposals(proposals)
    try:
        proposal = next(item for item in frozen if item.proposal_sha256 == identity)
    except StopIteration as error:
        raise HoldoutEvaluationError(
            "proposal SHA is absent from the frozen selected batch"
        ) from error
    values = _validated_bar_tuple(bars, timeframe_seconds=FIVE_MINUTE_SECONDS, label="5m bars")
    eligible = tuple(eligible_positions)
    if len(eligible) != len(values) or any(not isinstance(item, bool) for item in eligible):
        raise HoldoutEvaluationError("eligible positions differ from the five-minute bars")
    mask = tuple(
        allowed and proposal.rule.matches(morphology_features(item.bar).rule_values)
        for item, allowed in zip(values, eligible, strict=True)
    )
    return SignalMask(f"{identity}:real", identity, "REAL", mask)


@dataclass(frozen=True, slots=True)
class CircularNullResult:
    mask: SignalMask | None
    sample_eligible: bool
    reason: str | None

    def __post_init__(self) -> None:
        if self.sample_eligible != (self.mask is not None):
            raise HoldoutEvaluationError("circular-null eligibility differs from its mask")
        if self.sample_eligible == (self.reason is not None):
            raise HoldoutEvaluationError("circular-null reason differs from eligibility")

    def as_dict(self) -> dict[str, object]:
        return {
            "mask": None if self.mask is None else self.mask.as_dict(),
            "reason": self.reason,
            "sample_eligible": self.sample_eligible,
        }


def circular_shift_null_mask(
    real: SignalMask,
    bars: Sequence[BarWithOutcomeSpan],
    eligible_positions: Sequence[bool],
    *,
    master_seed: int | str = DEFAULT_NULL_SEED,
    stage_key: str = "UNSPECIFIED",
    fold_key_by_date: Mapping[date, str] | None = None,
    offset: int | None = None,
) -> CircularNullResult:
    """Rotate each causal path/date group, preserving exact signal counts."""

    if real.kind != "REAL":
        raise HoldoutEvaluationError("circular shift requires a REAL signal mask")
    values = _validated_bar_tuple(bars, timeframe_seconds=FIVE_MINUTE_SECONDS, label="5m bars")
    eligible = tuple(eligible_positions)
    if len(real.values) != len(values) or len(eligible) != len(values):
        raise HoldoutEvaluationError("mask lengths differ from five-minute bars")
    if isinstance(master_seed, bool) or not isinstance(master_seed, (int, str)):
        raise HoldoutEvaluationError("master seed must be an integer or string")
    _nonempty(stage_key, label="stage key")
    if offset is not None:
        _integer(offset, label="circular shift offset", minimum=1)
    fold_keys = {} if fold_key_by_date is None else dict(fold_key_by_date)
    output = [False] * len(values)
    positions_by_scope: dict[tuple[date, str, int, int], list[int]] = defaultdict(list)
    for index, (wrapped, allowed) in enumerate(zip(values, eligible, strict=True)):
        if allowed:
            positions_by_scope[
                (
                    wrapped.bar.source_date,
                    wrapped.bar.contract,
                    wrapped.outcome_span_id,
                    wrapped.bar.segment_id,
                )
            ].append(index)
        elif real.values[index]:
            raise HoldoutEvaluationError("real mask selects an ineligible position")
    seed_sha256s: list[str] = []
    for scope, positions in sorted(positions_by_scope.items()):
        source_date, _contract, _span, _segment = scope
        width = len(positions)
        selected_count = sum(real.values[index] for index in positions)
        if selected_count and (width < 2 or selected_count == width):
            return CircularNullResult(None, False, "CIRCULAR_GROUP_HAS_NO_NONIDENTICAL_ROTATION")
        fold_key = fold_keys.get(source_date, "NONE")
        seed_sha = _null_seed_sha256(
            master_seed=master_seed,
            proposal_sha256=real.proposal_sha256,
            stage_key=stage_key,
            fold_key=fold_key,
            source_date=source_date,
            null_kind="DATE_SPAN_CIRCULAR_SHIFT",
        )
        seed_sha256s.append(seed_sha)
        shift = (
            offset % width
            if offset is not None
            else 0
            if width == 1
            else 1 + int(seed_sha, 16) % (width - 1)
        )
        before = tuple(real.values[index] for index in positions)
        for local_index, source_index in enumerate(positions):
            if real.values[source_index]:
                output[positions[(local_index + shift) % width]] = True
        after = tuple(output[index] for index in positions)
        if selected_count and after == before:
            return CircularNullResult(None, False, "CIRCULAR_GROUP_ROTATION_NOT_DISTINCT")
    mask = SignalMask(
        f"{real.proposal_sha256}:shift",
        real.proposal_sha256,
        "CIRCULAR_SHIFT",
        tuple(output),
        tuple(sorted(set(seed_sha256s))),
    )
    if mask.signal_count != real.signal_count or mask.values == real.values:
        return CircularNullResult(None, False, "CIRCULAR_ROTATION_NOT_DISTINCT")
    return CircularNullResult(mask, True, None)


def causal_range_quartiles(
    bars: Sequence[BarWithOutcomeSpan],
) -> tuple[int | None, ...]:
    """Compute the frozen current-vs-prior-20 causal range quartile."""

    values = _validated_bar_tuple(bars, timeframe_seconds=FIVE_MINUTE_SECONDS, label="5m bars")
    output: list[int | None] = []
    for index, wrapped in enumerate(values):
        if index < 20:
            output.append(None)
            continue
        prior = values[index - 20 : index]
        expected_start = wrapped.bar.start_ns - 20 * FIVE_MINUTE_SECONDS * ONE_SECOND_NS
        same_path = all(
            item.bar.contract == wrapped.bar.contract
            and item.bar.segment_id == wrapped.bar.segment_id
            and item.outcome_span_id == wrapped.outcome_span_id
            for item in prior
        )
        contiguous = (
            prior[0].bar.start_ns == expected_start
            and all(left.bar.end_ns == right.bar.start_ns for left, right in pairwise(prior))
            and prior[-1].bar.end_ns == wrapped.bar.start_ns
        )
        if not same_path or not contiguous:
            output.append(None)
            continue
        current_range = morphology_features(wrapped.bar).range_ticks
        less_or_equal = sum(
            morphology_features(item.bar).range_ticks <= current_range for item in prior
        )
        output.append(min(3, 4 * less_or_equal // 21))
    return tuple(output)


@dataclass(frozen=True, slots=True)
class MatchedPair:
    real_start_ns: int
    matched_start_ns: int
    relaxation_level: int
    relaxation_policy: str
    real_range_stratum: str
    matched_range_stratum: str

    def __post_init__(self) -> None:
        if not 0 <= self.relaxation_level < len(MATCHED_RELAXATION_LEVELS):
            raise HoldoutEvaluationError("matched relaxation level is outside the frozen tiers")
        if self.relaxation_policy != MATCHED_RELAXATION_LEVELS[self.relaxation_level]:
            raise HoldoutEvaluationError("matched relaxation label differs from its level")
        _nonempty(self.real_range_stratum, label="real range stratum")
        _nonempty(self.matched_range_stratum, label="matched range stratum")

    def as_dict(self) -> dict[str, int]:
        return {
            "matched_start_ns": self.matched_start_ns,
            "real_start_ns": self.real_start_ns,
            "relaxation_level": self.relaxation_level,
            "relaxation_policy": self.relaxation_policy,
            "real_range_stratum": self.real_range_stratum,
            "matched_range_stratum": self.matched_range_stratum,
        }


@dataclass(frozen=True, slots=True)
class MatchedNullResult:
    mask: SignalMask | None
    pairs: tuple[MatchedPair, ...]
    sample_eligible: bool
    reason: str | None

    def __post_init__(self) -> None:
        if self.sample_eligible != (self.mask is not None):
            raise HoldoutEvaluationError("matched-null eligibility differs from its mask")
        if self.sample_eligible and self.reason is not None:
            raise HoldoutEvaluationError("eligible matched null cannot have a failure reason")
        if not self.sample_eligible and not self.reason:
            raise HoldoutEvaluationError("ineligible matched null requires a reason")

    def as_dict(self) -> dict[str, object]:
        return {
            "mask": None if self.mask is None else self.mask.as_dict(),
            "pairs": [item.as_dict() for item in self.pairs],
            "reason": self.reason,
            "sample_eligible": self.sample_eligible,
        }


def _null_seed_sha256(
    *,
    master_seed: int | str,
    proposal_sha256: str,
    stage_key: str,
    fold_key: str,
    source_date: date,
    null_kind: str,
) -> str:
    return canonical_sha256(
        {
            "fold": fold_key,
            "master_seed": master_seed,
            "null_kind": null_kind,
            "proposal_sha256": proposal_sha256,
            "source_date": source_date.isoformat(),
            "stage": stage_key,
        }
    )


def _match_score(seed_sha256: str, wrapped: BarWithOutcomeSpan) -> str:
    return canonical_sha256(
        {
            "candidate_contract": wrapped.bar.contract,
            "candidate_outcome_span_id": wrapped.outcome_span_id,
            "candidate_segment_id": wrapped.bar.segment_id,
            "candidate_start_ns": wrapped.bar.start_ns,
            "null_seed_sha256": seed_sha256,
        }
    )


def _range_stratum(value: int | None) -> str:
    return MISSING_PRIOR_20_HISTORY if value is None else f"Q{value}"


def matched_random_null_mask(
    real: SignalMask,
    bars: Sequence[BarWithOutcomeSpan],
    eligible_positions: Sequence[bool],
    *,
    master_seed: int | str = DEFAULT_NULL_SEED,
    stage_key: str = "UNSPECIFIED",
    fold_key_by_date: Mapping[date, str] | None = None,
) -> MatchedNullResult:
    """Build a same-count causal matched control without replacement.

    Matching always fixes date, contract, outcome span, and signal segment.
    It prefers the same 30-minute UTC bucket and causal range stratum, then an
    adjacent bucket retaining that stratum.  The last tier drops bucket and
    stratum but never the causal path.  ``MISSING_PRIOR_20_HISTORY`` is an
    explicit outcome-blind stratum.  SHA-256 is the frozen candidate tie-break;
    real positions are never selected as controls.
    """

    if real.kind != "REAL":
        raise HoldoutEvaluationError("matched null requires a REAL signal mask")
    if isinstance(master_seed, bool) or not isinstance(master_seed, (int, str)):
        raise HoldoutEvaluationError("master seed must be an integer or string")
    _nonempty(stage_key, label="stage key")
    fold_keys = {} if fold_key_by_date is None else dict(fold_key_by_date)
    values = _validated_bar_tuple(bars, timeframe_seconds=FIVE_MINUTE_SECONDS, label="5m bars")
    eligible = tuple(eligible_positions)
    if len(real.values) != len(values) or len(eligible) != len(values):
        raise HoldoutEvaluationError("mask lengths differ from five-minute bars")
    if any(
        selected and not allowed for selected, allowed in zip(real.values, eligible, strict=True)
    ):
        raise HoldoutEvaluationError("real mask selects an ineligible position")
    quartiles = causal_range_quartiles(values)
    targets = [index for index, selected in enumerate(real.values) if selected]
    if not targets:
        return MatchedNullResult(None, (), False, "REAL_SIGNAL_MASK_IS_EMPTY")

    real_positions = frozenset(targets)
    candidates = [
        index for index, allowed in enumerate(eligible) if allowed and index not in real_positions
    ]
    candidate_indexes: dict[tuple[date, str, int, int], list[int]] = defaultdict(list)
    for candidate in candidates:
        item = values[candidate]
        candidate_indexes[
            (
                item.bar.source_date,
                item.bar.contract,
                item.outcome_span_id,
                item.bar.segment_id,
            )
        ].append(candidate)
    ranked_edges: dict[int, tuple[int, ...]] = {}
    relaxation: dict[tuple[int, int], int] = {}
    seed_sha_by_target: dict[int, str] = {}
    for target in targets:
        target_bar = values[target]
        target_bucket = target_bar.bar.start_ns // HALF_HOUR_NS
        seed_sha = _null_seed_sha256(
            master_seed=master_seed,
            proposal_sha256=real.proposal_sha256,
            stage_key=stage_key,
            fold_key=fold_keys.get(target_bar.bar.source_date, "NONE"),
            source_date=target_bar.bar.source_date,
            null_kind="CAUSAL_MATCHED_ENTRY",
        )
        seed_sha_by_target[target] = seed_sha
        choices: list[tuple[int, str, int, int]] = []
        stratum = (
            target_bar.bar.source_date,
            target_bar.bar.contract,
            target_bar.outcome_span_id,
            target_bar.bar.segment_id,
        )
        for candidate in candidate_indexes.get(stratum, ()):
            item = values[candidate]
            distance = abs(item.bar.start_ns // HALF_HOUR_NS - target_bucket)
            same_range_stratum = _range_stratum(quartiles[candidate]) == _range_stratum(
                quartiles[target]
            )
            if same_range_stratum and distance == 0:
                level = 0
            elif same_range_stratum and distance <= 1:
                level = 1
            else:
                level = 2
            relaxation[target, candidate] = level
            choices.append(
                (
                    level,
                    _match_score(seed_sha, item),
                    item.bar.start_ns,
                    candidate,
                )
            )
        ranked_edges[target] = tuple(item[-1] for item in sorted(choices))

    matched_target_by_candidate: dict[int, int] = {}

    def augment(target: int, visited: set[int]) -> bool:
        for candidate in ranked_edges[target]:
            if candidate in visited:
                continue
            visited.add(candidate)
            incumbent = matched_target_by_candidate.get(candidate)
            if incumbent is None or augment(incumbent, visited):
                matched_target_by_candidate[candidate] = target
                return True
        return False

    target_order = sorted(
        targets, key=lambda item: (len(ranked_edges[item]), values[item].bar.start_ns)
    )
    for target in target_order:
        if not augment(target, set()):
            return MatchedNullResult(None, (), False, "INSUFFICIENT_CAUSAL_MATCHED_POOL")

    candidate_by_target = {
        target: candidate for candidate, target in matched_target_by_candidate.items()
    }
    output = [False] * len(values)
    pairs: list[MatchedPair] = []
    for target in sorted(targets, key=lambda item: values[item].bar.start_ns):
        candidate = candidate_by_target[target]
        output[candidate] = True
        pairs.append(
            MatchedPair(
                real_start_ns=values[target].bar.start_ns,
                matched_start_ns=values[candidate].bar.start_ns,
                relaxation_level=relaxation[target, candidate],
                relaxation_policy=MATCHED_RELAXATION_LEVELS[relaxation[target, candidate]],
                real_range_stratum=_range_stratum(quartiles[target]),
                matched_range_stratum=_range_stratum(quartiles[candidate]),
            )
        )
    mask = SignalMask(
        f"{real.proposal_sha256}:matched",
        real.proposal_sha256,
        "MATCHED_RANDOM",
        tuple(output),
        tuple(sorted(set(seed_sha_by_target.values()))),
    )
    if mask.signal_count != real.signal_count:
        raise HoldoutEvaluationError("matched null failed exact cardinality preservation")
    if mask.values == real.values:
        return MatchedNullResult(None, (), False, "MATCHED_NULL_NOT_DISTINCT")
    return MatchedNullResult(mask, tuple(pairs), True, None)


@dataclass(frozen=True, slots=True)
class TradeOutcome:
    signal_index: int
    signal_date: date
    signal_start_ns: int
    contract: str
    entry_date: date
    entry_start_ns: int
    entry_reference_ticks: int
    entry_fill_ticks: int
    direction: Direction
    disposition: TradeDisposition
    exit_ns: int
    exit_date: date
    exit_fill_ticks: int
    gross_pnl_ticks: int
    variable_debit_ticks: int
    allocated_fixed_cost_ticks: int
    fully_loaded_net_pnl_ticks: int
    same_second_stop_first: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "allocated_fixed_cost_ticks": self.allocated_fixed_cost_ticks,
            "direction": self.direction,
            "disposition": self.disposition,
            "contract": self.contract,
            "entry_date": self.entry_date.isoformat(),
            "entry_fill_ticks": self.entry_fill_ticks,
            "entry_reference_ticks": self.entry_reference_ticks,
            "entry_start_ns": self.entry_start_ns,
            "exit_fill_ticks": self.exit_fill_ticks,
            "exit_date": self.exit_date.isoformat(),
            "exit_ns": self.exit_ns,
            "fully_loaded_net_pnl_ticks": self.fully_loaded_net_pnl_ticks,
            "gross_pnl_ticks": self.gross_pnl_ticks,
            "same_second_stop_first": self.same_second_stop_first,
            "signal_date": self.signal_date.isoformat(),
            "signal_index": self.signal_index,
            "signal_start_ns": self.signal_start_ns,
            "variable_debit_ticks": self.variable_debit_ticks,
        }


@dataclass(frozen=True, slots=True)
class GroupSummary:
    group_key: str
    raw_signal_count: int
    signal_day_count: int
    trade_count: int
    active_entry_day_count: int
    active_exit_day_count: int
    gross_pnl_ticks: int
    fully_loaded_net_pnl_ticks: int
    net_gains_ticks: int
    net_losses_ticks: int
    expected_value_ticks: Fraction | None
    profit_factor: Fraction | None
    profit_factor_unbounded: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "fully_loaded_net_pnl_ticks": self.fully_loaded_net_pnl_ticks,
            "gross_pnl_ticks": self.gross_pnl_ticks,
            "group_key": self.group_key,
            "active_entry_day_count": self.active_entry_day_count,
            "active_exit_day_count": self.active_exit_day_count,
            "expected_value_ticks": _fraction_payload(self.expected_value_ticks),
            "net_gains_ticks": self.net_gains_ticks,
            "net_losses_ticks": self.net_losses_ticks,
            "profit_factor": _fraction_payload(self.profit_factor),
            "profit_factor_unbounded": self.profit_factor_unbounded,
            "raw_signal_count": self.raw_signal_count,
            "signal_day_count": self.signal_day_count,
            "trade_count": self.trade_count,
        }


@dataclass(frozen=True, slots=True)
class EvaluationSummary:
    raw_signal_count: int
    signal_day_count: int
    median_signals_per_signal_day: Fraction | None
    trade_count: int
    skipped_occupied_count: int
    active_entry_day_count: int
    active_exit_day_count: int
    contract_count: int
    take_profit_first_count: int
    stop_first_count: int
    timeout_count: int
    same_second_stop_first_count: int
    gross_pnl_ticks: int
    variable_cost_ticks: int
    allocated_fixed_cost_ticks: int
    fully_loaded_net_pnl_ticks: int
    maximum_drawdown_ticks: int
    net_gains_ticks: int
    net_losses_ticks: int
    expected_value_ticks: Fraction | None
    profit_factor: Fraction | None
    profit_factor_unbounded: bool
    daily_net_ticks: tuple[tuple[date, int], ...]
    daily_trade_counts: tuple[tuple[date, int], ...]
    daily_signal_counts: tuple[tuple[date, int], ...]
    group_summaries: tuple[GroupSummary, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "active_entry_day_count": self.active_entry_day_count,
            "active_exit_day_count": self.active_exit_day_count,
            "allocated_fixed_cost_ticks": self.allocated_fixed_cost_ticks,
            "contract_count": self.contract_count,
            "daily_net_ticks": [
                {"source_date": key.isoformat(), "ticks": value}
                for key, value in self.daily_net_ticks
            ],
            "daily_trade_counts": [
                {"source_date": key.isoformat(), "trade_count": value}
                for key, value in self.daily_trade_counts
            ],
            "daily_signal_counts": [
                {"source_date": key.isoformat(), "signal_count": value}
                for key, value in self.daily_signal_counts
            ],
            "expected_value_ticks": _fraction_payload(self.expected_value_ticks),
            "fully_loaded_net_pnl_ticks": self.fully_loaded_net_pnl_ticks,
            "gross_pnl_ticks": self.gross_pnl_ticks,
            "group_summaries": [item.as_dict() for item in self.group_summaries],
            "maximum_drawdown_ticks": self.maximum_drawdown_ticks,
            "median_signals_per_signal_day": _fraction_payload(self.median_signals_per_signal_day),
            "net_gains_ticks": self.net_gains_ticks,
            "net_losses_ticks": self.net_losses_ticks,
            "profit_factor": _fraction_payload(self.profit_factor),
            "profit_factor_unbounded": self.profit_factor_unbounded,
            "raw_signal_count": self.raw_signal_count,
            "signal_day_count": self.signal_day_count,
            "same_second_stop_first_count": self.same_second_stop_first_count,
            "skipped_occupied_count": self.skipped_occupied_count,
            "stop_first_count": self.stop_first_count,
            "take_profit_first_count": self.take_profit_first_count,
            "timeout_count": self.timeout_count,
            "trade_count": self.trade_count,
            "variable_cost_ticks": self.variable_cost_ticks,
        }


@dataclass(frozen=True, slots=True)
class PatternEvaluation:
    key: str
    proposal_sha256: str
    mask_kind: MaskKind
    direction: Direction
    trades: tuple[TradeOutcome, ...]
    summary: EvaluationSummary
    occupied_through_ns: int | None = None

    def as_dict(self, *, include_trades: bool = False) -> dict[str, object]:
        payload: dict[str, object] = {
            "direction": self.direction,
            "key": self.key,
            "mask_kind": self.mask_kind,
            "occupied_through_ns": self.occupied_through_ns,
            "proposal_sha256": self.proposal_sha256,
            "summary": self.summary.as_dict(),
        }
        if include_trades:
            payload["trades"] = [item.as_dict() for item in self.trades]
        return payload


@dataclass(frozen=True, slots=True)
class _OneSecondPath:
    bars: tuple[BarWithOutcomeSpan, ...]
    starts: tuple[int, ...]
    ends: tuple[int, ...]


def _one_second_paths(
    values: Sequence[BarWithOutcomeSpan],
) -> dict[tuple[int, str, int], _OneSecondPath]:
    bars = _validated_bar_tuple(values, timeframe_seconds=1, label="1s bars")
    grouped: dict[tuple[int, str, int], list[BarWithOutcomeSpan]] = defaultdict(list)
    for item in bars:
        grouped[item.outcome_span_id, item.bar.contract, item.bar.segment_id].append(item)
    output: dict[tuple[int, str, int], _OneSecondPath] = {}
    for key, items in grouped.items():
        ordered = tuple(items)
        starts = tuple(item.bar.start_ns for item in ordered)
        if any(left >= right for left, right in pairwise(starts)):
            raise HoldoutEvaluationError("one-second outcome path is not strictly ordered")
        output[key] = _OneSecondPath(
            ordered,
            starts,
            tuple(item.bar.end_ns for item in ordered),
        )
    return output


def _five_minute_path_end_ns(
    values: Sequence[BarWithOutcomeSpan],
) -> dict[tuple[int, str, int], int]:
    """Index deterministic signal-path boundaries once per evaluation call."""

    output: dict[tuple[int, str, int], int] = {}
    for item in values:
        key = item.outcome_span_id, item.bar.contract, item.bar.segment_id
        output[key] = max(output.get(key, 0), item.bar.end_ns)
    return output


def _linked_entry(
    signal_index: int,
    five_minute_bars: tuple[BarWithOutcomeSpan, ...],
    paths: Mapping[tuple[int, str, int], _OneSecondPath],
    path_end_ns_by_key: Mapping[tuple[int, str, int], int],
    *,
    allowed_stage_tail_end_ns: int,
    spec: ExecutionSpec,
) -> tuple[BarWithOutcomeSpan, _OneSecondPath, int, int, int]:
    if signal_index + 1 >= len(five_minute_bars):
        raise HoldoutEvaluationError("selected signal has no next five-minute bar")
    signal = five_minute_bars[signal_index]
    entry = five_minute_bars[signal_index + 1]
    if not (
        signal.bar.end_ns == entry.bar.start_ns
        and signal.bar.contract == entry.bar.contract
        and signal.bar.segment_id == entry.bar.segment_id
        and signal.outcome_span_id == entry.outcome_span_id
    ):
        raise HoldoutEvaluationError(
            "selected signal lacks an exactly contiguous same-path five-minute entry"
        )
    key = entry.outcome_span_id, entry.bar.contract, entry.bar.segment_id
    try:
        path = paths[key]
    except KeyError as error:
        raise HoldoutEvaluationError(
            "entry outcome span has no supplied one-second path"
        ) from error
    entry_start = entry.bar.first_trade_ns // ONE_SECOND_NS * ONE_SECOND_NS
    entry_index = bisect_left(path.starts, entry_start)
    if entry_index >= len(path.bars) or path.starts[entry_index] != entry_start:
        raise HoldoutEvaluationError("entry first-trade second is absent from the one-second path")
    entry_second = path.bars[entry_index]
    if (
        entry_second.bar.open_ticks != entry.bar.open_ticks
        or entry_second.bar.segment_id != entry.bar.segment_id
        or not entry.bar.start_ns <= entry_start < entry.bar.end_ns
    ):
        raise HoldoutEvaluationError("five-minute entry and first one-second bar disagree")
    horizon_ns = entry_start + spec.horizon_seconds * ONE_SECOND_NS
    try:
        outcome_span_end_ns = path_end_ns_by_key[key]
    except KeyError as error:
        raise HoldoutEvaluationError("entry path lacks its precomputed boundary") from error
    terminal_ns = min(horizon_ns, allowed_stage_tail_end_ns, outcome_span_end_ns)
    if terminal_ns <= entry_start:
        raise HoldoutEvaluationError("entry lies at or beyond its allowed outcome boundary")
    terminal_index = bisect_right(path.ends, terminal_ns) - 1
    if terminal_index < entry_index:
        raise HoldoutEvaluationError("one-second path has no last-known close by the horizon")
    return entry, path, entry_index, terminal_index, terminal_ns


def _trade_for_signal(
    signal_index: int,
    five_minute_bars: tuple[BarWithOutcomeSpan, ...],
    paths: Mapping[tuple[int, str, int], _OneSecondPath],
    path_end_ns_by_key: Mapping[tuple[int, str, int], int],
    direction: Direction,
    *,
    allowed_stage_tail_end_ns: int,
    spec: ExecutionSpec,
) -> TradeOutcome:
    signal = five_minute_bars[signal_index]
    entry, path, entry_index, terminal_index, terminal_ns = _linked_entry(
        signal_index,
        five_minute_bars,
        paths,
        path_end_ns_by_key,
        allowed_stage_tail_end_ns=allowed_stage_tail_end_ns,
        spec=spec,
    )
    sign = 1 if direction == "LONG" else -1
    reference = entry.bar.open_ticks
    entry_fill = reference + sign * spec.entry_adverse_ticks
    tp_fill = entry_fill + sign * spec.take_profit_ticks
    tp_threshold = tp_fill + sign * spec.take_profit_trade_through_ticks
    stop_trigger = entry_fill - sign * spec.stop_loss_ticks
    tp_index: int | None = None
    stop_index: int | None = None
    for index in range(entry_index, terminal_index + 1):
        second = path.bars[index].bar
        if direction == "LONG":
            tp_hit = second.high_ticks >= tp_threshold
            stop_hit = second.low_ticks <= stop_trigger
        else:
            tp_hit = second.low_ticks <= tp_threshold
            stop_hit = second.high_ticks >= stop_trigger
        if tp_hit and tp_index is None:
            tp_index = index
        if stop_hit and stop_index is None:
            stop_index = index
        if tp_index is not None or stop_index is not None:
            if stop_index is not None and (tp_index is None or stop_index <= tp_index):
                break
            if tp_index is not None and (stop_index is None or tp_index < stop_index):
                break
    same_second = tp_index is not None and tp_index == stop_index
    if stop_index is not None and (tp_index is None or stop_index <= tp_index):
        disposition: TradeDisposition = "STOP_FIRST"
        exit_bar = path.bars[stop_index].bar
        exit_ns = exit_bar.end_ns
        exit_fill = (
            min(exit_bar.open_ticks, stop_trigger - spec.stop_total_minimum_adverse_ticks)
            if direction == "LONG"
            else max(exit_bar.open_ticks, stop_trigger + spec.stop_total_minimum_adverse_ticks)
        )
    elif tp_index is not None:
        disposition = "TP_FIRST"
        exit_bar = path.bars[tp_index].bar
        exit_ns = exit_bar.end_ns
        exit_fill = tp_fill
    else:
        disposition = "TIMEOUT"
        exit_ns = terminal_ns
        terminal = path.bars[terminal_index].bar
        exit_bar = terminal
        exit_fill = (
            terminal.close_ticks - spec.terminal_exit_adverse_ticks
            if direction == "LONG"
            else terminal.close_ticks + spec.terminal_exit_adverse_ticks
        )
    gross = sign * (exit_fill - entry_fill)
    net = gross - spec.variable_debit_ticks - spec.allocated_fixed_cost_ticks
    return TradeOutcome(
        signal_index=signal_index,
        signal_date=signal.bar.source_date,
        signal_start_ns=signal.bar.start_ns,
        contract=signal.bar.contract,
        entry_date=entry.bar.source_date,
        entry_start_ns=path.starts[entry_index],
        entry_reference_ticks=reference,
        entry_fill_ticks=entry_fill,
        direction=direction,
        disposition=disposition,
        exit_ns=exit_ns,
        exit_date=exit_bar.source_date,
        exit_fill_ticks=exit_fill,
        gross_pnl_ticks=gross,
        variable_debit_ticks=spec.variable_debit_ticks,
        allocated_fixed_cost_ticks=spec.allocated_fixed_cost_ticks,
        fully_loaded_net_pnl_ticks=net,
        same_second_stop_first=same_second and disposition == "STOP_FIRST",
    )


def _median(values: Sequence[int]) -> Fraction | None:
    if not values:
        return None
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return Fraction(ordered[midpoint])
    return Fraction(ordered[midpoint - 1] + ordered[midpoint], 2)


def _maximum_drawdown(values: Sequence[int]) -> int:
    equity = 0
    peak = 0
    maximum = 0
    for value in values:
        equity += value
        peak = max(peak, equity)
        maximum = max(maximum, peak - equity)
    return maximum


def _group_summary(
    group_key: str,
    raw_dates: Sequence[date],
    trades: Sequence[TradeOutcome],
) -> GroupSummary:
    net_values = [item.fully_loaded_net_pnl_ticks for item in trades]
    gains = sum(max(item, 0) for item in net_values)
    losses = sum(max(-item, 0) for item in net_values)
    net = sum(net_values)
    return GroupSummary(
        group_key=group_key,
        raw_signal_count=len(raw_dates),
        signal_day_count=len(set(raw_dates)),
        trade_count=len(trades),
        active_entry_day_count=len({item.entry_date for item in trades}),
        active_exit_day_count=len({item.exit_date for item in trades}),
        gross_pnl_ticks=sum(item.gross_pnl_ticks for item in trades),
        fully_loaded_net_pnl_ticks=net,
        net_gains_ticks=gains,
        net_losses_ticks=losses,
        expected_value_ticks=None if not trades else Fraction(net, len(trades)),
        profit_factor=None if losses == 0 else Fraction(gains, losses),
        profit_factor_unbounded=bool(trades and gains > 0 and losses == 0),
    )


def _summarize(
    trades: Sequence[TradeOutcome],
    *,
    raw_signals: Sequence[BarWithOutcomeSpan],
    skipped_occupied_count: int,
    reporting_dates: Iterable[date],
    group_by_date: Mapping[date, str],
) -> EvaluationSummary:
    values = tuple(trades)
    raw_values = tuple(raw_signals)
    daily_net: dict[date, int] = {item: 0 for item in reporting_dates}
    daily_count: dict[date, int] = {item: 0 for item in reporting_dates}
    daily_signals: dict[date, int] = {item: 0 for item in reporting_dates}
    raw_dates_by_group: dict[str, list[date]] = defaultdict(list)
    trades_by_group: dict[str, list[TradeOutcome]] = defaultdict(list)
    for signal in raw_values:
        source_date = signal.bar.source_date
        daily_signals[source_date] = daily_signals.get(source_date, 0) + 1
        group = group_by_date.get(source_date)
        if group is not None:
            raw_dates_by_group[group].append(source_date)
    for trade in values:
        daily_net[trade.exit_date] = daily_net.get(trade.exit_date, 0) + (
            trade.fully_loaded_net_pnl_ticks
        )
        daily_count[trade.exit_date] = daily_count.get(trade.exit_date, 0) + 1
        group = group_by_date.get(trade.signal_date)
        if group is not None:
            trades_by_group[group].append(trade)
    all_groups = sorted(set(group_by_date.values()))
    net_values = [item.fully_loaded_net_pnl_ticks for item in values]
    gains = sum(max(item, 0) for item in net_values)
    losses = sum(max(-item, 0) for item in net_values)
    net = sum(net_values)
    nonzero_signal_counts = [count for count in daily_signals.values() if count]
    return EvaluationSummary(
        raw_signal_count=len(raw_values),
        signal_day_count=len(nonzero_signal_counts),
        median_signals_per_signal_day=_median(nonzero_signal_counts),
        trade_count=len(values),
        skipped_occupied_count=skipped_occupied_count,
        active_entry_day_count=len({item.entry_date for item in values}),
        active_exit_day_count=len({item.exit_date for item in values}),
        contract_count=len({item.contract for item in values}),
        take_profit_first_count=sum(item.disposition == "TP_FIRST" for item in values),
        stop_first_count=sum(item.disposition == "STOP_FIRST" for item in values),
        timeout_count=sum(item.disposition == "TIMEOUT" for item in values),
        same_second_stop_first_count=sum(item.same_second_stop_first for item in values),
        gross_pnl_ticks=sum(item.gross_pnl_ticks for item in values),
        variable_cost_ticks=sum(item.variable_debit_ticks for item in values),
        allocated_fixed_cost_ticks=sum(item.allocated_fixed_cost_ticks for item in values),
        fully_loaded_net_pnl_ticks=net,
        maximum_drawdown_ticks=_maximum_drawdown(net_values),
        net_gains_ticks=gains,
        net_losses_ticks=losses,
        expected_value_ticks=None if not values else Fraction(net, len(values)),
        profit_factor=None if losses == 0 else Fraction(gains, losses),
        profit_factor_unbounded=bool(values and gains > 0 and losses == 0),
        daily_net_ticks=tuple(sorted(daily_net.items())),
        daily_trade_counts=tuple(sorted(daily_count.items())),
        daily_signal_counts=tuple(sorted(daily_signals.items())),
        group_summaries=tuple(
            _group_summary(group, raw_dates_by_group[group], trades_by_group[group])
            for group in all_groups
        ),
    )


def evaluate_signal_masks(
    five_minute_bars: Sequence[BarWithOutcomeSpan],
    one_second_bars: Sequence[BarWithOutcomeSpan],
    masks_by_key: Mapping[str, SignalMask],
    directions_by_key: Mapping[str, Direction],
    spec: ExecutionSpec = DEFAULT_EXECUTION_SPEC,
    *,
    allowed_stage_tail_end_ns: int,
    reporting_dates: Iterable[date] = (),
    group_by_date: Mapping[date, str] | None = None,
    initial_occupied_through_by_key: Mapping[str, int | None] | None = None,
) -> dict[str, PatternEvaluation]:
    """Evaluate all real/control masks while sharing one in-memory 1s path index."""

    fives = _validated_bar_tuple(
        five_minute_bars, timeframe_seconds=FIVE_MINUTE_SECONDS, label="5m bars"
    )
    paths = _one_second_paths(one_second_bars)
    path_end_ns_by_key = _five_minute_path_end_ns(fives)
    if not masks_by_key or set(masks_by_key) != set(directions_by_key):
        raise HoldoutEvaluationError("mask and direction keys must be identical and non-empty")
    dates = tuple(sorted(set(reporting_dates)))
    groups = {} if group_by_date is None else dict(group_by_date)
    initial_occupancy = (
        {} if initial_occupied_through_by_key is None else dict(initial_occupied_through_by_key)
    )
    output: dict[str, PatternEvaluation] = {}
    for key in sorted(masks_by_key):
        mask = masks_by_key[key]
        direction = directions_by_key[key]
        if key != mask.key or len(mask.values) != len(fives):
            raise HoldoutEvaluationError("mask key or length differs from the evaluation request")
        if direction not in ("LONG", "SHORT"):
            raise HoldoutEvaluationError("evaluation direction must be LONG or SHORT")
        trades: list[TradeOutcome] = []
        skipped = 0
        occupied_through_ns = initial_occupancy.get(key)
        if occupied_through_ns is not None:
            _integer(occupied_through_ns, label="initial occupied-through timestamp", minimum=0)
        for index, selected in enumerate(mask.values):
            if not selected:
                continue
            trade = _trade_for_signal(
                index,
                fives,
                paths,
                path_end_ns_by_key,
                direction,
                allowed_stage_tail_end_ns=allowed_stage_tail_end_ns,
                spec=spec,
            )
            if occupied_through_ns is not None and trade.entry_start_ns <= occupied_through_ns:
                skipped += 1
                continue
            trades.append(trade)
            occupied_through_ns = trade.exit_ns
        summary = _summarize(
            trades,
            raw_signals=tuple(
                item for item, selected in zip(fives, mask.values, strict=True) if selected
            ),
            skipped_occupied_count=skipped,
            reporting_dates=dates,
            group_by_date=groups,
        )
        output[key] = PatternEvaluation(
            key=key,
            proposal_sha256=mask.proposal_sha256,
            mask_kind=mask.kind,
            direction=direction,
            trades=tuple(trades),
            summary=summary,
            occupied_through_ns=occupied_through_ns,
        )
    return output


def merge_evaluations(
    parts: Sequence[PatternEvaluation], *, group_by_date: Mapping[date, str] | None = None
) -> PatternEvaluation:
    """Merge chronological, disjoint outcome-span parts without float arithmetic."""

    if not parts:
        raise HoldoutEvaluationError("at least one evaluation part is required")
    first = parts[0]
    if any(
        (item.key, item.proposal_sha256, item.mask_kind, item.direction)
        != (first.key, first.proposal_sha256, first.mask_kind, first.direction)
        for item in parts
    ):
        raise HoldoutEvaluationError("evaluation parts have different identities")
    ordered_trades = sorted(
        (trade for item in parts for trade in item.trades), key=lambda item: item.signal_start_ns
    )
    trades_list: list[TradeOutcome] = []
    cross_part_skipped = 0
    occupied_through_ns: int | None = None
    for trade in ordered_trades:
        if occupied_through_ns is not None and trade.entry_start_ns <= occupied_through_ns:
            cross_part_skipped += 1
            continue
        trades_list.append(trade)
        occupied_through_ns = trade.exit_ns
    trades = tuple(trades_list)
    identities = [(item.signal_start_ns, item.signal_index) for item in trades]
    if len(set(identities)) != len(identities):
        raise HoldoutEvaluationError("evaluation parts overlap")
    reporting_dates = {
        source_date for item in parts for source_date, _value in item.summary.daily_net_ticks
    }
    groups = {} if group_by_date is None else dict(group_by_date)
    # Group totals cannot be recovered from dates when callers use arbitrary labels;
    # merge them directly after building the exact trade/daily core.
    # Reconstruct exact additive fields from part summaries; this keeps raw
    # signal accounting even though streaming parts intentionally release 5m views.
    daily_signal_counts: dict[date, int] = defaultdict(int)
    for item in parts:
        for source_date, count in item.summary.daily_signal_counts:
            daily_signal_counts[source_date] += count
    placeholder_raw: list[BarWithOutcomeSpan] = []
    summary = _summarize(
        trades,
        raw_signals=placeholder_raw,
        skipped_occupied_count=(
            sum(item.summary.skipped_occupied_count for item in parts) + cross_part_skipped
        ),
        reporting_dates=reporting_dates,
        group_by_date=groups,
    )
    grouped: dict[str, list[GroupSummary]] = defaultdict(list)
    for item in parts:
        for group in item.summary.group_summaries:
            grouped[group.group_key].append(group)

    def merged_group(key: str) -> GroupSummary:
        group_parts = grouped[key]
        rebuilt = next(
            (item for item in summary.group_summaries if item.group_key == key),
            _group_summary(key, (), ()),
        )
        raw_count = sum(item.raw_signal_count for item in group_parts)
        signal_days = sum(
            count > 0
            for source_date, count in daily_signal_counts.items()
            if groups.get(source_date) == key
        )
        return replace(
            rebuilt,
            raw_signal_count=raw_count,
            signal_day_count=signal_days,
        )

    nonzero_signal_counts = [value for value in daily_signal_counts.values() if value]
    summary = replace(
        summary,
        raw_signal_count=sum(nonzero_signal_counts),
        signal_day_count=len(nonzero_signal_counts),
        median_signals_per_signal_day=_median(nonzero_signal_counts),
        daily_signal_counts=tuple(sorted(daily_signal_counts.items())),
        group_summaries=tuple(
            merged_group(key) for key in sorted(set(grouped) | set(groups.values()))
        ),
    )
    return PatternEvaluation(
        first.key,
        first.proposal_sha256,
        first.mask_kind,
        first.direction,
        trades,
        summary,
        occupied_through_ns,
    )


def exact_one_sided_sign_test(values: Iterable[int]) -> Fraction:
    """Exact upper-tail sign test against median zero; zero differences are omitted."""

    signs = tuple(value for value in values if value != 0)
    if any(isinstance(value, bool) or not isinstance(value, int) for value in signs):
        raise HoldoutEvaluationError("sign-test values must be integers")
    count = len(signs)
    if count == 0:
        return Fraction(1)
    positives = sum(value > 0 for value in signs)
    return Fraction(sum(comb(count, index) for index in range(positives, count + 1)), 2**count)


def paired_daily_sign_test(
    real: Sequence[tuple[date, int]], comparator: Sequence[tuple[date, int]]
) -> Fraction:
    """Apply the exact sign test to paired daily real-minus-control net ticks."""

    real_map = dict(real)
    comparator_map = dict(comparator)
    if len(real_map) != len(real) or len(comparator_map) != len(comparator):
        raise HoldoutEvaluationError("daily summaries contain duplicate dates")
    dates = sorted(real_map.keys() | comparator_map.keys())
    return exact_one_sided_sign_test(
        real_map.get(item, 0) - comparator_map.get(item, 0) for item in dates
    )


@dataclass(frozen=True, slots=True)
class MultipleTestDecision:
    key: str
    raw_p_value: Fraction
    adjusted_p_value: Fraction
    critical_value: Fraction
    rejected: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "adjusted_p_value": _fraction_payload(self.adjusted_p_value),
            "critical_value": _fraction_payload(self.critical_value),
            "key": self.key,
            "raw_p_value": _fraction_payload(self.raw_p_value),
            "rejected": self.rejected,
        }


def _p_values(values: Mapping[str, Fraction]) -> tuple[tuple[str, Fraction], ...]:
    if not values:
        raise HoldoutEvaluationError("multiple-testing family must be non-empty")
    for key, value in values.items():
        _nonempty(key, label="multiple-testing key")
        if not isinstance(value, Fraction) or not 0 <= value <= 1:
            raise HoldoutEvaluationError("p-values must be Fractions in [0,1]")
    return tuple(sorted(values.items(), key=lambda item: (item[1], item[0])))


def benjamini_hochberg(
    p_values: Mapping[str, Fraction], *, q: Fraction = Fraction(1, 20)
) -> tuple[MultipleTestDecision, ...]:
    """Exact BH step-up decisions and monotone adjusted p-values."""

    ordered = _p_values(p_values)
    if not isinstance(q, Fraction) or not 0 < q <= 1:
        raise HoldoutEvaluationError("BH q must be a Fraction in (0,1]")
    width = len(ordered)
    largest_rejected_rank = 0
    for rank, (_key, value) in enumerate(ordered, start=1):
        if value <= q * rank / width:
            largest_rejected_rank = rank
    adjusted: list[Fraction] = [Fraction(1)] * width
    running = Fraction(1)
    for reverse_index in range(width - 1, -1, -1):
        rank = reverse_index + 1
        running = min(running, ordered[reverse_index][1] * width / rank, Fraction(1))
        adjusted[reverse_index] = running
    return tuple(
        MultipleTestDecision(
            key,
            value,
            adjusted[index],
            q * (index + 1) / width,
            index + 1 <= largest_rejected_rank,
        )
        for index, (key, value) in enumerate(ordered)
    )


def holm_step_down(
    p_values: Mapping[str, Fraction], *, alpha: Fraction = Fraction(1, 20)
) -> tuple[MultipleTestDecision, ...]:
    """Exact Holm step-down family-wise decisions and adjusted p-values."""

    ordered = _p_values(p_values)
    if not isinstance(alpha, Fraction) or not 0 < alpha <= 1:
        raise HoldoutEvaluationError("Holm alpha must be a Fraction in (0,1]")
    width = len(ordered)
    continue_rejecting = True
    running_adjusted = Fraction(0)
    output: list[MultipleTestDecision] = []
    for index, (key, value) in enumerate(ordered):
        remaining = width - index
        critical = alpha / remaining
        rejected = continue_rejecting and value <= critical
        continue_rejecting = rejected
        running_adjusted = max(running_adjusted, value * remaining)
        output.append(
            MultipleTestDecision(
                key,
                value,
                min(Fraction(1), running_adjusted),
                critical,
                rejected,
            )
        )
    return tuple(output)


@dataclass(frozen=True, slots=True)
class StageCandidateSummary:
    proposal_sha256: str
    real: EvaluationSummary
    circular_shift: EvaluationSummary
    matched_random: EvaluationSummary
    p_vs_zero: Fraction
    p_vs_circular_shift: Fraction
    p_vs_matched_random: Fraction
    conservative_p_value: Fraction
    rule_support_count: int = 0
    rule_support_day_count: int = 0
    rule_support_daily_counts: tuple[tuple[date, int], ...] = ()
    rule_support_group_counts: tuple[tuple[str, int], ...] = ()

    def as_dict(self) -> dict[str, object]:
        return {
            "circular_shift": self.circular_shift.as_dict(),
            "conservative_p_value": _fraction_payload(self.conservative_p_value),
            "matched_random": self.matched_random.as_dict(),
            "p_vs_circular_shift": _fraction_payload(self.p_vs_circular_shift),
            "p_vs_matched_random": _fraction_payload(self.p_vs_matched_random),
            "p_vs_zero": _fraction_payload(self.p_vs_zero),
            "proposal_sha256": self.proposal_sha256,
            "real": self.real.as_dict(),
            "rule_support_count": self.rule_support_count,
            "rule_support_daily_counts": [
                {"source_date": key.isoformat(), "support_count": value}
                for key, value in self.rule_support_daily_counts
            ],
            "rule_support_day_count": self.rule_support_day_count,
            "rule_support_group_counts": [
                {"group_key": key, "support_count": value}
                for key, value in self.rule_support_group_counts
            ],
        }


def summarize_stage_candidate(
    real: PatternEvaluation,
    circular_shift: PatternEvaluation,
    matched_random: PatternEvaluation,
) -> StageCandidateSummary:
    """Build exact daily null comparisons for one frozen proposal."""

    if (
        real.mask_kind != "REAL"
        or circular_shift.mask_kind != "CIRCULAR_SHIFT"
        or matched_random.mask_kind != "MATCHED_RANDOM"
        or len(
            {real.proposal_sha256, circular_shift.proposal_sha256, matched_random.proposal_sha256}
        )
        != 1
    ):
        raise HoldoutEvaluationError("stage candidate requires one real and its two controls")
    p_zero = exact_one_sided_sign_test(value for _date, value in real.summary.daily_net_ticks)
    p_shift = paired_daily_sign_test(
        real.summary.daily_net_ticks, circular_shift.summary.daily_net_ticks
    )
    p_matched = paired_daily_sign_test(
        real.summary.daily_net_ticks, matched_random.summary.daily_net_ticks
    )
    return StageCandidateSummary(
        real.proposal_sha256,
        real.summary,
        circular_shift.summary,
        matched_random.summary,
        p_zero,
        p_shift,
        p_matched,
        max(p_zero, p_shift, p_matched),
    )


@dataclass(frozen=True, slots=True)
class CandidateGateDecision:
    proposal_sha256: str
    economic_gate_passed: bool
    multiplicity_rejected: bool
    selected: bool
    failure_reasons: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "economic_gate_passed": self.economic_gate_passed,
            "failure_reasons": list(self.failure_reasons),
            "multiplicity_rejected": self.multiplicity_rejected,
            "proposal_sha256": self.proposal_sha256,
            "selected": self.selected,
        }


@dataclass(frozen=True, slots=True)
class StageEvaluationResult:
    stage_key: str
    candidates: tuple[StageCandidateSummary, ...]
    finalist_proposal_sha256s: tuple[str, ...]
    classification: str
    multiplicity_decisions: tuple[MultipleTestDecision, ...] = ()
    gate_decisions: tuple[CandidateGateDecision, ...] = ()

    def __post_init__(self) -> None:
        _nonempty(self.stage_key, label="stage key")
        _nonempty(self.classification, label="stage classification")
        identities = {item.proposal_sha256 for item in self.candidates}
        if len(identities) != len(self.candidates):
            raise HoldoutEvaluationError("stage candidates contain duplicate proposals")
        if len(set(self.finalist_proposal_sha256s)) != len(self.finalist_proposal_sha256s):
            raise HoldoutEvaluationError("stage finalists must be unique")
        if not set(self.finalist_proposal_sha256s) <= identities:
            raise HoldoutEvaluationError("stage finalist is absent from candidate summaries")

    def as_dict(self) -> dict[str, object]:
        return {
            "candidates": [item.as_dict() for item in self.candidates],
            "classification": self.classification,
            "finalist_proposal_sha256s": list(self.finalist_proposal_sha256s),
            "gate_decisions": [item.as_dict() for item in self.gate_decisions],
            "multiplicity_decisions": [item.as_dict() for item in self.multiplicity_decisions],
            "stage_key": self.stage_key,
        }


def _profit_factor_at_least(summary: EvaluationSummary, threshold: Fraction) -> bool:
    return summary.profit_factor_unbounded or (
        summary.profit_factor is not None and summary.profit_factor >= threshold
    )


def _group_profit_factor_at_least(summary: GroupSummary, threshold: Fraction) -> bool:
    return summary.profit_factor_unbounded or (
        summary.profit_factor is not None and summary.profit_factor >= threshold
    )


def _net_drawdown_at_least(summary: EvaluationSummary, threshold: Fraction) -> bool:
    if summary.maximum_drawdown_ticks == 0:
        return summary.fully_loaded_net_pnl_ticks > 0
    return Fraction(summary.fully_loaded_net_pnl_ticks, summary.maximum_drawdown_ticks) >= threshold


def _null_deltas_positive(candidate: StageCandidateSummary) -> bool:
    real_net = candidate.real.fully_loaded_net_pnl_ticks
    return (
        real_net > candidate.circular_shift.fully_loaded_net_pnl_ticks
        and real_net > candidate.matched_random.fully_loaded_net_pnl_ticks
    )


def _search_failure_reasons(candidate: StageCandidateSummary) -> tuple[str, ...]:
    real = candidate.real
    groups = real.group_summaries
    support_groups = dict(candidate.rule_support_group_counts)
    median_support = _median([value for _day, value in candidate.rule_support_daily_counts])
    failures: list[str] = []
    checks = (
        (candidate.rule_support_count >= 160, "RAW_SIGNALS_LT_160"),
        (candidate.rule_support_day_count >= 40, "SIGNAL_DAYS_LT_40"),
        (
            all(value >= 25 for value in support_groups.values()) and len(support_groups) == 4,
            "REPORTING_BLOCK_RAW_SIGNALS_LT_25",
        ),
        (median_support is not None and median_support <= 10, "MEDIAN_SIGNALS_PER_DAY_GT_10"),
        (real.trade_count >= 80, "FILLS_LT_80"),
        (real.active_exit_day_count >= 40, "ACTIVE_EXIT_DAYS_LT_40"),
        (
            all(item.trade_count >= 15 for item in groups) and len(groups) == 4,
            "REPORTING_BLOCK_FILLS_LT_15",
        ),
        (
            sum(item.fully_loaded_net_pnl_ticks > 0 for item in groups) >= 3,
            "POSITIVE_REPORTING_BLOCKS_LT_3",
        ),
        (
            all(
                item.expected_value_ticks is not None and item.expected_value_ticks >= -2
                for item in groups
            ),
            "WORST_REPORTING_BLOCK_EV_LT_NEG_2",
        ),
        (real.fully_loaded_net_pnl_ticks > 0, "NET_NOT_POSITIVE"),
        (_profit_factor_at_least(real, Fraction(21, 20)), "PROFIT_FACTOR_LT_21_20"),
        (_null_deltas_positive(candidate), "NULL_DELTAS_NOT_POSITIVE"),
    )
    failures.extend(reason for passed, reason in checks if not passed)
    return tuple(failures)


def _walk_forward_failure_reasons(candidate: StageCandidateSummary) -> tuple[str, ...]:
    real = candidate.real
    folds = real.group_summaries
    positive_fold_nets = sorted(
        item.fully_loaded_net_pnl_ticks for item in folds if item.fully_loaded_net_pnl_ticks > 0
    )
    median_positive = _median(positive_fold_nets)
    losing = [
        -item.fully_loaded_net_pnl_ticks for item in folds if item.fully_loaded_net_pnl_ticks < 0
    ]
    losing_ratio_ok = not losing or (
        median_positive is not None and Fraction(max(losing), 1) <= median_positive * Fraction(3, 2)
    )
    checks = (
        (real.trade_count >= 300, "FILLS_LT_300"),
        (real.active_entry_day_count >= 150, "ACTIVE_ENTRY_DAYS_LT_150"),
        (real.contract_count >= 5, "CONTRACTS_LT_5"),
        (len(folds) == 5 and all(item.trade_count >= 40 for item in folds), "FOLD_FILLS_LT_40"),
        (
            len(folds) == 5 and all(item.active_entry_day_count >= 20 for item in folds),
            "FOLD_ACTIVE_ENTRY_DAYS_LT_20",
        ),
        (sum(item.fully_loaded_net_pnl_ticks > 0 for item in folds) >= 4, "POSITIVE_FOLDS_LT_4"),
        (
            all(_group_profit_factor_at_least(item, Fraction(3, 4)) for item in folds),
            "WORST_FOLD_PROFIT_FACTOR_LT_3_4",
        ),
        (losing_ratio_ok, "WORST_LOSING_FOLD_TOO_LARGE"),
        (real.fully_loaded_net_pnl_ticks > 0, "NET_NOT_POSITIVE"),
        (_profit_factor_at_least(real, Fraction(6, 5)), "PROFIT_FACTOR_LT_6_5"),
        (_net_drawdown_at_least(real, Fraction(3, 2)), "NET_DRAWDOWN_LT_3_2"),
        (_null_deltas_positive(candidate), "NULL_DELTAS_NOT_POSITIVE"),
    )
    return tuple(reason for passed, reason in checks if not passed)


def _holdout_failure_reasons(candidate: StageCandidateSummary) -> tuple[str, ...]:
    real = candidate.real
    halves = real.group_summaries
    checks = (
        (real.trade_count >= 80, "FILLS_LT_80"),
        (real.active_entry_day_count >= 40, "ACTIVE_ENTRY_DAYS_LT_40"),
        (real.contract_count >= 2, "CONTRACTS_LT_2"),
        (real.fully_loaded_net_pnl_ticks > 0, "NET_NOT_POSITIVE"),
        (_profit_factor_at_least(real, Fraction(23, 20)), "PROFIT_FACTOR_LT_23_20"),
        (_net_drawdown_at_least(real, Fraction(1)), "NET_DRAWDOWN_LT_1"),
        (_null_deltas_positive(candidate), "NULL_DELTAS_NOT_POSITIVE"),
        (
            len(halves) == 2 and all(item.fully_loaded_net_pnl_ticks > 0 for item in halves),
            "CALENDAR_HALVES_NOT_BOTH_POSITIVE",
        ),
    )
    return tuple(reason for passed, reason in checks if not passed)


def _candidate_rank_key(candidate: StageCandidateSummary) -> tuple[object, ...]:
    groups = candidate.real.group_summaries
    worst_ev = min(
        (item.expected_value_ticks for item in groups if item.expected_value_ticks is not None),
        default=Fraction(-(10**9)),
    )
    total_ev = candidate.real.expected_value_ticks or Fraction(-(10**9))
    profit_factor = candidate.real.profit_factor or Fraction(0)
    return (
        candidate.conservative_p_value,
        -worst_ev,
        -total_ev,
        0 if candidate.real.profit_factor_unbounded else 1,
        -profit_factor,
        candidate.proposal_sha256,
    )


def select_stage_result(
    stage_key: str,
    raw_result: StageEvaluationResult,
    masks: StageMaskBundle,
    family_proposal_sha256s: Sequence[str],
) -> StageEvaluationResult:
    """Apply the frozen Search/WF/Holdout gates and exact family correction."""

    stage = stage_key.upper()
    if stage not in {"SEARCH", "WALK_FORWARD", "HOLDOUT"}:
        raise HoldoutEvaluationError("selection stage must be SEARCH, WALK_FORWARD, or HOLDOUT")
    if raw_result.stage_key != stage_key or masks.stage_key != stage_key:
        raise HoldoutEvaluationError("selection inputs belong to another stage")
    family = tuple(family_proposal_sha256s)
    if not family or len(set(family)) != len(family):
        raise HoldoutEvaluationError("multiplicity family must be non-empty and unique")
    candidates = {item.proposal_sha256: item for item in raw_result.candidates}
    mask_map = {item.proposal.proposal_sha256: item for item in masks.proposal_masks}
    p_values = {
        identity: (
            candidates[identity].conservative_p_value
            if identity in candidates
            and identity in mask_map
            and mask_map[identity].sample_eligible
            else Fraction(1)
        )
        for identity in family
    }
    multiplicity = holm_step_down(p_values) if stage == "HOLDOUT" else benjamini_hochberg(p_values)
    rejected = {item.key: item.rejected for item in multiplicity}
    failure_function = {
        "SEARCH": _search_failure_reasons,
        "WALK_FORWARD": _walk_forward_failure_reasons,
        "HOLDOUT": _holdout_failure_reasons,
    }[stage]
    decisions: list[CandidateGateDecision] = []
    selectable: list[StageCandidateSummary] = []
    for identity in family:
        candidate = candidates.get(identity)
        if (
            candidate is None
            or not mask_map.get(identity)
            or not mask_map[identity].sample_eligible
        ):
            reasons = ("SAMPLE_INELIGIBLE_OR_MISSING",)
            economic_pass = False
        else:
            reasons = failure_function(candidate)
            economic_pass = not reasons
        selected = economic_pass and rejected[identity]
        decisions.append(
            CandidateGateDecision(identity, economic_pass, rejected[identity], selected, reasons)
        )
        if selected and candidate is not None:
            selectable.append(candidate)
    limit = {"SEARCH": 4, "WALK_FORWARD": 3, "HOLDOUT": 3}[stage]
    finalists = tuple(
        item.proposal_sha256 for item in sorted(selectable, key=_candidate_rank_key)[:limit]
    )
    if stage == "HOLDOUT":
        any_ineligible = any(
            identity not in candidates
            or identity not in mask_map
            or not mask_map[identity].sample_eligible
            for identity in family
        )
        all_ineligible = all(
            identity not in candidates
            or identity not in mask_map
            or not mask_map[identity].sample_eligible
            for identity in family
        )
        classification = (
            "ONE_SHOT_UNSEALED_BAR_HOLDOUT_DIAGNOSTIC_PASS"
            if finalists
            else "ONE_SHOT_UNSEALED_BAR_HOLDOUT_DIAGNOSTIC_INCONCLUSIVE"
            if any_ineligible and all_ineligible
            else "ONE_SHOT_UNSEALED_BAR_HOLDOUT_DIAGNOSTIC_FAIL"
        )
    elif stage == "SEARCH":
        classification = "SEARCH_FINALISTS_SELECTED" if finalists else "NO_SEARCH_FINALISTS"
    else:
        classification = (
            "WALK_FORWARD_FINALISTS_SELECTED" if finalists else "NO_WALK_FORWARD_FINALISTS"
        )
    return StageEvaluationResult(
        stage_key,
        raw_result.candidates,
        finalists,
        classification,
        multiplicity,
        tuple(sorted(decisions, key=lambda item: item.proposal_sha256)),
    )


@dataclass(frozen=True, slots=True)
class ProposalMaskSet:
    proposal: FrozenProposal
    real: SignalMask
    circular_shift: SignalMask | None
    matched_random: SignalMask | None
    matched_pairs: tuple[MatchedPair, ...]
    rule_support_count: int
    rule_support_day_count: int
    rule_support_daily_counts: tuple[tuple[date, int], ...]
    rule_support_group_counts: tuple[tuple[str, int], ...]
    sample_eligible: bool
    ineligibility_reason: str | None

    def as_dict(self) -> dict[str, object]:
        return {
            "circular_shift": (
                None if self.circular_shift is None else self.circular_shift.as_dict()
            ),
            "ineligibility_reason": self.ineligibility_reason,
            "matched_pairs": [item.as_dict() for item in self.matched_pairs],
            "matched_random": (
                None if self.matched_random is None else self.matched_random.as_dict()
            ),
            "proposal_sha256": self.proposal.proposal_sha256,
            "real": self.real.as_dict(),
            "rule_support_count": self.rule_support_count,
            "rule_support_daily_counts": [
                {"source_date": key.isoformat(), "support_count": value}
                for key, value in self.rule_support_daily_counts
            ],
            "rule_support_day_count": self.rule_support_day_count,
            "rule_support_group_counts": [
                {"group_key": key, "support_count": value}
                for key, value in self.rule_support_group_counts
            ],
            "sample_eligible": self.sample_eligible,
            "selection_rank": self.proposal.selection_rank,
        }


@dataclass(frozen=True, slots=True)
class StageMaskBundle:
    stage_key: str
    five_minute_view_sha256: str
    eligible_positions: tuple[bool, ...]
    proposal_masks: tuple[ProposalMaskSet, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "eligible_position_count": sum(self.eligible_positions),
            "eligible_values_sha256": canonical_sha256(list(self.eligible_positions)),
            "five_minute_view_sha256": self.five_minute_view_sha256,
            "proposal_masks": [item.as_dict() for item in self.proposal_masks],
            "stage_key": self.stage_key,
        }


def build_stage_masks(
    stage_key: str,
    five_minute_bars: Sequence[BarWithOutcomeSpan],
    proposals: DirectionalProposalBatch | Sequence[_FrozenProposalLike],
    proposal_sha256s: Sequence[str],
    spec: ExecutionSpec,
    seed: int | str,
    group_by_date: Mapping[date, str],
) -> StageMaskBundle:
    """Build real and null masks using only the allowlisted five-minute view."""

    key = _nonempty(stage_key, label="stage key")
    frozen = freeze_proposals(proposals)
    selected = tuple(proposal_sha256s)
    if tuple(sorted(set(selected))) != tuple(sorted(selected)):
        raise HoldoutEvaluationError("requested proposal SHAs must be unique")
    bars = _validated_bar_tuple(
        five_minute_bars, timeframe_seconds=FIVE_MINUTE_SECONDS, label="5m bars"
    )
    eligible = five_minute_eligible_positions(
        bars,
        decision_dates=group_by_date,
        allowed_stage_tail_end_ns=max(item.bar.end_ns for item in bars),
        spec=spec,
    )
    by_sha = {item.proposal_sha256: item for item in frozen}
    view_sha256 = canonical_sha256(
        [{"bar": item.bar.as_dict(), "outcome_span_id": item.outcome_span_id} for item in bars]
    )
    output: list[ProposalMaskSet] = []
    for identity in sorted(selected):
        if identity not in by_sha:
            raise HoldoutEvaluationError("requested proposal is absent from frozen proposals")
        proposal = by_sha[identity]
        rule_support_values = tuple(
            item.bar.source_date in group_by_date
            and proposal.rule.matches(morphology_features(item.bar).rule_values)
            for item in bars
        )
        support_dates = {
            item.bar.source_date
            for item, selected_support in zip(bars, rule_support_values, strict=True)
            if selected_support
        }
        support_by_group: dict[str, int] = defaultdict(int)
        support_by_date: dict[date, int] = defaultdict(int)
        for item, selected_support in zip(bars, rule_support_values, strict=True):
            if selected_support:
                support_by_group[group_by_date[item.bar.source_date]] += 1
                support_by_date[item.bar.source_date] += 1
        real = proposal_signal_mask(frozen, identity, bars, eligible)
        fold_key_by_date = (
            group_by_date
            if "WALK" in stage_key.upper() or stage_key.upper().startswith("WF")
            else {}
        )
        shifted = circular_shift_null_mask(
            real,
            bars,
            eligible,
            master_seed=seed,
            stage_key=stage_key,
            fold_key_by_date=fold_key_by_date,
        )
        matched = matched_random_null_mask(
            real,
            bars,
            eligible,
            master_seed=seed,
            stage_key=stage_key,
            fold_key_by_date=fold_key_by_date,
        )
        output.append(
            ProposalMaskSet(
                proposal,
                real,
                shifted.mask,
                matched.mask,
                matched.pairs,
                sum(rule_support_values),
                len(support_dates),
                tuple(sorted(support_by_date.items())),
                tuple(sorted(support_by_group.items())),
                shifted.sample_eligible and matched.sample_eligible,
                shifted.reason or matched.reason,
            )
        )
    return StageMaskBundle(key, view_sha256, eligible, tuple(output))


def evaluate_stage_parts(
    stage_key: str,
    five_minute_bars: Sequence[BarWithOutcomeSpan],
    outcome_parts: Iterable[Sequence[BarWithOutcomeSpan]],
    masks: StageMaskBundle,
    proposals: DirectionalProposalBatch | Sequence[_FrozenProposalLike],
    spec: ExecutionSpec,
    gate: Callable[[StageCandidateSummary], bool],
    *,
    group_by_date: Mapping[date, str],
    classification_pass: str = "STAGE_FINALISTS_SELECTED",
    classification_fail: str = "NO_STAGE_FINALISTS",
) -> StageEvaluationResult:
    """Evaluate one-second outcome spans once each and apply a caller-frozen gate."""

    if stage_key != masks.stage_key:
        raise HoldoutEvaluationError("stage key differs from its mask bundle")
    if not callable(gate):
        raise HoldoutEvaluationError("stage gate must be callable")
    frozen = {item.proposal_sha256: item for item in freeze_proposals(proposals)}
    fives = _validated_bar_tuple(
        five_minute_bars, timeframe_seconds=FIVE_MINUTE_SECONDS, label="5m bars"
    )
    tail_end = max(item.bar.end_ns for item in fives)
    evaluations: dict[str, list[PatternEvaluation]] = defaultdict(list)
    assigned: dict[str, int] = defaultdict(int)
    occupied_through_by_key: dict[str, int | None] = {}
    seen_path_keys: set[tuple[int, str, int]] = set()
    previous_part_start: int | None = None
    for part in outcome_parts:
        ones = _validated_bar_tuple(part, timeframe_seconds=1, label="1s outcome part")
        part_start = ones[0].bar.start_ns
        if previous_part_start is not None and part_start <= previous_part_start:
            raise HoldoutEvaluationError("outcome parts must be supplied in chronological order")
        previous_part_start = part_start
        part_keys = {
            (item.outcome_span_id, item.bar.contract, item.bar.segment_id) for item in ones
        }
        if seen_path_keys & part_keys:
            raise HoldoutEvaluationError("outcome parts repeat an already streamed path")
        seen_path_keys.update(part_keys)
        part_masks: dict[str, SignalMask] = {}
        directions: dict[str, Direction] = {}
        for item in masks.proposal_masks:
            if (
                not item.sample_eligible
                or item.circular_shift is None
                or item.matched_random is None
            ):
                continue
            proposal = frozen[item.proposal.proposal_sha256]
            for mask in (item.real, item.circular_shift, item.matched_random):
                sliced_values = tuple(
                    selected
                    and index + 1 < len(fives)
                    and (
                        fives[index + 1].outcome_span_id,
                        fives[index + 1].bar.contract,
                        fives[index + 1].bar.segment_id,
                    )
                    in part_keys
                    for index, selected in enumerate(mask.values)
                )
                sliced = SignalMask(
                    mask.key,
                    mask.proposal_sha256,
                    mask.kind,
                    sliced_values,
                    mask.null_seed_sha256s,
                )
                part_masks[sliced.key] = sliced
                directions[sliced.key] = proposal.direction
                assigned[sliced.key] += sliced.signal_count
        if part_masks:
            result = evaluate_signal_masks(
                fives,
                ones,
                part_masks,
                directions,
                spec,
                allowed_stage_tail_end_ns=tail_end,
                reporting_dates={item.bar.source_date for item in fives},
                group_by_date=group_by_date,
                initial_occupied_through_by_key=occupied_through_by_key,
            )
            for key, value in result.items():
                evaluations[key].append(value)
                occupied_through_by_key[key] = value.occupied_through_ns

    summaries: list[StageCandidateSummary] = []
    for item in masks.proposal_masks:
        if not item.sample_eligible or item.circular_shift is None or item.matched_random is None:
            continue
        expected_masks = (item.real, item.circular_shift, item.matched_random)
        if any(assigned[mask.key] != mask.signal_count for mask in expected_masks):
            raise HoldoutEvaluationError("outcome parts do not cover every selected signal")
        merged = [
            merge_evaluations(evaluations[mask.key], group_by_date=group_by_date)
            for mask in expected_masks
        ]
        candidate_summary = summarize_stage_candidate(*merged)
        summaries.append(
            replace(
                candidate_summary,
                rule_support_count=item.rule_support_count,
                rule_support_day_count=item.rule_support_day_count,
                rule_support_daily_counts=item.rule_support_daily_counts,
                rule_support_group_counts=item.rule_support_group_counts,
            )
        )
    finalists = tuple(sorted(item.proposal_sha256 for item in summaries if gate(item)))
    return StageEvaluationResult(
        stage_key,
        tuple(sorted(summaries, key=lambda item: item.proposal_sha256)),
        finalists,
        classification_pass if finalists else classification_fail,
    )


__all__ = [
    "DEFAULT_EXECUTION_SPEC",
    "DEFAULT_NULL_SEED",
    "MATCHED_RELAXATION_LEVELS",
    "MISSING_PRIOR_20_HISTORY",
    "BarWithOutcomeSpan",
    "CandidateGateDecision",
    "CircularNullResult",
    "EvaluationSummary",
    "ExecutionSpec",
    "FrozenProposal",
    "GroupSummary",
    "HoldoutEvaluationError",
    "MatchedNullResult",
    "MatchedPair",
    "MorphologyFeatures",
    "MultipleTestDecision",
    "PatternEvaluation",
    "ProposalMaskSet",
    "SignalMask",
    "StageCandidateSummary",
    "StageEvaluationResult",
    "StageMaskBundle",
    "TradeOutcome",
    "benjamini_hochberg",
    "build_stage_masks",
    "causal_range_quartiles",
    "circular_shift_null_mask",
    "evaluate_signal_masks",
    "evaluate_stage_parts",
    "exact_one_sided_sign_test",
    "five_minute_eligible_positions",
    "freeze_proposals",
    "holm_step_down",
    "matched_random_null_mask",
    "merge_evaluations",
    "morphology_features",
    "paired_daily_sign_test",
    "proposal_signal_mask",
    "select_stage_result",
    "summarize_stage_candidate",
]
