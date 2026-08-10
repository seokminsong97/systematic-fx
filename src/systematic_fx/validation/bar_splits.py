"""Performance-independent chronological splits for bar-pattern research."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime
from typing import Final

BAR_SPLIT_SCHEMA: Final = "systematic_fx.bar_pattern_splits.v1"
MINIMUM_ACTIVE_DAYS: Final = 740
EMBARGO_DAYS: Final = 20
HOLDOUT_DECISION_DAYS: Final = 120
OUTCOME_TAIL_DAYS: Final = 20
BOUNDARY_TAIL_DAYS: Final = 20
WALK_FORWARD_FOLDS: Final = 5
DISCOVERY_REPORTING_BLOCKS: Final = 4


class BarSplitError(ValueError):
    """An eligible calendar cannot satisfy the frozen split policy."""


@dataclass(frozen=True, slots=True)
class BarDateRange:
    """One inclusive active-day range and its one-based calendar ordinals."""

    split_key: str
    role: str
    start_date: date
    end_date: date
    start_active_ordinal: int
    end_active_ordinal: int
    decision_end_date: date | None
    result_visibility: str
    fold_number: int | None = None

    @property
    def active_day_count(self) -> int:
        return self.end_active_ordinal - self.start_active_ordinal + 1

    def as_dict(self) -> dict[str, object]:
        return {
            "active_day_count": self.active_day_count,
            "decision_end_date": (
                None if self.decision_end_date is None else self.decision_end_date.isoformat()
            ),
            "end_active_ordinal": self.end_active_ordinal,
            "end_date": self.end_date.isoformat(),
            "fold_number": self.fold_number,
            "result_visibility": self.result_visibility,
            "role": self.role,
            "split_key": self.split_key,
            "start_active_ordinal": self.start_active_ordinal,
            "start_date": self.start_date.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class BarSplitPlan:
    """The complete discovery, walk-forward, embargo, and sealed holdout plan."""

    eligible_dates: tuple[date, ...]
    discovery: BarDateRange
    discovery_reporting_blocks: tuple[BarDateRange, ...]
    walk_forward_folds: tuple[BarDateRange, ...]
    embargo: BarDateRange
    holdout: BarDateRange
    outcome_tail: BarDateRange
    canonical_bytes: bytes
    sha256: str

    @property
    def ranges(self) -> tuple[BarDateRange, ...]:
        return (
            self.discovery,
            *self.walk_forward_folds,
            self.embargo,
            self.holdout,
            self.outcome_tail,
        )

    def as_dict(self) -> dict[str, object]:
        value = json.loads(self.canonical_bytes)
        if not isinstance(value, dict):  # pragma: no cover - canonical root is fixed
            raise BarSplitError("canonical split plan is not an object")
        return value


def _validated_dates(values: Sequence[date]) -> tuple[date, ...]:
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
        raise BarSplitError("eligible_dates must be a sequence")
    dates = tuple(values)
    if len(dates) < MINIMUM_ACTIVE_DAYS:
        raise BarSplitError(f"at least {MINIMUM_ACTIVE_DAYS} eligible active days are required")
    if any(isinstance(item, datetime) or not isinstance(item, date) for item in dates):
        raise BarSplitError("eligible_dates must contain date values")
    if dates != tuple(sorted(set(dates))):
        raise BarSplitError("eligible_dates must be unique and strictly increasing")
    return dates


def _lengths(total: int, groups: int) -> tuple[int, ...]:
    quotient, remainder = divmod(total, groups)
    return tuple(quotient + (1 if index < remainder else 0) for index in range(groups))


def _range(
    dates: tuple[date, ...],
    *,
    split_key: str,
    role: str,
    start_index: int,
    length: int,
    decision_length: int | None,
    visibility: str,
    fold_number: int | None = None,
) -> BarDateRange:
    if length <= 0 or start_index < 0 or start_index + length > len(dates):
        raise BarSplitError("split range is outside the eligible calendar")
    if decision_length is not None and not 0 < decision_length <= length:
        raise BarSplitError("decision length must be inside its split")
    return BarDateRange(
        split_key=split_key,
        role=role,
        start_date=dates[start_index],
        end_date=dates[start_index + length - 1],
        start_active_ordinal=start_index + 1,
        end_active_ordinal=start_index + length,
        decision_end_date=(
            None if decision_length is None else dates[start_index + decision_length - 1]
        ),
        result_visibility=visibility,
        fold_number=fold_number,
    )


def plan_bar_splits(eligible_dates: Sequence[date]) -> BarSplitPlan:
    """Apply the frozen v1 formula without consulting any signal or outcome."""

    dates = _validated_dates(eligible_dates)
    active_count = len(dates)
    reserved = EMBARGO_DAYS + HOLDOUT_DECISION_DAYS + OUTCOME_TAIL_DAYS
    pre_holdout_count = active_count - reserved
    discovery_count = 220 + (2 * (pre_holdout_count - 580)) // 5
    walk_forward_count = pre_holdout_count - discovery_count
    fold_lengths = _lengths(walk_forward_count, WALK_FORWARD_FOLDS)
    if min(fold_lengths) < 72:
        raise BarSplitError("every walk-forward fold must contain at least 72 active days")
    if discovery_count <= BOUNDARY_TAIL_DAYS:
        raise BarSplitError("discovery must retain decision days before its outcome tail")

    discovery_decisions = discovery_count - BOUNDARY_TAIL_DAYS
    discovery = _range(
        dates,
        split_key="discovery",
        role="DISCOVERY",
        start_index=0,
        length=discovery_count,
        decision_length=discovery_decisions,
        visibility="VISIBLE",
    )
    reporting_blocks: list[BarDateRange] = []
    cursor = 0
    for number, length in enumerate(
        _lengths(discovery_decisions, DISCOVERY_REPORTING_BLOCKS),
        start=1,
    ):
        reporting_blocks.append(
            _range(
                dates,
                split_key=f"discovery_block_{number}",
                role="DISCOVERY_REPORTING_BLOCK",
                start_index=cursor,
                length=length,
                decision_length=length,
                visibility="VISIBLE",
            )
        )
        cursor += length

    cursor = discovery_count
    folds: list[BarDateRange] = []
    for number, length in enumerate(fold_lengths, start=1):
        folds.append(
            _range(
                dates,
                split_key=f"walk_forward_{number}",
                role="WALK_FORWARD",
                start_index=cursor,
                length=length,
                decision_length=length - BOUNDARY_TAIL_DAYS,
                visibility="SEALED",
                fold_number=number,
            )
        )
        cursor += length

    embargo = _range(
        dates,
        split_key="holdout_embargo",
        role="EMBARGO",
        start_index=cursor,
        length=EMBARGO_DAYS,
        decision_length=None,
        visibility="SEALED",
    )
    cursor += EMBARGO_DAYS
    holdout = _range(
        dates,
        split_key="sealed_holdout",
        role="HOLDOUT",
        start_index=cursor,
        length=HOLDOUT_DECISION_DAYS,
        decision_length=HOLDOUT_DECISION_DAYS,
        visibility="SEALED",
    )
    cursor += HOLDOUT_DECISION_DAYS
    outcome_tail = _range(
        dates,
        split_key="holdout_outcome_tail",
        role="OUTCOME_TAIL",
        start_index=cursor,
        length=OUTCOME_TAIL_DAYS,
        decision_length=None,
        visibility="SEALED",
    )
    cursor += OUTCOME_TAIL_DAYS
    if cursor != active_count:
        raise BarSplitError("split allocation did not consume the eligible calendar")

    document = {
        "active_day_count": active_count,
        "eligible_end_date": dates[-1].isoformat(),
        "eligible_start_date": dates[0].isoformat(),
        "policy": {
            "boundary_tail_days": BOUNDARY_TAIL_DAYS,
            "discovery_formula": "220+floor(2*(P-580)/5)",
            "discovery_reporting_blocks": DISCOVERY_REPORTING_BLOCKS,
            "embargo_days": EMBARGO_DAYS,
            "holdout_decision_days": HOLDOUT_DECISION_DAYS,
            "outcome_tail_days": OUTCOME_TAIL_DAYS,
            "walk_forward_folds": WALK_FORWARD_FOLDS,
        },
        "ranges": [item.as_dict() for item in (discovery, *folds, embargo, holdout, outcome_tail)],
        "reporting_blocks": [item.as_dict() for item in reporting_blocks],
        "schema": BAR_SPLIT_SCHEMA,
    }
    canonical = json.dumps(
        document,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return BarSplitPlan(
        eligible_dates=dates,
        discovery=discovery,
        discovery_reporting_blocks=tuple(reporting_blocks),
        walk_forward_folds=tuple(folds),
        embargo=embargo,
        holdout=holdout,
        outcome_tail=outcome_tail,
        canonical_bytes=canonical,
        sha256=hashlib.sha256(canonical).hexdigest(),
    )
