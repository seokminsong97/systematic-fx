"""Nested chronological training plan for State-Conditional Bar Model V2."""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime
from typing import Final

from systematic_fx.research.bar_state_config import (
    BAR_STATE_LABEL_HORIZON_ACTIVE_DAYS,
    BAR_STATE_OUTER_SPLIT_SHA256,
)
from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256
from systematic_fx.validation.bar_splits import BarDateRange, BarSplitPlan, plan_bar_splits

BAR_STATE_SPLIT_SCHEMA: Final = "systematic_fx.bar_state_nested_splits.v1"
BAR_STATE_INNER_FOLD_COUNT: Final = 3
BAR_STATE_EXPECTED_ELIGIBLE_ACTIVE_DAYS: Final = 1_413
# Filled after computing the canonical plan against the frozen eligible calendar.
BAR_STATE_FROZEN_SPLIT_SHA256: Final = (
    "9a4833aa53fe03788ddf224efcd24abbcc492498b915f174b6473f9c37f3fc8a"
)
BAR_STATE_BOOTSTRAP_EVALUATION_CALENDAR_SCHEMA: Final = (
    "systematic_fx.bar_state_bootstrap_evaluation_calendar.v1"
)
BAR_STATE_FROZEN_BOOTSTRAP_EVALUATION_CALENDAR_SHA256: Final = (
    "0f00faa36d08feebec1fce003268823ff02aa52b9817a84edbfcc8f863a324f1"
)


class BarStateSplitError(ValueError):
    """The nested state-model split is invalid or differs from its frozen identity."""


@dataclass(frozen=True, slots=True)
class BarStateDateSpan:
    """One inclusive active-day span with an explicit modelling role."""

    role: str
    start_date: date
    end_date: date
    start_active_ordinal: int
    end_active_ordinal: int

    def __post_init__(self) -> None:
        if not isinstance(self.role, str) or not self.role:
            raise BarStateSplitError("span role must be non-empty")
        if (
            isinstance(self.start_date, datetime)
            or isinstance(self.end_date, datetime)
            or not isinstance(self.start_date, date)
            or not isinstance(self.end_date, date)
        ):
            raise BarStateSplitError("span dates must be dates")
        if self.start_date > self.end_date:
            raise BarStateSplitError("span dates must be increasing")
        if (
            isinstance(self.start_active_ordinal, bool)
            or isinstance(self.end_active_ordinal, bool)
            or not isinstance(self.start_active_ordinal, int)
            or not isinstance(self.end_active_ordinal, int)
            or self.start_active_ordinal <= 0
            or self.start_active_ordinal > self.end_active_ordinal
        ):
            raise BarStateSplitError("span active ordinals must be positive and increasing")

    @property
    def active_day_count(self) -> int:
        return self.end_active_ordinal - self.start_active_ordinal + 1

    def as_dict(self) -> dict[str, object]:
        return {
            "active_day_count": self.active_day_count,
            "end_active_ordinal": self.end_active_ordinal,
            "end_date": self.end_date.isoformat(),
            "role": self.role,
            "start_active_ordinal": self.start_active_ordinal,
            "start_date": self.start_date.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class BarStateInnerFold:
    """One expanding fit followed by a purged Discovery OOS interval."""

    fold_number: int
    train: BarStateDateSpan
    purge: BarStateDateSpan
    oos_decisions: BarStateDateSpan
    outcome_tail: BarStateDateSpan

    def __post_init__(self) -> None:
        if not 1 <= self.fold_number <= BAR_STATE_INNER_FOLD_COUNT:
            raise BarStateSplitError("inner fold number is outside the frozen schedule")
        if self.train.start_active_ordinal != 1:
            raise BarStateSplitError("inner training must expand from active ordinal one")
        if self.train.end_active_ordinal + 1 != self.purge.start_active_ordinal:
            raise BarStateSplitError("inner train and purge spans must be adjacent")
        if self.purge.end_active_ordinal + 1 != self.oos_decisions.start_active_ordinal:
            raise BarStateSplitError("inner purge and OOS spans must be adjacent")
        if self.oos_decisions.end_active_ordinal + 1 != self.outcome_tail.start_active_ordinal:
            raise BarStateSplitError("inner OOS and tail spans must be adjacent")
        if self.purge.active_day_count != BAR_STATE_LABEL_HORIZON_ACTIVE_DAYS:
            raise BarStateSplitError("inner label purge must contain exactly 20 active days")
        if self.outcome_tail.active_day_count != BAR_STATE_LABEL_HORIZON_ACTIVE_DAYS:
            raise BarStateSplitError("inner outcome tail must contain exactly 20 active days")

    def as_dict(self) -> dict[str, object]:
        return {
            "fold_number": self.fold_number,
            "oos_decisions": self.oos_decisions.as_dict(),
            "outcome_tail": self.outcome_tail.as_dict(),
            "purge": self.purge.as_dict(),
            "train": self.train.as_dict(),
        }


@dataclass(frozen=True, slots=True)
class BarStateOuterFit:
    """Frozen expanding fit and sealed OOS range for one outer fold."""

    fold_number: int
    training_rows_through: BarStateDateSpan
    label_maturity_tail: BarStateDateSpan
    oos_decisions: BarStateDateSpan
    outcome_tail: BarStateDateSpan
    result_visibility: str = "SEALED_UNTIL_ALL_FIVE_FOLDS_COMPLETE"

    def __post_init__(self) -> None:
        if not 1 <= self.fold_number <= 5:
            raise BarStateSplitError("outer fold number must be one through five")
        if self.training_rows_through.start_active_ordinal != 1:
            raise BarStateSplitError("outer training must expand from active ordinal one")
        if (
            self.training_rows_through.end_active_ordinal + 1
            != self.label_maturity_tail.start_active_ordinal
            or self.label_maturity_tail.end_active_ordinal + 1
            != self.oos_decisions.start_active_ordinal
            or self.oos_decisions.end_active_ordinal + 1 != self.outcome_tail.start_active_ordinal
        ):
            raise BarStateSplitError("outer train, tail, decisions, and outcome tail must align")
        if (
            self.label_maturity_tail.active_day_count != BAR_STATE_LABEL_HORIZON_ACTIVE_DAYS
            or self.outcome_tail.active_day_count != BAR_STATE_LABEL_HORIZON_ACTIVE_DAYS
        ):
            raise BarStateSplitError("outer maturity and outcome tails must contain 20 days")
        if self.result_visibility != "SEALED_UNTIL_ALL_FIVE_FOLDS_COMPLETE":
            raise BarStateSplitError("outer fold result visibility must remain sealed")

    def as_dict(self) -> dict[str, object]:
        return {
            "fold_number": self.fold_number,
            "label_maturity_tail": self.label_maturity_tail.as_dict(),
            "oos_decisions": self.oos_decisions.as_dict(),
            "outcome_tail": self.outcome_tail.as_dict(),
            "result_visibility": self.result_visibility,
            "training_rows_through": self.training_rows_through.as_dict(),
        }


@dataclass(frozen=True, slots=True)
class BarStateHoldoutFit:
    """Final pre-holdout refit boundary and sealed evaluation ranges."""

    training_rows_through: BarStateDateSpan
    label_maturity_tail: BarStateDateSpan
    embargo: BarStateDateSpan
    holdout_decisions: BarStateDateSpan
    holdout_outcome_tail: BarStateDateSpan

    def __post_init__(self) -> None:
        if self.training_rows_through.start_active_ordinal != 1:
            raise BarStateSplitError("holdout training must expand from active ordinal one")
        if (
            self.training_rows_through.end_active_ordinal + 1
            != self.label_maturity_tail.start_active_ordinal
            or self.label_maturity_tail.end_active_ordinal + 1 != self.embargo.start_active_ordinal
            or self.embargo.end_active_ordinal + 1 != self.holdout_decisions.start_active_ordinal
            or self.holdout_decisions.end_active_ordinal + 1
            != self.holdout_outcome_tail.start_active_ordinal
        ):
            raise BarStateSplitError("holdout fit, embargo, decisions, and tail must align")
        if (
            self.label_maturity_tail.active_day_count != BAR_STATE_LABEL_HORIZON_ACTIVE_DAYS
            or self.embargo.active_day_count != BAR_STATE_LABEL_HORIZON_ACTIVE_DAYS
            or self.holdout_outcome_tail.active_day_count != BAR_STATE_LABEL_HORIZON_ACTIVE_DAYS
            or self.holdout_decisions.active_day_count != 120
        ):
            raise BarStateSplitError("holdout spans differ from the frozen 20/20/120/20 policy")

    def as_dict(self) -> dict[str, object]:
        return {
            "embargo": self.embargo.as_dict(),
            "holdout_decisions": self.holdout_decisions.as_dict(),
            "holdout_outcome_tail": self.holdout_outcome_tail.as_dict(),
            "label_maturity_tail": self.label_maturity_tail.as_dict(),
            "training_rows_through": self.training_rows_through.as_dict(),
        }


@dataclass(frozen=True, slots=True)
class BarStateSplitPlan:
    """Complete V2 inner schedule plus the still-sealed outer schedule."""

    eligible_dates: tuple[date, ...]
    outer_plan: BarSplitPlan
    inner_folds: tuple[BarStateInnerFold, ...]
    discovery_final_fit: BarStateDateSpan
    discovery_final_label_tail: BarStateDateSpan
    outer_fits: tuple[BarStateOuterFit, ...]
    holdout_fit: BarStateHoldoutFit
    canonical_bytes: bytes
    sha256: str

    @property
    def discovery_oos_active_day_count(self) -> int:
        return sum(item.oos_decisions.active_day_count for item in self.inner_folds)

    def as_dict(self) -> dict[str, object]:
        value = json.loads(self.canonical_bytes)
        if not isinstance(value, dict):  # pragma: no cover - canonical root is fixed
            raise BarStateSplitError("canonical state-model split is not an object")
        return value


def _span(
    dates: tuple[date, ...],
    *,
    role: str,
    start_ordinal: int,
    end_ordinal: int,
) -> BarStateDateSpan:
    if start_ordinal <= 0 or end_ordinal < start_ordinal or end_ordinal > len(dates):
        raise BarStateSplitError(f"{role} span is outside the eligible calendar")
    return BarStateDateSpan(
        role=role,
        start_date=dates[start_ordinal - 1],
        end_date=dates[end_ordinal - 1],
        start_active_ordinal=start_ordinal,
        end_active_ordinal=end_ordinal,
    )


def _active_ordinal_for_date(dates: tuple[date, ...], value: date) -> int:
    try:
        return dates.index(value) + 1
    except ValueError as error:  # pragma: no cover - outer plan derives from dates
        raise BarStateSplitError("outer boundary is absent from the eligible calendar") from error


def _required_decision_date(value: BarDateRange) -> date:
    if value.decision_end_date is None:
        raise BarStateSplitError(f"{value.split_key} is missing a decision boundary")
    return value.decision_end_date


def _inner_folds(dates: tuple[date, ...], outer: BarSplitPlan) -> tuple[BarStateInnerFold, ...]:
    blocks = outer.discovery_reporting_blocks
    if len(blocks) != 4:
        raise BarStateSplitError("V2 requires exactly four Discovery reporting blocks")
    horizon = BAR_STATE_LABEL_HORIZON_ACTIVE_DAYS
    if blocks[0].active_day_count <= horizon or any(
        item.active_day_count <= horizon for item in blocks[1:3]
    ):
        raise BarStateSplitError("Discovery reporting blocks are too short for V2 tails")

    folds: list[BarStateInnerFold] = []
    for index in range(BAR_STATE_INNER_FOLD_COUNT):
        train_end = blocks[index].end_active_ordinal - horizon
        purge_start = train_end + 1
        purge_end = blocks[index].end_active_ordinal
        oos_block = blocks[index + 1]
        if index < BAR_STATE_INNER_FOLD_COUNT - 1:
            oos_end = oos_block.end_active_ordinal - horizon
            tail_start = oos_end + 1
            tail_end = oos_block.end_active_ordinal
        else:
            oos_end = oos_block.end_active_ordinal
            tail_start = oos_end + 1
            tail_end = outer.discovery.end_active_ordinal
        folds.append(
            BarStateInnerFold(
                fold_number=index + 1,
                train=_span(dates, role="EXPANDING_TRAIN", start_ordinal=1, end_ordinal=train_end),
                purge=_span(
                    dates,
                    role="FIXED_LABEL_PURGE",
                    start_ordinal=purge_start,
                    end_ordinal=purge_end,
                ),
                oos_decisions=_span(
                    dates,
                    role="DISCOVERY_OOS_DECISIONS",
                    start_ordinal=oos_block.start_active_ordinal,
                    end_ordinal=oos_end,
                ),
                outcome_tail=_span(
                    dates,
                    role="DISCOVERY_OOS_OUTCOME_TAIL",
                    start_ordinal=tail_start,
                    end_ordinal=tail_end,
                ),
            )
        )
    return tuple(folds)


def _outer_fits(dates: tuple[date, ...], outer: BarSplitPlan) -> tuple[BarStateOuterFit, ...]:
    prior_range = outer.discovery
    fits: list[BarStateOuterFit] = []
    for fold in outer.walk_forward_folds:
        train_end = _active_ordinal_for_date(dates, _required_decision_date(prior_range))
        maturity_end = prior_range.end_active_ordinal
        decision_end = _active_ordinal_for_date(dates, _required_decision_date(fold))
        fits.append(
            BarStateOuterFit(
                fold_number=fold.fold_number or len(fits) + 1,
                training_rows_through=_span(
                    dates,
                    role="OUTER_EXPANDING_TRAIN",
                    start_ordinal=1,
                    end_ordinal=train_end,
                ),
                label_maturity_tail=_span(
                    dates,
                    role="PRIOR_LABEL_MATURITY_TAIL",
                    start_ordinal=train_end + 1,
                    end_ordinal=maturity_end,
                ),
                oos_decisions=_span(
                    dates,
                    role="SEALED_WALK_FORWARD_DECISIONS",
                    start_ordinal=fold.start_active_ordinal,
                    end_ordinal=decision_end,
                ),
                outcome_tail=_span(
                    dates,
                    role="SEALED_WALK_FORWARD_OUTCOME_TAIL",
                    start_ordinal=decision_end + 1,
                    end_ordinal=fold.end_active_ordinal,
                ),
            )
        )
        prior_range = fold
    return tuple(fits)


def plan_bar_state_splits(eligible_dates: Sequence[date]) -> BarStateSplitPlan:
    """Build the deterministic nested V2 schedule without consulting outcomes."""

    outer = plan_bar_splits(eligible_dates)
    dates = outer.eligible_dates
    if len(dates) != BAR_STATE_EXPECTED_ELIGIBLE_ACTIVE_DAYS:
        raise BarStateSplitError("V2 requires exactly 1,413 eligible active days")
    inner = _inner_folds(dates, outer)
    discovery_decision_end = _active_ordinal_for_date(
        dates, _required_decision_date(outer.discovery)
    )
    discovery_final_fit = _span(
        dates,
        role="DISCOVERY_FINAL_REFIT_ROWS",
        start_ordinal=1,
        end_ordinal=discovery_decision_end,
    )
    discovery_final_tail = _span(
        dates,
        role="DISCOVERY_FINAL_LABEL_MATURITY_TAIL",
        start_ordinal=discovery_decision_end + 1,
        end_ordinal=outer.discovery.end_active_ordinal,
    )
    outer_fits = _outer_fits(dates, outer)
    last_fold = outer.walk_forward_folds[-1]
    final_training_end = _active_ordinal_for_date(dates, _required_decision_date(last_fold))
    holdout_fit = BarStateHoldoutFit(
        training_rows_through=_span(
            dates,
            role="FINAL_PRE_HOLDOUT_TRAIN",
            start_ordinal=1,
            end_ordinal=final_training_end,
        ),
        label_maturity_tail=_span(
            dates,
            role="FINAL_PRE_HOLDOUT_LABEL_MATURITY_TAIL",
            start_ordinal=final_training_end + 1,
            end_ordinal=last_fold.end_active_ordinal,
        ),
        embargo=_span(
            dates,
            role="SEALED_HOLDOUT_EMBARGO",
            start_ordinal=outer.embargo.start_active_ordinal,
            end_ordinal=outer.embargo.end_active_ordinal,
        ),
        holdout_decisions=_span(
            dates,
            role="SEALED_HOLDOUT_DECISIONS",
            start_ordinal=outer.holdout.start_active_ordinal,
            end_ordinal=outer.holdout.end_active_ordinal,
        ),
        holdout_outcome_tail=_span(
            dates,
            role="SEALED_HOLDOUT_OUTCOME_TAIL",
            start_ordinal=outer.outcome_tail.start_active_ordinal,
            end_ordinal=outer.outcome_tail.end_active_ordinal,
        ),
    )
    document = {
        "authorized_stage": "DISCOVERY_ONLY",
        "discovery_final_fit": discovery_final_fit.as_dict(),
        "discovery_final_label_tail": discovery_final_tail.as_dict(),
        "discovery_oos_active_day_count": sum(
            item.oos_decisions.active_day_count for item in inner
        ),
        "eligible_active_day_count": len(dates),
        "eligible_end_date": dates[-1].isoformat(),
        "eligible_start_date": dates[0].isoformat(),
        "holdout_fit": holdout_fit.as_dict(),
        "inner_folds": [item.as_dict() for item in inner],
        "outer_fits": [item.as_dict() for item in outer_fits],
        "outer_split_plan_sha256": outer.sha256,
        "policy": {
            "coefficient_refit": "EXPANDING_PRIOR_MATURED_ROWS_ONLY",
            "embargo_fit_policy": "NEVER_TRAIN_SELECT_OR_CALIBRATE",
            "feature_warmup_may_use_prior_rows": True,
            "hyperparameter_refit_allowed": False,
            "label_horizon_active_days": BAR_STATE_LABEL_HORIZON_ACTIVE_DAYS,
            "label_policy": "SPLIT_INDEPENDENT_CHRONOLOGICAL_LABEL",
            "label_purge_policy": "FIXED_HORIZON_NOT_REALIZED_EARLY_HIT",
            "oos_portfolio_boundary_policy": "TERMINAL_EXIT_AT_SPLIT_END",
            "result_visibility": "BATCH_ONLY_AFTER_ALL_RELEVANT_FOLDS_COMPLETE",
        },
        "schema": BAR_STATE_SPLIT_SCHEMA,
    }
    canonical = canonical_json_bytes(document)
    return BarStateSplitPlan(
        eligible_dates=dates,
        outer_plan=outer,
        inner_folds=inner,
        discovery_final_fit=discovery_final_fit,
        discovery_final_label_tail=discovery_final_tail,
        outer_fits=outer_fits,
        holdout_fit=holdout_fit,
        canonical_bytes=canonical,
        sha256=canonical_sha256(document),
    )


def require_frozen_bar_state_split(plan: BarStateSplitPlan) -> None:
    """Fail closed unless a plan matches both frozen outer and nested identities."""

    if not isinstance(plan, BarStateSplitPlan):
        raise BarStateSplitError("state-model split plan has the wrong type")
    if plan.outer_plan.sha256 != BAR_STATE_OUTER_SPLIT_SHA256:
        raise BarStateSplitError("state-model outer split differs from the frozen identity")
    if plan.sha256 != BAR_STATE_FROZEN_SPLIT_SHA256:
        raise BarStateSplitError("state-model nested split differs from the frozen identity")


def frozen_bar_state_bootstrap_evaluation_calendar(
    plan: BarStateSplitPlan,
) -> dict[str, object]:
    """Project the exact three fold-local bootstrap calendars from a frozen plan."""

    require_frozen_bar_state_split(plan)
    folds = [
        {
            "active_date_count": (
                fold.outcome_tail.end_active_ordinal - fold.oos_decisions.start_active_ordinal + 1
            ),
            "active_dates": [
                value.isoformat()
                for value in plan.eligible_dates[
                    fold.oos_decisions.start_active_ordinal
                    - 1 : fold.outcome_tail.end_active_ordinal
                ]
            ],
            "fold_key": f"discovery_inner_{fold.fold_number}",
        }
        for fold in plan.inner_folds
    ]
    document: dict[str, object] = {
        "evaluation_calendar": "OOS_DECISIONS_PLUS_20_ACTIVE_DAY_OUTCOME_TAIL",
        "folds": folds,
        "nested_split_plan_sha256": BAR_STATE_FROZEN_SPLIT_SHA256,
        "outer_split_plan_sha256": BAR_STATE_OUTER_SPLIT_SHA256,
        "schema": BAR_STATE_BOOTSTRAP_EVALUATION_CALENDAR_SCHEMA,
    }
    if tuple(len(item["active_dates"]) for item in folds) != (117, 117, 137):
        raise BarStateSplitError("bootstrap evaluation calendars differ from 117/117/137")
    if canonical_sha256(document) != BAR_STATE_FROZEN_BOOTSTRAP_EVALUATION_CALENDAR_SHA256:
        raise BarStateSplitError("bootstrap evaluation calendar identity drifted")
    return document
