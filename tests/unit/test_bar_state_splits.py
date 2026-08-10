from __future__ import annotations

from datetime import date, timedelta

import pytest

from systematic_fx.validation.bar_state_splits import (
    BAR_STATE_FROZEN_SPLIT_SHA256,
    BarStateSplitError,
    plan_bar_state_splits,
    require_frozen_bar_state_split,
)


def _dates(count: int) -> tuple[date, ...]:
    first = date(2022, 1, 3)
    return tuple(first + timedelta(days=index) for index in range(count))


def test_nested_discovery_schedule_has_three_purged_oos_folds() -> None:
    plan = plan_bar_state_splits(_dates(1_413))

    assert [item.train.end_active_ordinal for item in plan.inner_folds] == [98, 215, 332]
    assert [item.purge.start_active_ordinal for item in plan.inner_folds] == [99, 216, 333]
    assert [item.purge.end_active_ordinal for item in plan.inner_folds] == [118, 235, 352]
    assert [item.oos_decisions.start_active_ordinal for item in plan.inner_folds] == [
        119,
        236,
        353,
    ]
    assert [item.oos_decisions.end_active_ordinal for item in plan.inner_folds] == [
        215,
        332,
        469,
    ]
    assert [item.outcome_tail.end_active_ordinal for item in plan.inner_folds] == [
        235,
        352,
        489,
    ]
    assert all(item.purge.active_day_count == 20 for item in plan.inner_folds)
    assert all(item.outcome_tail.active_day_count == 20 for item in plan.inner_folds)
    assert plan.discovery_oos_active_day_count == 311
    assert plan.discovery_final_fit.end_active_ordinal == 469
    assert plan.discovery_final_label_tail.start_active_ordinal == 470
    assert plan.discovery_final_label_tail.end_active_ordinal == 489


def test_outer_refits_use_only_prior_matured_labels() -> None:
    plan = plan_bar_state_splits(_dates(1_413))

    assert [item.training_rows_through.end_active_ordinal for item in plan.outer_fits] == [
        469,
        622,
        775,
        928,
        1_081,
    ]
    assert [item.label_maturity_tail.end_active_ordinal for item in plan.outer_fits] == [
        489,
        642,
        795,
        948,
        1_101,
    ]
    assert [item.oos_decisions.end_active_ordinal for item in plan.outer_fits] == [
        622,
        775,
        928,
        1_081,
        1_233,
    ]
    assert [item.outcome_tail.end_active_ordinal for item in plan.outer_fits] == [
        642,
        795,
        948,
        1_101,
        1_253,
    ]
    assert all(
        item.result_visibility == "SEALED_UNTIL_ALL_FIVE_FOLDS_COMPLETE" for item in plan.outer_fits
    )


def test_holdout_fit_keeps_fold_tail_and_embargo_separate() -> None:
    holdout = plan_bar_state_splits(_dates(1_413)).holdout_fit

    assert holdout.training_rows_through.end_active_ordinal == 1_233
    assert holdout.label_maturity_tail.start_active_ordinal == 1_234
    assert holdout.label_maturity_tail.end_active_ordinal == 1_253
    assert holdout.embargo.start_active_ordinal == 1_254
    assert holdout.embargo.end_active_ordinal == 1_273
    assert holdout.holdout_decisions.start_active_ordinal == 1_274
    assert holdout.holdout_decisions.end_active_ordinal == 1_393
    assert holdout.holdout_outcome_tail.start_active_ordinal == 1_394
    assert holdout.holdout_outcome_tail.end_active_ordinal == 1_413


def test_plan_is_canonical_but_synthetic_calendar_is_not_frozen_identity() -> None:
    first = plan_bar_state_splits(_dates(1_413))
    second = plan_bar_state_splits(_dates(1_413))

    assert first.canonical_bytes == second.canonical_bytes
    assert first.sha256 == second.sha256
    assert len(first.sha256) == len(BAR_STATE_FROZEN_SPLIT_SHA256) == 64
    with pytest.raises(BarStateSplitError, match="outer split"):
        require_frozen_bar_state_split(first)


@pytest.mark.parametrize("count", [740, 1_412, 1_414])
def test_v2_rejects_non_frozen_calendar_size(count: int) -> None:
    with pytest.raises(BarStateSplitError, match="exactly 1,413"):
        plan_bar_state_splits(_dates(count))
