from __future__ import annotations

from datetime import date, timedelta

import pytest

from systematic_fx.validation.bar_splits import BarSplitError, plan_bar_splits


def _dates(count: int) -> tuple[date, ...]:
    first = date(2022, 1, 1)
    return tuple(first + timedelta(days=index) for index in range(count))


def test_minimum_calendar_has_five_72_day_walk_forward_folds() -> None:
    plan = plan_bar_splits(_dates(740))

    assert plan.discovery.active_day_count == 220
    assert plan.discovery.decision_end_date == _dates(740)[199]
    assert [item.active_day_count for item in plan.discovery_reporting_blocks] == [50] * 4
    assert [item.active_day_count for item in plan.walk_forward_folds] == [72] * 5
    assert all(item.decision_end_date is not None for item in plan.walk_forward_folds)
    assert plan.embargo.active_day_count == 20
    assert plan.holdout.active_day_count == 120
    assert plan.outcome_tail.active_day_count == 20
    assert plan.ranges[-1].end_active_ordinal == 740


def test_realistic_calendar_assigns_remainders_to_oldest_periods() -> None:
    plan = plan_bar_splits(_dates(1427))

    assert plan.discovery.active_day_count == 494
    assert [item.active_day_count for item in plan.discovery_reporting_blocks] == [
        119,
        119,
        118,
        118,
    ]
    assert [item.active_day_count for item in plan.walk_forward_folds] == [155, 155, 155, 154, 154]
    assert all(item.result_visibility == "SEALED" for item in plan.walk_forward_folds)
    assert plan.holdout.result_visibility == "SEALED"
    assert len(plan.sha256) == 64
    assert plan.as_dict()["active_day_count"] == 1427


@pytest.mark.parametrize(
    "values",
    [
        _dates(739),
        tuple(reversed(_dates(740))),
        _dates(739) + (_dates(739)[-1],),
    ],
)
def test_invalid_calendar_is_rejected(values: tuple[date, ...]) -> None:
    with pytest.raises(BarSplitError):
        plan_bar_splits(values)
