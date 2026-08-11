from __future__ import annotations

import json
from dataclasses import replace
from datetime import date, timedelta
from fractions import Fraction

import pytest

from systematic_fx.research.bar_state_features import (
    MORPHOLOGY_FEATURE_NAMES,
    BarStateFeatureRow,
)
from systematic_fx.research.bar_state_labels import (
    BarStateLabel,
    StateCensorReason,
    StatePathLabel,
)
from systematic_fx.research.bar_state_model import (
    BAR_STATE_V2_MODEL_HYPERPARAMETERS,
    BAR_STATE_V2A_MODEL_HYPERPARAMETERS,
    BarStateModelError,
    BarStateModelHyperparameters,
    CanonicalBarStateModel,
    StateTradeDecision,
    classify_state_probabilities,
    fit_bar_state_model,
)


def _training_data() -> tuple[tuple[BarStateFeatureRow, ...], tuple[BarStateLabel, ...]]:
    rows = []
    labels = []
    first = date(2022, 1, 3)
    classes = (
        StatePathLabel.UP_FIRST,
        StatePathLabel.DOWN_FIRST,
        StatePathLabel.CENSORED,
    )
    for index in range(90):
        label = classes[index % 3]
        center = {StatePathLabel.UP_FIRST: 2.0, StatePathLabel.DOWN_FIRST: -2.0}.get(label, 0.0)
        start = index * 300_000_000_000
        row = BarStateFeatureRow(
            feature_set_id="MORPHOLOGY",
            feature_names=MORPHOLOGY_FEATURE_NAMES,
            timeframe_seconds=300,
            segment_id=1,
            contract="6EH2",
            source_date=first + timedelta(days=index // 10),
            signal_start_ns=start,
            decision_ns=start + 300_000_000_000,
            atr_true_range_sum_ticks=480,
            volatility_ticks=24,
            values=(
                center + index / 10_000,
                center / 2,
                1.0 + index % 5 / 10,
                0.1,
                0.2,
                (index % 7) / 7,
            ),
        )
        censor = StateCensorReason.NO_TOUCH if label is StatePathLabel.CENSORED else None
        bound = BarStateLabel(
            label=label,
            timeframe_seconds=300,
            segment_id=1,
            contract="6EH2",
            signal_start_ns=start,
            decision_ns=start + 300_000_000_000,
            entry_path_id=1,
            entry_path_index=index,
            entry_signal_bar_start_ns=start + 300_000_000_000,
            entry_signal_bar_end_ns=start + 600_000_000_000,
            entry_start_ns=start + 300_000_000_000,
            entry_price_ticks=20_000,
            volatility_ticks=24,
            upper_barrier_ticks=20_024,
            lower_barrier_ticks=19_976,
            upper_hit_path_index=index if label is StatePathLabel.UP_FIRST else None,
            lower_hit_path_index=index if label is StatePathLabel.DOWN_FIRST else None,
            terminal_path_index=index,
            terminal_start_ns=start + 300_000_000_000,
            horizon_start_date=first,
            horizon_terminal_date=first + timedelta(days=19),
            path_truncated_before_horizon=False,
            censor_reason=censor,
        )
        rows.append(row)
        labels.append(bound)
    return tuple(rows), tuple(labels)


def test_model_fit_is_byte_deterministic_and_json_round_trips_without_pickle() -> None:
    rows, labels = _training_data()
    first = fit_bar_state_model(rows, labels, model_id="model_a")
    second = fit_bar_state_model(rows, labels, model_id="model_a")

    assert first.canonical_bytes == second.canonical_bytes
    assert first.sha256 == second.sha256
    assert json.loads(first.canonical_bytes)["artifact_encoding"] == (
        "CANONICAL_JSON_FLOAT_HEX_NO_PICKLE"
    )
    loaded = CanonicalBarStateModel.from_canonical_bytes(first.canonical_bytes)
    assert loaded == first
    assert loaded.predict_probabilities(rows[0].values) == first.predict_probabilities(
        rows[0].values
    )


def test_v2a_fit_and_decoder_require_the_explicit_expected_hyperparameters() -> None:
    rows, labels = _training_data()
    model = fit_bar_state_model(
        rows,
        labels,
        model_id="model_v2a",
        hyperparameters=BAR_STATE_V2A_MODEL_HYPERPARAMETERS,
    )

    assert model.hyperparameters is BAR_STATE_V2A_MODEL_HYPERPARAMETERS
    assert model.as_dict()["hyperparameters"]["max_iter"] == 50_000
    loaded = CanonicalBarStateModel.from_canonical_bytes(
        model.canonical_bytes,
        expected_hyperparameters=BAR_STATE_V2A_MODEL_HYPERPARAMETERS,
    )
    assert loaded == model
    with pytest.raises(BarStateModelError, match="hyperparameters drifted"):
        CanonicalBarStateModel.from_canonical_bytes(model.canonical_bytes)

    predecessor = fit_bar_state_model(rows, labels, model_id="model_v2")
    with pytest.raises(BarStateModelError, match="hyperparameters drifted"):
        CanonicalBarStateModel.from_canonical_bytes(
            predecessor.canonical_bytes,
            expected_hyperparameters=BAR_STATE_V2A_MODEL_HYPERPARAMETERS,
        )


def test_v2a_model_policy_diff_is_only_the_positive_iteration_cap() -> None:
    predecessor = BAR_STATE_V2_MODEL_HYPERPARAMETERS.as_dict()
    successor = BAR_STATE_V2A_MODEL_HYPERPARAMETERS.as_dict()

    assert predecessor.pop("max_iter") == 5_000
    assert successor.pop("max_iter") == 50_000
    assert successor == predecessor
    assert BarStateModelHyperparameters(max_iter=50_000) == (BAR_STATE_V2A_MODEL_HYPERPARAMETERS)
    with pytest.raises(BarStateModelError, match="positive integer"):
        BarStateModelHyperparameters(max_iter=0)
    with pytest.raises(BarStateModelError, match="positive integer"):
        BarStateModelHyperparameters(max_iter=True)


def test_canonical_model_loader_rejects_unknown_or_drifted_fields() -> None:
    rows, labels = _training_data()
    model = fit_bar_state_model(rows, labels, model_id="model_a")
    document = json.loads(model.canonical_bytes)
    document["forged_extension"] = True
    forged = (
        json.dumps(document, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode(
            "ascii"
        )
        + b"\n"
    )
    with pytest.raises(BarStateModelError, match="frozen schema"):
        CanonicalBarStateModel.from_canonical_bytes(forged)

    document.pop("forged_extension")
    document["artifact_encoding"] = "EXECUTABLE_STATE"
    forged = (
        json.dumps(document, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode(
            "ascii"
        )
        + b"\n"
    )
    with pytest.raises(BarStateModelError, match="encoding"):
        CanonicalBarStateModel.from_canonical_bytes(forged)


def test_model_rejects_shuffled_feature_label_identity() -> None:
    rows, labels = _training_data()
    shuffled = (labels[1], labels[0], *labels[2:])
    with pytest.raises(BarStateModelError, match="identities differ"):
        fit_bar_state_model(rows, shuffled, model_id="bad")

    with pytest.raises(BarStateModelError, match="chronologically unique"):
        fit_bar_state_model((rows[1], rows[0], *rows[2:]), labels, model_id="bad")


def test_probability_margin_rule_is_exact_and_censored_reduces_trading() -> None:
    assert (
        classify_state_probabilities((0.55, 0.40, 0.05), margin=Fraction(3, 20)).decision
        is StateTradeDecision.LONG
    )
    assert (
        classify_state_probabilities((0.30, 0.45, 0.25), margin=Fraction(1, 10)).decision
        is StateTradeDecision.SHORT
    )
    assert (
        classify_state_probabilities((0.35, 0.30, 0.35), margin=Fraction(1, 20)).decision
        is StateTradeDecision.NO_TRADE
    )


def test_model_artifact_tamper_is_rejected() -> None:
    rows, labels = _training_data()
    model = fit_bar_state_model(rows, labels, model_id="model_a")
    with pytest.raises(BarStateModelError, match="canonical"):
        CanonicalBarStateModel.from_canonical_bytes(model.canonical_bytes.replace(b"\n", b""))
    assert replace(model, model_id="model_b").sha256 != model.sha256
