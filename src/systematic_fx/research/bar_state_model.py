"""Deterministic multinomial state model with a JSON-only coefficient artifact."""

from __future__ import annotations

import hashlib
import json
import math
import platform
import warnings
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from fractions import Fraction
from typing import Final

import numpy as np
import sklearn
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

from systematic_fx.research.bar_state_features import FEATURE_NAMES_BY_SET, BarStateFeatureRow
from systematic_fx.research.bar_state_labels import BarStateLabel, StatePathLabel

STATE_MODEL_SCHEMA: Final = "systematic_fx.bar_state_multinomial_logit.v1"
STATE_MODEL_CLASSES: Final = (
    StatePathLabel.UP_FIRST.value,
    StatePathLabel.DOWN_FIRST.value,
    StatePathLabel.CENSORED.value,
)
STATE_MODEL_DOCUMENT_KEYS: Final = frozenset(
    {
        "artifact_encoding",
        "classes",
        "coefficients_hex",
        "feature_names",
        "feature_set_id",
        "hyperparameters",
        "intercepts_hex",
        "model_id",
        "numpy_version",
        "optimizer_iterations",
        "python_version",
        "scaler_mean_hex",
        "scaler_scale_hex",
        "schema",
        "sklearn_version",
        "timeframe_seconds",
        "training_class_counts",
        "training_row_count",
        "training_rows_sha256",
    }
)
DECISION_MARGINS: Final = (Fraction(1, 20), Fraction(1, 10), Fraction(3, 20))


class BarStateModelError(ValueError):
    """Training data, model convergence, or canonical artifacts are invalid."""


class StateTradeDecision(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"
    NO_TRADE = "NO_TRADE"


@dataclass(frozen=True, slots=True)
class BarStateModelHyperparameters:
    """The frozen objective plus an explicit campaign-owned iteration cap."""

    solver: str = "saga"
    c: Fraction = Fraction(1, 10)
    l1_ratio: Fraction = Fraction(1, 2)
    class_weight: str = "balanced"
    fit_intercept: bool = True
    max_iter: int = 5_000
    tolerance: Fraction = Fraction(1, 100_000_000)
    random_state: int = 20_260_809

    def __post_init__(self) -> None:
        if self.solver != "saga":
            raise BarStateModelError("the frozen model solver must be saga")
        if self.c != Fraction(1, 10) or self.l1_ratio != Fraction(1, 2):
            raise BarStateModelError("elastic-net strength differs from the frozen model")
        if self.class_weight != "balanced" or not self.fit_intercept:
            raise BarStateModelError("class weighting/intercept differs from the frozen model")
        if (
            isinstance(self.max_iter, bool)
            or not isinstance(self.max_iter, int)
            or self.max_iter <= 0
        ):
            raise BarStateModelError("optimizer max_iter must be a positive integer")
        if self.tolerance != Fraction(1, 100_000_000) or self.random_state != 20_260_809:
            raise BarStateModelError("optimizer controls differ from the frozen model")

    def as_dict(self) -> dict[str, object]:
        return {
            "C": {"denominator": self.c.denominator, "numerator": self.c.numerator},
            "class_weight": self.class_weight,
            "fit_intercept": self.fit_intercept,
            "l1_ratio": {
                "denominator": self.l1_ratio.denominator,
                "numerator": self.l1_ratio.numerator,
            },
            "max_iter": self.max_iter,
            "n_jobs": "OMITTED_SKLEARN_1_9_NO_EFFECT",
            "penalty": "OMITTED_SKLEARN_1_9_L1_RATIO_IMPLIES_ELASTICNET",
            "random_state": self.random_state,
            "solver": self.solver,
            "tol": {
                "denominator": self.tolerance.denominator,
                "numerator": self.tolerance.numerator,
            },
        }


BAR_STATE_V2_MODEL_HYPERPARAMETERS: Final = BarStateModelHyperparameters()
BAR_STATE_V2A_MODEL_HYPERPARAMETERS: Final = BarStateModelHyperparameters(max_iter=50_000)
# Backward-compatible public name used by the original V2 engine and artifacts.
FROZEN_MODEL_HYPERPARAMETERS: Final = BAR_STATE_V2_MODEL_HYPERPARAMETERS


def _canonical_bytes(value: object) -> bytes:
    try:
        return (
            json.dumps(
                value,
                allow_nan=False,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("ascii")
            + b"\n"
        )
    except (TypeError, ValueError) as error:
        raise BarStateModelError("model artifact is not canonical JSON") from error


def _float_hex_matrix(values: Sequence[Sequence[float]]) -> list[list[str]]:
    return [[float(value).hex() for value in row] for row in values]


def _decode_hex_vector(values: object, *, label: str) -> tuple[float, ...]:
    if not isinstance(values, list) or not values:
        raise BarStateModelError(f"{label} must be a non-empty list")
    result: list[float] = []
    for value in values:
        if not isinstance(value, str):
            raise BarStateModelError(f"{label} values must be hexadecimal strings")
        try:
            decoded = float.fromhex(value)
        except ValueError as error:
            raise BarStateModelError(f"{label} contains invalid hexadecimal float") from error
        if not math.isfinite(decoded):
            raise BarStateModelError(f"{label} contains a non-finite float")
        if decoded.hex() != value:
            raise BarStateModelError(f"{label} float encoding is not canonical")
        result.append(decoded)
    return tuple(result)


@dataclass(frozen=True, slots=True)
class BarStatePrediction:
    censored_probability: float
    down_first_probability: float
    up_first_probability: float
    directional_score: float
    margin: Fraction
    decision: StateTradeDecision

    def __post_init__(self) -> None:
        probabilities = (
            self.censored_probability,
            self.down_first_probability,
            self.up_first_probability,
        )
        if any(not math.isfinite(value) or value < 0 or value > 1 for value in probabilities):
            raise BarStateModelError("prediction probabilities are invalid")
        if not math.isclose(sum(probabilities), 1.0, rel_tol=1e-12, abs_tol=1e-12):
            raise BarStateModelError("prediction probabilities do not sum to one")
        if self.margin not in DECISION_MARGINS:
            raise BarStateModelError("prediction margin is outside the frozen grid")


@dataclass(frozen=True, slots=True)
class CanonicalBarStateModel:
    """Portable StandardScaler and multinomial coefficients; never pickle."""

    model_id: str
    timeframe_seconds: int
    feature_set_id: str
    feature_names: tuple[str, ...]
    classes: tuple[str, ...]
    scaler_mean: tuple[float, ...]
    scaler_scale: tuple[float, ...]
    coefficients: tuple[tuple[float, ...], ...]
    intercepts: tuple[float, ...]
    training_row_count: int
    training_class_counts: tuple[tuple[str, int], ...]
    training_rows_sha256: str
    sklearn_version: str
    numpy_version: str
    python_version: str
    optimizer_iterations: tuple[int, ...]
    hyperparameters: BarStateModelHyperparameters = FROZEN_MODEL_HYPERPARAMETERS

    def __post_init__(self) -> None:
        if not self.model_id:
            raise BarStateModelError("model_id must be non-empty")
        if self.timeframe_seconds not in {300, 1_800}:
            raise BarStateModelError("model timeframe must be 5m or 30m")
        if not self.feature_names or len(self.feature_names) > 40:
            raise BarStateModelError("model feature_names are invalid")
        if (
            self.feature_set_id not in FEATURE_NAMES_BY_SET
            or self.feature_names != FEATURE_NAMES_BY_SET[self.feature_set_id]
        ):
            raise BarStateModelError("model feature set differs from the frozen schema")
        if self.classes != STATE_MODEL_CLASSES:
            raise BarStateModelError("model classes differ from the frozen order")
        feature_count = len(self.feature_names)
        if len(self.scaler_mean) != feature_count or len(self.scaler_scale) != feature_count:
            raise BarStateModelError("scaler width differs from feature_names")
        if any(value <= 0 or not math.isfinite(value) for value in self.scaler_scale):
            raise BarStateModelError("scaler scales must be positive and finite")
        if any(not math.isfinite(value) for value in self.scaler_mean):
            raise BarStateModelError("scaler means must be finite")
        if len(self.coefficients) != len(self.classes) or any(
            len(row) != feature_count for row in self.coefficients
        ):
            raise BarStateModelError("coefficient shape differs from classes/features")
        if len(self.intercepts) != len(self.classes):
            raise BarStateModelError("intercept shape differs from classes")
        if any(not math.isfinite(value) for row in self.coefficients for value in row):
            raise BarStateModelError("coefficients must be finite")
        if any(not math.isfinite(value) for value in self.intercepts):
            raise BarStateModelError("intercepts must be finite")
        if self.training_row_count <= 0 or sum(
            count for _, count in self.training_class_counts
        ) != (self.training_row_count):
            raise BarStateModelError("training class counts differ from row count")
        if tuple(label for label, _ in self.training_class_counts) != self.classes:
            raise BarStateModelError("training class counts differ from class order")
        if any(count <= 0 for _, count in self.training_class_counts):
            raise BarStateModelError("every frozen target class must have training support")
        if len(self.training_rows_sha256) != 64 or any(
            value not in "0123456789abcdef" for value in self.training_rows_sha256
        ):
            raise BarStateModelError("training row hash is invalid")
        if (
            len(self.optimizer_iterations) != len(self.classes)
            and len(self.optimizer_iterations) != 1
        ):
            raise BarStateModelError("optimizer iteration shape is invalid")
        if any(
            value <= 0 or value >= self.hyperparameters.max_iter
            for value in self.optimizer_iterations
        ):
            raise BarStateModelError("optimizer iteration values are invalid")
        if not self.sklearn_version or not self.numpy_version or not self.python_version:
            raise BarStateModelError("model runtime versions must be non-empty")

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_encoding": "CANONICAL_JSON_FLOAT_HEX_NO_PICKLE",
            "classes": list(self.classes),
            "coefficients_hex": _float_hex_matrix(self.coefficients),
            "feature_names": list(self.feature_names),
            "feature_set_id": self.feature_set_id,
            "hyperparameters": self.hyperparameters.as_dict(),
            "intercepts_hex": [value.hex() for value in self.intercepts],
            "model_id": self.model_id,
            "numpy_version": self.numpy_version,
            "optimizer_iterations": list(self.optimizer_iterations),
            "python_version": self.python_version,
            "scaler_mean_hex": [value.hex() for value in self.scaler_mean],
            "scaler_scale_hex": [value.hex() for value in self.scaler_scale],
            "schema": STATE_MODEL_SCHEMA,
            "sklearn_version": self.sklearn_version,
            "timeframe_seconds": self.timeframe_seconds,
            "training_class_counts": [
                {"count": count, "label": label} for label, count in self.training_class_counts
            ],
            "training_row_count": self.training_row_count,
            "training_rows_sha256": self.training_rows_sha256,
        }

    @property
    def canonical_bytes(self) -> bytes:
        return _canonical_bytes(self.as_dict())

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()

    def predict_probabilities(self, values: Sequence[float]) -> tuple[float, float, float]:
        if len(values) != len(self.feature_names):
            raise BarStateModelError("prediction feature width differs from model")
        vector = np.asarray(values, dtype=np.float64)
        if vector.ndim != 1 or not np.isfinite(vector).all():
            raise BarStateModelError("prediction features must be a finite vector")
        standardized = (vector - np.asarray(self.scaler_mean)) / np.asarray(self.scaler_scale)
        logits = np.asarray(self.coefficients) @ standardized + np.asarray(self.intercepts)
        logits -= np.max(logits)
        probabilities = np.exp(logits)
        probabilities /= probabilities.sum()
        return tuple(float(value) for value in probabilities)  # type: ignore[return-value]

    def predict(self, values: Sequence[float], *, margin: Fraction) -> BarStatePrediction:
        probabilities = self.predict_probabilities(values)
        return classify_state_probabilities(probabilities, margin=margin)

    @classmethod
    def from_canonical_bytes(
        cls,
        raw: bytes,
        *,
        expected_hyperparameters: BarStateModelHyperparameters = (FROZEN_MODEL_HYPERPARAMETERS),
    ) -> CanonicalBarStateModel:
        """Strictly load JSON under one explicit campaign hyperparameter policy."""

        if not isinstance(expected_hyperparameters, BarStateModelHyperparameters):
            raise BarStateModelError(
                "expected_hyperparameters must be BarStateModelHyperparameters"
            )

        if not isinstance(raw, bytes) or not raw.endswith(b"\n"):
            raise BarStateModelError("canonical model bytes must end with LF")
        try:
            document = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise BarStateModelError("canonical model bytes are invalid JSON") from error
        if not isinstance(document, dict) or document.get("schema") != STATE_MODEL_SCHEMA:
            raise BarStateModelError("canonical model schema is invalid")
        if _canonical_bytes(document) != raw:
            raise BarStateModelError("model JSON bytes are not canonical")
        if set(document) != STATE_MODEL_DOCUMENT_KEYS:
            raise BarStateModelError("canonical model fields differ from the frozen schema")
        if document.get("artifact_encoding") != "CANONICAL_JSON_FLOAT_HEX_NO_PICKLE":
            raise BarStateModelError("canonical model encoding is invalid")
        if document.get("hyperparameters") != expected_hyperparameters.as_dict():
            raise BarStateModelError("canonical model hyperparameters drifted")
        coefficients_raw = document.get("coefficients_hex")
        if not isinstance(coefficients_raw, list) or not coefficients_raw:
            raise BarStateModelError("canonical coefficients are invalid")
        coefficients = tuple(
            _decode_hex_vector(row, label="coefficients_hex") for row in coefficients_raw
        )
        counts_raw = document.get("training_class_counts")
        if not isinstance(counts_raw, list):
            raise BarStateModelError("canonical training_class_counts are invalid")
        try:
            counts = tuple((str(item["label"]), int(item["count"])) for item in counts_raw)
            model = cls(
                model_id=str(document["model_id"]),
                timeframe_seconds=int(document["timeframe_seconds"]),
                feature_set_id=str(document["feature_set_id"]),
                feature_names=tuple(str(value) for value in document["feature_names"]),
                classes=tuple(str(value) for value in document["classes"]),
                scaler_mean=_decode_hex_vector(document["scaler_mean_hex"], label="scaler_mean"),
                scaler_scale=_decode_hex_vector(document["scaler_scale_hex"], label="scaler_scale"),
                coefficients=coefficients,
                intercepts=_decode_hex_vector(document["intercepts_hex"], label="intercepts"),
                training_row_count=int(document["training_row_count"]),
                training_class_counts=counts,
                training_rows_sha256=str(document["training_rows_sha256"]),
                sklearn_version=str(document["sklearn_version"]),
                numpy_version=str(document["numpy_version"]),
                python_version=str(document["python_version"]),
                optimizer_iterations=tuple(
                    int(value) for value in document["optimizer_iterations"]
                ),
                hyperparameters=expected_hyperparameters,
            )
        except (KeyError, TypeError, ValueError) as error:
            raise BarStateModelError("canonical model fields are invalid") from error
        return model


def _bound_label_value(row: BarStateFeatureRow, value: BarStateLabel) -> str:
    if not isinstance(value, BarStateLabel):
        raise BarStateModelError("training labels must be lineage-bound BarStateLabel values")
    if (
        value.timeframe_seconds != row.timeframe_seconds
        or value.segment_id != row.segment_id
        or value.contract != row.contract
        or value.signal_start_ns != row.signal_start_ns
        or value.decision_ns != row.decision_ns
    ):
        raise BarStateModelError("training feature and label identities differ")
    return value.label.value


def _training_rows_sha256(
    rows: Sequence[BarStateFeatureRow],
    labels: Sequence[str],
) -> str:
    digest = hashlib.sha256()
    digest.update(b'{"schema":"systematic_fx.bar_state_training_rows.v1","rows":[')
    for index, (row, label) in enumerate(zip(rows, labels, strict=True)):
        if index:
            digest.update(b",")
        digest.update(
            json.dumps(
                {
                    "contract": row.contract,
                    "decision_ns": row.decision_ns,
                    "label": label,
                    "segment_id": row.segment_id,
                    "signal_start_ns": row.signal_start_ns,
                    "values_hex": [value.hex() for value in row.values],
                },
                allow_nan=False,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("ascii")
        )
    digest.update(b"]}\n")
    return digest.hexdigest()


def fit_bar_state_model(
    rows: Sequence[BarStateFeatureRow],
    labels: Sequence[BarStateLabel],
    *,
    model_id: str,
    hyperparameters: BarStateModelHyperparameters = FROZEN_MODEL_HYPERPARAMETERS,
) -> CanonicalBarStateModel:
    """Fit the sole fixed StandardScaler + elastic-net multinomial model.

    Only the supplied training rows reach ``StandardScaler.fit``.  Any warning,
    including non-convergence or a future API deprecation, fails closed so a
    silent dependency change cannot publish different model semantics.
    """

    if not isinstance(hyperparameters, BarStateModelHyperparameters):
        raise BarStateModelError("hyperparameters must be BarStateModelHyperparameters")
    if not rows or len(rows) != len(labels):
        raise BarStateModelError("training rows and labels must be non-empty and aligned")
    first = rows[0]
    if any(
        row.feature_names != first.feature_names
        or row.feature_set_id != first.feature_set_id
        or row.timeframe_seconds != first.timeframe_seconds
        for row in rows
    ):
        raise BarStateModelError("training feature schema/timeframe is heterogeneous")
    ordering = tuple((row.decision_ns, row.contract, row.signal_start_ns) for row in rows)
    if ordering != tuple(sorted(set(ordering))):
        raise BarStateModelError("training feature rows must be chronologically unique")
    label_values = tuple(
        _bound_label_value(row, value) for row, value in zip(rows, labels, strict=True)
    )
    counts = Counter(label_values)
    if set(counts) != set(STATE_MODEL_CLASSES):
        raise BarStateModelError("training data must contain all three target classes")
    matrix = np.asarray([row.values for row in rows], dtype=np.float64)
    if matrix.shape != (len(rows), len(first.feature_names)) or not np.isfinite(matrix).all():
        raise BarStateModelError("training feature matrix is invalid")
    scaler = StandardScaler(copy=True, with_mean=True, with_std=True)
    transformed = scaler.fit_transform(matrix)
    estimator = LogisticRegression(
        solver=hyperparameters.solver,
        C=float(hyperparameters.c),
        l1_ratio=float(hyperparameters.l1_ratio),
        class_weight=hyperparameters.class_weight,
        fit_intercept=hyperparameters.fit_intercept,
        max_iter=hyperparameters.max_iter,
        tol=float(hyperparameters.tolerance),
        random_state=hyperparameters.random_state,
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        estimator.fit(transformed, np.asarray(label_values))
    if caught:
        messages = "; ".join(f"{item.category.__name__}: {item.message}" for item in caught)
        raise BarStateModelError(f"model fit emitted a warning: {messages}")
    fitted_classes = tuple(str(value) for value in estimator.classes_)
    if set(fitted_classes) != set(STATE_MODEL_CLASSES):
        raise BarStateModelError("fitted classes differ from the frozen classes")
    reorder = tuple(fitted_classes.index(label) for label in STATE_MODEL_CLASSES)
    iterations = tuple(int(value) for value in estimator.n_iter_)
    if not iterations or max(iterations) >= hyperparameters.max_iter:
        raise BarStateModelError("model optimizer did not converge")
    return CanonicalBarStateModel(
        model_id=model_id,
        timeframe_seconds=first.timeframe_seconds,
        feature_set_id=first.feature_set_id,
        feature_names=first.feature_names,
        classes=STATE_MODEL_CLASSES,
        scaler_mean=tuple(float(value) for value in scaler.mean_),
        scaler_scale=tuple(float(value) for value in scaler.scale_),
        coefficients=tuple(
            tuple(float(value) for value in estimator.coef_[index]) for index in reorder
        ),
        intercepts=tuple(float(estimator.intercept_[index]) for index in reorder),
        training_row_count=len(rows),
        training_class_counts=tuple((label, counts[label]) for label in STATE_MODEL_CLASSES),
        training_rows_sha256=_training_rows_sha256(rows, label_values),
        sklearn_version=sklearn.__version__,
        numpy_version=np.__version__,
        python_version=platform.python_version(),
        optimizer_iterations=iterations,
        hyperparameters=hyperparameters,
    )


def classify_state_probabilities(
    probabilities: Sequence[float],
    *,
    margin: Fraction,
) -> BarStatePrediction:
    """Map p(UP)-p(DOWN) to LONG/SHORT/NO_TRADE at an exact margin."""

    if margin not in DECISION_MARGINS:
        raise BarStateModelError("decision margin is outside the frozen grid")
    if len(probabilities) != 3:
        raise BarStateModelError("state probabilities must contain three classes")
    up, down, censored = (float(value) for value in probabilities)
    if any(not math.isfinite(value) or value < 0 or value > 1 for value in (censored, down, up)):
        raise BarStateModelError("state probabilities are invalid")
    total = censored + down + up
    if not math.isclose(total, 1.0, rel_tol=1e-12, abs_tol=1e-12):
        raise BarStateModelError("state probabilities do not sum to one")
    score = up - down
    exact_score = Fraction.from_float(score)
    if exact_score >= margin:
        decision = StateTradeDecision.LONG
    elif exact_score <= -margin:
        decision = StateTradeDecision.SHORT
    else:
        decision = StateTradeDecision.NO_TRADE
    return BarStatePrediction(
        censored_probability=censored,
        down_first_probability=down,
        up_first_probability=up,
        directional_score=score,
        margin=margin,
        decision=decision,
    )


__all__ = [
    "BAR_STATE_V2A_MODEL_HYPERPARAMETERS",
    "BAR_STATE_V2_MODEL_HYPERPARAMETERS",
    "DECISION_MARGINS",
    "FROZEN_MODEL_HYPERPARAMETERS",
    "STATE_MODEL_CLASSES",
    "STATE_MODEL_SCHEMA",
    "BarStateModelError",
    "BarStateModelHyperparameters",
    "BarStatePrediction",
    "CanonicalBarStateModel",
    "StateTradeDecision",
    "classify_state_probabilities",
    "fit_bar_state_model",
]
