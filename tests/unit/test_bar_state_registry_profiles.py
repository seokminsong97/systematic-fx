from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest

from systematic_fx.db.bar_state_registry import (
    BAR_STATE_ARTIFACT_SCHEMA_BY_KIND,
    BAR_STATE_BAR_DATASET_MANIFEST_SHA256,
    BAR_STATE_COST_VERSION,
    BAR_STATE_DATASET_KEY,
    BAR_STATE_EXECUTION_VERSION,
    BAR_STATE_FEATURE_VERSION,
    BAR_STATE_OUTCOME_VERSION,
    BAR_STATE_RAW_SOURCE_MANIFEST_SHA256,
    BarStatePredecessorGateReport,
    BarStateRegistryDriftError,
    BarStateRegistryError,
    BarStateRegistryStateError,
    _require_clean_bar_state_predecessor_connection,
)
from systematic_fx.research.bar_state_config import (
    BAR_STATE_V2_PROFILE,
    BAR_STATE_V2A_PROFILE,
)


class _Result:
    def __init__(self, row: dict[str, Any]) -> None:
        self._row = row

    def fetchone(self) -> dict[str, Any]:
        return self._row


class _Connection:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, query: str, parameters: tuple[object, ...]) -> _Result:
        self.calls.append((query, parameters))
        return _Result(self._rows.pop(0))


def _predecessor_rows() -> list[dict[str, Any]]:
    identity = {
        "campaign_id": 41,
        "campaign_key": BAR_STATE_V2_PROFILE.campaign_key,
        "name": BAR_STATE_V2_PROFILE.campaign_name,
        "status": "FROZEN",
        "data_manifest_sha256": BAR_STATE_BAR_DATASET_MANIFEST_SHA256,
        "feature_version": BAR_STATE_FEATURE_VERSION,
        "outcome_version": BAR_STATE_OUTCOME_VERSION,
        "cost_model_version": BAR_STATE_COST_VERSION,
        "execution_model_version": BAR_STATE_EXECUTION_VERSION,
        "code_commit": BAR_STATE_V2A_PROFILE.predecessor_code_commit,
        "config_sha256": BAR_STATE_V2_PROFILE.campaign_definition_sha256,
        "trial_budget": 12,
        "finalist_budget": 4,
        "frozen_at": object(),
        "holdout_revealed_at": None,
        "closed_at": None,
        "dataset_key": BAR_STATE_DATASET_KEY,
        "raw_manifest_sha256": BAR_STATE_RAW_SOURCE_MANIFEST_SHA256,
        "dataset_status": "READY",
        "experiment_id": 43,
        "experiment_key": BAR_STATE_V2_PROFILE.experiment_key,
        "experiment_status": "FROZEN",
        "primary_family": "CONDITIONAL_BAR_STATE_MODEL",
        "model_family": "ELASTIC_NET_MULTINOMIAL_LOGISTIC",
        "direction": "BOTH",
        "experiment_trial_budget": 12,
        "trials_registered": 12,
        "experiment_frozen_at": object(),
        "completed_at": None,
        "registration_artifact_type": BAR_STATE_V2_PROFILE.artifact_type,
        "registration_schema": BAR_STATE_ARTIFACT_SCHEMA_BY_KIND["REGISTRATION"],
        "registration_kind": "REGISTRATION",
        "registration_campaign_key": BAR_STATE_V2_PROFILE.campaign_key,
    }
    catalog = {
        "trial_count": 12,
        "registered_count": 12,
        "bound_count": 12,
        "invalid_binding_count": 0,
        "distinct_candidate_count": 12,
        "distinct_spec_count": 12,
        "total_experiment_spec_count": 12,
    }
    attempts = {
        "attempt_count": 12,
        "failed_count": 12,
        "exact_failed_count": 12,
        "distinct_spec_count": 12,
    }
    return [identity, catalog, attempts, {"linked_artifact_count": 0}]


def test_v2a_predecessor_gate_requires_exact_failed_v2_without_research_evidence() -> None:
    connection = _Connection(_predecessor_rows())

    report = _require_clean_bar_state_predecessor_connection(
        connection,  # type: ignore[arg-type]
        successor_profile=BAR_STATE_V2A_PROFILE,
    )

    assert report == BarStatePredecessorGateReport(
        predecessor_campaign_id=41,
        predecessor_experiment_id=43,
        candidate_count=12,
        failed_attempt_count=12,
        linked_artifact_count=0,
    )
    assert len(connection.calls) == 4
    assert connection.calls[0][1] == (
        BAR_STATE_V2_PROFILE.campaign_key,
        BAR_STATE_V2_PROFILE.experiment_key,
    )
    assert "a.started_at IS NOT NULL" in connection.calls[2][0]
    assert connection.calls[-1][1] == (41,)


@pytest.mark.parametrize(
    ("row_index", "field", "value", "error_type", "message"),
    [
        (0, "holdout_revealed_at", object(), BarStateRegistryDriftError, "drifted"),
        (0, "code_commit", "f" * 40, BarStateRegistryDriftError, "drifted"),
        (1, "invalid_binding_count", 1, BarStateRegistryDriftError, "drifted"),
        (2, "failed_count", 11, BarStateRegistryDriftError, "drifted"),
        (2, "exact_failed_count", 11, BarStateRegistryDriftError, "drifted"),
        (3, "linked_artifact_count", 1, BarStateRegistryStateError, "research evidence"),
    ],
)
def test_v2a_predecessor_gate_rejects_any_identity_lifecycle_or_evidence_drift(
    row_index: int,
    field: str,
    value: object,
    error_type: type[BarStateRegistryError],
    message: str,
) -> None:
    rows = deepcopy(_predecessor_rows())
    rows[row_index][field] = value

    with pytest.raises(error_type, match=message):
        _require_clean_bar_state_predecessor_connection(
            _Connection(rows),  # type: ignore[arg-type]
            successor_profile=BAR_STATE_V2A_PROFILE,
        )


def test_predecessor_gate_is_not_available_to_the_immutable_v2_profile() -> None:
    with pytest.raises(BarStateRegistryError, match="exact V2 predecessor gate"):
        _require_clean_bar_state_predecessor_connection(
            _Connection([]),  # type: ignore[arg-type]
            successor_profile=BAR_STATE_V2_PROFILE,
        )
