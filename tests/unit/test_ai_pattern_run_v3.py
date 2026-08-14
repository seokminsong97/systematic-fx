from __future__ import annotations

from pathlib import Path

import pytest

from systematic_fx import cli
from systematic_fx.research.ai_pattern_config_v3 import (
    AIPatternConfigV3Error,
    load_ai_pattern_discovery_config_v3,
    proposer_implementation_sha256_v3,
    verify_committed_implementation_v3,
)
from systematic_fx.research.ai_pattern_discovery import ProposalLedger
from systematic_fx.research.ai_pattern_run_v3 import _start_after_precommit

ROOT = Path(__file__).resolve().parents[2]


def test_cli_runs_batch_three_and_keeps_historical_replay() -> None:
    parser = cli.build_parser()
    run = parser.parse_args(["research", "ai-pattern", "run", "--json"])
    verify_default = parser.parse_args(["research", "ai-pattern", "verify"])
    verify_v1 = parser.parse_args(["research", "ai-pattern", "verify", "--batch", "1"])
    verify_v2 = parser.parse_args(["research", "ai-pattern", "verify", "--batch", "2"])

    assert run.ai_pattern_action == "run"
    assert not hasattr(run, "database_url")
    assert verify_default.batch == 3
    assert verify_v1.batch == 1
    assert verify_v2.batch == 2


def test_v3_config_binds_runtime_to_reconstructible_commit() -> None:
    config = load_ai_pattern_discovery_config_v3(ROOT)

    verify_committed_implementation_v3(ROOT, config.request.code_commit)
    assert config.request.proposer_implementation_sha256 == proposer_implementation_sha256_v3(ROOT)
    assert config.request.candidate_evaluation_budget == 560
    assert config.request.proposal_budget == 12
    assert config.envelope.as_dict()["execution_prohibited"]["performance_evaluation"] is True

    with pytest.raises(AIPatternConfigV3Error, match="runtime file set|committed source"):
        verify_committed_implementation_v3(ROOT, "b6c525e3be1d772ea047beaf59c1e957a3c431a2")


def test_v3_source_open_occurs_only_after_commit_bound_precommit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = load_ai_pattern_discovery_config_v3(ROOT)
    observed: list[str] = []

    def load(_: Path) -> object:
        events = ProposalLedger(tmp_path / "ledger").verify()
        observed.extend(event.event_type for event in events)
        assert events[0].payload["request_artifact"]["artifact_type"] == (
            "AI_PATTERN_DIRECTIONAL_PROPOSAL_REQUEST"
        )
        assert events[0].request_sha256 != config.request.sha256
        raise RuntimeError("deliberate v3 source stop")

    monkeypatch.setattr("systematic_fx.research.ai_pattern_run_v3.load_ai_discovery_context", load)
    with pytest.raises(RuntimeError, match="deliberate"):
        _start_after_precommit(project_root=ROOT, run_root=tmp_path, config=config)

    assert observed == ["PRECOMMITTED"]
    assert [event.event_type for event in ProposalLedger(tmp_path / "ledger").verify()] == [
        "PRECOMMITTED",
        "FAILED",
    ]
