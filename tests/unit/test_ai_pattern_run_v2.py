from __future__ import annotations

from pathlib import Path

import pytest

from systematic_fx import cli
from systematic_fx.research.ai_pattern_config_v2 import (
    AIPatternDiscoveryConfigV2,
    load_ai_pattern_discovery_config_v2,
)
from systematic_fx.research.ai_pattern_discovery import ProposalLedger
from systematic_fx.research.ai_pattern_run_v2 import _start_after_precommit

ROOT = Path(__file__).resolve().parents[2]


def test_cli_runs_corrected_batch_and_can_verify_both_immutable_batches() -> None:
    parser = cli.build_parser()
    run = parser.parse_args(["research", "ai-pattern", "run", "--json"])
    verify_v1 = parser.parse_args(["research", "ai-pattern", "verify", "--batch", "1"])
    verify_v2 = parser.parse_args(["research", "ai-pattern", "verify", "--batch", "2"])

    assert run.ai_pattern_action == "run"
    assert not hasattr(run, "database_url")
    assert verify_v1.batch == 1
    assert verify_v2.batch == 2


def test_v2_source_open_occurs_only_after_directional_precommit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = load_ai_pattern_discovery_config_v2(ROOT)
    observed: list[str] = []

    def load(_: Path) -> object:
        events = ProposalLedger(tmp_path / "ledger").verify()
        observed.extend(event.event_type for event in events)
        assert events[0].request_sha256 != config.request.sha256
        assert events[0].payload["request_artifact"]["artifact_type"] == (
            "AI_PATTERN_DIRECTIONAL_PROPOSAL_REQUEST"
        )
        raise RuntimeError("deliberate v2 source stop")

    monkeypatch.setattr("systematic_fx.research.ai_pattern_run_v2.load_ai_discovery_context", load)
    with pytest.raises(RuntimeError, match="deliberate"):
        _start_after_precommit(project_root=ROOT, run_root=tmp_path, config=config)

    assert observed == ["PRECOMMITTED"]
    assert [event.event_type for event in ProposalLedger(tmp_path / "ledger").verify()] == [
        "PRECOMMITTED",
        "FAILED",
    ]


def test_checked_in_v2_config_binds_directional_catalog_and_correction() -> None:
    config: AIPatternDiscoveryConfigV2 = load_ai_pattern_discovery_config_v2(ROOT)

    assert config.request.candidate_evaluation_budget == 560
    assert config.request.proposal_budget == 12
    assert config.envelope.as_dict()["derivation"]["rejected_candidate_count"] == 60
    assert config.envelope.as_dict()["execution_prohibited"]["performance_evaluation"] is True
