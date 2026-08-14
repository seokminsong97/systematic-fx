from __future__ import annotations

import inspect
import tomllib
from pathlib import Path

import pytest

from scripts.ai_pattern_exhaustive_search_config import (
    AI_PATTERN_EXHAUSTIVE_CONFIG_RELATIVE_PATH,
    AIPatternExhaustiveConfigError,
    expected_ai_pattern_exhaustive_contract,
    load_ai_pattern_exhaustive_config,
    render_ai_pattern_exhaustive_toml_template,
)
from scripts.ai_pattern_exhaustive_search_run import (
    precommit_ai_pattern_exhaustive_search,
    run_ai_pattern_exhaustive_search,
    verify_ai_pattern_exhaustive_search,
)


def test_template_freezes_518_family_fingerprints_and_all_masks_barrier() -> None:
    document = tomllib.loads(render_ai_pattern_exhaustive_toml_template())
    expected = expected_ai_pattern_exhaustive_contract()

    for key, value in expected.items():
        assert document[key] == value
    assert document["catalog"] == {
        "batch_manifest_sha256": (
            "022af03de649f829b5ae44f58c840bea05440bda36d7eeca9d5fc6d33fb0f322"
        ),
        "candidate_catalog_count": 560,
        "candidate_catalog_sha256": (
            "01653f2faacd50b62552e649152ce3baf965aa45ca70fdaae6871d0ab75b0f71"
        ),
        "family_count": 518,
        "family_pattern_sha256": (
            "e269800244d62c346497dbbcdfdda540eb361f7273027f387fbc2efe27db4d59"
        ),
        "initial_evaluated_count": 12,
        "minimum_session_count": 80,
        "minimum_stability_ppm": 0,
        "minimum_support_rows": 500,
        "remaining_assessment_catalog_sha256": (
            "088c35d2b6781b74e058aa1eef4be8a87a7818e3a5b42d1bd000fb3883d36c3b"
        ),
        "remaining_count": 506,
        "remaining_pattern_sha_list_sha256": (
            "f34c5b2e6189136e758cc6f441622d6b2e417046f580b42a75bf367432aa77d3"
        ),
        "selected_pattern_order_sha256": (
            "ad128cef2cb2ee5797cc85d987d3cf2145566ac397059fdef2e46f02551a95d0"
        ),
        "selected_proposal_order_sha256": (
            "b33301df855fb4528044446cdf3e1f42b1f4007872bbc02fb68ed59387852956"
        ),
    }
    assert document["lifecycle"]["all_43_masks_frozen_before_first_new_one_second_loader"]
    assert document["scope"]["fresh_preregistered_or_oos_claim"] is False
    assert document["code_commit"].startswith("PENDING_")


def test_loader_rejects_uncommitted_pending_template(tmp_path: Path) -> None:
    path = tmp_path / AI_PATTERN_EXHAUSTIVE_CONFIG_RELATIVE_PATH
    path.parent.mkdir(parents=True)
    path.write_text(render_ai_pattern_exhaustive_toml_template(), encoding="utf-8")

    with pytest.raises(AIPatternExhaustiveConfigError, match="code_commit"):
        load_ai_pattern_exhaustive_config(tmp_path)


def test_public_entry_points_accept_only_project_root() -> None:
    assert tuple(inspect.signature(precommit_ai_pattern_exhaustive_search).parameters) == (
        "project_root",
    )
    assert tuple(inspect.signature(run_ai_pattern_exhaustive_search).parameters) == (
        "project_root",
    )
    assert tuple(inspect.signature(verify_ai_pattern_exhaustive_search).parameters) == (
        "project_root",
    )
