from __future__ import annotations

import json
import os
import subprocess
import sys
import tomllib
from pathlib import Path
from types import SimpleNamespace

import pytest

from campaigns.ai_all_cases_v1 import config as all_cases_config
from campaigns.ai_all_cases_v1.config import (
    AI_ALL_CASES_CONFIG_RELATIVE_PATH,
    DETERMINISTIC_RUNTIME_ENV,
    AllCasesConfigError,
    all_cases_implementation_document,
    all_cases_implementation_sha256,
    expected_ai_all_cases_contract,
    load_ai_all_cases_config,
    render_ai_all_cases_toml_template,
)


@pytest.fixture(autouse=True)
def _deterministic_runtime_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for key, value in DETERMINISTIC_RUNTIME_ENV.items():
        monkeypatch.setenv(key, value)
    for key in tuple(os.environ):
        if key.startswith(("GIT_", "DYLD_", "LD_")):
            monkeypatch.delenv(key)
    monkeypatch.setenv("PATH", all_cases_config._TRUSTED_PATH)
    monkeypatch.setenv(
        "AI_ALL_CASES_TRUSTED_GIT_PATH",
        str(all_cases_config._TRUSTED_GIT_PATH),
    )
    monkeypatch.setenv(
        "AI_ALL_CASES_TRUSTED_GIT_SHA256",
        all_cases_config._TRUSTED_GIT_SHA256,
    )
    monkeypatch.setenv(
        "AI_ALL_CASES_TRUSTED_GIT_VERSION",
        all_cases_config._TRUSTED_GIT_VERSION,
    )


def _bindings() -> dict[str, object]:
    return {
        "anchor_policy_recipe_sha256": "5" * 64,
        "catalog_summaries_canonical_json": "{}",
        "catalog_summaries_sha256": "1" * 64,
        "complete_strategy_recipe_sha256": "4" * 64,
        "direct_catalog_sha256": "6" * 64,
        "entry_catalog_sha256": "7" * 64,
        "exit_catalog_sha256": "8" * 64,
        "ml_contract_canonical_json": "{}",
        "ml_contract_sha256": "2" * 64,
        "meta_catalog_sha256": "9" * 64,
        "stage_a_chunk_plan_sha256": "a" * 64,
        "symbolic_contract_canonical_json": "{}",
        "symbolic_contract_sha256": "3" * 64,
    }


def test_template_freezes_access_order_budgets_and_invalid_provenance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(all_cases_config, "_research_bindings", _bindings)
    document = tomllib.loads(render_ai_all_cases_toml_template())
    expected = expected_ai_all_cases_contract()

    for key, value in expected.items():
        assert document[key] == value
    assert document["selection"] == {
        "holdout_family_maximum": 3,
        "search_selection_maximum": 12,
        "semantic_subset_order": "PRESERVE_FROZEN_ECONOMIC_RANK_ORDER",
        "walk_forward_fold_keys": ["WF1", "WF2", "WF3", "WF4", "WF5"],
        "walk_forward_selection_maximum": 3,
    }
    lifecycle = document["lifecycle"]
    assert lifecycle["search_outcomes_before_universe_freeze"] == "PROHIBITED"
    assert lifecycle["walk_forward_masks_before_any_walk_forward_outcome_loader"] is True
    assert lifecycle["holdout_masks_before_holdout_outcome_loader"] is True
    assert lifecycle["crash_policy"].endswith("BEFORE_HOLDOUT_AUTHORIZED")
    assert lifecycle["holdout_one_shot_error_policy"].endswith("FAILED_TERMINAL_NO_RETRY")
    assert lifecycle["zero_finalist_policy"].startswith("SKIP_ALL_LATER")
    assert document["provenance"]["dependency_provisioning"].startswith(
        "SEPARATE_UV_SYNC_LOCKED_OFFLINE"
    )
    assert document["provenance"]["public_dependency_injection"] == "PROHIBITED"
    assert document["code_commit"].startswith("PENDING_")


def test_runtime_guard_fails_before_numerical_config_loading(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PYTHONHASHSEED")
    with pytest.raises(AllCasesConfigError, match="deterministic runtime"):
        all_cases_config._require_deterministic_runtime_environment()


def test_runtime_identity_binds_environment_libraries_and_campaign_origins() -> None:
    identity = all_cases_config._runtime_identity_document()

    assert identity["environment"] == DETERMINISTIC_RUNTIME_ENV
    assert set(identity["libraries"]) == {"numpy", "scipy", "sklearn"}
    assert set(identity["campaign_module_origins"]) == {
        "campaigns",
        "campaigns.ai_all_cases_v1",
        "campaigns.ai_all_cases_v1.__main__",
        "campaigns.ai_all_cases_v1.bootstrap",
        "campaigns.ai_all_cases_v1.config",
        "campaigns.ai_all_cases_v1.ml",
        "campaigns.ai_all_cases_v1.pipeline",
        "campaigns.ai_all_cases_v1.run",
        "campaigns.ai_all_cases_v1.symbolic",
    }
    assert {
        "scripts.ai_pattern_holdout_config",
        "scripts.ai_pattern_holdout_engine",
        "systematic_fx",
        "systematic_fx.features.bars",
        "systematic_fx.research.bar_pipeline",
        "systematic_fx.research.hypotheses",
        "systematic_fx.validation.bar_splits",
    }.issubset(identity["legacy_module_origins"])
    assert identity["python"]["implementation"]
    assert identity["python"] == {
        "base_prefix": (
            "/Users/seokminsong/.local/share/uv/python/cpython-3.12.13-macos-aarch64-none"
        ),
        "executable": (
            "/Users/seokminsong/.local/share/uv/python/"
            "cpython-3.12.13-macos-aarch64-none/bin/python3.12"
        ),
        "implementation": "CPython",
        "operator_trust_boundary": ("PINNED_USER_OWNED_DISTRIBUTION_AND_LOCKED_SITE_PACKAGES"),
        "sha256": "f64cf6322e4f20cd0458aab89c0d332895817bb8f243b943109b6a957582fd5d",
        "size": 18_041_104,
        "version": "3.12.13",
    }
    assert identity["trusted_preexec"]["env_path"] == "/usr/bin/env"
    assert identity["trusted_preexec"]["env_sha256"] == (
        "6e506aec3c0cff703ac1e66cedc6f1945354ad41339a38db4425c7c88227128f"
    )
    assert identity["trusted_preexec"]["env_size"] == 102_368
    assert identity["trusted_preexec"]["operator_uid"] == 501
    assert identity["trusted_preexec"]["invocation_prefix"] == list(
        all_cases_config._production_launcher_prefix(Path(all_cases_config.__file__).parents[2])
    )
    assert identity["trusted_git"] == {
        "path": "/usr/bin/git",
        "sha256": "179301dcb41ea78accc3fa0048a7e6f6710d891945a751a34addd622020c1818",
        "size": 118_928,
        "subprocess_environment": all_cases_config._trusted_git_environment(),
        "version": "git version 2.50.1 (Apple Git-155)",
    }
    assert identity["bootstrap_policy"] == all_cases_config.TRUSTED_BOOTSTRAP_POLICY
    assert identity["bootstrap_runtime"]["pycache_prefix"].startswith("UNIQUE_EXTERNAL")
    assert identity["bootstrap_runtime"]["workspace_packages"] == [
        "EXPLICIT_SOURCE_LOADED:campaigns",
        "EXPLICIT_SOURCE_LOADED:scripts",
        "EXPLICIT_SOURCE_LOADED:systematic_fx",
    ]


def test_trusted_bootstrap_gate_is_structural_and_uses_an_external_empty_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _implementation_fixture(tmp_path)
    external = tmp_path.parent / f"{tmp_path.name}-external-pycache"
    external.mkdir(mode=0o700)
    monkeypatch.setattr(
        all_cases_config,
        "_trusted_runtime_flag_document",
        lambda: {
            "dont_write_bytecode": True,
            "hash_probe": -4_299_525_529_514_689_000,
            "hash_randomization": 0,
            "ignore_environment": 0,
            "isolated": 0,
            "no_site": 1,
            "no_user_site": 1,
            "safe_path": True,
            "utf8_mode": 1,
        },
    )
    monkeypatch.setattr(all_cases_config, "_require_trusted_preexec_runtime", lambda _root: None)
    monkeypatch.setattr(sys, "pycache_prefix", str(external))
    venv = tmp_path / ".venv"
    site = venv / f"lib/python{sys.version_info.major}.{sys.version_info.minor}/site-packages"
    site.mkdir(parents=True)
    base_paths = [str(Path(sys.base_prefix).resolve())]
    base_json = all_cases_config._canonical_json_bytes(base_paths).decode("ascii")
    monkeypatch.setattr(
        sys,
        "path",
        [*base_paths, str(site)],
    )
    for key in tuple(os.environ):
        monkeypatch.delenv(key)
    expected_environment = {
        **all_cases_config._clean_entry_environment(tmp_path),
        "AI_ALL_CASES_CLEAN_ENTRY_ENV_SHA256": (
            all_cases_config._clean_entry_environment_sha256(tmp_path)
        ),
        "AI_ALL_CASES_STDLIB_BASE_PATHS_JSON": base_json,
        "AI_ALL_CASES_STDLIB_BASE_PATHS_SHA256": __import__("hashlib")
        .sha256(base_json.encode("ascii"))
        .hexdigest(),
        "AI_ALL_CASES_TRUSTED_GIT_PATH": str(all_cases_config._TRUSTED_GIT_PATH),
        "AI_ALL_CASES_TRUSTED_GIT_SHA256": all_cases_config._TRUSTED_GIT_SHA256,
        "AI_ALL_CASES_TRUSTED_GIT_VERSION": all_cases_config._TRUSTED_GIT_VERSION,
        "PATH": all_cases_config._TRUSTED_PATH,
        "PYTHONPYCACHEPREFIX": str(external),
    }
    for key, value in expected_environment.items():
        monkeypatch.setenv(key, value)

    all_cases_config._require_trusted_bootstrap_runtime(tmp_path)
    assert sys.path[-1] == str(site)
    assert all(
        Path(item).resolve().is_relative_to(Path(sys.base_prefix).resolve())
        for item in sys.path[:-1]
    )

    sys.path.append(str(tmp_path.parent / "evil-import-root"))
    with pytest.raises(AllCasesConfigError, match="path identity"):
        all_cases_config._require_trusted_bootstrap_runtime(tmp_path)
    sys.path.pop()

    (tmp_path / "scripts/__pycache__").mkdir()
    with pytest.raises(AllCasesConfigError, match="import surface"):
        all_cases_config._require_trusted_bootstrap_runtime(tmp_path)


def test_trusted_bootstrap_gate_rejects_the_ambient_untrusted_test_process(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _implementation_fixture(tmp_path)
    monkeypatch.setattr(all_cases_config, "_require_trusted_preexec_runtime", lambda _root: None)
    with pytest.raises(AllCasesConfigError, match="deterministic hash"):
        all_cases_config._require_trusted_bootstrap_runtime(tmp_path)


def test_trusted_git_rejects_hostile_entry_before_any_environment_sanitization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from campaigns.ai_all_cases_v1 import bootstrap

    fake_root = tmp_path / "fake-bin"
    fake_root.mkdir()
    fake_git = fake_root / "git"
    fake_git.write_text("#!/bin/sh\necho forged\n", encoding="utf-8")
    fake_git.chmod(0o755)
    monkeypatch.setenv("PATH", str(fake_root))
    monkeypatch.setenv("GIT_DIR", str(tmp_path / "forged.git"))
    monkeypatch.setenv("DYLD_INSERT_LIBRARIES", str(tmp_path / "forged.dylib"))

    root = Path(bootstrap.__file__).resolve().parents[2]
    with pytest.raises(bootstrap.BootstrapError, match="entry environment"):
        bootstrap._require_clean_entry_environment(root)
    with pytest.raises(AllCasesConfigError, match="trusted Git"):
        all_cases_config._require_trusted_git_runtime()

    for key in tuple(os.environ):
        monkeypatch.delenv(key)
    for key, value in bootstrap._clean_entry_environment(root).items():
        monkeypatch.setenv(key, value)
    bootstrap._require_clean_entry_environment(root)
    bootstrap._configure_trusted_git()
    assert os.environ["PATH"] == "/usr/bin:/bin"
    assert "GIT_DIR" not in os.environ
    assert "DYLD_INSERT_LIBRARIES" not in os.environ
    assert os.environ["AI_ALL_CASES_TRUSTED_GIT_PATH"] == "/usr/bin/git"
    all_cases_config._require_trusted_git_runtime()


def test_pinned_env_i_launcher_accepts_only_the_exact_clean_entry_environment() -> None:
    from campaigns.ai_all_cases_v1 import bootstrap

    root = Path(bootstrap.__file__).resolve().parents[2]
    command = [
        *all_cases_config._production_launcher_prefix(root),
        "template",
        "--project-root",
        str(root),
        "--json",
    ]
    clean = subprocess.run(command, check=False, capture_output=True, stdin=subprocess.DEVNULL)
    assert clean.returncode == 0, clean.stderr.decode("utf-8", errors="replace")
    assert (
        'schema_version = "systematic_fx.ai_all_cases_config.v1"'
        in json.loads(clean.stdout)["toml"]
    )

    hostile_environment = bootstrap._clean_entry_environment(root)
    hostile_environment["GIT_DIR"] = str(root / "forged.git")
    hostile_environment["DYLD_LIBRARY_PATH"] = str(root / "forged-dylib")
    hostile_environment["PYTHONPATH"] = str(root / "forged-pythonpath")
    hostile = subprocess.run(
        [
            str(all_cases_config._TRUSTED_PYTHON_PATH),
            "-s",
            "-P",
            "-B",
            "-S",
            str(root / all_cases_config.TRUSTED_BOOTSTRAP_RELATIVE_PATH),
            "template",
            "--project-root",
            str(root),
        ],
        check=False,
        capture_output=True,
        env=hostile_environment,
        stdin=subprocess.DEVNULL,
    )
    assert hostile.returncode != 0
    assert b"entry environment differs" in hostile.stderr


def test_stdlib_bootstrap_cache_scan_runs_before_workspace_import(tmp_path: Path) -> None:
    from campaigns.ai_all_cases_v1 import bootstrap

    _implementation_fixture(tmp_path)
    assert bootstrap._local_bytecode_entries(tmp_path) == ()
    cache = tmp_path / "campaigns/__pycache__"
    cache.mkdir()
    assert bootstrap._local_bytecode_entries(tmp_path) == (cache,)


@pytest.mark.parametrize("unsafe", ("scripts/native.so", "scripts/linked.py"))
def test_stdlib_bootstrap_rejects_every_non_source_import_surface(
    tmp_path: Path,
    unsafe: str,
) -> None:
    from campaigns.ai_all_cases_v1 import bootstrap

    _implementation_fixture(tmp_path)
    path = tmp_path / unsafe
    if path.name == "linked.py":
        path.symlink_to(tmp_path / "scripts/helper.py")
    else:
        path.write_bytes(b"native")
    assert bootstrap._local_bytecode_entries(tmp_path) == (path,)


def test_bootstrap_and_provenance_reject_campaign_siblings_and_hidden_ancestor_symlink(
    tmp_path: Path,
) -> None:
    from campaigns.ai_all_cases_v1 import bootstrap

    sibling_root = tmp_path / "sibling"
    sibling_root.mkdir()
    _implementation_fixture(sibling_root)
    sibling = sibling_root / "campaigns/evil.py"
    sibling.write_text("raise RuntimeError\n", encoding="utf-8")
    assert bootstrap._local_bytecode_entries(sibling_root) == (sibling,)
    with pytest.raises(AllCasesConfigError, match="unbound import sibling"):
        all_cases_implementation_document(sibling_root)

    linked_root = tmp_path / "linked"
    linked_root.mkdir()
    _implementation_fixture(linked_root)
    external_src = tmp_path / "external-src"
    (linked_root / "src").rename(external_src)
    (linked_root / "src").symlink_to(external_src, target_is_directory=True)
    with pytest.raises(bootstrap.BootstrapError, match="unsafe component"):
        bootstrap._local_bytecode_entries(linked_root)
    with pytest.raises(AllCasesConfigError, match="unsafe component"):
        all_cases_implementation_document(linked_root)


def test_stdlib_bootstrap_requires_exact_absolute_nonsymbolic_argv0(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from campaigns.ai_all_cases_v1 import bootstrap

    root = Path(bootstrap.__file__).resolve().parents[2]
    monkeypatch.setattr(sys, "argv", ["campaigns/ai_all_cases_v1/bootstrap.py"])
    with pytest.raises(bootstrap.BootstrapError, match="missing|differ"):
        bootstrap._project_root_from_argv(("template", "--project-root", str(root)))

    linked = tmp_path / "bootstrap.py"
    linked.symlink_to(Path(bootstrap.__file__).resolve())
    monkeypatch.setattr(sys, "argv", [str(linked)])
    with pytest.raises(bootstrap.BootstrapError, match="differ"):
        bootstrap._project_root_from_argv(("template", "--project-root", str(root)))


def test_runtime_identity_bytes_do_not_depend_on_unrelated_import_history(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = all_cases_config._canonical_json_bytes(all_cases_config._runtime_identity_document())
    origin = Path(all_cases_config.__file__).resolve().parents[2] / (
        "src/systematic_fx/__init__.py"
    )
    monkeypatch.setitem(
        sys.modules,
        "systematic_fx.unrelated_committed_module",
        SimpleNamespace(__spec__=SimpleNamespace(origin=str(origin))),
    )
    after = all_cases_config._canonical_json_bytes(all_cases_config._runtime_identity_document())
    assert after == before


def test_runtime_origin_guard_rejects_a_different_checkout(tmp_path: Path) -> None:
    with pytest.raises(AllCasesConfigError, match="origin|incomplete"):
        all_cases_config._runtime_module_origins(tmp_path)


def test_real_symbolic_ml_bindings_and_full_scientific_counts_are_exact() -> None:
    document = expected_ai_all_cases_contract()
    bindings = document["bindings"]
    symbolic = json.loads(bindings["symbolic_contract_canonical_json"])
    ml = json.loads(bindings["ml_contract_canonical_json"])
    catalogs = json.loads(bindings["catalog_summaries_canonical_json"])

    assert symbolic["axes"]["logical_anchor_policy_count"] == 1_900_080
    assert symbolic["axes"]["reference_score_cell_count"] == 9_500_400
    assert symbolic["stage_a_selection"]["maximum"] == 256
    assert catalogs["entry"]["candidate_count"] == 9
    assert catalogs["exit"]["candidate_count"] == 85
    assert (
        bindings["complete_strategy_recipe_sha256"]
        == symbolic["axes"]["complete_strategy_recipe_sha256"]
        == document["execution"]["entry_exit_recipe_sha256"]
    )
    assert ml["catalogs"]["direct_count"] == 288
    assert ml["catalogs"]["meta_count"] == 192
    assert ml["direct_row_execution"] == {
        "decision_date": "VERIFIED_NATIVE_BAR_SOURCE_DATE",
        "decision_ns": "EXACT_FEATURE_CUTOFF_CLOCK_COMPLETED_NATIVE_BAR_END",
        "economics": {
            "allocated_fixed_cost_ticks": 5,
            "entry_adverse_ticks": 2,
            "exit_adverse_ticks": 2,
            "total_friction_ticks": 14,
            "variable_cost_ticks": 5,
        },
        "eligibility": "ONE_STRUCTURALLY_ELIGIBLE_COMPLETED_NATIVE_TIMEFRAME_BAR",
        "entry": (
            "FLOOR_NEXT_EXACT_SAME_LINEAGE_5M_FIRST_TRADE_NS_TO_ONE_SECOND;"
            "DECISION_NS_LTE_ENTRY_NS_LT_DECISION_NS_PLUS_300S"
        ),
        "entry_price": "EXACT_NEXT_SAME_LINEAGE_5M_OPEN_TICKS",
        "entry_schedule_proof": (
            "RUNNER_BINDS_ROW_DECISION_ENTRY_NEXT5M_LINEAGE_SHA_BEFORE_1S;"
            "LATER_1S_STREAM_MUST_VALIDATE_SCHEDULED_INTERVAL_AND_OPEN"
        ),
        "exact_evaluation_response": (
            "CALLER_SUPPLIED_SIGNED_INT64_TERMINAL_MOVE_TICKS;DIRECTION_TIMES_MOVE_MINUS_14"
        ),
        "post_freeze_invalid_no_fill_shortened_censored_or_cross_lineage": (
            "FATAL_INTEGRITY_FAILURE_NEVER_ROW_EXCLUSION"
        ),
        "path": "EXACT_SAME_CONTRACT_OUTCOME_SPAN_SEGMENT_FULL_1H_3H_6H",
        "target": "EXACT_SIGNED_TERMINAL_MOVE_TICKS_DIV_CAUSAL_NATIVE_ATR20_TICKS",
    }
    assert document["search_design"]["maximum_primary_hypothesis_units"] == 9_696_720
    assert document["search_design"]["maximum_stage_b_candidate_world_units"] == 587_520
    assert document["search_design"]["maximum_ml_candidate_world_units"] == 1_440
    assert document["search_design"]["maximum_real_and_control_scoring_units"] == 10_089_360
    assert document["ml"]["maximum_search_fit_count_after_rate_recipe_sharing"] == 5_040
    assert document["execution"]["entry_variant_count"] == 9
    assert document["execution"]["exit_variant_count"] == 85
    assert document["nulls"]["master_seed"] == "ai-all-cases-v1"
    assert document["multiplicity"]["search_method"] == "NO_INFERENTIAL_GATE"
    assert document["multiplicity"]["walk_forward_method"] == "BENJAMINI_HOCHBERG"
    assert document["multiplicity"]["daily_vector_domain"].endswith("EXPLICIT_ZERO_FILL")
    assert document["multiplicity"]["p_star_zero_differences"] == "EXCLUDED"
    assert document["multiplicity"]["p_star_no_nonzero_differences"] == "ONE"
    assert document["multiplicity"]["walk_forward_economics_application"] == ("AFTER_BH_REJECTION")
    assert document["multiplicity"]["holdout_economics_application"] == ("AFTER_HOLM_REJECTION")
    assert document["execution"]["structural_complete_case_lookahead_seconds"] == 25_200
    assert document["execution"]["structural_complete_case_unexpected_post_freeze_censor"] == (
        "FAIL_CLOSED"
    )
    assert document["execution"]["direct_ml_feature_and_target_price_unit"] == ("INTEGER_TICKS")
    assert document["execution"]["direct_ml_feature_cutoff"] == (
        "BAR_END_NS_LTE_DECISION_NS_NOT_ENTRY_NS"
    )
    assert (
        document["execution"]["direct_ml_decision_anchor"]
        == ml["direct_row_execution"]["decision_ns"]
        == "EXACT_FEATURE_CUTOFF_CLOCK_COMPLETED_NATIVE_BAR_END"
    )
    assert document["execution"]["direct_ml_entry_rule"].startswith(
        "FLOOR_NEXT_EXACT_SAME_LINEAGE_5M_FIRST_TRADE_NS"
    )
    assert document["execution"]["direct_ml_evaluation_response"] == (
        "SIGNED_INT64_TERMINAL_MOVE_TICKS"
    )
    assert document["execution"]["direct_ml_post_freeze_invalid_path_policy"] == (
        "FATAL_NO_ROW_EXCLUSION"
    )
    assert document["compute_caps"]["search_wall_seconds_maximum"] == 345_600
    assert document["compute_caps"]["verifier_wall_seconds_maximum"] == 172_800
    assert document["compute_caps"]["actual_model_fit_concurrency"] == 1
    assert document["compute_caps"]["feature_and_model_workers_maximum"] == 1
    assert (
        ml["compute_feasibility"]["execution_path"]
        == "SEQUENTIAL_ONE_FIT_AT_A_TIME_NO_WORKER_DIVISOR"
    )
    projected = ml["compute_feasibility"]["extrapolation"]["total_campaign_projected_seconds"]
    assert projected == document["compute_caps"]["search_fresh_campaign_projected_seconds"]
    assert (
        projected * document["compute_caps"]["search_source_replay_and_resume_multiplier_maximum"]
        == document["compute_caps"]["search_source_replay_and_resume_projected_seconds_maximum"]
        <= document["compute_caps"]["search_wall_seconds_maximum"]
    )
    assert projected <= document["compute_caps"]["verifier_wall_seconds_maximum"]
    assert document["compute_caps"]["stage_b_anchor_entry_world_pair_budget"] == 100_000
    assert document["stage_a_gates"]["selection_pair_budget_maximum"] == 100_000
    assert document["stage_a_gates"]["selection_pair_budget_formula"].endswith("LE_100000")
    assert document["holdout_gates"]["terminal_inconclusive_status"].endswith(
        "DIAGNOSTIC_INCONCLUSIVE"
    )
    assert document["dataset"] == {
        "active_calendar_payload": "BARE_ORDERED_ISO_DATE_STRING_LIST",
        "active_calendar_sha256": (
            "b414eae72afdb1c149977ff0ea5b672069380997d91e74adf0407e35836e8ac1"
        ),
        "active_date_count": 1_413,
        "active_date_end": "2026-07-31",
        "active_date_start": "2022-01-03",
        "dataset_handoff_sha256": (
            "26b1bb96f7323cae13bbe5d670c12f3e85615bbb9aab56932ce6523e67af7b00"
        ),
        "dataset_manifest_relative_path": (
            "data/derived/bar_patterns/trade_bar_dataset_manifest/"
            "identity_sha256=b0ecab04cdd3626d3c488f9108c8e9184f5dd610f51950ab7e7f74a5b7524297/"
            "sha256=e2c066ce4c8a97c4059dd2499f881300f905f4bab589240f87532d5cc49599dc.json"
        ),
        "dataset_manifest_sha256": (
            "e2c066ce4c8a97c4059dd2499f881300f905f4bab589240f87532d5cc49599dc"
        ),
        "embargo_access": "PROHIBITED",
        "available_timeframes_seconds": [1, 60, 300, 1_800, 3_600],
        "consumed_timeframes_seconds": [1, 300, 1_800, 3_600],
        "source_manifest_sha256": (
            "14db710d8a522a83d495faeac1c05c9a0169f80f088dfbeb7a66b38f14b6e3de"
        ),
        "split_plan_sha256": ("5594725f6769a706018d414a5b27e3903f1d7d1cc22c98e93b6e973ead1af043"),
        "timeframe_60_policy": "AVAILABLE_IN_MANIFEST_BUT_NOT_OPENED",
    }
    assert document["search_design"]["search_block_lengths"] == [
        59,
        59,
        59,
        59,
        59,
        58,
        58,
        58,
    ]
    assert document["compute_caps"]["stage_a_chunk_count"] == 64
    assert document["compute_caps"]["stage_a_policy_rows_per_chunk_maximum"] == 29_689
    assert document["compute_caps"]["stage_b_chunk_count"] == 64
    assert document["compute_caps"]["stage_b_recipe_rows_per_chunk_maximum"] == 3_060
    assert document["compute_caps"]["direct_candidate_rows_per_chunk"] == 12
    assert document["compute_caps"]["meta_candidate_rows_per_chunk"] == 8
    assert document["search_gates"]["selection_symbolic_maximum"] == 6
    assert document["search_gates"]["selection_direct_maximum"] == 4
    assert document["search_gates"]["selection_meta_maximum"] == 4
    assert document["search_design"]["search_subledger_exact_event_count"] == 181
    assert "DIRECT_288_FULL" in document["search_design"]["search_empty_symbolic_policy"]


def test_static_contract_comparison_rejects_bool_for_frozen_integer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(all_cases_config, "_research_bindings", _bindings)
    value = json.loads(all_cases_config._canonical_json_bytes(expected_ai_all_cases_contract()))
    value.update(
        {
            "code_commit": "a" * 40,
            "dependency_lock_sha256": "b" * 64,
            "implementation_sha256": "c" * 64,
            "precommitted_at_utc": "2026-08-15T00:00:00Z",
        }
    )
    value["execution"]["maximum_concurrent_positions_per_strategy"] = True

    with pytest.raises(AllCasesConfigError, match="execution drifted"):
        all_cases_config._validated_document(value)


@pytest.mark.parametrize(
    ("field", "replacement", "message"),
    (
        ("code_commit", 1, "code_commit"),
        ("dependency_lock_sha256", True, "dependency_lock_sha256"),
        ("implementation_sha256", 7, "implementation_sha256"),
        ("precommitted_at_utc", 20260815, "precommitted_at_utc"),
    ),
)
def test_dynamic_provenance_scalars_reject_non_string_coercion(
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    replacement: object,
    message: str,
) -> None:
    monkeypatch.setattr(all_cases_config, "_research_bindings", _bindings)
    value = json.loads(all_cases_config._canonical_json_bytes(expected_ai_all_cases_contract()))
    value.update(
        {
            "code_commit": "a" * 40,
            "dependency_lock_sha256": "b" * 64,
            "implementation_sha256": "c" * 64,
            "precommitted_at_utc": "2026-08-15T00:00:00Z",
        }
    )
    value[field] = replacement
    with pytest.raises(AllCasesConfigError, match=message):
        all_cases_config._validated_document(value)


def _implementation_fixture(tmp_path: Path) -> None:
    package = tmp_path / "campaigns/ai_all_cases_v1"
    package.mkdir(parents=True)
    (tmp_path / "campaigns/__init__.py").write_text("\n", encoding="utf-8")
    (package / "__init__.py").write_text("\n", encoding="utf-8")
    (package / "config.py").write_text(
        "from systematic_fx.features.bars import TradeBar\n", encoding="utf-8"
    )
    legacy = tmp_path / "src/systematic_fx/features"
    legacy.mkdir(parents=True)
    (tmp_path / "src/systematic_fx/__init__.py").write_text("\n", encoding="utf-8")
    (legacy / "__init__.py").write_text("\n", encoding="utf-8")
    (legacy / "bars.py").write_text("class TradeBar: pass\n", encoding="utf-8")
    (tmp_path / "scripts").mkdir()
    (tmp_path / "scripts/helper.py").write_text("VALUE = 1\n", encoding="utf-8")
    (tmp_path / "pyproject.toml").write_text("[project]\n", encoding="utf-8")
    (tmp_path / "uv.lock").write_text("version = 1\n", encoding="utf-8")


def test_implementation_closure_is_campaign_plus_exact_legacy_and_project_blobs(
    tmp_path: Path,
) -> None:
    _implementation_fixture(tmp_path)
    document = all_cases_implementation_document(tmp_path)

    assert [row["relative_path"] for row in document["campaign_files"]] == [
        "campaigns/__init__.py",
        "campaigns/ai_all_cases_v1/__init__.py",
        "campaigns/ai_all_cases_v1/config.py",
    ]
    assert [row["relative_path"] for row in document["legacy_runtime_blobs"]] == [
        "scripts/helper.py",
        "src/systematic_fx/__init__.py",
        "src/systematic_fx/features/__init__.py",
        "src/systematic_fx/features/bars.py",
    ]
    assert [row["relative_path"] for row in document["project_blobs"]] == [
        "pyproject.toml",
        "uv.lock",
    ]

    original = all_cases_implementation_sha256(tmp_path)
    (tmp_path / "src/systematic_fx/features/bars.py").write_text(
        "class TradeBar: changed = True\n", encoding="utf-8"
    )
    assert all_cases_implementation_sha256(tmp_path) != original


@pytest.mark.parametrize("unsafe_name", ["notes.txt", "__pycache__/config.pyc"])
def test_implementation_closure_rejects_non_python_and_pycache(
    tmp_path: Path, unsafe_name: str
) -> None:
    _implementation_fixture(tmp_path)
    unsafe = tmp_path / "campaigns/ai_all_cases_v1" / unsafe_name
    unsafe.parent.mkdir(parents=True, exist_ok=True)
    unsafe.write_bytes(b"unsafe")
    with pytest.raises(AllCasesConfigError, match="non-Python|__pycache__"):
        all_cases_implementation_document(tmp_path)


def test_implementation_closure_rejects_symlink(tmp_path: Path) -> None:
    _implementation_fixture(tmp_path)
    package = tmp_path / "campaigns/ai_all_cases_v1"
    (package / "linked.py").symlink_to(package / "config.py")
    with pytest.raises(AllCasesConfigError, match="symlink"):
        all_cases_implementation_document(tmp_path)


@pytest.mark.parametrize(
    "unsafe_name",
    ("src/systematic_fx/__pycache__", "scripts/helper.pyc", "scripts/helper.pyo"),
)
def test_implementation_closure_rejects_legacy_bytecode_cache(
    tmp_path: Path,
    unsafe_name: str,
) -> None:
    _implementation_fixture(tmp_path)
    unsafe = tmp_path / unsafe_name
    if unsafe.suffix:
        unsafe.write_bytes(b"bytecode")
    else:
        unsafe.mkdir()
    with pytest.raises(AllCasesConfigError, match="non-Python|symbolic"):
        all_cases_implementation_document(tmp_path)


def test_loader_rejects_unfilled_data_only_template_before_any_git_access(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(all_cases_config, "_research_bindings", _bindings)
    path = tmp_path / AI_ALL_CASES_CONFIG_RELATIVE_PATH
    path.parent.mkdir(parents=True)
    path.write_text(render_ai_all_cases_toml_template(), encoding="utf-8")

    with pytest.raises(AllCasesConfigError, match="code_commit"):
        load_ai_all_cases_config(tmp_path)


def _git_fixture(tmp_path: Path) -> tuple[str, bytes]:
    def git(*arguments: str) -> str:
        return subprocess.run(
            ["git", "-C", str(tmp_path), *arguments],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    git("init", "-q")
    git("config", "user.email", "all-cases@example.invalid")
    git("config", "user.name", "All Cases Test")
    (tmp_path / "source.txt").write_text("source\n", encoding="utf-8")
    git("add", "source.txt")
    git("commit", "-q", "-m", "source")
    source_commit = git("rev-parse", "HEAD")
    raw = b"config_id = 'fixture'\n"
    path = tmp_path / AI_ALL_CASES_CONFIG_RELATIVE_PATH
    path.parent.mkdir(parents=True)
    path.write_bytes(raw)
    return source_commit, raw


@pytest.mark.parametrize(
    "relative",
    (
        ".git/commondir",
        ".git/info/grafts",
        ".git/objects/info/alternates",
        ".git/objects/info/http-alternates",
        ".git/refs/replace/forged",
        ".git/shallow",
    ),
)
def test_trusted_git_rejects_every_local_history_or_object_redirection(
    tmp_path: Path,
    relative: str,
) -> None:
    _git_fixture(tmp_path)
    attack = tmp_path / relative
    attack.parent.mkdir(parents=True, exist_ok=True)
    attack.write_text("forged\n", encoding="ascii")

    with pytest.raises(AllCasesConfigError, match="redirection|replacement"):
        all_cases_config._git(tmp_path, "rev-parse", "HEAD")


def test_trusted_git_invocation_disables_replace_objects_and_lazy_fetch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _git_fixture(tmp_path)
    observed: list[list[str]] = []

    def run(arguments: list[str], **_kwargs: object) -> SimpleNamespace:
        observed.append(arguments)
        return SimpleNamespace(returncode=0, stderr=b"", stdout=b"commit\n")

    monkeypatch.setattr(all_cases_config, "_require_trusted_git_runtime", lambda: None)
    monkeypatch.setattr(all_cases_config.subprocess, "run", run)
    assert all_cases_config._git(tmp_path, "cat-file", "-t", "HEAD") == b"commit\n"
    assert observed == [
        [
            "/usr/bin/git",
            "--no-replace-objects",
            "--no-lazy-fetch",
            "--no-pager",
            "--no-optional-locks",
            "-c",
            "core.fsmonitor=false",
            "-c",
            "core.commitGraph=false",
            "-c",
            "core.hooksPath=/dev/null",
            f"--git-dir={tmp_path / '.git'}",
            f"--work-tree={tmp_path}",
            "cat-file",
            "-t",
            "HEAD",
        ]
    ]
    assert all_cases_config._trusted_git_environment()["GIT_NO_REPLACE_OBJECTS"] == "1"


def test_data_only_config_commit_proves_parent_diff_head_and_clean_bytes(
    tmp_path: Path,
) -> None:
    source_commit, raw = _git_fixture(tmp_path)

    def git(*arguments: str) -> None:
        subprocess.run(
            ["git", "-C", str(tmp_path), *arguments],
            check=True,
            capture_output=True,
        )

    git("add", AI_ALL_CASES_CONFIG_RELATIVE_PATH.as_posix())
    git("commit", "-q", "-m", "data-only config")
    all_cases_config._verify_data_only_config_commit(tmp_path, source_commit, raw)

    dirty_raw = raw + b"dirty = true\n"
    (tmp_path / AI_ALL_CASES_CONFIG_RELATIVE_PATH).write_bytes(dirty_raw)
    with pytest.raises(AllCasesConfigError, match="bytes differ"):
        all_cases_config._verify_data_only_config_commit(tmp_path, source_commit, dirty_raw)


def test_data_only_config_commit_rejects_an_extra_file_or_wrong_parent(
    tmp_path: Path,
) -> None:
    source_commit, raw = _git_fixture(tmp_path)

    def git(*arguments: str) -> None:
        subprocess.run(
            ["git", "-C", str(tmp_path), *arguments],
            check=True,
            capture_output=True,
        )

    (tmp_path / "extra.txt").write_text("extra\n", encoding="utf-8")
    git("add", AI_ALL_CASES_CONFIG_RELATIVE_PATH.as_posix(), "extra.txt")
    git("commit", "-q", "-m", "invalid config commit")
    with pytest.raises(AllCasesConfigError, match="data-only"):
        all_cases_config._verify_data_only_config_commit(tmp_path, source_commit, raw)

    other = tmp_path / "other"
    other.mkdir()
    other_source, other_raw = _git_fixture(other)
    (other / "intermediate.txt").write_text("intermediate\n", encoding="utf-8")
    subprocess.run(
        ["git", "-C", str(other), "add", "intermediate.txt"],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["git", "-C", str(other), "commit", "-q", "-m", "intermediate"],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        [
            "git",
            "-C",
            str(other),
            "add",
            AI_ALL_CASES_CONFIG_RELATIVE_PATH.as_posix(),
        ],
        check=True,
        capture_output=True,
    )
    subprocess.run(
        ["git", "-C", str(other), "commit", "-q", "-m", "config"],
        check=True,
        capture_output=True,
    )
    with pytest.raises(AllCasesConfigError, match="parent differs"):
        all_cases_config._verify_data_only_config_commit(other, other_source, other_raw)
