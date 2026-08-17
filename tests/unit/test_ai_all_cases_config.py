from __future__ import annotations

import hashlib
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
    AI_ALL_CASES_CAMPAIGN_DESIGN_ID,
    AI_ALL_CASES_CONFIG_ID,
    AI_ALL_CASES_CONFIG_RELATIVE_PATH,
    AI_ALL_CASES_CONFIG_SCHEMA,
    AI_ALL_CASES_RUN_RELATIVE_ROOT,
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


def test_template_freezes_access_order_budgets_and_invalid_provenance() -> None:
    document = tomllib.loads(render_ai_all_cases_toml_template())
    expected = expected_ai_all_cases_contract()

    for key, value in expected.items():
        assert document[key] == value
    assert document["schema_version"] == AI_ALL_CASES_CONFIG_SCHEMA
    assert document["config_id"] == AI_ALL_CASES_CONFIG_ID
    assert document["campaign_design_id"] == AI_ALL_CASES_CAMPAIGN_DESIGN_ID
    assert AI_ALL_CASES_CONFIG_RELATIVE_PATH.as_posix().endswith("_attempt5.toml")
    assert AI_ALL_CASES_RUN_RELATIVE_ROOT.as_posix().endswith("_attempt5")
    recovery = document["recovery"]
    assert recovery["attempt_number"] == 5
    assert recovery["failure_boundary"] == (
        "AFTER_OUTER_COMPLETED_DURING_TERMINAL_SEARCH_PREFIX_SEMANTIC_REPLAY"
    )
    assert recovery["failure_class"] == "OUTER_COMPLETED_LEDGER_WITH_NONZERO_WRITER_EXIT"
    assert recovery["failure_outcomes_opened"] is True
    assert recovery["failure_cause"] == (
        "ML_INELIGIBILITY_PROVENANCE_BOUND_RECIPE_SEED_ID_INSTEAD_OF_PUBLIC_CANDIDATE_ID;"
        "COMPLETED_PRECEDED_TERMINAL_SEMANTIC_REPLAY"
    )
    assert recovery["search_1s_opened"] is True
    assert recovery["walk_forward_opened"] is True
    assert recovery["embargo_opened"] is False
    assert recovery["holdout_opened"] is False
    assert recovery["implementation_delta"] == (
        "SEPARATE_PUBLIC_ML_CANDIDATE_ID_FROM_SHARED_FIT_RECIPE_SEED_ID;"
        "SEARCH_SEMANTIC_PREFLIGHT_BEFORE_RELEASE;COMPLETION_PREFLIGHT_BEFORE_FINAL_EVENT"
    )
    assert recovery["terminal_post_freeze_failure_policy_preserved"] is True
    assert recovery["predecessor_guard_config_ids"] == [
        "ai_all_cases_v1",
        "ai_all_cases_v1_attempt2",
        "ai_all_cases_v1_attempt3",
        "ai_all_cases_v1_attempt4",
    ]
    assert recovery["predecessor_internal_search_event_count"] == 181
    assert recovery["predecessor_outer_event_count"] == 7
    assert recovery["predecessor_universe_leaf_count"] == 64
    assert recovery["predecessor_search_universe_frozen"] is True
    assert recovery["predecessor_search_result_released"] is True
    assert recovery["predecessor_search_selected_candidate_count"] == 4
    assert recovery["predecessor_stage_a_score_chunk_count"] == 64
    assert recovery["predecessor_stage_a_top256_complete"] is True
    assert recovery["predecessor_stage_b_plan_frozen"] is True
    assert recovery["predecessor_stage_b_raw_chunk_count"] == 64
    assert recovery["predecessor_symbolic_top24_complete"] is True
    assert recovery["predecessor_direct_ml_chunk_count"] == 24
    assert recovery["predecessor_meta_ml_chunk_count"] == 24
    assert recovery["predecessor_meta_plan_frozen"] is True
    assert recovery["predecessor_root_file_count"] == 441
    assert recovery["predecessor_root_directory_count"] == 14
    assert recovery["predecessor_root_file_bytes"] == 9_822_771_691
    assert recovery["predecessor_root_row_count"] == 455
    assert recovery["predecessor_failed_event_present"] is False
    assert recovery["predecessor_failure_code_present"] is False
    assert recovery["predecessor_writer_exit_code_observed"] == 1
    assert recovery["predecessor_writer_exit_evidence_scope"] == (
        "EXTERNAL_GOVERNED_SESSION_OBSERVATION_NOT_TREE_REPLAYABLE"
    )
    observed_writer_output = recovery["predecessor_writer_merged_output_observed"].encode("utf-8")
    assert (
        len(observed_writer_output)
        == recovery["predecessor_writer_merged_output_byte_count_observed"]
        == 85
    )
    assert (
        hashlib.sha256(observed_writer_output).hexdigest()
        == recovery["predecessor_writer_merged_output_sha256_observed"]
        == "d8c1d8d82180b42ff929c2c7cfd4b926540ea5fc58a7c65fce0df1a7f0e39563"
    )
    assert recovery["predecessor_final_status"] == ("NO_WALK_FORWARD_FINALISTS_HOLDOUT_NOT_OPENED")
    assert recovery["ml_ineligibility_candidate_binding_mismatch_count"] == 480
    assert recovery["ml_ineligibility_direct_mismatch_count"] == 288
    assert recovery["ml_ineligibility_meta_mismatch_count"] == 192
    assert recovery["ml_ineligibility_direct_binding_observed_sha256"] == (
        "1e11f58d64718fa6408adf727bfb682c1a8ba0e9e61a5706a1dd53ff2c0c8510"
    )
    assert recovery["ml_ineligibility_direct_binding_expected_sha256"] == (
        "65816380013ba7ac2e933d42f43c7675c5f3287b756ccca8c59ba5c1822394e8"
    )
    assert recovery["ml_ineligibility_meta_binding_observed_sha256"] == (
        "fd8b4ecae9f93c2d3b3b491a3cb1a019caed2f229307fe9fb2a8b0074bb79d76"
    )
    assert recovery["ml_ineligibility_meta_binding_expected_sha256"] == (
        "9b7b6a3755c9427c9201d258cdc7377c5b9f598232ead886f52d675f95755b05"
    )
    assert recovery["scientific_contract_equality_claim"] is True
    assert recovery["scientific_delta"] == "NONE"
    assert recovery["current_scientific_section_sha256"] == (
        all_cases_config._scientific_section_sha256(document)
    )
    assert (
        recovery["current_scientific_section_sha256"]
        == (recovery["predecessor_scientific_section_sha256"])
    )
    assert recovery["current_scientific_section_sha256"] == (
        "677248a6e59973445a08888ee2334e7a07095ee683803233542f349a9615bc04"
    )
    assert recovery["catalogs_unchanged"] is True
    assert recovery["gates_unchanged"] is True
    assert recovery["costs_unchanged"] is True
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
        'schema_version = "systematic_fx.ai_all_cases_config.v5"'
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


def test_static_contract_comparison_rejects_bool_for_frozen_integer() -> None:
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
    field: str,
    replacement: object,
    message: str,
) -> None:
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
    tmp_path: Path,
) -> None:
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


def test_failed_predecessor_guard_recomputes_exact_evidence_without_mutation() -> None:
    root = Path(all_cases_config.__file__).resolve().parents[2]
    predecessor = root / "data/derived/bar_patterns/ai_all_cases_v1"
    before = all_cases_config._predecessor_lstat_snapshot(predecessor)

    all_cases_config.verify_failed_predecessor_attempt(root)

    assert all_cases_config._predecessor_lstat_snapshot(predecessor) == before
    raw, document = all_cases_config._verify_predecessor_config(root)
    assert __import__("hashlib").sha256(raw).hexdigest() == (
        "d63278a150345a086c73dc38daa4fff8a478fd43caaa1ea374e3584c793ccbd4"
    )
    assert all_cases_config._scientific_section_sha256(document) == (
        "11ed94cf78e796a9faec78142c9cfc1d797c50de97716e234531d44d124b5444"
    )


def test_failed_predecessor_guard_rejects_metadata_drift_without_touching_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(all_cases_config.__file__).resolve().parents[2]
    predecessor = root / "data/derived/bar_patterns/ai_all_cases_v1"
    before = all_cases_config._predecessor_lstat_snapshot(predecessor)
    forged = dict(all_cases_config._PREDECESSOR_TREE_CONTRACT)
    kind, mode, nlink, size, digest = forged[".mutation.lock"]
    forged[".mutation.lock"] = (kind, mode, nlink, size + 1, digest)
    monkeypatch.setattr(all_cases_config, "_PREDECESSOR_TREE_CONTRACT", forged)

    with pytest.raises(AllCasesConfigError, match="metadata"):
        all_cases_config.verify_failed_predecessor_attempt(root)

    assert all_cases_config._predecessor_lstat_snapshot(predecessor) == before


def test_attempt2_predecessor_guard_closes_manifest_internal_prefix_and_is_read_only() -> None:
    root = Path(all_cases_config.__file__).resolve().parents[2]
    attempt1 = root / "data/derived/bar_patterns/ai_all_cases_v1"
    attempt2 = root / "data/derived/bar_patterns/ai_all_cases_v1_attempt2"
    config1 = root / "configs/research/ai_all_cases_v1.toml"
    config2 = root / "configs/research/ai_all_cases_v1_attempt2.toml"
    before1 = all_cases_config._predecessor_lstat_snapshot(attempt1)
    before2 = all_cases_config._predecessor_lstat_snapshot(attempt2)
    config_before1 = all_cases_config._predecessor_config_snapshot(config1)
    config_before2 = all_cases_config._predecessor_config_snapshot(config2)

    all_cases_config.verify_failed_predecessor_attempt(root)
    all_cases_config.verify_failed_attempt2_predecessor(root)

    assert all_cases_config._predecessor_lstat_snapshot(attempt1) == before1
    assert all_cases_config._predecessor_lstat_snapshot(attempt2) == before2
    assert all_cases_config._predecessor_config_snapshot(config1) == config_before1
    assert all_cases_config._predecessor_config_snapshot(config2) == config_before2
    manifest, files = all_cases_config._attempt2_tree_manifest(attempt2)
    assert all_cases_config._canonical_sha256(manifest) == (
        "62fe4e87a2df0e23068685e6b0cc15b8816f459e7bebed6c404c0ec499d93c70"
    )
    assert len(manifest["rows"]) == 214
    assert len(files) == 200
    assert sum(row["size"] for row in manifest["rows"] if row["kind"] == "FILE") == (6_259_097_194)
    assert not any("stage_b" in relative for relative in files)
    assert (
        files[
            "internal/search/artifacts/"
            "stage_a_top256-000000-"
            "5f2830da226dc53aa3cfaeea0fa781a9ec07b7ed7e5579c590d4be5a99cfb446.json"
        ]
        == "5f2830da226dc53aa3cfaeea0fa781a9ec07b7ed7e5579c590d4be5a99cfb446"
    )


def test_attempt2_predecessor_guard_rejects_bound_head_tamper_without_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(all_cases_config.__file__).resolve().parents[2]
    attempt2 = root / "data/derived/bar_patterns/ai_all_cases_v1_attempt2"
    before = all_cases_config._predecessor_lstat_snapshot(attempt2)
    monkeypatch.setattr(all_cases_config, "_ATTEMPT2_INTERNAL_HEAD_SHA256", "0" * 64)

    with pytest.raises(AllCasesConfigError, match="internal boundary"):
        all_cases_config.verify_failed_attempt2_predecessor(root)

    assert all_cases_config._predecessor_lstat_snapshot(attempt2) == before


def test_attempt3_predecessor_guard_closes_symbolic_prefix_and_is_read_only() -> None:
    root = Path(all_cases_config.__file__).resolve().parents[2]
    run_roots = tuple(
        root / relative
        for relative in (
            "data/derived/bar_patterns/ai_all_cases_v1",
            "data/derived/bar_patterns/ai_all_cases_v1_attempt2",
            "data/derived/bar_patterns/ai_all_cases_v1_attempt3",
        )
    )
    config_paths = tuple(
        root / relative
        for relative in (
            "configs/research/ai_all_cases_v1.toml",
            "configs/research/ai_all_cases_v1_attempt2.toml",
            "configs/research/ai_all_cases_v1_attempt3.toml",
        )
    )
    run_before = tuple(all_cases_config._predecessor_lstat_snapshot(path) for path in run_roots)
    config_before = tuple(
        all_cases_config._predecessor_config_snapshot(path) for path in config_paths
    )

    all_cases_config.verify_failed_predecessor_attempt(root)
    all_cases_config.verify_failed_attempt2_predecessor(root)
    all_cases_config.verify_failed_attempt3_predecessor(root)

    assert tuple(all_cases_config._predecessor_lstat_snapshot(path) for path in run_roots) == (
        run_before
    )
    assert (
        tuple(all_cases_config._predecessor_config_snapshot(path) for path in config_paths)
        == config_before
    )
    attempt3 = run_roots[-1]
    manifest, files = all_cases_config._attempt3_tree_manifest(attempt3)
    assert all_cases_config._canonical_sha256(manifest) == (
        "f99a43c29acd87e5c6cc73aef36c41276c33476b112de5c8fbdcca8fbae3f5a9"
    )
    assert len(manifest["rows"]) == 346
    assert len(files) == 332
    assert sum(row["size"] for row in manifest["rows"] if row["kind"] == "FILE") == (9_815_674_432)
    assert not any(
        marker in relative
        for relative in files
        for marker in ("direct_ml_chunks", "meta_plan_frozen", "meta_ml_chunks", "final_max12")
    )
    assert (
        files[
            "internal/search/artifacts/"
            "symbolic_top24-000000-"
            "d60490551ee36f518000700e281b6816295f053ef8b0366438b2d3ab95bf9f30.json"
        ]
        == "d60490551ee36f518000700e281b6816295f053ef8b0366438b2d3ab95bf9f30"
    )


def test_attempt3_predecessor_guard_rejects_config_identity_tamper_read_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(all_cases_config.__file__).resolve().parents[2]
    attempt3 = root / "data/derived/bar_patterns/ai_all_cases_v1_attempt3"
    config3 = root / "configs/research/ai_all_cases_v1_attempt3.toml"
    before = all_cases_config._predecessor_lstat_snapshot(attempt3)
    config_before = all_cases_config._predecessor_config_snapshot(config3)
    monkeypatch.setattr(all_cases_config, "_ATTEMPT3_CONFIG_SEMANTIC_SHA256", "0" * 64)

    with pytest.raises(AllCasesConfigError, match="semantic identity"):
        all_cases_config.verify_failed_attempt3_predecessor(root)

    assert all_cases_config._predecessor_lstat_snapshot(attempt3) == before
    assert all_cases_config._predecessor_config_snapshot(config3) == config_before


def test_attempt3_predecessor_guard_rejects_manifest_tamper_read_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(all_cases_config.__file__).resolve().parents[2]
    attempt3 = root / "data/derived/bar_patterns/ai_all_cases_v1_attempt3"
    before = all_cases_config._predecessor_lstat_snapshot(attempt3)
    monkeypatch.setattr(all_cases_config, "_ATTEMPT3_TREE_MANIFEST_SHA256", "0" * 64)

    with pytest.raises(AllCasesConfigError, match="tree manifest"):
        all_cases_config._attempt3_tree_manifest(attempt3)

    assert all_cases_config._predecessor_lstat_snapshot(attempt3) == before


def test_attempt3_predecessor_guard_rejects_outer_or_internal_tamper_read_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(all_cases_config.__file__).resolve().parents[2]
    attempt3 = root / "data/derived/bar_patterns/ai_all_cases_v1_attempt3"
    before = all_cases_config._predecessor_lstat_snapshot(attempt3)
    _raw, predecessor_config = all_cases_config._verify_attempt3_config(root)
    monkeypatch.setattr(all_cases_config, "_ATTEMPT3_FAILURE_CODE", "INTEGRITY_" + "0" * 24)

    with pytest.raises(AllCasesConfigError, match="outer boundary"):
        all_cases_config._verify_attempt3_outer_evidence(attempt3, predecessor_config)

    monkeypatch.undo()
    _manifest, files = all_cases_config._attempt3_tree_manifest(attempt3)
    universe = all_cases_config._verify_attempt3_outer_evidence(attempt3, predecessor_config)
    monkeypatch.setattr(all_cases_config, "_ATTEMPT3_INTERNAL_HEAD_SHA256", "0" * 64)
    with pytest.raises(AllCasesConfigError, match="internal boundary"):
        all_cases_config._verify_attempt3_internal_evidence(attempt3, universe, files)

    assert all_cases_config._predecessor_lstat_snapshot(attempt3) == before


def test_attempt4_guard_closes_completed_tree_and_ml_defect_signature_read_only() -> None:
    root = Path(all_cases_config.__file__).resolve().parents[2]
    attempt4 = root / "data/derived/bar_patterns/ai_all_cases_v1_attempt4"
    config4 = root / "configs/research/ai_all_cases_v1_attempt4.toml"
    before = all_cases_config._predecessor_lstat_snapshot(attempt4)
    config_before = all_cases_config._predecessor_config_snapshot(config4)

    all_cases_config.verify_failed_attempt4_predecessor(root)

    assert all_cases_config._predecessor_lstat_snapshot(attempt4) == before
    assert all_cases_config._predecessor_config_snapshot(config4) == config_before


def test_attempt4_predecessor_guard_rejects_outer_tamper_without_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(all_cases_config.__file__).resolve().parents[2]
    attempt4 = root / "data/derived/bar_patterns/ai_all_cases_v1_attempt4"
    before = all_cases_config._predecessor_lstat_snapshot(attempt4)
    _raw, predecessor_config = all_cases_config._verify_attempt4_config(root)
    forged = (*all_cases_config._ATTEMPT4_OUTER_EVENT_SHA256S[:-1], "0" * 64)
    monkeypatch.setattr(all_cases_config, "_ATTEMPT4_OUTER_EVENT_SHA256S", forged)

    with pytest.raises(AllCasesConfigError, match="outer chain"):
        all_cases_config._verify_attempt4_outer_evidence(attempt4, predecessor_config)

    assert all_cases_config._predecessor_lstat_snapshot(attempt4) == before


def test_attempt4_predecessor_internal_head_tamper_fails_closed_without_large_rehash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(all_cases_config.__file__).resolve().parents[2]
    attempt4 = root / "data/derived/bar_patterns/ai_all_cases_v1_attempt4"
    _raw, predecessor_config = all_cases_config._verify_attempt4_config(root)
    universe, search = all_cases_config._verify_attempt4_outer_evidence(
        attempt4, predecessor_config
    )
    files: dict[str, str] = {}
    for row in universe["feature_mask_chunk_artifacts"]:
        files[f"internal/universe/{row['relative_path']}"] = row["artifact_sha256"]
    for index in range(1, 182):
        relative = f"internal/search/events/event-{index:08d}.json"
        event_path = attempt4 / relative
        event = json.loads(event_path.read_bytes())
        files[relative] = __import__("hashlib").sha256(event_path.read_bytes()).hexdigest()
        files[f"internal/search/artifacts/{event['artifact_relative_path']}"] = event[
            "artifact_sha256"
        ]
    monkeypatch.setattr(all_cases_config, "_ATTEMPT4_INTERNAL_HEAD_SHA256", "0" * 64)

    with pytest.raises(AllCasesConfigError, match="internal boundary"):
        all_cases_config._verify_attempt4_internal_evidence(attempt4, universe, search, files)


def test_attempt4_ml_defect_guard_scans_through_last_direct_chunk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = Path(all_cases_config.__file__).resolve().parents[2]
    attempt4 = root / "data/derived/bar_patterns/ai_all_cases_v1_attempt4"
    artifacts = attempt4 / "internal/search/artifacts"
    files = {
        f"internal/search/artifacts/{path.name}": path.name.removesuffix(".json").rsplit("-", 1)[1]
        for path in artifacts.glob("*.json")
    }
    original_json = all_cases_config._attempt4_json

    def forged_json(path: Path, *, label: str) -> dict[str, object]:
        document = original_json(path, label=label)
        if label == "DIRECT_ML_CHUNKS 23":
            document = json.loads(all_cases_config._canonical_json_bytes(document))
            document["payload"]["candidates"][-1]["ineligibility"]["candidate_id"] = "0" * 64
        return document

    monkeypatch.setattr(all_cases_config, "_attempt4_json", forged_json)

    with pytest.raises(AllCasesConfigError, match="ML binding defect"):
        all_cases_config._verify_attempt4_ml_binding_defect(attempt4, files)
