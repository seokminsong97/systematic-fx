from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from systematic_fx.data.cme_reference import load_cme_6e_reference
from systematic_fx.research.hypotheses import canonical_json_bytes
from systematic_fx.research.m0b import (
    RealSliceError,
    build_real_slice,
    load_materialized_real_slice,
    load_real_slice_config,
    materialize_real_slice,
    verify_real_slice,
)
from systematic_fx.research.m0b.materialize import _verify_source_registry

CONFIG = Path(__file__).resolve().parents[2] / "configs/research/m0b_real_slice_v1.toml"
REFERENCE_CONFIG = CONFIG.parents[1] / "data/cme_6e_reference_v1.toml"


@dataclass(frozen=True)
class _Session:
    session_id: str
    trading_date: date
    open_ts_ns: int
    close_ts_ns: int


@dataclass(frozen=True)
class _Contract:
    raw_symbol: str
    tick_size_numerator: int = 1
    tick_size_denominator: int = 20_000


class _Reference:
    sha256 = "43b58dd0a4f69efffb5f34e6d649bbca56afffecf631c5ef74de5f2c8ac6f721"

    def session_for(self, trading_date: date) -> _Session:
        prior = trading_date.fromordinal(trading_date.toordinal() - 1)
        return _Session(
            f"CME_GLOBEX_6E:{trading_date.isoformat()}",
            trading_date,
            int(datetime(prior.year, prior.month, prior.day, 22, tzinfo=UTC).timestamp() * 1e9),
            int(
                datetime(
                    trading_date.year,
                    trading_date.month,
                    trading_date.day,
                    21,
                    tzinfo=UTC,
                ).timestamp()
                * 1e9
            ),
        )

    def contract(self, raw_symbol: str, *, as_of_date: date) -> _Contract:
        del as_of_date
        return _Contract(raw_symbol)


def test_load_is_finite_search_only_and_exposes_roll_cache_gap() -> None:
    config = load_real_slice_config(CONFIG)
    assert config.source_dates == (
        date(2022, 8, 30),
        date(2022, 8, 31),
        date(2022, 9, 1),
        date(2022, 9, 2),
    )
    assert config.trading_dates == (
        date(2022, 8, 31),
        date(2022, 9, 1),
        date(2022, 9, 2),
    )
    assert config.expected_contracts == ("6EU2", "6EZ2", "6EZ2")
    assert config.cache_expectations[1].status == "MISSING_BUILD_FROM_RAW"
    assert config.roles[1] == "CONTRACT_TRANSITION_CONTEXT_NOT_ACTIVE_SELECTION"
    assert config.active_selection_proven == (False, False, False)
    assert config.previous_source_volume_context[1].selected_trade_volume == 2_850
    assert config.previous_source_volume_context[1].other_trade_volume == 261_517
    assert config.research_authority == "SEARCH_ONLY_NOT_HOLDOUT_NOT_FORWARD"


def test_plan_merges_two_utc_sources_per_cme_trading_session() -> None:
    config = load_real_slice_config(CONFIG)
    build = build_real_slice(config, reference=_Reference())
    assert [item.source_dates for item in build.sessions] == [
        (date(2022, 8, 30), date(2022, 8, 31)),
        (date(2022, 8, 31), date(2022, 9, 1)),
        (date(2022, 9, 1), date(2022, 9, 2)),
    ]
    assert build.sessions[0].instrument_id != build.sessions[1].instrument_id
    assert build.sessions[1].instrument_id == build.sessions[2].instrument_id
    assert build.sessions[1].cache_status == "MISSING_BUILD_FROM_RAW"
    assert not any(item.active_selection_proven for item in build.sessions)
    assert build.source_manifest.row_count == 4
    assert build.quote_manifest.parent_sha256 == build.source_manifest.content_sha256
    assert build.feature_manifest.parent_sha256 == build.quote_manifest.content_sha256
    assert build.label_manifest.parent_sha256 == build.feature_manifest.content_sha256
    verify_real_slice(
        build, config, data_root=CONFIG.parents[2] / "data", verify_source_bytes=False
    )


def test_plan_integrates_with_frozen_cme_reference() -> None:
    config = load_real_slice_config(CONFIG)
    reference = load_cme_6e_reference(REFERENCE_CONFIG)
    build = build_real_slice(config, reference=reference)
    assert build.sessions[1].session_id == "CME_GLOBEX_6E:2022-09-01"
    assert build.sessions[1].source_dates == (date(2022, 8, 31), date(2022, 9, 1))
    assert build.sessions[1].raw_symbol == "6EZ2"


def test_reference_gap_or_tick_drift_fails_closed() -> None:
    config = load_real_slice_config(CONFIG)

    class BadReference(_Reference):
        def contract(self, raw_symbol: str, *, as_of_date: date) -> _Contract:
            del as_of_date
            return _Contract(raw_symbol, tick_size_numerator=2)

    with pytest.raises(RealSliceError, match="tick size"):
        build_real_slice(config, reference=BadReference())


def test_verifier_rejects_broken_lineage() -> None:
    config = load_real_slice_config(CONFIG)
    build = build_real_slice(config, reference=_Reference())
    with pytest.raises(RealSliceError, match="lineage"):
        replace(
            build,
            feature_manifest=replace(build.feature_manifest, parent_sha256="0" * 64),
        )


def test_holdout_or_forward_environment_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SYSTEMATIC_FX_HOLDOUT_TOKEN", "forbidden")
    with pytest.raises(RealSliceError, match="holdout/forward"):
        load_real_slice_config(CONFIG)


def test_any_active_execution_contract_claim_fails_closed() -> None:
    config = load_real_slice_config(CONFIG)
    claimed = replace(config, active_selection_proven=(False, True, False))
    with pytest.raises(RealSliceError, match="in-memory M0b config"):
        build_real_slice(claimed, reference=_Reference())


def test_config_path_rejects_lexical_traversal_and_unsafe_storage_tokens(
    tmp_path: Path,
) -> None:
    traversed = CONFIG.parent / ".." / "research" / CONFIG.name
    with pytest.raises(RealSliceError, match="traversal"):
        load_real_slice_config(traversed)

    unsafe = tmp_path / "sealed-holdout-config.toml"
    unsafe.write_bytes(CONFIG.read_bytes())
    with pytest.raises(RealSliceError, match="holdout"):
        load_real_slice_config(unsafe)


def test_config_and_reference_reject_symlinked_path_components(tmp_path: Path) -> None:
    alias = tmp_path / "config-alias"
    alias.symlink_to(CONFIG.parent, target_is_directory=True)
    with pytest.raises(RealSliceError, match="symbolic link"):
        load_real_slice_config(alias / CONFIG.name)

    project = tmp_path / "project"
    manifest = project / "configs/research" / CONFIG.name
    manifest.parent.mkdir(parents=True)
    manifest.write_bytes(CONFIG.read_bytes())
    (project / "configs/data").parent.mkdir(parents=True, exist_ok=True)
    (project / "configs/data").symlink_to(REFERENCE_CONFIG.parent, target_is_directory=True)
    config = load_real_slice_config(manifest)
    with pytest.raises(RealSliceError, match="symbolic link"):
        build_real_slice(config)


@pytest.mark.parametrize(
    "unsafe_uri",
    (
        "../../search-source.parquet",
        "2022/08/30/sealed-holdout-source.parquet",
        r"2022\08\30\source.parquet",
    ),
)
def test_source_allowlist_rejects_traversal_tokens_and_foreign_separators(
    tmp_path: Path, unsafe_uri: str
) -> None:
    original = 'relative_uri = "2022/08/30/glbx-mdp3-20220830.mbp-10.parquet"'
    mutated = CONFIG.read_text().replace(original, f"relative_uri = '{unsafe_uri}'", 1)
    manifest = tmp_path / "m0b-real-slice.toml"
    manifest.write_text(mutated)
    with pytest.raises(RealSliceError, match="source relative_uri"):
        load_real_slice_config(manifest)


def test_source_registry_rejects_symlinked_parent(tmp_path: Path) -> None:
    project = tmp_path / "project"
    manifest = project / "configs/research" / CONFIG.name
    manifest.parent.mkdir(parents=True)
    manifest.write_bytes(CONFIG.read_bytes())
    (project / "data").symlink_to(CONFIG.parents[2] / "data", target_is_directory=True)
    config = load_real_slice_config(manifest)
    with pytest.raises(RealSliceError, match="symbolic link"):
        _verify_source_registry(config, project)


def test_materializer_rejects_mutated_config_and_forged_reference(tmp_path: Path) -> None:
    config = load_real_slice_config(CONFIG)
    injected = replace(config, route_delay_seconds=0)
    with pytest.raises(RealSliceError, match="in-memory M0b config"):
        materialize_real_slice(injected, data_root=tmp_path)

    with pytest.raises(RealSliceError, match="immutable CME config"):
        materialize_real_slice(config, data_root=tmp_path, reference=_Reference())


def _write_canonical_staged_build(directory: Path) -> Path:
    directory.mkdir(parents=True)
    build = build_real_slice(load_real_slice_config(CONFIG), reference=_Reference())
    path = directory / f"build-{build.sha256}.json"
    path.write_bytes(canonical_json_bytes(build.as_dict()))
    return path


def test_load_materialized_build_rejects_symlinked_parent(tmp_path: Path) -> None:
    actual = _write_canonical_staged_build(tmp_path / "actual-search-builds")
    alias = tmp_path / "search-build-alias"
    alias.symlink_to(actual.parent, target_is_directory=True)
    with pytest.raises(RealSliceError, match="symbolic link"):
        load_materialized_real_slice(alias / actual.name)


@pytest.mark.parametrize("token", ("holdout", "sealed", "credential", "forward"))
def test_load_materialized_build_rejects_unsafe_storage_path(tmp_path: Path, token: str) -> None:
    path = _write_canonical_staged_build(tmp_path / f"{token}-builds")
    with pytest.raises(RealSliceError, match="cannot name"):
        load_materialized_real_slice(path)
