from __future__ import annotations

import hashlib
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from systematic_fx import cli
from systematic_fx.data.cme_active_contract import (
    ActiveContractEvidenceError,
    MaterializedActiveContractMapping,
    SessionVolume,
    active_contract_mapping_as_of,
    load_active_contract_volume_manifest,
)
from systematic_fx.data.cme_schedule import (
    load_cme_schedule_archive,
    verify_schedule_upstream_source,
)

PROJECT = Path(__file__).resolve().parents[2]
MANIFEST = PROJECT / "configs/data/cme_6e_active_contract_roll_context_v1.toml"


def _roll_schedule_archive(tmp_path: Path) -> tuple[Path, Path]:
    source = tmp_path / "archived-schedule-source.txt"
    source.write_text("deterministic roll schedule source\n")
    archive = tmp_path / "roll-schedule.toml"
    archive.write_text(
        """[archive]
schema = "systematic_fx.cme_schedule_archive.v1"
version = "roll_context_schedule_fixture_v1"
evidence_kind = "DETERMINISTIC_TEST_FIXTURE"
venue = "CME_GLOBEX"
product_root = "6E"
timezone = "UTC"
source_id = "TEST_ONLY_NOT_CME_SCHEDULE_EVIDENCE"
source_sha256 = """
        + f'"{hashlib.sha256(source.read_bytes()).hexdigest()}"\n'
        + """covered_start = 2022-09-15
covered_end_exclusive = 2022-09-20

[[sessions]]
trading_date = 2022-09-15
revision = 1
published_ts_ns = 1660000000000000000
open_ts_ns = 1663192800000000000
close_ts_ns = 1663275600000000000
breaks = []
schedule_kind = "SYNTHETIC_REGULAR"
holiday_name = ""

[[sessions]]
trading_date = 2022-09-16
revision = 1
published_ts_ns = 1660000000000000000
open_ts_ns = 1663279200000000000
close_ts_ns = 1663362000000000000
breaks = []
schedule_kind = "SYNTHETIC_REGULAR"
holiday_name = ""

[[sessions]]
trading_date = 2022-09-19
revision = 1
published_ts_ns = 1660000000000000000
open_ts_ns = 1663538400000000000
close_ts_ns = 1663621200000000000
breaks = []
schedule_kind = "SYNTHETIC_REGULAR"
holiday_name = ""
"""
    )
    return archive, source


def test_real_roll_context_is_exact_finite_and_point_in_time() -> None:
    manifest = load_active_contract_volume_manifest(
        MANIFEST,
        allow_bounded_weekday_fallback=True,
    )
    assert manifest.sha256 == hashlib.sha256(MANIFEST.read_bytes()).hexdigest()
    assert manifest.sha256 == "0df60badcfc0f191f22ca26b2d0ee6a439cb6c90b715580398577af8bcfc5b82"
    assert manifest.semantic_sha256 == (
        "cfe0f62425f5cdb2e1d7687d1163de800334aca2881d5775a4f74b19ac2d5626"
    )
    assert len(manifest.sources) == 4
    assert [item.trading_date.isoformat() for item in manifest.mappings] == [
        "2022-09-16",
        "2022-09-19",
    ]
    assert manifest.mappings[0].evidence_trading_date.isoformat() == "2022-09-15"
    assert manifest.mappings[1].evidence_trading_date.isoformat() == "2022-09-16"
    assert manifest.mappings[0].selection_available_ts_ns <= (
        manifest.mappings[0].target_session_open_ts_ns
    )
    assert manifest.mappings[0].expected[0].trade_volume == 158_500
    assert manifest.mappings[1].expected[1].trade_volume == 224_580


def test_same_day_or_target_context_evidence_fails_closed(tmp_path: Path) -> None:
    text = MANIFEST.read_text()
    same_day = tmp_path / "same-day.toml"
    same_day.write_text(
        text.replace("evidence_trading_date = 2022-09-15", "evidence_trading_date = 2022-09-16", 1)
    )
    with pytest.raises(ActiveContractEvidenceError, match="must predate"):
        load_active_contract_volume_manifest(same_day, allow_bounded_weekday_fallback=True)

    target = tmp_path / "target.toml"
    target.write_text(
        text.replace(
            "evidence_source_dates = [2022-09-15, 2022-09-16]",
            "evidence_source_dates = [2022-09-16, 2022-09-19]",
            1,
        )
    )
    with pytest.raises(ActiveContractEvidenceError, match="UTC partitions"):
        load_active_contract_volume_manifest(target, allow_bounded_weekday_fallback=True)


def test_mapping_cannot_be_observed_before_evidence_session_close() -> None:
    mapping = MaterializedActiveContractMapping(
        trading_date=date(2022, 9, 19),
        evidence_trading_date=date(2022, 9, 16),
        selection_available_ts_ns=100,
        selected=SessionVolume("6EZ2", 191_026, 3, 10),
        candidates=(
            SessionVolume("6EZ2", 191_026, 3, 10),
            SessionVolume("6EU2", 44_629, 2, 4),
        ),
        evidence_manifest_sha256="a" * 64,
        policy_version="previous_completed_trading_date_volume_v1",
    )
    with pytest.raises(ActiveContractEvidenceError, match="not yet observable"):
        active_contract_mapping_as_of((mapping,), trading_date=date(2022, 9, 19), as_of_ts_ns=99)
    assert (
        active_contract_mapping_as_of((mapping,), trading_date=date(2022, 9, 19), as_of_ts_ns=100)
        == mapping
    )


def test_manifest_and_data_root_reject_ancestor_symlinks_and_protected_tokens(
    tmp_path: Path,
) -> None:
    actual = tmp_path / "actual"
    actual.mkdir()
    copied = actual / "manifest.toml"
    copied.write_bytes(MANIFEST.read_bytes())
    alias = tmp_path / "alias"
    alias.symlink_to(actual, target_is_directory=True)
    with pytest.raises(ActiveContractEvidenceError, match="symbolic link"):
        load_active_contract_volume_manifest(
            alias / copied.name,
            allow_bounded_weekday_fallback=True,
        )

    protected = tmp_path / "sealed-context.toml"
    protected.write_bytes(MANIFEST.read_bytes())
    with pytest.raises(ActiveContractEvidenceError, match="protected storage"):
        load_active_contract_volume_manifest(protected, allow_bounded_weekday_fallback=True)


def test_active_contract_manifest_requires_archive_or_explicit_bounded_fallback() -> None:
    with pytest.raises(ActiveContractEvidenceError, match="requires a verified schedule"):
        load_active_contract_volume_manifest(MANIFEST)


def test_bounded_fallback_is_pinned_to_the_checked_in_manifest(tmp_path: Path) -> None:
    changed = tmp_path / "plausible-but-unpinned.toml"
    changed.write_text(
        MANIFEST.read_text().replace(
            'version = "cme_6e_active_contract_roll_context_2022_v1"',
            'version = "plausible_other_engineering_manifest_v1"',
        )
    )
    with pytest.raises(ActiveContractEvidenceError, match="exact checked-in manifest"):
        load_active_contract_volume_manifest(
            changed,
            allow_bounded_weekday_fallback=True,
        )


def test_archive_binds_exact_prior_and_target_session_boundaries(tmp_path: Path) -> None:
    archive_path, source_path = _roll_schedule_archive(tmp_path)
    archive = verify_schedule_upstream_source(
        load_cme_schedule_archive(archive_path, allow_test_fixture=True),
        source_path,
    )
    manifest = load_active_contract_volume_manifest(MANIFEST, schedule_archive=archive)
    assert [item.evidence_trading_date for item in manifest.mappings] == [
        date(2022, 9, 15),
        date(2022, 9, 16),
    ]

    unverified = load_cme_schedule_archive(archive_path, allow_test_fixture=True)
    with pytest.raises(ActiveContractEvidenceError, match="previous completed session"):
        load_active_contract_volume_manifest(MANIFEST, schedule_archive=unverified)

    replacements = (
        (
            (
                "evidence_open_ts_ns = 1663192800000000000",
                "evidence_open_ts_ns = 1663192800000000001",
            ),
        ),
        (
            (
                "evidence_close_ts_ns = 1663275600000000000",
                "evidence_close_ts_ns = 1663275600000000001",
            ),
            (
                "selection_available_ts_ns = 1663275600000000000",
                "selection_available_ts_ns = 1663275600000000001",
            ),
        ),
        (
            (
                "target_session_open_ts_ns = 1663279200000000000",
                "target_session_open_ts_ns = 1663279200000000001",
            ),
        ),
    )
    for index, changes in enumerate(replacements):
        changed = tmp_path / f"changed-boundary-{index}.toml"
        text = MANIFEST.read_text()
        for old, new in changes:
            text = text.replace(old, new, 1)
        changed.write_text(text)
        with pytest.raises(ActiveContractEvidenceError, match="archived sessions"):
            load_active_contract_volume_manifest(changed, schedule_archive=archive)


def test_active_selection_cannot_use_schedule_revision_published_after_evidence_close(
    tmp_path: Path,
) -> None:
    archive_path, source_path = _roll_schedule_archive(tmp_path)
    text = archive_path.read_text()
    late_revision = """
[[sessions]]
trading_date = 2022-09-15
revision = 2
published_ts_ns = 1663277000000000000
open_ts_ns = 1663192800000000000
close_ts_ns = 1663275600000000001
breaks = []
schedule_kind = "SYNTHETIC_LATE_REVISION"
holiday_name = "TEST_ONLY"
"""
    marker = "\n[[sessions]]\ntrading_date = 2022-09-16"
    archive_path.write_text(text.replace(marker, late_revision + marker, 1))
    archive = verify_schedule_upstream_source(
        load_cme_schedule_archive(archive_path, allow_test_fixture=True),
        source_path,
    )
    # Selection becomes available at 2022-09-15 close. The later revision is
    # known by the target open, but point-in-time selection must ignore it.
    manifest = load_active_contract_volume_manifest(MANIFEST, schedule_archive=archive)
    assert manifest.mappings[0].evidence_close_ts_ns == 1663275600000000000


def test_active_contract_cli_consumes_verified_schedule_archive(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    archive, source = _roll_schedule_archive(tmp_path)
    observed: dict[str, object] = {}

    class _Artifact:
        content_sha256 = "a" * 64

        @staticmethod
        def as_dict() -> dict[str, object]:
            return {"artifact_schema": "test.active.mapping.v1"}

    def materialize(manifest: object, **kwargs: object) -> _Artifact:
        observed["manifest"] = manifest
        observed.update(kwargs)
        return _Artifact()

    monkeypatch.setattr(
        "systematic_fx.data.cme_active_contract.materialize_active_contract_mapping_artifact",
        materialize,
    )
    monkeypatch.setattr(
        cli.Settings,
        "from_env",
        staticmethod(lambda: SimpleNamespace(data_root=tmp_path)),
    )
    status = cli.main(
        [
            "research",
            "m0b",
            "verify-active-contract-mapping",
            "--manifest",
            str(MANIFEST),
            "--schedule-archive",
            str(archive),
            "--schedule-source",
            str(source),
            "--allow-test-fixture",
            "--data-root",
            str(tmp_path),
            "--json",
        ]
    )
    assert status == 0
    assert observed["data_root"] == tmp_path
    assert "POINT_IN_TIME_ACTIVE_MAPPING_VERIFIED" in capsys.readouterr().out
