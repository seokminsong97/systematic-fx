from __future__ import annotations

import hashlib
import json
import shutil
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from systematic_fx.features.screening import FEATURE_VERSION, FIVE_MINUTE_SCHEMA, FORMULA_SHA256
from systematic_fx.research import discovery_slice as discovery

CODE_SNAPSHOT_SHA256 = "c" * 64
RUN_FINGERPRINT = "f" * 64
CONTRACT = "6EH2"
TICK_RAW = 50_000
BASE_MID_X2_RAW = 2_200_100_000


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _metadata(
    source_date: date,
    *,
    code_snapshot_sha256: str = CODE_SNAPSHOT_SHA256,
    contract: str = CONTRACT,
) -> dict[bytes, bytes]:
    hashes = {
        "source_sha256": "1" * 64,
        "source_schema_sha256": "2" * 64,
        "source_manifest_sha256": "3" * 64,
        "qc_manifest_sha256": "4" * 64,
        "qc_config_sha256": "5" * 64,
        "calendar_sha256": "6" * 64,
        "config_sha256": "7" * 64,
        "contract_selection_sha256": "8" * 64,
        "previous_volume_sha256": "9" * 64,
    }
    values = {
        "feature_version": FEATURE_VERSION,
        "formula_sha256": FORMULA_SHA256,
        "granularity": "5m",
        "price_scale": "1e-9",
        "tick_size_raw": str(TICK_RAW),
        "screening_only": "true",
        "research_eligible": "false",
        "definition_status_available": "false",
        "source_date": source_date.isoformat(),
        "code_snapshot_sha256": code_snapshot_sha256,
        "instrument_id": "28727",
        "contract": contract,
        "contract_month": "2022-03-01",
        "previous_source_date": (source_date - timedelta(days=1)).isoformat(),
        "previous_trade_rows": "100",
        "previous_trade_volume": "1000",
        "source_start_boundary_policy": "EXCLUDE_PARTIAL_RIGHT_CLOSED",
        "source_end_boundary_policy": "UNPROVEN_CLOSED_BOUNDARY",
        **hashes,
    }
    return {f"systematic_fx.{key}".encode(): value.encode() for key, value in values.items()}


def _row(
    source_date: date,
    minute: int,
    *,
    open_x2: int = BASE_MID_X2_RAW,
    high_x2: int = BASE_MID_X2_RAW,
    low_x2: int = BASE_MID_X2_RAW,
    close_x2: int = BASE_MID_X2_RAW,
    eligible: bool = True,
) -> dict[str, object]:
    values: dict[str, object] = {
        "feature_version": FEATURE_VERSION,
        "screening_only": True,
        "definition_status_available": False,
        "source_date": source_date,
        "contract": CONTRACT,
        "instrument_id": 28_727,
        "bucket_end": datetime(
            source_date.year,
            source_date.month,
            source_date.day,
            tzinfo=UTC,
        )
        + timedelta(minutes=minute),
        "source_local_signal_input_valid": eligible,
        "signal_input_valid": False,
        "observed_seconds": 300,
        "valid_seconds": 300 if eligible else 0,
        "missing_seconds": 0,
        "invalid_seconds": 0 if eligible else 300,
        "stale_seconds": 0,
        "reset_seen_seconds": 0,
        "maybe_bad_book_seconds": 0,
        "last_spread_ticks": 1,
        "spread_raw_max": TICK_RAW,
        "mid_px_x2_raw_open": open_x2,
        "mid_px_x2_raw_high": high_x2,
        "mid_px_x2_raw_low": low_x2,
        "mid_px_x2_raw_close": close_x2,
        "trade_count": 10,
        "trade_volume": 100,
        "signed_trade_volume": 0,
        "event_count": 100,
        "bid_cum_size_l5_first": 1_000,
        "bid_cum_size_l5_last": 1_000,
        "ask_cum_size_l5_first": 1_000,
        "ask_cum_size_l5_last": 1_000,
    }
    for level in (1, 3, 5, 10):
        values[f"imbalance_signed_ppm_l{level}_last"] = 0
        values[f"imbalance_signed_ppm_l{level}_mean_trunc"] = 0
        values[f"imbalance_sign_changes_l{level}"] = 0
        values[f"imbalance_last_sign_persistence_ppm_l{level}"] = 0
    assert set(values) == set(discovery._REQUIRED_COLUMNS)
    return values


def _canonical_feature_path(data_root: Path, source_date: date, contract: str = CONTRACT) -> Path:
    return (
        data_root
        / "derived/research_5m"
        / f"version={FEATURE_VERSION}"
        / f"contract={contract}"
        / f"source_date={source_date.isoformat()}"
        / "part-000.parquet"
    )


def _write_feature(
    data_root: Path,
    source_date: date,
    rows: list[dict[str, object]],
    *,
    metadata: dict[bytes, bytes] | None = None,
    path: Path | None = None,
) -> Path:
    target = path or _canonical_feature_path(data_root, source_date)
    target.parent.mkdir(parents=True, exist_ok=True)
    schema = FIVE_MINUTE_SCHEMA.with_metadata(metadata or _metadata(source_date))
    expanded_rows: list[dict[str, object]] = []
    for row in rows:
        expanded: dict[str, object] = {}
        for field in FIVE_MINUTE_SCHEMA:
            if field.name in row:
                expanded[field.name] = row[field.name]
            elif field.nullable:
                expanded[field.name] = None
            elif pa.types.is_timestamp(field.type):
                expanded[field.name] = row["bucket_end"]
            elif pa.types.is_date(field.type):
                expanded[field.name] = source_date
            elif pa.types.is_boolean(field.type):
                expanded[field.name] = False
            elif pa.types.is_integer(field.type):
                expanded[field.name] = 0
            elif pa.types.is_string(field.type):
                expanded[field.name] = ""
            else:  # pragma: no cover - the frozen schema uses only the cases above
                raise AssertionError(f"unsupported synthetic field: {field}")
        expanded_rows.append(expanded)
    pq.write_table(
        pa.Table.from_pylist(expanded_rows, schema=schema),
        target,
        compression="zstd",
        use_dictionary=False,
    )
    return target


def _slice_inputs(
    tmp_path: Path,
) -> tuple[
    Path,
    tuple[date, ...],
    dict[date, Path],
    dict[date, str],
    dict[date, str],
]:
    data_root = tmp_path / "data"
    (data_root / "derived").mkdir(parents=True)
    requested = tuple(date(2022, 1, day) for day in range(2, 7))
    feature_paths: dict[date, Path] = {}

    source_date = requested[1]
    rows = [_row(source_date, 5)]
    rows[0]["imbalance_signed_ppm_l1_last"] = 500_000
    rows[0]["imbalance_last_sign_persistence_ppm_l1"] = 800_000
    for step in range(1, 13):
        close = BASE_MID_X2_RAW + step * TICK_RAW
        rows.append(
            _row(
                source_date,
                5 * (step + 1),
                open_x2=close,
                high_x2=close + 2 * TICK_RAW,
                low_x2=BASE_MID_X2_RAW - step * TICK_RAW,
                close_x2=close,
            )
        )
    late = _row(source_date, 23 * 60 + 55)
    late["imbalance_signed_ppm_l1_last"] = -500_000
    late["imbalance_last_sign_persistence_ppm_l1"] = 800_000
    rows.append(late)
    feature_paths[source_date] = _write_feature(data_root, source_date, rows)

    for source_date in requested[2:]:
        feature_paths[source_date] = _write_feature(
            data_root,
            source_date,
            [_row(source_date, 5)],
        )
    hashes = {source_date: _sha256(path) for source_date, path in feature_paths.items()}
    return data_root, requested, feature_paths, hashes, {requested[0]: "NO_PREVIOUS_SOURCE"}


def _analyze(
    data_root: Path,
    requested: tuple[date, ...],
    feature_paths: dict[date, Path],
    hashes: dict[date, str],
    no_entry: dict[date, str],
):
    return discovery.analyze_phase1a_discovery_slice(
        feature_paths,
        expected_sha256_by_date=hashes,
        requested_source_dates=requested,
        no_entry_reasons=no_entry,
        data_root=data_root,
        code_snapshot_sha256=CODE_SNAPSHOT_SHA256,
        run_fingerprint=RUN_FINGERPRINT,
    )


def _assert_no_floats(value: object) -> None:
    assert not isinstance(value, float)
    if isinstance(value, dict):
        for child in value.values():
            _assert_no_floats(child)
    elif isinstance(value, list):
        for child in value:
            _assert_no_floats(child)


def test_frozen_config_has_exactly_eleven_nonadaptive_queries(tmp_path: Path) -> None:
    config = discovery.load_discovery_slice_config()

    assert len(config.candidate_queries) == 11
    assert len({query.query_id for query in config.candidate_queries}) == 11
    assert tuple(query.query_id for query in config.candidate_queries) == tuple(discovery._RULES)
    document = discovery.tomllib.loads(config.path.read_text())
    assert document["query"]["source_dates_per_slice"] == 5
    assert document["query"]["performance_based_early_stop"] is False
    assert document["query"]["cross_source_date_forward_fill"] is False
    assert document["query"]["emit_zero_support_queries"] is True
    assert document["query"]["retain_all_occurrences"] is True
    assert document["query"]["retain_all_query_variables"] is True
    assert document["query"]["retain_per_occurrence_forward_results"] is True
    assert document["query"]["forward_path_requires_contiguous_valid_bars"] is True
    assert document["query"]["excursion_includes_entry_zero"] is True

    tampered = tmp_path / "phase1a_discovery_slice_v1.toml"
    tampered.write_text(
        config.path.read_text().replace(
            "performance_based_early_stop = false",
            "performance_based_early_stop = true",
        )
    )
    with pytest.raises(discovery.DiscoverySliceError, match="semantics drifted"):
        discovery.load_discovery_slice_config(tampered)


def test_analyzer_preserves_partition_occurrences_unresolved_zero_support_and_integers(
    tmp_path: Path,
) -> None:
    inputs = _slice_inputs(tmp_path)
    first = _analyze(*inputs)
    second = _analyze(*inputs)

    assert first.disposition == "CREATED"
    assert second.disposition == "REUSED"
    assert first.path == second.path
    assert first.sha256 == _sha256(first.path)
    assert first.candidate_query_count == 11
    assert first.nonzero_support_query_count == 1
    assert first.requested_source_dates == tuple(value.isoformat() for value in inputs[1])
    assert first.no_entry_source_dates == ("2022-01-02",)
    assert first.feature_source_dates == (
        "2022-01-03",
        "2022-01-04",
        "2022-01-05",
        "2022-01-06",
    )
    assert first.path.is_relative_to(inputs[0] / "derived")
    assert first.path.stat().st_mode & 0o222 == 0

    document = json.loads(first.path.read_bytes())
    _assert_no_floats(document)
    assert document["requested_source_dates"] == [
        "2022-01-02",
        "2022-01-03",
        "2022-01-04",
        "2022-01-05",
        "2022-01-06",
    ]
    assert document["no_entry_reasons"] == {"2022-01-02": "NO_PREVIOUS_SOURCE"}
    assert len(document["coverage"]) == 5
    assert len(document["feature_inputs"]) == 4
    assert document["feature_inputs"][0]["metadata"]["code_snapshot_sha256"] == (
        CODE_SNAPSHOT_SHA256
    )
    assert document["feature_inputs"][0]["metadata"]["calendar_sha256"] == "6" * 64
    assert document["code_snapshot_sha256"] == CODE_SNAPSHOT_SHA256
    assert document["config"]["relative_path"] == discovery.CONFIG_RELATIVE_PATH
    assert document["summary"]["candidate_query_count"] == 11
    assert document["summary"]["zero_support_query_count"] == 10

    by_id = {result["definition"]["id"]: result for result in document["query_results"]}
    assert set(by_id) == set(discovery._RULES)
    supported = by_id["p2_01_l1_persistent_continuation"]
    assert supported["support_count"] == 2
    assert supported["direction_counts"] == {"LONG": 1, "SHORT": 1}
    assert len(supported["occurrences"]) == 2
    long_occurrence, short_occurrence = supported["occurrences"]
    assert long_occurrence["forward"]["1"] == {
        "aligned_close_x2_ticks": 1,
        "maximum_adverse_excursion_x2_ticks": -1,
        "maximum_favorable_excursion_x2_ticks": 3,
    }
    assert long_occurrence["forward"]["12"] == {
        "aligned_close_x2_ticks": 12,
        "maximum_adverse_excursion_x2_ticks": -12,
        "maximum_favorable_excursion_x2_ticks": 14,
    }
    assert short_occurrence["source_date"] == "2022-01-03"
    assert short_occurrence["forward"] == {"1": None, "3": None, "6": None, "12": None}
    assert "observed_seconds" in long_occurrence["variables"]
    assert "imbalance_signed_ppm_l10_mean_trunc" in long_occurrence["variables"]
    assert long_occurrence["variables"]["spread_max_ticks"] == 1
    assert set(long_occurrence["variables"]) == (
        set(discovery._REQUIRED_COLUMNS) - {"source_date", "bucket_end"}
        | {"bar_move_x2_ticks", "bar_range_x2_ticks", "signed_flow_ppm", "spread_max_ticks"}
    )
    assert supported["forward"]["12"]["resolved_count"] == 1
    assert supported["forward"]["12"]["unresolved_count"] == 1
    assert supported["forward"]["12"]["positive_rate_ppm"] == 1_000_000
    for query_id, result in by_id.items():
        if query_id != "p2_01_l1_persistent_continuation":
            assert result["support_count"] == 0
            assert result["occurrences"] == []
            assert result["forward"]["1"]["resolved_count"] == 0
            assert result["forward"]["1"]["unresolved_count"] == 0

    first.path.chmod(0o644)
    first.path.write_bytes(b"drift")
    with pytest.raises(discovery.DiscoverySliceError, match="immutable slice artifact drift"):
        _analyze(*inputs)


def test_forward_results_are_direction_aligned_and_clamped_at_entry_zero() -> None:
    start = 300_000_000_000
    current = {
        "bucket_end_ns": start,
        "mid_px_x2_raw_close": 100 * TICK_RAW,
    }
    short_future = {
        "source_local_signal_input_valid": True,
        "mid_px_x2_raw_close": 98 * TICK_RAW,
        "mid_px_x2_raw_high": 103 * TICK_RAW,
        "mid_px_x2_raw_low": 95 * TICK_RAW,
    }
    result = discovery._forward_result(
        {start + discovery.FIVE_MINUTE_NS: short_future},
        current,
        direction=-1,
        horizon=1,
    )
    assert result == {
        "aligned_close_x2_ticks": 2,
        "maximum_adverse_excursion_x2_ticks": -3,
        "maximum_favorable_excursion_x2_ticks": 5,
    }

    below_entry = {
        **short_future,
        "mid_px_x2_raw_close": 98 * TICK_RAW,
        "mid_px_x2_raw_high": 99 * TICK_RAW,
        "mid_px_x2_raw_low": 95 * TICK_RAW,
    }
    long_result = discovery._forward_result(
        {start + discovery.FIVE_MINUTE_NS: below_entry},
        current,
        direction=1,
        horizon=1,
    )
    assert long_result == {
        "aligned_close_x2_ticks": -2,
        "maximum_adverse_excursion_x2_ticks": -5,
        "maximum_favorable_excursion_x2_ticks": 0,
    }


@pytest.mark.parametrize(
    ("query_id", "updates", "expected_direction"),
    [
        (
            "p2_01_l1_persistent_continuation",
            {
                "imbalance_signed_ppm_l1_last": 500_000,
                "imbalance_last_sign_persistence_ppm_l1": 800_000,
            },
            1,
        ),
        (
            "p2_02_multilevel_agreement_continuation",
            {
                **{f"imbalance_signed_ppm_l{level}_last": 300_000 for level in (1, 3, 5, 10)},
                **{
                    f"imbalance_last_sign_persistence_ppm_l{level}": 700_000
                    for level in (1, 3, 5, 10)
                },
            },
            1,
        ),
        (
            "p2_05_stable_l5_low_flip_continuation",
            {
                "imbalance_signed_ppm_l5_mean_trunc": -300_000,
                "imbalance_last_sign_persistence_ppm_l5": 900_000,
                "imbalance_sign_changes_l5": 5,
            },
            -1,
        ),
        (
            "p3_01_flow_price_confirmation_continuation",
            {"signed_trade_volume": 40, "move": 4},
            1,
        ),
        (
            "p3_02_flow_absorption_reversal",
            {"signed_trade_volume": 50},
            -1,
        ),
        (
            "p1_01_depth_supported_move_continuation",
            {"imbalance_signed_ppm_l5_last": 300_000, "move": 16},
            1,
        ),
        (
            "p1_05_unconfirmed_move_reversal",
            {"move": -16},
            1,
        ),
        (
            "p1_03_spread_shock_recovery_reversal",
            {"move": 8, "spread_raw_max": 4 * TICK_RAW},
            -1,
        ),
        (
            "p4_01_opposite_depth_depletion_continuation",
            {"move": 8, "ask_cum_size_l5_last": 900},
            1,
        ),
        (
            "p4_02_depth_resistance_reversal",
            {
                "move": -8,
                "bid_cum_size_l5_last": 1_100,
                "ask_cum_size_l5_last": 900,
            },
            1,
        ),
        (
            "p5_01_range_expansion_flow_continuation",
            {"move": -8, "range": 32, "signed_trade_volume": -40},
            -1,
        ),
    ],
)
def test_each_frozen_candidate_rule(
    query_id: str,
    updates: dict[str, int],
    expected_direction: int,
) -> None:
    row = _row(date(2022, 1, 3), 5)
    updates = dict(updates)
    move = updates.pop("move", 0)
    range_ticks = updates.pop("range", abs(move))
    row.update(updates)
    row["mid_px_x2_raw_close"] = BASE_MID_X2_RAW + move * TICK_RAW
    row["mid_px_x2_raw_high"] = BASE_MID_X2_RAW + max(move, range_ticks + move) * TICK_RAW
    row["mid_px_x2_raw_low"] = row["mid_px_x2_raw_high"] - range_ticks * TICK_RAW
    row["mid_px_x2_raw_low"] = min(
        row["mid_px_x2_raw_low"],
        row["mid_px_x2_raw_open"],
        row["mid_px_x2_raw_close"],
    )
    row["mid_px_x2_raw_high"] = max(
        row["mid_px_x2_raw_high"],
        row["mid_px_x2_raw_open"],
        row["mid_px_x2_raw_close"],
    )
    state = discovery._row_state(row)

    assert discovery._RULES[query_id](row, state) == expected_direction


def test_rejects_sha_metadata_schema_partition_and_slice_date_drift(tmp_path: Path) -> None:
    data_root, requested, feature_paths, hashes, no_entry = _slice_inputs(tmp_path)
    source_date = requested[1]

    bad_hashes = dict(hashes)
    bad_hashes[source_date] = "0" * 64
    with pytest.raises(discovery.DiscoverySliceError, match="feature SHA-256 mismatch"):
        _analyze(data_root, requested, feature_paths, bad_hashes, no_entry)

    original = feature_paths[source_date]
    rows = pq.read_table(original).to_pylist()
    bad_metadata = _metadata(source_date, code_snapshot_sha256="d" * 64)
    _write_feature(data_root, source_date, rows, metadata=bad_metadata)
    changed_hashes = dict(hashes)
    changed_hashes[source_date] = _sha256(original)
    with pytest.raises(discovery.DiscoverySliceError, match="code_snapshot_sha256"):
        _analyze(data_root, requested, feature_paths, changed_hashes, no_entry)

    incomplete_metadata = _metadata(source_date)
    del incomplete_metadata[b"systematic_fx.calendar_sha256"]
    _write_feature(data_root, source_date, rows, metadata=incomplete_metadata)
    changed_hashes[source_date] = _sha256(original)
    with pytest.raises(discovery.DiscoverySliceError, match="calendar_sha256"):
        _analyze(data_root, requested, feature_paths, changed_hashes, no_entry)

    bad_metadata = _metadata(source_date, contract="6EM2")
    _write_feature(data_root, source_date, rows, metadata=bad_metadata)
    changed_hashes[source_date] = _sha256(original)
    with pytest.raises(discovery.DiscoverySliceError, match="canonical.*partition"):
        _analyze(data_root, requested, feature_paths, changed_hashes, no_entry)

    wrong_day_rows = [_row(source_date, 24 * 60)]
    _write_feature(data_root, source_date, wrong_day_rows)
    changed_hashes[source_date] = _sha256(original)
    with pytest.raises(discovery.DiscoverySliceError, match="strictly inside source_date"):
        _analyze(data_root, requested, feature_paths, changed_hashes, no_entry)

    duplicate_paths: dict[date | str, Path] = dict(feature_paths)
    duplicate_paths[source_date.isoformat()] = original
    with pytest.raises(discovery.DiscoverySliceError, match="duplicate normalized dates"):
        discovery.analyze_phase1a_discovery_slice(
            duplicate_paths,
            expected_sha256_by_date=hashes,
            requested_source_dates=requested,
            no_entry_reasons=no_entry,
            data_root=data_root,
            code_snapshot_sha256=CODE_SNAPSHOT_SHA256,
            run_fingerprint=RUN_FINGERPRINT,
        )

    with pytest.raises(discovery.DiscoverySliceError, match="exactly partition"):
        discovery.analyze_phase1a_discovery_slice(
            feature_paths,
            expected_sha256_by_date=changed_hashes,
            requested_source_dates=requested,
            no_entry_reasons={},
            data_root=data_root,
            code_snapshot_sha256=CODE_SNAPSHOT_SHA256,
            run_fingerprint=RUN_FINGERPRINT,
        )


def test_rejects_input_and_output_symlink_components(tmp_path: Path) -> None:
    inputs = _slice_inputs(tmp_path)
    data_root, requested, feature_paths, hashes, no_entry = inputs
    source_date = requested[1]
    canonical = feature_paths[source_date]
    actual = tmp_path / "actual.parquet"
    shutil.copyfile(canonical, actual)
    canonical.unlink()
    canonical.symlink_to(actual)
    with pytest.raises(discovery.DiscoverySliceError, match="symbolic link"):
        _analyze(data_root, requested, feature_paths, hashes, no_entry)

    canonical.unlink()
    shutil.copyfile(actual, canonical)
    hashes[source_date] = _sha256(canonical)
    manifest_parent = data_root / "derived/manifests"
    manifest_parent.mkdir()
    redirected = data_root / "derived/redirected"
    redirected.mkdir()
    (manifest_parent / discovery.DISCOVERY_SLICE_VERSION).symlink_to(redirected)
    with pytest.raises(discovery.DiscoverySliceError, match="symbolic link"):
        _analyze(data_root, requested, feature_paths, hashes, no_entry)


def test_rejects_signal_authority_and_required_column_schema_drift(tmp_path: Path) -> None:
    data_root, requested, feature_paths, hashes, no_entry = _slice_inputs(tmp_path)
    source_date = requested[1]
    path = feature_paths[source_date]
    rows = pq.read_table(path).to_pylist()
    rows[0]["signal_input_valid"] = True
    _write_feature(data_root, source_date, rows)
    hashes[source_date] = _sha256(path)
    with pytest.raises(discovery.DiscoverySliceError, match="cannot claim signal_input_valid"):
        _analyze(data_root, requested, feature_paths, hashes, no_entry)

    fields = [FIVE_MINUTE_SCHEMA.field(name) for name in discovery._REQUIRED_COLUMNS]
    index = next(index for index, field in enumerate(fields) if field.name == "trade_volume")
    fields[index] = pa.field("trade_volume", pa.float64(), nullable=False)
    bad_schema = pa.schema(fields, metadata=_metadata(source_date))
    converted = [{**row, "trade_volume": float(row["trade_volume"])} for row in rows]
    pq.write_table(pa.Table.from_pylist(converted, schema=bad_schema), path)
    hashes[source_date] = _sha256(path)
    with pytest.raises(discovery.DiscoverySliceError, match="schema drift"):
        _analyze(data_root, requested, feature_paths, hashes, no_entry)


def test_stat_identity_includes_ctime() -> None:
    fields = {
        "st_dev": 1,
        "st_ino": 2,
        "st_mode": 0o100644,
        "st_size": 3,
        "st_mtime_ns": 4,
    }
    first = SimpleNamespace(**fields, st_ctime_ns=5)
    second = SimpleNamespace(**fields, st_ctime_ns=6)

    assert discovery._stat_identity(first) != discovery._stat_identity(second)
