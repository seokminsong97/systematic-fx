from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from collections import Counter
from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

from systematic_fx.backtest.barriers import Direction
from systematic_fx.backtest.event_cache import DailyCacheReport
from systematic_fx.db.data_registry import SourceFileRegistration, SourceManifestBundle
from systematic_fx.research.discovery_slice import DISCOVERY_VARIABLE_FIELDS
from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256
from systematic_fx.research.outcome_config import P5_QUERY_ID
from systematic_fx.research.outcome_inputs import (
    CanonicalDiscoveryArtifact,
    DailyReplayPartition,
    OutcomeInputError,
    P5DiscoveryInputs,
    ReplaySignal,
    apply_terminal_resolution,
    load_p5_discovery_inputs,
    plan_p5_replay_inputs,
    resolve_terminal_partitions,
)

_P5_DEFINITION = {
    "conditions": [
        "bar_range_x2_ticks>=32",
        "abs(bar_move_x2_ticks)>=8",
        "same_sign_signed_flow",
        "last_spread_ticks<=2",
    ],
    "direction_rule": "SIGN_BAR_MOVE",
    "id": P5_QUERY_ID,
    "parent_hypothesis_ids": ["p5_03_volatility_expansion_continuation"],
}
_BOOLEAN_VARIABLES = {
    "definition_status_available",
    "screening_only",
    "signal_input_valid",
    "source_local_signal_input_valid",
}


def _variables(contract: str) -> dict[str, object]:
    values: dict[str, object] = {field: 0 for field in DISCOVERY_VARIABLE_FIELDS}
    values.update(
        {
            "contract": contract,
            "feature_version": "phase1a_mbp10_screening_v1",
            "instrument_id": 17,
        }
    )
    for field in _BOOLEAN_VARIABLES:
        values[field] = field in {"screening_only", "source_local_signal_input_valid"}
    return values


def _epoch_ns(day: date, *, minutes: int = 1) -> int:
    moment = datetime.combine(day, time(), tzinfo=UTC) + timedelta(minutes=minutes)
    return int(moment.timestamp()) * 1_000_000_000


def _query_result(
    definition: dict[str, object],
    occurrences: list[dict[str, object]],
) -> dict[str, object]:
    directions = Counter(str(occurrence["direction"]) for occurrence in occurrences)
    return {
        "definition": definition,
        "direction_counts": {
            "LONG": directions["LONG"],
            "SHORT": directions["SHORT"],
        },
        "forward": {},
        "occurrences": occurrences,
        "source_date_count": len({occurrence["source_date"] for occurrence in occurrences}),
        "support_count": len(occurrences),
    }


def _artifact_document(
    *,
    requested_dates: tuple[date, ...],
    occurrences: list[dict[str, object]],
) -> dict[str, object]:
    query_results = [
        _query_result({"id": f"synthetic_query_{index:02d}"}, []) for index in range(10)
    ]
    query_results.append(_query_result(_P5_DEFINITION, occurrences))
    return {
        "artifact_schema": "systematic_fx.phase1a_discovery_slice.v1",
        "artifact_version": "phase1a_discovery_slice_v1",
        "authority": {
            "maximum_authority": "OPEN_OBSERVATION",
            "pass_backtest_allowed": False,
            "screening_survivor_allowed": False,
            "screening_only": True,
        },
        "code_snapshot_sha256": "a" * 64,
        "config": {
            "definition_sha256": "b" * 64,
            "relative_path": "configs/research/phase1a_discovery_slice_v1.toml",
            "sha256": "c" * 64,
        },
        "coverage": [],
        "feature_distributions": {},
        "feature_inputs": [],
        "no_entry_reasons": {},
        "query_results": query_results,
        "requested_source_dates": [day.isoformat() for day in requested_dates],
        "run_fingerprint": "d" * 64,
        "summary": {
            "candidate_query_count": 11,
            "eligible_rows": len(occurrences),
            "feature_rows": len(occurrences),
            "nonzero_support_query_count": int(bool(occurrences)),
            "zero_support_query_count": 11 - int(bool(occurrences)),
        },
    }


def _write_artifact(
    root: Path,
    *,
    slice_index: int,
    document: dict[str, object],
) -> CanonicalDiscoveryArtifact:
    payload = canonical_json_bytes(document) + b"\n"
    path = root / f"slice-{slice_index:02d}.json"
    path.write_bytes(payload)
    return CanonicalDiscoveryArtifact(
        slice_index=slice_index,
        path=path,
        sha256=hashlib.sha256(payload).hexdigest(),
        byte_size=len(payload),
    )


def _frozen_artifacts(root: Path) -> list[CanonicalDiscoveryArtifact]:
    artifacts: list[CanonicalDiscoveryArtifact] = []
    first_day = date(2022, 1, 1)
    global_occurrence = 0
    for slice_index in range(99):
        requested = tuple(
            first_day + timedelta(days=(slice_index * 5) + offset) for offset in range(5)
        )
        support = 12 if slice_index < 22 else 11
        occurrences: list[dict[str, object]] = []
        for local_index in range(support):
            direction = "LONG" if global_occurrence < 529 else "SHORT"
            occurrences.append(
                {
                    "bucket_end_ns": _epoch_ns(requested[0], minutes=local_index + 1),
                    "direction": direction,
                    "forward": {key: None for key in ("1", "3", "6", "12")},
                    "source_date": requested[0].isoformat(),
                    "variables": _variables("6EZ3"),
                }
            )
            global_occurrence += 1
        artifacts.append(
            _write_artifact(
                root,
                slice_index=slice_index,
                document=_artifact_document(
                    requested_dates=requested,
                    occurrences=occurrences,
                ),
            )
        )
    assert global_occurrence == 1_111
    return artifacts


def _signal(
    signal_id: str,
    *,
    day: date,
    contract: str,
    direction: Direction,
    slice_index: int,
) -> ReplaySignal:
    variables = _variables(contract)
    return ReplaySignal(
        signal_id=signal_id,
        slice_index=slice_index,
        occurrence_index=0,
        source_date=day,
        bucket_end_ns=_epoch_ns(day),
        direction=direction,
        contract=contract,
        variables=tuple((field, variables[field]) for field in DISCOVERY_VARIABLE_FIELDS),
    )


def _source_bundle(
    root: Path,
    dates: tuple[date, ...],
) -> tuple[SourceManifestBundle, dict[date, int]]:
    records: list[SourceFileRegistration] = []
    offsets: dict[date, int] = {}
    offset = 0
    for index, day in enumerate(dates):
        relative = f"{day:%Y/%m/%d}/source.parquet"
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = bytes([index + 1])
        path.write_bytes(payload)
        row_count = 100 + index
        offsets[day] = offset
        offset += row_count
        records.append(
            SourceFileRegistration(
                relative_uri=relative,
                source_date=day,
                byte_size=len(payload),
                sha256=hashlib.sha256(payload).hexdigest(),
                row_count=row_count,
                schema_fingerprint="e" * 64,
                provider_dataset="GLBX.MDP3",
                data_schema="mbp-10",
                price_scale="1e-9",
                footer_metadata={},
            )
        )
    return (
        SourceManifestBundle(
            footer_manifest_path=root / "footer.jsonl",
            hash_manifest_path=root / "hash.jsonl",
            footer_manifest_sha256="f" * 64,
            hash_manifest_sha256="0" * 64,
            records=tuple(records),
            total_source_bytes=sum(record.byte_size for record in records),
            first_source_date=dates[0],
            last_source_date=dates[-1],
        ),
        offsets,
    )


def _cache_report(
    partition: DailyReplayPartition,
    *,
    valid_quote_count: int,
) -> DailyCacheReport:
    cache_spec = partition.cache_spec
    last_valid_event_index = None if valid_quote_count == 0 else cache_spec.event_index_offset + 8
    last_valid_ts_recv_ns = None if valid_quote_count == 0 else 1_700_000_000_000_000_008
    return DailyCacheReport(
        path=Path(f"/{cache_spec.source_date}-{cache_spec.raw_symbol}.parquet"),
        sha256="1" * 64,
        byte_size=100,
        disposition="CREATED",
        source_date=cache_spec.source_date,
        source_path=str(cache_spec.source_parquet_path),
        source_sha256=cache_spec.source_sha256,
        raw_symbol=cache_spec.raw_symbol,
        instrument_id=17,
        event_index_offset=cache_spec.event_index_offset,
        source_row_count=10,
        cached_quote_count=10,
        valid_quote_count=valid_quote_count,
        first_event_index=cache_spec.event_index_offset,
        last_event_index=cache_spec.event_index_offset + 9,
        first_ts_recv_ns=1_700_000_000_000_000_000,
        last_ts_recv_ns=1_700_000_000_000_000_009,
        last_valid_event_index=last_valid_event_index,
        last_valid_ts_recv_ns=last_valid_ts_recv_ns,
    )


class P5DiscoveryInputTests(unittest.TestCase):
    def test_verifies_all_artifacts_and_retains_frozen_signal_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            artifacts = _frozen_artifacts(Path(directory))

            inputs = load_p5_discovery_inputs(tuple(reversed(artifacts)))

            self.assertEqual(tuple(item.slice_index for item in inputs.artifacts), tuple(range(99)))
            self.assertEqual(len(inputs.signals), 1_111)
            self.assertEqual(
                Counter(signal.direction for signal in inputs.signals),
                Counter({Direction.LONG: 529, Direction.SHORT: 582}),
            )
            self.assertEqual(
                tuple(inputs.signals[0].variable_map),
                DISCOVERY_VARIABLE_FIELDS,
            )
            self.assertEqual(inputs.signals[0].contract, "6EZ3")
            self.assertEqual(inputs.signals[0].to_seed().contract_key, "6EZ3")
            self.assertEqual(len(inputs.artifact_manifest_sha256), 64)
            self.assertEqual(len(inputs.signal_manifest_sha256), 64)
            self.assertEqual(len(inputs.input_manifest_sha256), 64)
            self.assertEqual(
                canonical_sha256(inputs.as_dict()["signals"]),
                inputs.signal_manifest_sha256,
            )

    def test_rejects_size_hash_symlink_and_occurrence_schema_drift(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifacts = _frozen_artifacts(root)
            with self.assertRaisesRegex(OutcomeInputError, "byte-size drift"):
                load_p5_discovery_inputs(
                    [replace(artifacts[0], byte_size=artifacts[0].byte_size + 1), *artifacts[1:]]
                )
            with self.assertRaisesRegex(OutcomeInputError, "SHA-256 drift"):
                load_p5_discovery_inputs([replace(artifacts[0], sha256="1" * 64), *artifacts[1:]])

            link = root / "linked-slice.json"
            link.symlink_to(artifacts[0].path)
            with self.assertRaisesRegex(OutcomeInputError, "non-symlink regular file"):
                load_p5_discovery_inputs([replace(artifacts[0], path=link), *artifacts[1:]])

            document = json.loads(artifacts[0].path.read_bytes())
            target = document["query_results"][-1]
            target["occurrences"][0]["variables"].pop(DISCOVERY_VARIABLE_FIELDS[0])
            artifacts[0] = _write_artifact(
                root,
                slice_index=0,
                document=document,
            )
            with self.assertRaisesRegex(OutcomeInputError, "variable schema drift"):
                load_p5_discovery_inputs(artifacts)


class P5ReplayPlanTests(unittest.TestCase):
    def test_plans_sorted_unique_caches_offsets_ordinals_and_expiry_terminal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            all_dates = (
                date(2024, 1, 2),
                date(2024, 1, 3),
                date(2024, 1, 15),  # manifest-only date still contributes to global offsets
                date(2024, 2, 1),
                date(2024, 2, 29),
                date(2024, 3, 1),
                date(2024, 5, 31),
                date(2024, 6, 1),
            )
            calendar = tuple(day for day in all_dates if day != date(2024, 1, 15))
            bundle, offsets = _source_bundle(root, all_dates)
            discovery = P5DiscoveryInputs(
                artifacts=(),
                signals=(
                    _signal(
                        "h-signal",
                        day=date(2024, 1, 3),
                        contract="6EH4",
                        direction=Direction.LONG,
                        slice_index=0,
                    ),
                    _signal(
                        "m-signal",
                        day=date(2024, 2, 1),
                        contract="6EM4",
                        direction=Direction.SHORT,
                        slice_index=1,
                    ),
                ),
            )

            first = plan_p5_replay_inputs(
                discovery,
                source_manifest=bundle,
                mbp10_root=root,
                calendar_source_dates=calendar,
            )
            second = plan_p5_replay_inputs(
                discovery,
                source_manifest=bundle,
                mbp10_root=root,
                calendar_source_dates=calendar,
            )

            expected_keys = (
                (date(2024, 1, 3), "6EH4"),
                (date(2024, 2, 1), "6EH4"),
                (date(2024, 2, 1), "6EM4"),
                (date(2024, 2, 29), "6EH4"),
                (date(2024, 2, 29), "6EM4"),
                (date(2024, 3, 1), "6EM4"),
                (date(2024, 5, 31), "6EM4"),
            )
            self.assertEqual(tuple(partition.key for partition in first.partitions), expected_keys)
            self.assertEqual(
                first.session_ordinal_by_key,
                {
                    (date(2024, 1, 3), "6EH4"): 0,
                    (date(2024, 2, 1), "6EH4"): 1,
                    (date(2024, 2, 29), "6EH4"): 2,
                    (date(2024, 2, 1), "6EM4"): 0,
                    (date(2024, 2, 29), "6EM4"): 1,
                    (date(2024, 3, 1), "6EM4"): 2,
                    (date(2024, 5, 31), "6EM4"): 3,
                },
            )
            terminal_keys = tuple(
                partition.key for partition in first.partitions if partition.terminal
            )
            self.assertEqual(
                terminal_keys,
                ((date(2024, 2, 29), "6EH4"), (date(2024, 5, 31), "6EM4")),
            )
            for partition in first.partitions:
                self.assertEqual(
                    partition.cache_spec.event_index_offset,
                    offsets[partition.cache_spec.source_date],
                )
            self.assertEqual(first.cache_plan_sha256, second.cache_plan_sha256)
            self.assertEqual(first.plan_sha256, second.plan_sha256)
            self.assertEqual(
                first.as_dict()["first_touch_observation_policy"],
                {
                    "active_sessions": 20,
                    "portfolio_position_continues_after_censor": True,
                },
            )
            self.assertNotIn(str(root), canonical_json_bytes(first.as_dict()).decode())

    def test_terminal_resolution_falls_back_to_last_partition_with_valid_quote(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            calendar = (
                date(2024, 1, 3),
                date(2024, 2, 1),
                date(2024, 2, 29),
                date(2024, 3, 1),
                date(2024, 5, 31),
                date(2024, 6, 1),
            )
            bundle, _ = _source_bundle(root, calendar)
            discovery = P5DiscoveryInputs(
                artifacts=(),
                signals=(
                    _signal(
                        "h-signal",
                        day=date(2024, 1, 3),
                        contract="6EH4",
                        direction=Direction.LONG,
                        slice_index=0,
                    ),
                    _signal(
                        "m-signal",
                        day=date(2024, 2, 1),
                        contract="6EM4",
                        direction=Direction.SHORT,
                        slice_index=1,
                    ),
                ),
            )
            plan = plan_p5_replay_inputs(
                discovery,
                source_manifest=bundle,
                mbp10_root=root,
                calendar_source_dates=calendar,
            )
            frozen_plan_sha256 = plan.plan_sha256
            reports = tuple(
                _cache_report(
                    partition,
                    valid_quote_count=(0 if partition.key == (date(2024, 2, 29), "6EH4") else 1),
                )
                for partition in plan.partitions
            )

            first = resolve_terminal_partitions(plan, reports)
            second = resolve_terminal_partitions(plan, reports)
            resolved = apply_terminal_resolution(plan, first)

            self.assertEqual(first.sha256, second.sha256)
            self.assertEqual(plan.plan_sha256, frozen_plan_sha256)
            self.assertEqual(
                {partition.key for partition in resolved if partition.terminal},
                {
                    (date(2024, 2, 1), "6EH4"),
                    (date(2024, 5, 31), "6EM4"),
                },
            )
            h_resolution = first.contracts[0]
            self.assertEqual(h_resolution.contract_key, "6EH4")
            self.assertEqual(h_resolution.trailing_non_executable_partition_count, 1)
            self.assertEqual(
                first.as_dict()["partition_resolution_policy"],
                "REVERSE_SCAN_LAST_VALID_EXECUTABLE_QUOTE_PARTITION_V1",
            )

    def test_terminal_resolution_fails_when_contract_has_no_executable_quote(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            calendar = (date(2024, 1, 2), date(2024, 2, 29), date(2024, 3, 1))
            bundle, _ = _source_bundle(root, calendar)
            discovery = P5DiscoveryInputs(
                artifacts=(),
                signals=(
                    _signal(
                        "signal",
                        day=date(2024, 1, 2),
                        contract="6EH4",
                        direction=Direction.LONG,
                        slice_index=0,
                    ),
                ),
            )
            plan = plan_p5_replay_inputs(
                discovery,
                source_manifest=bundle,
                mbp10_root=root,
                calendar_source_dates=calendar,
            )
            reports = tuple(
                _cache_report(partition, valid_quote_count=0) for partition in plan.partitions
            )

            with self.assertRaisesRegex(
                OutcomeInputError,
                "no executable quote before expiry month",
            ):
                resolve_terminal_partitions(plan, reports)

    def test_rejects_missing_sources_insufficient_expiry_coverage_and_symlinks(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            all_dates = (date(2024, 1, 2), date(2024, 2, 29), date(2024, 3, 1))
            bundle, _ = _source_bundle(root, all_dates)
            discovery = P5DiscoveryInputs(
                artifacts=(),
                signals=(
                    _signal(
                        "signal",
                        day=date(2024, 1, 2),
                        contract="6EH4",
                        direction=Direction.LONG,
                        slice_index=0,
                    ),
                ),
            )
            with self.assertRaisesRegex(OutcomeInputError, "no source manifest record"):
                plan_p5_replay_inputs(
                    discovery,
                    source_manifest=bundle,
                    mbp10_root=root,
                    calendar_source_dates=(
                        date(2024, 1, 2),
                        date(2024, 2, 1),
                        date(2024, 3, 1),
                    ),
                )
            with self.assertRaisesRegex(OutcomeInputError, "does not reach expiry month"):
                plan_p5_replay_inputs(
                    discovery,
                    source_manifest=bundle,
                    mbp10_root=root,
                    calendar_source_dates=(date(2024, 1, 2), date(2024, 2, 29)),
                )

            source_path = root / bundle.records[0].relative_uri
            real_path = source_path.with_name("real.parquet")
            source_path.rename(real_path)
            source_path.symlink_to(real_path)
            with self.assertRaisesRegex(OutcomeInputError, "symbolic link"):
                plan_p5_replay_inputs(
                    discovery,
                    source_manifest=bundle,
                    mbp10_root=root,
                    calendar_source_dates=all_dates,
                )


if __name__ == "__main__":
    unittest.main()
