#!/usr/bin/env python3
"""Reproduce e2a with physical raw-row timing under the named lab-minimal policy."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from campaigns.e2a_month_end_v1 import config as config_module
from campaigns.e2a_month_end_v1 import engine as engine_module
from campaigns.e2a_month_end_v1.config import (
    DATASET_MANIFEST_SHA256,
    HANDOVER_SOURCE_ARTIFACT_SHA256S,
    frozen_config,
)
from campaigns.e2a_month_end_v1.engine import RawDataset, verified_readonly_file
from campaigns.e2a_month_end_v1.strict import (
    RESET_POLICY,
    SECOND_NS,
    SEMANTIC_AMBIGUITY,
    StrictPhysicalTrade,
    derive_strict_signals,
    implementation_path,
    replay_strict_physical,
    strict_summary,
)
from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256


@dataclass(frozen=True, slots=True)
class ExpectedRow:
    window_key: str
    event_date: date
    direction: int
    fill_epoch: int | None
    exit_epoch: int | None
    entry_px: int | None
    exit_px: int | None
    gross_ticks: int
    exit_kind: str


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _london_event_date(epoch: int) -> date:
    from zoneinfo import ZoneInfo

    return datetime.fromtimestamp(epoch, UTC).astimezone(ZoneInfo("Europe/London")).date()


def _load_expected(project_root: Path) -> tuple[ExpectedRow, ...]:
    trade_root = project_root / "data/handover_lab/e2a_trades"
    artifact_sha256s = dict(HANDOVER_SOURCE_ARTIFACT_SHA256S)
    output: list[ExpectedRow] = []
    for window_key, filename, relative_path in (
        (
            "R1",
            "R1_2022-01_2023-07.parquet",
            "data/handover_lab/e2a_trades/R1_2022-01_2023-07.parquet",
        ),
        (
            "R2",
            "R2_2023-09_2024-12.parquet",
            "data/handover_lab/e2a_trades/R2_2023-09_2024-12.parquet",
        ),
        (
            "R3",
            "R3_2025-01_2026-01.parquet",
            "data/handover_lab/e2a_trades/R3_2025-01_2026-01.parquet",
        ),
    ):
        with verified_readonly_file(
            trade_root / filename,
            expected_sha256=artifact_sha256s[relative_path],
            relative_path=relative_path,
        ) as (handle, _identity):
            raw_rows = pq.ParquetFile(handle).read().to_pylist()
        for row in raw_rows:
            fill_epoch = int(row["fill_epoch"])
            output.append(
                ExpectedRow(
                    window_key=window_key,
                    event_date=_london_event_date(fill_epoch),
                    direction=int(row["direction"]),
                    fill_epoch=fill_epoch,
                    exit_epoch=(
                        int(row["exit_epoch"]) if row.get("exit_epoch") is not None else None
                    ),
                    entry_px=int(row["entry_px"]),
                    exit_px=int(row["exit_px"]),
                    gross_ticks=int(row["gross"]),
                    exit_kind=str(row["exit_kind"]),
                )
            )
    holdout_relative_path = "data/handover_lab/verdicts/holdout_e2a.json"
    with verified_readonly_file(
        project_root / holdout_relative_path,
        expected_sha256=artifact_sha256s[holdout_relative_path],
        relative_path=holdout_relative_path,
    ) as (handle, _identity):
        holdout = json.load(handle)
    for row in holdout["detail"]:
        output.append(
            ExpectedRow(
                window_key="HO",
                event_date=date.fromisoformat(row["event"]),
                direction=int(row["direction"]),
                fill_epoch=None,
                exit_epoch=None,
                entry_px=None,
                exit_px=None,
                gross_ticks=int(row["gross"]),
                exit_kind=str(row["exit_kind"]),
            )
        )
    return tuple(sorted(output, key=lambda item: (item.event_date, item.window_key)))


def _normalized_exit_kind(value: str) -> str:
    return "BOUNDARY" if value.startswith("BOUNDARY") else value


def _summary_for(trades: tuple[StrictPhysicalTrade, ...]) -> dict[str, object]:
    return strict_summary(trades)


def build_audit(project_root: Path, *, verify_source_sha256: bool = True) -> dict[str, object]:
    """Build the canonical audit document and fail on any frozen-result drift."""

    resolved_root = project_root.resolve()
    config = frozen_config()
    expected = _load_expected(resolved_root)
    dataset = RawDataset(
        resolved_root,
        verify_source_sha256=verify_source_sha256,
        progress=lambda message: print(message, file=sys.stderr, flush=True),
    )
    signals = derive_strict_signals(dataset, config)
    replay = replay_strict_physical(dataset, config, signals)
    trade_by_key = {
        (item.signal.window_key, item.signal.event_date): item for item in replay.trades
    }
    signal_by_key = {(item.window_key, item.event_date): item for item in signals}
    skip_by_key = {(item.signal.window_key, item.signal.event_date): item for item in replay.skips}
    expected_by_key = {(item.window_key, item.event_date): item for item in expected}

    exposed_trades = tuple(
        trade_by_key[key]
        for key in sorted(expected_by_key, key=lambda item: item[1])
        if key in trade_by_key
    )
    added_keys = tuple(sorted(set(signal_by_key) - set(expected_by_key), key=lambda item: item[1]))

    direction_mismatches: list[dict[str, object]] = []
    row_differences: list[dict[str, object]] = []
    entry_price_mismatches = 0
    exit_price_mismatches = 0
    gross_mismatches = 0
    exit_kind_mismatches = 0
    entry_integer_second_mismatches = 0
    exit_integer_second_mismatches = 0
    entry_exact_nanosecond_mismatches = 0
    exit_exact_nanosecond_mismatches = 0
    exposed_no_fill = 0
    timestamp_differences: list[dict[str, object]] = []
    for key, expected_row in expected_by_key.items():
        signal = signal_by_key.get(key)
        if signal is not None and signal.direction != expected_row.direction:
            direction_mismatches.append(
                {
                    "event_date": signal.event_date.isoformat(),
                    "handover_direction": expected_row.direction,
                    "month_open_date": signal.month_open_date.isoformat(),
                    "month_open_ticks": signal.month_open_ticks,
                    "p15_ticks": signal.p15_ticks,
                    "strict_direction": signal.direction,
                    "window_key": signal.window_key,
                }
            )
        trade = trade_by_key.get(key)
        if trade is None:
            exposed_no_fill += 1
            row_differences.append(
                {
                    "event_date": expected_row.event_date.isoformat(),
                    "handover_gross_ticks": expected_row.gross_ticks,
                    "reason": skip_by_key[key].reason if key in skip_by_key else "NO_SIGNAL",
                    "window_key": expected_row.window_key,
                }
            )
            continue
        entry_matches = expected_row.entry_px is None or trade.entry_px == expected_row.entry_px
        exit_matches = expected_row.exit_px is None or trade.exit_px == expected_row.exit_px
        gross_matches = trade.gross_ticks == expected_row.gross_ticks
        kind_matches = _normalized_exit_kind(trade.exit_kind) == expected_row.exit_kind
        entry_time_matches = expected_row.fill_epoch is None or (
            trade.entry.ts_recv_ns // SECOND_NS == expected_row.fill_epoch
        )
        exit_time_matches = expected_row.exit_epoch is None or (
            trade.exit.ts_recv_ns // SECOND_NS == expected_row.exit_epoch
        )
        entry_exact_time_matches = expected_row.fill_epoch is None or (
            trade.entry.ts_recv_ns == expected_row.fill_epoch * SECOND_NS
        )
        exit_exact_time_matches = expected_row.exit_epoch is None or (
            trade.exit.ts_recv_ns == expected_row.exit_epoch * SECOND_NS
        )
        entry_price_mismatches += int(not entry_matches)
        exit_price_mismatches += int(not exit_matches)
        gross_mismatches += int(not gross_matches)
        exit_kind_mismatches += int(not kind_matches)
        entry_integer_second_mismatches += int(not entry_time_matches)
        exit_integer_second_mismatches += int(not exit_time_matches)
        entry_exact_nanosecond_mismatches += int(not entry_exact_time_matches)
        exit_exact_nanosecond_mismatches += int(not exit_exact_time_matches)
        if not entry_exact_time_matches or not exit_exact_time_matches:
            timestamp_differences.append(
                {
                    "event_date": expected_row.event_date.isoformat(),
                    "expected_exit_epoch": expected_row.exit_epoch,
                    "expected_fill_epoch": expected_row.fill_epoch,
                    "strict_entry_ts_recv_ns": trade.entry.ts_recv_ns,
                    "strict_exit_ts_recv_ns": trade.exit.ts_recv_ns,
                    "entry_same_integer_second": entry_time_matches,
                    "exit_same_integer_second": exit_time_matches,
                    "window_key": expected_row.window_key,
                }
            )
        if not all((entry_matches, exit_matches, gross_matches, kind_matches)):
            row_differences.append(
                {
                    "event_date": expected_row.event_date.isoformat(),
                    "field_match": {
                        "entry_px": entry_matches,
                        "exit_kind": kind_matches,
                        "exit_px": exit_matches,
                        "gross_ticks": gross_matches,
                    },
                    "handover": {
                        "direction": expected_row.direction,
                        "entry_px": expected_row.entry_px,
                        "exit_kind": expected_row.exit_kind,
                        "exit_px": expected_row.exit_px,
                        "gross_ticks": expected_row.gross_ticks,
                    },
                    "strict": trade.as_dict(),
                    "window_key": expected_row.window_key,
                }
            )

    added_rows: list[dict[str, object]] = []
    for key in added_keys:
        trade = trade_by_key.get(key)
        if trade is not None:
            added_rows.append({"status": "COMPLETED", **trade.as_dict()})
        else:
            skip = skip_by_key[key]
            added_rows.append({"status": skip.reason, **skip.signal.as_dict()})

    calendar_summary = _summary_for(replay.trades)
    exposed_summary = _summary_for(exposed_trades)
    frozen_invariants = {
        "added_calendar_candidate_count": len(added_keys) == 4,
        "calendar_completed_count": calendar_summary["completed_count"] == 50,
        "calendar_gross_ticks": calendar_summary["gross_ticks"] == 1_043,
        "calendar_net_at_1_5_ticks": calendar_summary["net_at_1_5_ticks"] == "968.0",
        "calendar_signal_count": len(signals) == 51,
        "direction_mismatch_count": len(direction_mismatches) == 1,
        "entry_price_mismatch_count": entry_price_mismatches == 20,
        "entry_exact_nanosecond_mismatch_count": (entry_exact_nanosecond_mismatches == 42),
        "entry_integer_second_mismatch_count": entry_integer_second_mismatches == 0,
        "exit_kind_mismatch_count": exit_kind_mismatches == 0,
        "exit_exact_nanosecond_mismatch_count": exit_exact_nanosecond_mismatches == 32,
        "exit_integer_second_mismatch_count": exit_integer_second_mismatches == 11,
        "exit_price_mismatch_count": exit_price_mismatches == 18,
        "exposed_completed_count": exposed_summary["completed_count"] == 47,
        "exposed_gross_mismatch_count": gross_mismatches == 36,
        "exposed_gross_ticks": exposed_summary["gross_ticks"] == 993,
        "exposed_net_at_1_5_ticks": exposed_summary["net_at_1_5_ticks"] == "922.5",
        "exposed_no_fill_count": exposed_no_fill == 0,
        "handover_row_count": len(expected) == 47,
        "strict_skip_count": len(replay.skips) == 1,
        "strict_skip_is_november_2025": len(replay.skips) == 1
        and replay.skips[0].signal.event_date == date(2025, 11, 28)
        and replay.skips[0].reason == "ENTRY_NO_FILL",
    }
    if not all(frozen_invariants.values()):
        failures = ", ".join(name for name, passed in frozen_invariants.items() if not passed)
        raise RuntimeError(f"strict physical e2a invariant drifted: {failures}")

    result: dict[str, object] = {
        "artifact_schema": "systematic_fx.e2a_strict_physical_audit.v1",
        "audit_status": "CONFLICT",
        "calendar_rule": {
            "added_handover_rows": added_rows,
            "candidate_count": len(signals),
            "skip_count": len(replay.skips),
            "skips": [item.as_dict() for item in replay.skips],
            "summary": calendar_summary,
        },
        "campaign_config_sha256": config.semantic_sha256,
        "dataset_manifest_sha256": DATASET_MANIFEST_SHA256,
        "direction_mismatches": direction_mismatches,
        "exposed_handover_comparison": {
            "entry_exact_nanosecond_mismatch_count_of_42": (entry_exact_nanosecond_mismatches),
            "entry_integer_second_mismatch_count": entry_integer_second_mismatches,
            "entry_price_mismatch_count_of_42": entry_price_mismatches,
            "exact_subsecond_timestamp_semantics": (
                "HANDOVER_GRID_EPOCH_IS_SECOND_RESOLUTION;_EXACT_PHYSICAL_ROW_"
                "TIMESTAMPS_ARE_DIAGNOSTIC_NOT_EXPECTED_TO_MATCH"
            ),
            "exit_exact_nanosecond_mismatch_count_of_32": (exit_exact_nanosecond_mismatches),
            "exit_kind_mismatch_count": exit_kind_mismatches,
            "exit_integer_second_mismatch_count": exit_integer_second_mismatches,
            "exit_price_mismatch_count_of_42": exit_price_mismatches,
            "gross_mismatch_count_of_47": gross_mismatches,
            "no_fill_count": exposed_no_fill,
            "row_differences": row_differences,
            "summary": exposed_summary,
            "timestamp_differences": timestamp_differences,
        },
        "frozen_invariants": frozen_invariants,
        "governed_dependency_sha256s": {
            "config_py": _sha256_file(Path(config_module.__file__).resolve()),
            "engine_py": _sha256_file(Path(engine_module.__file__).resolve()),
        },
        "implementation_sha256": _sha256_file(implementation_path()),
        "handover_input_sha256s": [
            {"relative_path": relative_path, "sha256": sha256}
            for relative_path, sha256 in HANDOVER_SOURCE_ARTIFACT_SHA256S
            if relative_path.endswith((".parquet", "holdout_e2a.json"))
        ],
        "raw_source_sha256_verification": {
            "enabled": verify_source_sha256,
            "manifest_verified": True,
            "opened_source_count": len(dataset._verified),
            "opened_sources_verified": verify_source_sha256,
        },
        "reset_policy": RESET_POLICY,
        "runner_sha256": _sha256_file(Path(__file__).resolve()),
        "semantic_ambiguity": SEMANTIC_AMBIGUITY,
        "semantic_authority": "AUDIT_ONLY_NOT_REPO_GOVERNED_EXECUTION",
    }
    result["audit_sha256"] = canonical_sha256(result)
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument(
        "--skip-source-sha256",
        action="store_true",
        help="diagnostic only; default behavior verifies every opened raw source",
    )
    return parser


def main() -> int:
    arguments = build_parser().parse_args()
    result = build_audit(
        arguments.project_root,
        verify_source_sha256=not arguments.skip_source_sha256,
    )
    sys.stdout.buffer.write(canonical_json_bytes(result) + b"\n")
    # Section 2 did not freeze reset/recovery semantics.  This remains a
    # conflict even when all reconstruction assertions pass.
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
