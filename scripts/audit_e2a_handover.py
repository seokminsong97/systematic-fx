#!/usr/bin/env python3
"""Independently audit the e2a handover against raw selected-contract data."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
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
from campaigns.e2a_month_end_v1.engine import (
    RawDataset,
    SignalEvent,
    derive_signals,
    replay_legacy_lab_grid,
    verified_readonly_file,
    window_summary,
)
from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256

HANDOVER_ARTIFACT_SHA256S = dict(HANDOVER_SOURCE_ARTIFACT_SHA256S)


@dataclass(frozen=True, slots=True)
class ExpectedRow:
    window_key: str
    event_date: date
    direction: int
    decision_epoch: int | None
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


def _verify_handover_artifacts(project_root: Path) -> list[dict[str, object]]:
    identities: list[dict[str, object]] = []
    for relative_uri, expected_sha256 in sorted(HANDOVER_ARTIFACT_SHA256S.items()):
        path = project_root / relative_uri
        with verified_readonly_file(
            path,
            expected_sha256=expected_sha256,
            relative_path=relative_uri,
        ) as (_handle, identity):
            identities.append(
                {
                    "byte_size": identity.byte_size,
                    "relative_uri": identity.relative_path,
                    "sha256": identity.sha256,
                }
            )
    return identities


def _event_date(epoch: int) -> date:
    from zoneinfo import ZoneInfo

    return datetime.fromtimestamp(epoch, UTC).astimezone(ZoneInfo("Europe/London")).date()


def _load_expected(project_root: Path) -> tuple[ExpectedRow, ...]:
    root = project_root / "data/handover_lab/e2a_trades"
    rows: list[ExpectedRow] = []
    definitions = (
        (
            "R1",
            root / "R1_2022-01_2023-07.parquet",
            "data/handover_lab/e2a_trades/R1_2022-01_2023-07.parquet",
        ),
        (
            "R2",
            root / "R2_2023-09_2024-12.parquet",
            "data/handover_lab/e2a_trades/R2_2023-09_2024-12.parquet",
        ),
        (
            "R3",
            root / "R3_2025-01_2026-01.parquet",
            "data/handover_lab/e2a_trades/R3_2025-01_2026-01.parquet",
        ),
    )
    for window_key, path, relative_path in definitions:
        with verified_readonly_file(
            path,
            expected_sha256=HANDOVER_ARTIFACT_SHA256S[relative_path],
            relative_path=relative_path,
        ) as (handle, _identity):
            raw_rows = pq.ParquetFile(handle).read().to_pylist()
        for raw in raw_rows:
            fill_epoch = int(raw["fill_epoch"])
            rows.append(
                ExpectedRow(
                    window_key=window_key,
                    event_date=_event_date(fill_epoch),
                    direction=int(raw["direction"]),
                    decision_epoch=(
                        int(raw["dec_epoch"]) if raw.get("dec_epoch") is not None else None
                    ),
                    fill_epoch=fill_epoch,
                    exit_epoch=(
                        int(raw["exit_epoch"]) if raw.get("exit_epoch") is not None else None
                    ),
                    entry_px=int(raw["entry_px"]),
                    exit_px=int(raw["exit_px"]),
                    gross_ticks=int(raw["gross"]),
                    exit_kind=str(raw["exit_kind"]),
                )
            )
    holdout_path = project_root / "data/handover_lab/verdicts/holdout_e2a.json"
    holdout_relative_path = "data/handover_lab/verdicts/holdout_e2a.json"
    with verified_readonly_file(
        holdout_path,
        expected_sha256=HANDOVER_ARTIFACT_SHA256S[holdout_relative_path],
        relative_path=holdout_relative_path,
    ) as (handle, _identity):
        holdout = json.load(handle)
    for raw in holdout["detail"]:
        rows.append(
            ExpectedRow(
                window_key="HO",
                event_date=date.fromisoformat(raw["event"]),
                direction=int(raw["direction"]),
                decision_epoch=None,
                fill_epoch=None,
                exit_epoch=None,
                entry_px=None,
                exit_px=None,
                gross_ticks=int(raw["gross"]),
                exit_kind=str(raw["exit_kind"]),
            )
        )
    return tuple(sorted(rows, key=lambda item: (item.event_date, item.window_key)))


def _signal_with_expected_direction(signal: SignalEvent, direction: int) -> SignalEvent:
    return SignalEvent(
        window_key=signal.window_key,
        event_date=signal.event_date,
        decision_epoch=signal.decision_epoch,
        month_open_date=signal.month_open_date,
        month_open_ticks=signal.month_open_ticks,
        p15_ticks=signal.p15_ticks,
        direction=direction,
    )


def _artifact_summary(expected: tuple[ExpectedRow, ...]) -> dict[str, object]:
    gross = sum(item.gross_ticks for item in expected)
    by_window: dict[str, dict[str, object]] = {}
    for key in ("R1", "R2", "R3", "HO"):
        selected = [item for item in expected if item.window_key == key]
        selected_gross = sum(item.gross_ticks for item in selected)
        by_window[key] = {
            "event_count": len(selected),
            "gross_ticks": selected_gross,
            "net_at_1_5_ticks": f"{selected_gross - 1.5 * len(selected):.1f}",
        }
    return {
        "by_window": by_window,
        "event_count": len(expected),
        "gross_ticks": gross,
        "net_at_1_5_ticks": f"{gross - 1.5 * len(expected):.1f}",
        "net_at_10_ticks": gross - 10 * len(expected),
    }


def build_audit(project_root: Path, *, verify_source_sha256: bool) -> dict[str, object]:
    config = frozen_config()
    handover_source_artifacts = _verify_handover_artifacts(project_root)
    expected = _load_expected(project_root)
    dataset = RawDataset(
        project_root,
        verify_source_sha256=verify_source_sha256,
        progress=lambda message: print(message, file=sys.stderr, flush=True),
    )
    calendar_signals = derive_signals(dataset, config)
    legacy_signals = derive_signals(dataset, config, calendar_mode="LEGACY_METAS_ADJACENCY")
    expected_by_key = {(item.window_key, item.event_date): item for item in expected}
    calendar_by_key = {(item.window_key, item.event_date): item for item in calendar_signals}

    missing_from_handover = [
        item
        for item in calendar_signals
        if (item.window_key, item.event_date) not in expected_by_key
    ]
    handover_not_in_calendar = [
        item for item in expected if (item.window_key, item.event_date) not in calendar_by_key
    ]
    direction_mismatches = []
    for key, expected_row in expected_by_key.items():
        observed = calendar_by_key.get(key)
        if observed is not None and observed.direction != expected_row.direction:
            direction_mismatches.append(
                {
                    "event_date": observed.event_date.isoformat(),
                    "expected_direction": expected_row.direction,
                    "month_open_date": observed.month_open_date.isoformat(),
                    "month_open_ticks": observed.month_open_ticks,
                    "raw_rule_direction": observed.direction,
                    "raw_true_p15_ticks": observed.p15_ticks,
                    "window_key": observed.window_key,
                }
            )

    # Execution compatibility is evaluated on the handover's exact dates and
    # stored directions.  This isolates quote/PnL reproduction from signal-rule
    # disagreements.
    comparison_signals = tuple(
        _signal_with_expected_direction(calendar_by_key[key], row.direction)
        for key, row in sorted(expected_by_key.items(), key=lambda item: item[0][1])
        if key in calendar_by_key
    )
    compatibility_trades = replay_legacy_lab_grid(dataset, config, comparison_signals)
    compatibility_by_key = {
        (item.signal.window_key, item.signal.event_date): item for item in compatibility_trades
    }
    execution_differences: list[dict[str, object]] = []
    applicable_field_counts: Counter[str] = Counter()
    exact_field_counts: Counter[str] = Counter()
    for key, expected_row in expected_by_key.items():
        observed = compatibility_by_key.get(key)
        if observed is None:
            execution_differences.append(
                {
                    "event_date": expected_row.event_date.isoformat(),
                    "reason": "NO_COMPATIBILITY_TRADE",
                    "window_key": expected_row.window_key,
                }
            )
            continue
        checks: dict[str, bool] = {
            "exit_kind": observed.exit_kind == expected_row.exit_kind,
            "gross_ticks": observed.gross_ticks == expected_row.gross_ticks,
        }
        optional_checks = {
            "decision_epoch": (expected_row.decision_epoch, observed.signal.decision_epoch),
            "entry_px": (expected_row.entry_px, observed.entry_px),
            "exit_epoch": (expected_row.exit_epoch, observed.exit_epoch),
            "exit_px": (expected_row.exit_px, observed.exit_px),
            "fill_epoch": (expected_row.fill_epoch, observed.fill_epoch),
        }
        checks.update(
            {
                name: observed_value == expected_value
                for name, (expected_value, observed_value) in optional_checks.items()
                if expected_value is not None
            }
        )
        applicable_field_counts.update(checks)
        exact_field_counts.update(name for name, passed in checks.items() if passed)
        if not all(checks.values()):
            execution_differences.append(
                {
                    "event_date": expected_row.event_date.isoformat(),
                    "expected": {
                        "entry_px": expected_row.entry_px,
                        "exit_epoch": expected_row.exit_epoch,
                        "exit_kind": expected_row.exit_kind,
                        "exit_px": expected_row.exit_px,
                        "fill_epoch": expected_row.fill_epoch,
                        "gross_ticks": expected_row.gross_ticks,
                    },
                    "field_match": checks,
                    "legacy_raw_replay": observed.as_dict(),
                    "window_key": expected_row.window_key,
                }
            )

    calendar_legacy_trades = replay_legacy_lab_grid(dataset, config, calendar_signals)
    stale_examples = [
        item.as_dict()
        for item in calendar_legacy_trades
        if float(item.exit_state_age_seconds) > 300 or float(item.entry_state_age_seconds) > 300
    ]
    artifact_summary = _artifact_summary(expected)
    frozen_invariants = {
        "calendar_rule_completed": len(calendar_legacy_trades) == 50,
        "calendar_rule_gross": sum(item.gross_ticks for item in calendar_legacy_trades) == 1_184,
        "calendar_rule_signals": len(calendar_signals) == 51,
        "direction_mismatches": len(direction_mismatches) == 1,
        "handover_gross": artifact_summary["gross_ticks"] == 1_324,
        "handover_rows": len(expected) == 47,
        "handover_source_hashes": len(handover_source_artifacts) == 12,
        "legacy_adjacency_signals": len(legacy_signals) == 46,
        "legacy_execution_exact": not execution_differences and len(compatibility_trades) == 47,
        "missing_calendar_signals": len(missing_from_handover) == 4,
        "stale_legacy_trades": len(stale_examples) == 6,
    }
    if not all(frozen_invariants.values()):
        failed = ", ".join(name for name, passed in frozen_invariants.items() if not passed)
        raise RuntimeError(f"frozen e2a audit invariant drifted: {failed}")
    result: dict[str, object] = {
        "artifact_schema": "systematic_fx.e2a_handover_raw_audit.v1",
        "audit_status": "CONFLICT",
        "calendar_rule_signal_count": len(calendar_signals),
        "calendar_rule_legacy_grid_completed_count": len(calendar_legacy_trades),
        "calendar_rule_legacy_grid_summary": window_summary(calendar_legacy_trades),
        "campaign_config_sha256": config.semantic_sha256,
        "dataset_manifest_sha256": DATASET_MANIFEST_SHA256,
        "direction_mismatches": direction_mismatches,
        "execution_compatibility": {
            "comparison_semantics": "LEGACY_LAB_GRID_V1_NOT_GOVERNED_AUTHORITY",
            "difference_count": len(execution_differences),
            "differences": execution_differences,
            "disclosure_scope": {
                "holdout_summary_only_rows": 5,
                "parquet_rows_with_price_and_gross": 42,
                "parquet_rows_with_full_integer_second_timestamps": 32,
                "stored_direction_conditioned_rows": 47,
            },
            "field_applicable_counts": dict(sorted(applicable_field_counts.items())),
            "field_match_counts": dict(sorted(exact_field_counts.items())),
            "matched_disclosed_row_count": len(compatibility_trades) - len(execution_differences),
        },
        "frozen_invariants": frozen_invariants,
        "handover_artifact_summary": artifact_summary,
        "handover_event_count": len(expected),
        "handover_source_artifacts": handover_source_artifacts,
        "handover_rows_not_in_calendar_rule": [
            {"event_date": item.event_date.isoformat(), "window_key": item.window_key}
            for item in handover_not_in_calendar
        ],
        "implementation_sha256s": {
            "config_py": _sha256_file(Path(config_module.__file__).resolve()),
            "engine_py": _sha256_file(Path(engine_module.__file__).resolve()),
            "runner": _sha256_file(Path(__file__).resolve()),
        },
        "legacy_adjacency_signal_count": len(legacy_signals),
        "missing_calendar_rule_signals_from_handover": [
            item.as_dict() for item in missing_from_handover
        ],
        "raw_source_sha256_verified": verify_source_sha256,
        "stale_over_300_second_legacy_grid_examples": stale_examples,
    }
    result["audit_sha256"] = canonical_sha256(result)
    return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=PROJECT_ROOT)
    parser.add_argument(
        "--skip-source-sha256",
        action="store_true",
        help="diagnostic-only faster scan; the reported audit remains unverified",
    )
    return parser


def main() -> int:
    arguments = build_parser().parse_args()
    result = build_audit(
        arguments.project_root,
        verify_source_sha256=not arguments.skip_source_sha256,
    )
    sys.stdout.buffer.write(canonical_json_bytes(result) + b"\n")
    return 2 if result["audit_status"] == "CONFLICT" else 0


if __name__ == "__main__":
    raise SystemExit(main())
