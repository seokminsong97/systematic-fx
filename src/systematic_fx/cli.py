"""Command-line composition root for local research workflows."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

from systematic_fx.config import Settings
from systematic_fx.data.inventory import summarize_inventory


def _format_bytes(value: int) -> str:
    amount = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if amount < 1024.0 or unit == "TiB":
            return f"{amount:.2f} {unit}"
        amount /= 1024.0
    raise AssertionError("unreachable")


def _inventory_command(args: argparse.Namespace) -> int:
    settings = Settings.from_env()
    dataset_root = args.root or settings.mbp10_root

    try:
        summary = summarize_inventory(dataset_root)
    except FileNotFoundError as error:
        print(error)
        return 2

    if args.json:
        print(json.dumps(summary.as_dict(), indent=2, sort_keys=True))
    else:
        print(f"dataset_root: {summary.dataset_root}")
        print(f"parquet_files: {summary.file_count}")
        print(f"total_size: {_format_bytes(summary.total_bytes)}")
        print(f"first_source_date: {summary.first_source_date or '-'}")
        print(f"last_source_date: {summary.last_source_date or '-'}")
        print(f"invalid_layout_files: {len(summary.invalid_layout_files)}")

    return 1 if summary.invalid_layout_files else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="systematic-fx",
        description="CME 6E research and deterministic backtesting tools",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    data_parser = commands.add_parser("data", help="inspect and prepare market data")
    data_commands = data_parser.add_subparsers(dest="data_command", required=True)

    inventory_parser = data_commands.add_parser(
        "inventory",
        help="summarize daily MBP-10 Parquet files without loading event rows",
    )
    inventory_parser.add_argument(
        "--root",
        type=Path,
        help="MBP-10 dataset root (default: $SYSTEMATIC_FX_DATA_ROOT/mbp-10)",
    )
    inventory_parser.add_argument("--json", action="store_true", help="emit JSON")
    inventory_parser.set_defaults(handler=_inventory_command)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.handler(args))
