"""CLI for the governed Batch 3 performance evaluation."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from scripts.ai_pattern_holdout_config import AIPatternHoldoutConfigError
from scripts.ai_pattern_holdout_run import (
    AIPatternHoldoutRunError,
    publish_ai_pattern_holdout_report,
    render_ai_pattern_holdout_report,
    run_ai_pattern_holdout,
    verify_ai_pattern_holdout,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m scripts.run_ai_pattern_holdout",
        description="Run or read-only replay the governed Batch 3 bar-screening evaluation.",
    )
    parser.add_argument("action", choices=("run", "verify"))
    parser.add_argument("--json", action="store_true", help="emit canonical compact JSON")
    parser.add_argument(
        "--project-root",
        type=Path,
        default=Path.cwd(),
        help="systematic-fx checkout (default: current directory)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        if arguments.action == "run":
            result = run_ai_pattern_holdout(arguments.project_root)
            if not arguments.json:
                publish_ai_pattern_holdout_report(arguments.project_root, result)
        else:
            result = verify_ai_pattern_holdout(arguments.project_root)
    except (AIPatternHoldoutConfigError, AIPatternHoldoutRunError, OSError, ValueError) as error:
        print(f"AI pattern holdout failed closed: {error}", file=sys.stderr)
        return 1
    if arguments.json:
        print(
            json.dumps(
                result.as_dict(),
                allow_nan=False,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            )
        )
    else:
        print(render_ai_pattern_holdout_report(result), end="")
    return 0


if __name__ == "__main__":  # pragma: no cover - module execution boundary
    raise SystemExit(main())
