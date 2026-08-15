"""CLI for the governed delayed multi-timeframe v1 campaign."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from scripts.ai_delayed_mtf_config import (
    AIDelayedMTFConfigError,
    render_ai_delayed_mtf_toml_template,
)
from scripts.ai_delayed_mtf_run import (
    AIDelayedMTFRunError,
    precommit_ai_delayed_mtf,
    run_ai_delayed_mtf,
    verify_ai_delayed_mtf,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m scripts.run_ai_delayed_mtf",
        description="Render, precommit, resume, or freshly verify delayed-MTF research v1.",
    )
    parser.add_argument("action", choices=("template", "precommit", "run", "verify"))
    parser.add_argument("--json", action="store_true", help="emit canonical final JSON")
    parser.add_argument(
        "--project-root",
        type=Path,
        default=Path.cwd(),
        help="systematic-fx checkout (default: current directory)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    if arguments.action == "template":
        if arguments.json:
            print(
                json.dumps(
                    {"toml": render_ai_delayed_mtf_toml_template()},
                    ensure_ascii=True,
                    separators=(",", ":"),
                    sort_keys=True,
                )
            )
        else:
            print(render_ai_delayed_mtf_toml_template(), end="")
        return 0
    try:
        if arguments.action == "precommit":
            result = precommit_ai_delayed_mtf(arguments.project_root)
        elif arguments.action == "run":
            result = run_ai_delayed_mtf(arguments.project_root)
        else:
            result = verify_ai_delayed_mtf(arguments.project_root)
    except (AIDelayedMTFConfigError, AIDelayedMTFRunError, OSError, ValueError) as error:
        print(f"AI delayed-MTF research failed closed: {error}", file=sys.stderr)
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
        print(
            "AI delayed-MTF research "
            f"status={result.status} events={result.event_count} "
            f"finalists={len(result.finalist_candidate_ids)}"
        )
    return 0


if __name__ == "__main__":  # pragma: no cover - module execution boundary
    raise SystemExit(main())
