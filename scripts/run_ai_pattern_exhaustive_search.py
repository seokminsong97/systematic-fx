"""CLI for the governed 518-member exhaustive AI-pattern Search."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from scripts.ai_pattern_exhaustive_search_config import (
    AIPatternExhaustiveConfigError,
)
from scripts.ai_pattern_exhaustive_search_run import (
    AIPatternExhaustiveSearchError,
    _run_ai_pattern_exhaustive_search_for_cli,
    precommit_ai_pattern_exhaustive_search,
    verify_ai_pattern_exhaustive_search,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m scripts.run_ai_pattern_exhaustive_search",
        description=("Precommit, resume, or verify the Search-only 518-member AI-pattern family."),
    )
    parser.add_argument("action", choices=("precommit", "run", "verify"))
    parser.add_argument("--json", action="store_true", help="emit final canonical JSON")
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
        if arguments.action == "precommit":
            result = precommit_ai_pattern_exhaustive_search(arguments.project_root)
        elif arguments.action == "run":
            # This fixed entry point emits one immutable summary line immediately
            # after each new batch event.  It cannot alter order, masks, or gates.
            result = _run_ai_pattern_exhaustive_search_for_cli(arguments.project_root)
        else:
            result = verify_ai_pattern_exhaustive_search(arguments.project_root)
    except (
        AIPatternExhaustiveConfigError,
        AIPatternExhaustiveSearchError,
        OSError,
        ValueError,
    ) as error:
        print(f"AI pattern exhaustive Search failed closed: {error}", file=sys.stderr)
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
            "AI pattern exhaustive Search "
            f"status={result.status} batches={result.batches_completed}/43 "
            f"masks={result.masks_frozen}/43 finalists="
            f"{len(result.finalist_hypothesis_ids)}"
        )
    return 0


if __name__ == "__main__":  # pragma: no cover - module execution boundary
    raise SystemExit(main())
