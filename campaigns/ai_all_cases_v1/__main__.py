"""CLI for the governed all-cases v1 campaign.

Invoke the pinned CPython and absolute ``bootstrap.py`` through the frozen
absolute ``/usr/bin/env -i`` command.  The stdlib-only bootstrap verifies that
clean entry environment before adding this checkout to ``sys.path`` and
supplies the unique external cache prefix.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from .config import (
    AllCasesConfigError,
    _require_deterministic_runtime_environment,
    render_ai_all_cases_toml_template,
)
from .run import (
    AllCasesRunError,
    precommit_ai_all_cases,
    run_ai_all_cases,
    verify_ai_all_cases,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=(
            "/usr/bin/env -i VIRTUAL_ENV=/absolute/project/.venv <frozen-env> "
            "/Users/seokminsong/.local/share/uv/python/"
            "cpython-3.12.13-macos-aarch64-none/bin/python3.12 -s -P -B -S "
            "/absolute/project/campaigns/ai_all_cases_v1/bootstrap.py"
        ),
        description=(
            "Render, precommit, resume, or freshly verify the governed all-cases v1 campaign."
        ),
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
    try:
        _require_deterministic_runtime_environment()
    except AllCasesConfigError as error:
        print(f"AI all-cases research failed closed: {error}", file=sys.stderr)
        return 1
    if arguments.action == "template":
        try:
            template = render_ai_all_cases_toml_template()
        except AllCasesConfigError as error:
            print(f"AI all-cases research failed closed: {error}", file=sys.stderr)
            return 1
        if arguments.json:
            print(
                json.dumps(
                    {"toml": template},
                    ensure_ascii=True,
                    separators=(",", ":"),
                    sort_keys=True,
                )
            )
        else:
            print(template, end="")
        return 0
    try:
        if arguments.action == "precommit":
            result = precommit_ai_all_cases(arguments.project_root)
        elif arguments.action == "run":
            result = run_ai_all_cases(arguments.project_root)
        else:
            result = verify_ai_all_cases(arguments.project_root)
    except (AllCasesConfigError, AllCasesRunError, OSError, ValueError) as error:
        print(f"AI all-cases research failed closed: {error}", file=sys.stderr)
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
            "AI all-cases research "
            f"status={result.status} events={result.event_count} "
            f"finalists={len(result.finalist_candidate_ids)}"
        )
    return 0


if __name__ == "__main__":  # pragma: no cover - module execution boundary
    raise SystemExit(main())
