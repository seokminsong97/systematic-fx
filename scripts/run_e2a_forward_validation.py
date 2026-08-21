"""Precommit or verify the permanently shadow-only e2a forward plan."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from campaigns.e2a_month_end_v1.forward import (
    FORWARD_AUTHORITY_SCOPE,
    FORWARD_LIFECYCLE_STATUS,
    E2AForwardError,
    E2AForwardStatus,
    E2AForwardUnavailable,
    observe_shadow_e2a_forward,
    precommit_e2a_forward,
    status_e2a_forward,
    verify_e2a_forward,
)
from systematic_fx.research.hypotheses import canonical_json_bytes


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m scripts.run_e2a_forward_validation",
        description=("Precommit, inspect, or verify the offline shadow-only e2a forward plan."),
    )
    parser.add_argument(
        "action",
        choices=("precommit", "status", "verify", "observe-shadow"),
    )
    parser.add_argument(
        "--project-root",
        type=Path,
        default=PROJECT_ROOT,
        help="systematic-fx checkout",
    )
    parser.add_argument(
        "--state-root",
        type=Path,
        help="optional forward-state root; relative paths are below project-root",
    )
    parser.add_argument("--json", action="store_true", help="emit canonical compact JSON")
    return parser


def _emit_status(result: E2AForwardStatus, *, as_json: bool) -> None:
    if as_json:
        sys.stdout.write(canonical_json_bytes(result.as_dict()).decode("utf-8") + "\n")
        return
    print(
        "e2a forward validation "
        f"status={FORWARD_LIFECYCLE_STATUS} "
        f"authority={FORWARD_AUTHORITY_SCOPE} "
        f"registration={result.as_dict()['registration_status']} "
        f"events={result.event_count}"
    )


def _emit_failure(
    *,
    action: str,
    code: str,
    as_json: bool,
    unavailable: bool,
) -> None:
    if as_json:
        document = {
            "action": action,
            "authority_scope": FORWARD_AUTHORITY_SCOPE,
            "error_code": code,
            "lifecycle_status": FORWARD_LIFECYCLE_STATUS,
            "status": "UNAVAILABLE" if unavailable else "FAILED_CLOSED",
        }
        sys.stderr.write(canonical_json_bytes(document).decode("utf-8") + "\n")
        return
    prefix = "unavailable" if unavailable else "failed closed"
    print(f"e2a forward validation {prefix}: {code}", file=sys.stderr)


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        if arguments.action == "precommit":
            result = precommit_e2a_forward(
                arguments.project_root,
                state_root=arguments.state_root,
            )
        elif arguments.action == "status":
            result = status_e2a_forward(
                arguments.project_root,
                state_root=arguments.state_root,
            )
        elif arguments.action == "verify":
            result = verify_e2a_forward(
                arguments.project_root,
                state_root=arguments.state_root,
            )
        else:
            observe_shadow_e2a_forward(
                arguments.project_root,
                state_root=arguments.state_root,
            )
    except E2AForwardUnavailable as error:
        _emit_failure(
            action=arguments.action,
            code=error.code,
            as_json=arguments.json,
            unavailable=True,
        )
        return 2
    except (E2AForwardError, OSError, ValueError) as error:
        _emit_failure(
            action=arguments.action,
            code=str(error),
            as_json=arguments.json,
            unavailable=False,
        )
        return 1
    _emit_status(result, as_json=arguments.json)
    return 0


if __name__ == "__main__":  # pragma: no cover - module execution boundary
    raise SystemExit(main())
