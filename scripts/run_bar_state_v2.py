"""Plan or execute the governed State-Conditional Bar Model v2 Discovery."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from systematic_fx.config.settings import Settings
from systematic_fx.research.bar_state_run import (
    BarStateRunProgress,
    run_governed_bar_state_discovery,
)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--plan-only", action="store_true", help="validate and print the plan")
    mode.add_argument("--run", action="store_true", help="execute Discovery after governance")
    parser.add_argument("--json", action="store_true", help="emit canonical JSON records")
    parser.add_argument("--database-url", help="override the configured PostgreSQL URL")
    parser.add_argument("--manifest-path", type=Path, help="override the frozen manifest path")
    parser.add_argument("--project-root", type=Path, default=Path.cwd())
    return parser.parse_args()


def main() -> int:
    arguments = _arguments()
    root = arguments.project_root.expanduser().resolve()
    mode = "RUN" if arguments.run else "PLAN_ONLY"
    database_url = arguments.database_url
    if mode == "RUN" and not database_url:
        database_url = Settings.from_env(working_directory=root).database_url

    def progress(item: BarStateRunProgress) -> None:
        payload = item.as_dict()
        if arguments.json:
            print(json.dumps({"progress": payload}, sort_keys=True), file=sys.stderr, flush=True)
        else:
            print(
                f"{item.stage} {item.completed}/{item.total} rss={item.rss_bytes}",
                file=sys.stderr,
                flush=True,
            )

    report = run_governed_bar_state_discovery(
        root,
        mode=mode,
        database_url=database_url,
        manifest_path=arguments.manifest_path,
        progress=progress,
    )
    if arguments.json:
        print(json.dumps(report.as_dict(), sort_keys=True), flush=True)
    else:
        print(
            f"{report.disposition}: candidates={len(report.plan.candidate_keys)} "
            f"scope={report.plan.discovery_active_dates[0]}.."
            f"{report.plan.discovery_active_dates[-1]}",
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
