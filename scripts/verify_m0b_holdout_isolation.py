"""Verify the sealed-holdout denial using only the daemon's actual login URL."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict

from systematic_fx.db.holdout_isolation import (
    HoldoutIsolationError,
    verify_research_holdout_isolation,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database-url",
        default=os.environ.get("SYSTEMATIC_FX_RESEARCH_DATABASE_URL"),
    )
    parser.add_argument(
        "--expected-session-user",
        default=os.environ.get("SYSTEMATIC_FX_RESEARCH_DATABASE_USER"),
    )
    arguments = parser.parse_args()
    if not arguments.database_url or not arguments.expected_session_user:
        parser.error(
            "set SYSTEMATIC_FX_RESEARCH_DATABASE_URL and SYSTEMATIC_FX_RESEARCH_DATABASE_USER"
        )
    try:
        report = verify_research_holdout_isolation(
            arguments.database_url,
            expected_session_user=arguments.expected_session_user,
        )
    except HoldoutIsolationError as error:
        print(json.dumps({"status": "NOT_PROVISIONED", "error": str(error)}, sort_keys=True))
        return 2
    print(json.dumps(asdict(report), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
