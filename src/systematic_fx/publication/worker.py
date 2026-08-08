from __future__ import annotations

import argparse
import os
import socket
import sys
import time
import uuid
from pathlib import Path

from dotenv import load_dotenv

from systematic_fx.publication.config import load_publication_config
from systematic_fx.publication.contract import validate_public_payload
from systematic_fx.publication.outbox import (
    acknowledge_refresh,
    claim_refresh,
    connect_research,
    fail_refresh,
)
from systematic_fx.publication.provision import (
    provision_public_database,
    write_public_runtime_environment,
)
from systematic_fx.publication.public_store import (
    bootstrap_public_database,
    connect_public,
    publish_snapshot,
)
from systematic_fx.publication.snapshot import build_public_snapshot

REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PUBLICATION_CONFIG = REPOSITORY_ROOT / "configs/publication/research_site_v2.toml"
DEFAULT_HYPOTHESIS_CONFIG = REPOSITORY_ROOT / "configs/research/phase1_parent_hypotheses_v1.toml"
DEFAULT_CONTRACT = REPOSITORY_ROOT / "contracts/publication/research-snapshot.v2.schema.json"
DEFAULT_PUBLIC_MIGRATION = REPOSITORY_ROOT / "publication/migrations/0001_public_projection.sql"


def _required(value: str | None, *, name: str) -> str:
    if value and value.strip():
        return value.strip()
    raise RuntimeError(f"missing required database setting: {name}")


def process_once(
    *,
    research_database_url: str,
    public_database_url: str,
    worker_id: str,
    publication_config_path: Path = DEFAULT_PUBLICATION_CONFIG,
    hypothesis_config_path: Path = DEFAULT_HYPOTHESIS_CONFIG,
    contract_path: Path = DEFAULT_CONTRACT,
) -> bool:
    config = load_publication_config(publication_config_path)
    with connect_research(research_database_url) as research:
        request = claim_refresh(
            research,
            scope_key=config.scope_key,
            worker_id=worker_id,
        )
        if request is None:
            return False
        try:
            with research.transaction():
                research.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                payload = build_public_snapshot(
                    research,
                    revision=request.revision,
                    config=config,
                    hypothesis_config_path=hypothesis_config_path,
                )
            validate_public_payload(payload, contract_path)
            with connect_public(public_database_url) as public:
                publish_snapshot(
                    public,
                    campaign_key=config.campaign_key,
                    revision=request.revision,
                    payload=payload,
                )
            acknowledge_refresh(research, request=request, worker_id=worker_id)
        except Exception as error:
            fail_refresh(research, request=request, worker_id=worker_id, error=error)
            raise
    return True


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Project private research state into the isolated public database."
    )
    parser.add_argument("--env-file", type=Path, default=REPOSITORY_ROOT / ".env")
    parser.add_argument("--research-database-url")
    parser.add_argument("--public-database-url")
    parser.add_argument("--publication-config", type=Path, default=DEFAULT_PUBLICATION_CONFIG)
    parser.add_argument("--hypothesis-config", type=Path, default=DEFAULT_HYPOTHESIS_CONFIG)
    parser.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT)
    subparsers = parser.add_subparsers(dest="command", required=True)
    provision = subparsers.add_parser("provision-public")
    provision.add_argument("--runtime-env", type=Path, default=REPOSITORY_ROOT / ".env")
    provision.add_argument(
        "--web-env",
        type=Path,
        default=REPOSITORY_ROOT / "web/.env.local",
    )
    subparsers.add_parser("bootstrap-public")
    subparsers.add_parser("once")
    watch = subparsers.add_parser("watch")
    watch.add_argument("--interval-seconds", type=float, default=5.0)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    load_dotenv(args.env_file, override=False)
    if args.command == "provision-public":
        admin_url = _required(
            os.environ.get("SYSTEMATIC_FX_ADMIN_DATABASE_URL"),
            name="SYSTEMATIC_FX_ADMIN_DATABASE_URL",
        )
        report = provision_public_database(
            admin_database_url=admin_url,
            public_migration_path=DEFAULT_PUBLIC_MIGRATION,
        )
        write_public_runtime_environment(
            source_env_path=args.env_file,
            runtime_env_path=args.runtime_env,
            web_env_path=args.web_env,
            report=report,
        )
        print(f"public database: systematic_fx_public (created_now={report.database_created})")
        print(f"owner role created_now: {report.owner_created}")
        print(f"writer role created_now: {report.writer_created}")
        print(f"reader role created_now: {report.reader_created}")
        print("runtime credentials written with mode 0600")
        return 0

    public_url = _required(
        args.public_database_url or os.environ.get("SYSTEMATIC_FX_PUBLIC_DATABASE_URL"),
        name="SYSTEMATIC_FX_PUBLIC_DATABASE_URL",
    )
    if args.command == "bootstrap-public":
        bootstrap_public_database(public_url, DEFAULT_PUBLIC_MIGRATION)
        print("public projection database is ready")
        return 0

    research_url = _required(
        args.research_database_url or os.environ.get("SYSTEMATIC_FX_DATABASE_URL"),
        name="SYSTEMATIC_FX_DATABASE_URL",
    )
    worker_id = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
    kwargs = {
        "research_database_url": research_url,
        "public_database_url": public_url,
        "worker_id": worker_id,
        "publication_config_path": args.publication_config,
        "hypothesis_config_path": args.hypothesis_config,
        "contract_path": args.contract,
    }
    if args.command == "once":
        published = process_once(**kwargs)
        print("published latest revision" if published else "no pending publication")
        return 0

    interval = args.interval_seconds
    if interval <= 0 or interval > 60:
        raise ValueError("interval-seconds must be greater than 0 and at most 60")
    while True:
        try:
            process_once(**kwargs)
        except Exception as error:  # noqa: BLE001 - long-running worker records and retries
            print(f"publication attempt failed: {type(error).__name__}", file=sys.stderr)
        time.sleep(interval)


if __name__ == "__main__":
    raise SystemExit(main())
