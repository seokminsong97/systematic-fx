"""Build and seal the frozen trade-bar dataset from the raw SHA manifest."""

from __future__ import annotations

import json
from pathlib import Path

from systematic_fx.features.bars import BAR_VERSION, DailyBarBuildReport
from systematic_fx.research.bar_artifacts import (
    BarArtifactDescriptor,
    publish_bar_artifact_bytes,
)
from systematic_fx.research.bar_config import load_bar_pattern_config
from systematic_fx.research.bar_pipeline import (
    BAR_DATASET_MANIFEST_SCHEMA,
    execute_bar_dataset_build,
    load_bar_dataset_plan,
)
from systematic_fx.research.hypotheses import canonical_sha256


def main() -> None:
    project_root = Path.cwd().resolve()
    config = load_bar_pattern_config(project_root)
    plan = load_bar_dataset_plan(project_root)

    def progress(ordinal: int, total: int, report: DailyBarBuildReport) -> None:
        if ordinal == 1 or ordinal % 25 == 0 or ordinal == total:
            source_date = report.plan.source_date
            status = report.plan.status.value
            print(
                json.dumps(
                    {
                        "completed": ordinal,
                        "source_date": source_date.isoformat(),
                        "status": status,
                        "total": total,
                    },
                    sort_keys=True,
                ),
                flush=True,
            )

    result = execute_bar_dataset_build(plan, progress=progress)
    document = result.as_dict()
    schema_sha256 = canonical_sha256(
        {
            "artifact_schema": BAR_DATASET_MANIFEST_SCHEMA,
            "bar_version": BAR_VERSION,
            "required_top_level_keys": sorted(document),
        }
    )
    descriptor = BarArtifactDescriptor(
        artifact_key=f"bar_pattern_discovery_v1:trade_bar_dataset:{result.sha256}",
        artifact_type="trade_bar_dataset_manifest",
        artifact_schema=BAR_DATASET_MANIFEST_SCHEMA,
        artifact_version=1,
        record_count=len(result.reports),
        schema_sha256=schema_sha256,
        source_manifest_sha256=plan.source_manifest_sha256,
        logical_identity={
            "bar_campaign_definition_sha256": config.definition_sha256,
            "bar_version": BAR_VERSION,
            "build_plan_sha256": plan.sha256,
            "eligible_active_date_count": len(result.eligible_active_dates),
            "manifest_sha256": result.sha256,
        },
        media_type="application/json",
        file_suffix=".json",
    )
    artifact = publish_bar_artifact_bytes(
        project_root,
        descriptor,
        result.canonical_bytes,
    )
    print(
        json.dumps(
            {
                "artifact_identity_sha256": descriptor.identity_sha256,
                "artifact_path": artifact.path.as_posix(),
                "artifact_sha256": artifact.sha256,
                "bar_artifact_count": sum(len(item.artifacts) for item in result.reports),
                "eligible_active_date_count": len(result.eligible_active_dates),
                "manifest_sha256": result.sha256,
                "source_file_count": len(result.reports),
            },
            sort_keys=True,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
