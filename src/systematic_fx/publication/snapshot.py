from __future__ import annotations

from collections import Counter, defaultdict
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import psycopg

from systematic_fx.publication.config import PublicationConfig
from systematic_fx.research.hypotheses import HypothesisSpec, load_hypothesis_bundle

_REJECTION_REASON_LABELS = {
    "JOINT_POSITIVE_REGION_NOT_SINGLE_CONTIGUOUS_COMPONENT": (
        "No single contiguous positive region survived across the registered scenarios."
    ),
    "NO_INTERIOR_7_OF_9_STABLE_CELL": (
        "No interior neighborhood met the registered local-stability rule."
    ),
    "NO_STABLE_REGION_MEDOID": (
        "No admissible representative cell remained after the stability gates."
    ),
}


def _iso(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def _experiment_public_id(experiment_key: str) -> str:
    marker = ":experiment:"
    if marker not in experiment_key:
        return experiment_key
    suffix = experiment_key.split(marker, 1)[1]
    head, separator, version = suffix.rpartition(":v")
    return head if separator and version.isdigit() else suffix


def _pattern_public_id(pattern_key: str) -> str:
    return pattern_key.split(":", 1)[1] if ":" in pattern_key else pattern_key


def _humanize(identifier: str) -> str:
    words = identifier.split("_", 2)[-1].replace("_", " ")
    return words[:1].upper() + words[1:]


def _gate_state(result: str | None) -> str:
    if result is None:
        return "PENDING"
    if result == "ERROR":
        return "FAIL"
    return result


def _model_is_ready(version: str | None) -> bool:
    if not version or not version.strip():
        return False
    normalized = version.lower()
    return not any(marker in normalized for marker in ("pending", "placeholder", "unresolved"))


def _reason_categories(reasons: object) -> list[str]:
    if not isinstance(reasons, list):
        return []
    return [
        _REJECTION_REASON_LABELS.get(
            str(reason), "A registered screening-stability gate did not pass."
        )
        for reason in reasons
    ]


def _validation_state(outcome: dict[str, Any] | None) -> str:
    if outcome is None:
        return "PENDING"
    if outcome.get("audit_passed") is True:
        return "PASSED"
    return {
        None: "PENDING",
        "QUEUED": "PENDING",
        "RUNNING": "RUNNING",
        "FAILED": "FAILED",
        "REJECTED": "FAILED",
        "CANCELLED": "FAILED",
        # A successful attempt and its authoritative audit row are committed
        # atomically. Fail closed if an inconsistent snapshot is ever observed.
        "SUCCEEDED": "FAILED",
        "SKIPPED_DUPLICATE": "PENDING",
    }.get(outcome.get("validation_attempt_status"), "PENDING")


def _candidate_title(
    candidate_id: str,
    parent_ids: list[str],
    hypothesis_by_id: dict[str, HypothesisSpec],
) -> str:
    titles = [hypothesis_by_id[item].title for item in parent_ids if item in hypothesis_by_id]
    if len(titles) == 1:
        return titles[0]
    if titles:
        return " / ".join(titles)
    return _humanize(candidate_id)


def build_public_snapshot(
    connection: psycopg.Connection,
    *,
    revision: int,
    config: PublicationConfig,
    hypothesis_config_path: Path,
) -> dict[str, Any]:
    campaign = connection.execute(
        """
        SELECT c.campaign_id, c.campaign_key, c.name, c.status,
               c.trial_budget, c.finalist_budget, c.code_commit,
               c.cost_model_version, c.execution_model_version,
               c.created_at, c.frozen_at, c.closed_at,
               d.dataset_id, d.status AS dataset_status,
               d.expected_start_date, d.expected_end_date,
               d.created_at AS dataset_created_at
        FROM systematic_fx.campaigns AS c
        JOIN systematic_fx.datasets AS d ON d.dataset_id = c.dataset_id
        WHERE c.campaign_key = %s
        """,
        (config.campaign_key,),
    ).fetchone()
    if campaign is None:
        raise RuntimeError(f"campaign not found: {config.campaign_key}")

    campaign_id = campaign["campaign_id"]
    dataset_id = campaign["dataset_id"]
    source_stats = connection.execute(
        """
        SELECT count(*) AS source_files,
               count(*) FILTER (WHERE sha256 IS NOT NULL) AS identified_files
        FROM systematic_fx.source_files
        WHERE dataset_id = %s
        """,
        (dataset_id,),
    ).fetchone()
    campaign_counts = connection.execute(
        """
        SELECT
          (SELECT count(*) FROM systematic_fx.campaign_splits
            WHERE campaign_id = %s) AS splits,
          (SELECT count(*) FROM systematic_fx.campaign_days
            WHERE campaign_id = %s) AS calendar_days,
          (SELECT count(*) FROM systematic_fx.campaign_days
            WHERE campaign_id = %s AND eligibility_status = 'ELIGIBLE') AS eligible_days,
          (SELECT count(*) FROM systematic_fx.campaign_days
            WHERE campaign_id = %s AND eligibility_status = 'INELIGIBLE') AS ineligible_days,
          (SELECT count(*) FROM systematic_fx.campaign_days
            WHERE campaign_id = %s AND eligibility_status = 'PENDING') AS pending_days
        """,
        (campaign_id, campaign_id, campaign_id, campaign_id, campaign_id),
    ).fetchone()
    aggregate_quality = connection.execute(
        """
        SELECT result, observed, checked_at
        FROM systematic_fx.quality_checks
        WHERE dataset_id = %s
          AND check_name = 'FULL_MBP10_STRUCTURAL_SCAN_AGGREGATE'
        ORDER BY checked_at DESC, quality_check_id DESC
        LIMIT 1
        """,
        (dataset_id,),
    ).fetchone()
    provider_warning = connection.execute(
        """
        SELECT observed
        FROM systematic_fx.quality_checks
        WHERE dataset_id = %s AND check_name = 'provider_partial_metadata'
        ORDER BY checked_at DESC, quality_check_id DESC
        LIMIT 1
        """,
        (dataset_id,),
    ).fetchone()
    failed_dates = connection.execute(
        """
        SELECT observed->>'source_date' AS source_date,
               (observed->>'hard_violation_count')::bigint AS violations
        FROM systematic_fx.quality_checks AS q
        JOIN systematic_fx.source_files AS s ON s.source_file_id = q.source_file_id
        WHERE s.dataset_id = %s
          AND q.check_name = 'FULL_MBP10_STRUCTURAL_SCAN_FILE'
          AND q.result = 'FAIL'
        ORDER BY observed->>'source_date'
        """,
        (dataset_id,),
    ).fetchall()

    experiment_rows = connection.execute(
        """
        SELECT experiment_id, experiment_key, primary_family, status,
               hypothesis, direction, model_family, trial_budget,
               registered_at, frozen_at, completed_at
        FROM systematic_fx.experiments
        WHERE campaign_id = %s
        ORDER BY primary_family, experiment_key
        """,
        (campaign_id,),
    ).fetchall()
    pattern_rows = connection.execute(
        """
        WITH query_rows AS (
          SELECT e.query_spec #>> '{candidate_query,id}' AS query_id,
                 e.query_spec #> '{candidate_query,parent_hypothesis_ids}' AS parent_ids,
                 e.created_at
          FROM systematic_fx.discovery_exposures AS e
          WHERE e.campaign_id = %s
            AND e.exposure_type = 'QUERY'
            AND e.query_spec #>> '{candidate_query,id}' IS NOT NULL
        ),
        query_identity AS (
          SELECT DISTINCT ON (query_id) query_id, parent_ids
          FROM query_rows
          ORDER BY query_id, created_at
        ),
        query_counts AS (
          SELECT query_id, count(*) AS exposure_count
          FROM query_rows
          GROUP BY query_id
        )
        SELECT p.pattern_key, p.status, p.direction, p.support_count,
               jsonb_array_length(p.counterexamples) AS counterexample_count,
               p.updated_at, identity.parent_ids, counts.exposure_count
        FROM systematic_fx.pattern_ledger AS p
        LEFT JOIN query_identity AS identity
          ON identity.query_id = CASE
               WHEN position(':' IN p.pattern_key) > 0
               THEN split_part(p.pattern_key, ':', 2)
               ELSE p.pattern_key
             END
        LEFT JOIN query_counts AS counts ON counts.query_id = identity.query_id
        WHERE p.campaign_id = %s
        ORDER BY p.updated_at DESC, p.pattern_key
        """,
        (campaign_id, campaign_id),
    ).fetchall()
    exposure_counts = connection.execute(
        """
        SELECT exposure_type, count(*) AS exposure_count, max(created_at) AS latest_at
        FROM systematic_fx.discovery_exposures
        WHERE campaign_id = %s
        GROUP BY exposure_type
        """,
        (campaign_id,),
    ).fetchall()

    resolved_run_rows = connection.execute(
        """
        WITH resolved AS (
          SELECT spec.research_run_spec_id, spec.run_kind,
                 CASE
                   WHEN bool_or(attempt.status = 'SUCCEEDED') THEN 'SUCCEEDED'
                   WHEN bool_or(attempt.status = 'RUNNING') THEN 'RUNNING'
                   WHEN bool_or(attempt.status = 'QUEUED') THEN 'QUEUED'
                   ELSE COALESCE(
                     (array_agg(attempt.status ORDER BY attempt.attempt_number DESC)
                       FILTER (WHERE attempt.status IS NOT NULL))[1],
                     'QUEUED'
                   )
                 END AS resolved_status
          FROM systematic_fx.research_run_specs AS spec
          LEFT JOIN systematic_fx.research_run_attempts AS attempt
            ON attempt.research_run_spec_id = spec.research_run_spec_id
          WHERE spec.campaign_id = %s
          GROUP BY spec.research_run_spec_id, spec.run_kind
        )
        SELECT run_kind, resolved_status, count(*) AS run_count
        FROM resolved
        GROUP BY run_kind, resolved_status
        ORDER BY run_kind, resolved_status
        """,
        (campaign_id,),
    ).fetchall()
    attempt_stats = connection.execute(
        """
        SELECT count(*) AS attempts,
               count(*) FILTER (WHERE attempt.status = 'SKIPPED_DUPLICATE') AS reused,
               max(COALESCE(attempt.finished_at, attempt.started_at, attempt.queued_at)) AS latest_at
        FROM systematic_fx.research_run_attempts AS attempt
        JOIN systematic_fx.research_run_specs AS spec
          ON spec.research_run_spec_id = attempt.research_run_spec_id
        WHERE spec.campaign_id = %s
        """,
        (campaign_id,),
    ).fetchone()
    latest_source_revision = connection.execute(
        """
        SELECT spec.code_commit
        FROM systematic_fx.research_run_attempts AS attempt
        JOIN systematic_fx.research_run_specs AS spec
          ON spec.research_run_spec_id = attempt.research_run_spec_id
        WHERE spec.campaign_id = %s
        ORDER BY COALESCE(attempt.finished_at, attempt.started_at, attempt.queued_at) DESC,
                 attempt.research_run_attempt_id DESC
        LIMIT 1
        """,
        (campaign_id,),
    ).fetchone()

    outcome_rows = connection.execute(
        """
        SELECT manifest.outcome_replay_manifest_id, manifest.pattern_key,
               manifest.status, manifest.source_slice_count,
               manifest.source_occurrence_count, manifest.scenario_count,
               manifest.direction_count, manifest.cell_count_per_surface,
               manifest.expected_summary_count, manifest.expected_detail_record_count,
               manifest.planned_source_date_count, manifest.started_at,
               manifest.finished_at, manifest.created_at,
               checkpoint.completed_dates, cells.summary_cells,
               validation.status AS validation_attempt_status,
               validation.updated_at AS validation_updated_at,
               audit.passed AS audit_passed, audit.created_at AS audit_created_at
        FROM systematic_fx.phase1a_outcome_replay_manifests AS manifest
        LEFT JOIN LATERAL (
          SELECT count(*) AS completed_dates
          FROM systematic_fx.phase1a_outcome_replay_checkpoints AS checkpoint
          WHERE checkpoint.outcome_replay_manifest_id = manifest.outcome_replay_manifest_id
        ) AS checkpoint ON true
        LEFT JOIN LATERAL (
          SELECT count(*) AS summary_cells
          FROM systematic_fx.phase1a_outcome_cell_summaries AS cell
          WHERE cell.outcome_replay_manifest_id = manifest.outcome_replay_manifest_id
        ) AS cells ON true
        LEFT JOIN LATERAL (
          SELECT attempt.status,
                 COALESCE(attempt.finished_at, attempt.started_at, attempt.queued_at) AS updated_at
          FROM systematic_fx.research_run_specs AS spec
          JOIN systematic_fx.research_run_attempts AS attempt
            ON attempt.research_run_spec_id = spec.research_run_spec_id
          WHERE spec.campaign_id = manifest.campaign_id
            AND spec.run_kind = 'VALIDATION'
            AND spec.engine_version = 'phase1a_outcome_equivalence_audit_v1'
            AND spec.canonical_spec #>> '{parameters,predecessor_outcome_replay_manifest_id}'
                = manifest.outcome_replay_manifest_id::text
          ORDER BY attempt.attempt_number DESC
          LIMIT 1
        ) AS validation ON true
        LEFT JOIN systematic_fx.phase1a_outcome_replay_equivalence_audits AS audit
          ON audit.predecessor_outcome_replay_manifest_id = manifest.outcome_replay_manifest_id
        WHERE manifest.campaign_id = %s
        ORDER BY manifest.created_at
        """,
        (campaign_id,),
    ).fetchall()
    decision_rows = connection.execute(
        """
        SELECT decision.outcome_replay_manifest_id, decision.direction,
               decision.decision_label, decision.positive_region_size,
               decision.rejection_reasons, decision.created_at
        FROM systematic_fx.phase1a_outcome_screening_decisions AS decision
        JOIN systematic_fx.phase1a_outcome_replay_manifests AS manifest
          ON manifest.outcome_replay_manifest_id = decision.outcome_replay_manifest_id
        WHERE manifest.campaign_id = %s
        ORDER BY decision.outcome_replay_manifest_id, decision.direction
        """,
        (campaign_id,),
    ).fetchall()

    bundle = load_hypothesis_bundle(hypothesis_config_path)
    hypothesis_by_id = {item.hypothesis_id: item for item in bundle.hypotheses}
    experiments = {_experiment_public_id(row["experiment_key"]): row for row in experiment_rows}
    exposure_by_type = {row["exposure_type"]: row for row in exposure_counts}
    discovery_slices = int(exposure_by_type.get("AI_SLICE", {}).get("exposure_count", 0))
    query_exposures = int(exposure_by_type.get("QUERY", {}).get("exposure_count", 0))

    patterns: list[dict[str, Any]] = []
    pattern_by_id: dict[str, dict[str, Any]] = {}
    patterns_by_parent: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in pattern_rows:
        pattern_id = _pattern_public_id(row["pattern_key"])
        parent_ids = [str(item) for item in (row["parent_ids"] or [])]
        parent_specs = [hypothesis_by_id[item] for item in parent_ids if item in hypothesis_by_id]
        family = parent_specs[0].family if parent_specs else pattern_id[:2].upper()
        description = (
            " ".join(item.hypothesis for item in parent_specs)
            if parent_specs
            else "A governed Discovery observation registered from the fixed query ledger."
        )
        pattern = {
            "id": pattern_id,
            "family": family,
            "title": _candidate_title(pattern_id, parent_ids, hypothesis_by_id),
            "status": row["status"],
            "direction": row["direction"],
            "description": description,
            "parentHypothesisIds": parent_ids,
            "counterexampleCount": int(row["counterexample_count"]),
            "supportCount": int(row["support_count"]),
            "observedSlices": int(row["exposure_count"] or 0),
            "evidenceState": "DISCOVERY_OBSERVED",
            "screeningDecision": "PENDING",
            "updatedAt": row["updated_at"].isoformat(),
        }
        patterns.append(pattern)
        pattern_by_id[pattern_id] = pattern
        for parent_id in parent_ids:
            patterns_by_parent[parent_id].append(pattern)

    outcomes_by_pattern = {row["pattern_key"]: dict(row) for row in outcome_rows}
    decisions_by_manifest: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in decision_rows:
        decisions_by_manifest[row["outcome_replay_manifest_id"]].append(dict(row))

    candidates: list[dict[str, Any]] = []
    candidate_by_id: dict[str, dict[str, Any]] = {}
    for candidate_config in config.outcome_candidates:
        candidate_id = candidate_config.candidate_id
        pattern = pattern_by_id.get(candidate_id)
        parent_ids = list(pattern["parentHypothesisIds"]) if pattern else []
        outcome = outcomes_by_pattern.get(candidate_id)
        validation_state = _validation_state(outcome)
        predecessor = (
            candidate_by_id.get(candidate_config.predecessor_id)
            if candidate_config.predecessor_id is not None
            else None
        )
        if outcome is not None:
            replay_status = outcome["status"]
        elif predecessor is not None and predecessor["validation"]["status"] != "PASSED":
            replay_status = "BLOCKED"
        else:
            replay_status = "NOT_STARTED"

        raw_decisions = (
            decisions_by_manifest.get(outcome["outcome_replay_manifest_id"], [])
            if outcome is not None
            else []
        )
        decisions = []
        for direction in ("LONG", "SHORT"):
            raw = next((item for item in raw_decisions if item["direction"] == direction), None)
            decisions.append(
                {
                    "direction": direction,
                    "label": raw["decision_label"] if raw else "PENDING",
                    "positiveRegionSize": int(raw["positive_region_size"]) if raw else None,
                    "reasonCategories": _reason_categories(raw["rejection_reasons"]) if raw else [],
                }
            )

        labels = {item["label"] for item in decisions}
        if replay_status == "BLOCKED":
            candidate_stage = "BLOCKED"
        elif replay_status == "NOT_STARTED":
            candidate_stage = "NOT_STARTED"
        elif replay_status == "QUEUED":
            candidate_stage = "REPLAY_QUEUED"
        elif replay_status == "RUNNING":
            candidate_stage = "REPLAY_RUNNING"
        elif replay_status == "FAILED":
            candidate_stage = "FAILED"
        elif labels == {"SCREENING_REJECT"}:
            candidate_stage = "SCREENING_REJECTED"
        elif labels == {"SCREENING_SURVIVOR"}:
            candidate_stage = "SCREENING_SURVIVOR"
        elif "SCREENING_REJECT" in labels and "SCREENING_SURVIVOR" in labels:
            candidate_stage = "MIXED_DECISION"
        elif validation_state == "RUNNING":
            candidate_stage = "VALIDATION_RUNNING"
        else:
            candidate_stage = "DECISION_PENDING"

        candidate = {
            "id": candidate_id,
            "order": candidate_config.order,
            "title": _candidate_title(candidate_id, parent_ids, hypothesis_by_id),
            "family": pattern["family"] if pattern else candidate_id[:2].upper(),
            "parentHypothesisIds": parent_ids,
            "stage": candidate_stage,
            "discoveryOccurrences": int(pattern["supportCount"]) if pattern else 0,
            "replay": {
                "status": replay_status,
                "completedDates": int(outcome["completed_dates"]) if outcome else 0,
                "plannedDates": int(outcome["planned_source_date_count"]) if outcome else None,
                "sourceSlices": int(outcome["source_slice_count"]) if outcome else None,
                "sourceOccurrences": int(outcome["source_occurrence_count"]) if outcome else None,
                "scenarioCount": int(outcome["scenario_count"]) if outcome else None,
                "directionCount": int(outcome["direction_count"]) if outcome else None,
                "cellsPerSurface": int(outcome["cell_count_per_surface"]) if outcome else None,
                "summaryCells": int(outcome["summary_cells"]) if outcome else 0,
                "expectedSummaryCells": int(outcome["expected_summary_count"]) if outcome else None,
                "detailRecords": int(outcome["expected_detail_record_count"]) if outcome else None,
                "surfaceComplete": bool(
                    outcome
                    and outcome["status"] == "SUCCEEDED"
                    and outcome["summary_cells"] == outcome["expected_summary_count"]
                ),
                "startedAt": _iso(outcome["started_at"]) if outcome else None,
                "finishedAt": _iso(outcome["finished_at"]) if outcome else None,
            },
            "validation": {
                "kind": "UNINTERRUPTED_RESUME_BYTE_EQUIVALENCE",
                "status": validation_state,
                "updatedAt": _iso(
                    (outcome or {}).get("audit_created_at")
                    or (outcome or {}).get("validation_updated_at")
                ),
            },
            "decisions": decisions,
            "authorityNote": (
                "This is a conservative screening decision only. It grants no Backtest, "
                "Paper, or Live authority."
            ),
        }
        candidates.append(candidate)
        candidate_by_id[candidate_id] = candidate
        if pattern is not None:
            pattern["evidenceState"] = candidate_stage
            if labels == {"SCREENING_REJECT"}:
                pattern["screeningDecision"] = "SCREENING_REJECT"
            elif labels == {"SCREENING_SURVIVOR"}:
                pattern["screeningDecision"] = "SCREENING_SURVIVOR"
            elif "SCREENING_REJECT" in labels and "SCREENING_SURVIVOR" in labels:
                pattern["screeningDecision"] = "MIXED"

    hypotheses: list[dict[str, Any]] = []
    for spec in bundle.hypotheses:
        row = experiments.get(spec.hypothesis_id)
        related_patterns = patterns_by_parent.get(spec.hypothesis_id, [])
        related_candidates = [
            candidate_by_id[item["id"]]
            for item in related_patterns
            if item["id"] in candidate_by_id
        ]
        stages = {item["stage"] for item in related_candidates}
        if "SCREENING_SURVIVOR" in stages:
            decision = "SCREENING_SURVIVOR"
        elif "SCREENING_REJECTED" in stages:
            decision = "SCREENING_REJECT"
        elif "MIXED_DECISION" in stages:
            decision = "MIXED"
        elif "FAILED" in stages:
            decision = "FAILED"
        elif "REPLAY_RUNNING" in stages or "VALIDATION_RUNNING" in stages:
            decision = "OUTCOME_RUNNING"
        elif "BLOCKED" in stages:
            decision = "BLOCKED"
        elif related_patterns:
            decision = "DISCOVERY_OBSERVED"
        else:
            decision = "NOT_OBSERVED"
        updated_values = [item["updatedAt"] for item in related_patterns]
        if row:
            experiment_updated = row["completed_at"] or row["frozen_at"] or row["registered_at"]
            updated_values.append(experiment_updated.isoformat())
        hypotheses.append(
            {
                "id": spec.hypothesis_id,
                "family": spec.family,
                "title": spec.title,
                "direction": row["direction"] if row else spec.direction,
                "modelFamily": row["model_family"] if row else spec.model_family,
                "hypothesis": row["hypothesis"] if row else spec.hypothesis,
                "entryCondition": spec.entry_condition,
                "economicRationale": spec.economic_rationale,
                "features": list(spec.features),
                "status": row["status"] if row else "PROPOSED",
                "decision": decision,
                "observedPatternIds": [item["id"] for item in related_patterns],
                "supportCount": sum(int(item["supportCount"]) for item in related_patterns),
                "updatedAt": max(updated_values) if updated_values else None,
                "visibility": "PUBLIC_NOW",
            }
        )

    family_counts = Counter(item["family"] for item in hypotheses)
    families = [
        {
            "id": item.family_id,
            "title": item.title,
            "question": item.question,
            "description": item.description,
            "hypothesisCount": family_counts[item.family_id],
        }
        for item in config.families
    ]

    run_by_kind: dict[str, Counter[str]] = defaultdict(Counter)
    for row in resolved_run_rows:
        run_by_kind[row["run_kind"]][row["resolved_status"]] = int(row["run_count"])
    run_ledger = []
    for run_kind in sorted(run_by_kind):
        statuses = run_by_kind[run_kind]
        run_ledger.append(
            {
                "kind": run_kind,
                "total": sum(statuses.values()),
                "succeeded": statuses["SUCCEEDED"],
                "running": statuses["RUNNING"],
                "queued": statuses["QUEUED"],
                "failed": statuses["FAILED"],
                "rejected": statuses["REJECTED"],
                "cancelled": statuses["CANCELLED"],
            }
        )
    resolved_totals = Counter()
    for statuses in run_by_kind.values():
        resolved_totals.update(statuses)
    run_specs = sum(resolved_totals.values())

    observed = aggregate_quality["observed"] if aggregate_quality else {}
    result_counts = observed.get("result_counts", {})
    source_files = int(source_stats["source_files"])
    identified_files = int(source_stats["identified_files"])
    structural_state = _gate_state(aggregate_quality["result"] if aggregate_quality else None)
    source_identity_state = (
        "PASS" if source_files > 0 and identified_files == source_files else "PENDING"
    )
    calendar_state = (
        "PASS"
        if int(campaign_counts["calendar_days"]) > 0
        and int(campaign_counts["eligible_days"]) > 0
        and int(campaign_counts["pending_days"]) == 0
        else "PENDING"
    )
    split_state = "PASS" if int(campaign_counts["splits"]) > 0 else "PENDING"
    model_state = (
        "PASS"
        if _model_is_ready(campaign["cost_model_version"])
        and _model_is_ready(campaign["execution_model_version"])
        else "PENDING"
    )
    discovery_state = (
        "PASS"
        if discovery_slices >= config.discovery_slice_target and query_exposures > 0
        else "PENDING"
    )
    screening_authorized = all(
        state == "PASS"
        for state in (source_identity_state, calendar_state, split_state, model_state)
    )
    research_eligible = False

    active_validation = any(item["validation"]["status"] == "RUNNING" for item in candidates)
    active_replay = any(item["replay"]["status"] == "RUNNING" for item in candidates)
    completed_replays = sum(item["replay"]["status"] == "SUCCEEDED" for item in candidates)
    if active_validation:
        stage = "OUTCOME_VALIDATION"
    elif active_replay:
        stage = "OUTCOME_REPLAY"
    elif discovery_state != "PASS":
        stage = "DISCOVERY"
    elif completed_replays:
        stage = "ORDERED_SCREENING"
    else:
        stage = "OUTCOME_SCREENING"

    if any(item["validation"]["status"] == "FAILED" for item in candidates):
        audit_gate_state = "FAIL"
    elif any(item["validation"]["status"] == "RUNNING" for item in candidates):
        audit_gate_state = "PENDING"
    elif any(item["validation"]["status"] == "PASSED" for item in candidates):
        audit_gate_state = "PASS"
    else:
        audit_gate_state = "PENDING"

    gates = [
        {
            "id": "source-identity",
            "label": "Source identity",
            "state": source_identity_state,
            "scope": "BOTH",
            "detail": f"{identified_files:,} of {source_files:,} source files have full-content identities.",
        },
        {
            "id": "structural-quality",
            "label": "Full structural quality",
            "state": structural_state,
            "scope": "BACKTEST",
            "detail": (
                f"The complete scan records {int(observed.get('hard_violation_count', 0)):,} "
                "hard violations. This remains a Backtest-level blocker."
            ),
        },
        {
            "id": "screening-calendar",
            "label": "Conservative screening calendar",
            "state": calendar_state,
            "scope": "SCREENING",
            "detail": (
                f"{int(campaign_counts['eligible_days']):,} source dates are eligible and "
                f"{int(campaign_counts['ineligible_days']):,} are conservatively excluded."
            ),
        },
        {
            "id": "sealed-splits",
            "label": "Performance-independent splits",
            "state": split_state,
            "scope": "SCREENING",
            "detail": f"{int(campaign_counts['splits']):,} registered campaign split records are sealed.",
        },
        {
            "id": "screening-models",
            "label": "Conservative cost and execution policy",
            "state": model_state,
            "scope": "SCREENING",
            "detail": (
                "Frozen conservative assumptions authorize screening only; they are not actual "
                "broker cost or production execution evidence."
            ),
        },
        {
            "id": "discovery",
            "label": "Governed Discovery",
            "state": discovery_state,
            "scope": "SCREENING",
            "detail": (
                f"{discovery_slices:,} of {config.discovery_slice_target:,} slices and "
                f"{query_exposures:,} fixed-query exposures are registered."
            ),
        },
        {
            "id": "outcome-replay",
            "label": "Chronological outcome replay",
            "state": "PASS" if completed_replays else "PENDING",
            "scope": "SCREENING",
            "detail": f"{completed_replays:,} ordered candidate replay has completed successfully.",
        },
        {
            "id": "outcome-equivalence",
            "label": "Independent replay equivalence",
            "state": audit_gate_state,
            "scope": "SCREENING",
            "detail": (
                "An uninterrupted replay must match the resumed result byte for byte before the "
                "next ordered candidate can run."
            ),
        },
        {
            "id": "backtest-authority",
            "label": "Backtest / Paper / Live authority",
            "state": "BLOCKED",
            "scope": "BACKTEST",
            "detail": (
                "Point-in-time reference data, actual costs, measured execution, walk-forward, "
                "stress, and sealed-holdout evidence remain required."
            ),
        },
    ]

    evaluated_candidates = sum(
        item["stage"] in {"SCREENING_REJECTED", "SCREENING_SURVIVOR", "MIXED_DECISION"}
        for item in candidates
    )
    rejected_candidates = sum(item["stage"] == "SCREENING_REJECTED" for item in candidates)
    survivor_candidates = sum(item["stage"] == "SCREENING_SURVIVOR" for item in candidates)
    blocked_candidates = sum(item["stage"] == "BLOCKED" for item in candidates)
    pending_candidates = len(candidates) - evaluated_candidates

    timeline = [
        {
            "date": campaign["dataset_created_at"].date().isoformat(),
            "state": "PASS",
            "title": "Source catalog registered",
            "detail": f"{source_files:,} source files entered the immutable research control plane.",
        },
        {
            "date": campaign["created_at"].date().isoformat(),
            "state": campaign["status"],
            "title": f"{campaign['name']} created",
            "detail": f"The screening campaign opened with {campaign['trial_budget']:,} bounded variants.",
        },
    ]
    if aggregate_quality:
        timeline.append(
            {
                "date": aggregate_quality["checked_at"].date().isoformat(),
                "state": structural_state,
                "title": "Full structural quality scan completed",
                "detail": (
                    f"Complete coverage retained {int(observed.get('hard_violation_count', 0)):,} "
                    "hard violations and the Backtest-level failure."
                ),
            }
        )
    discovery_latest = exposure_by_type.get("QUERY", {}).get("latest_at")
    if discovery_latest:
        timeline.append(
            {
                "date": discovery_latest.date().isoformat(),
                "state": discovery_state,
                "title": "Governed Discovery completed",
                "detail": f"{query_exposures:,} fixed-query exposures accumulated {len(patterns):,} patterns.",
            }
        )
    for candidate in candidates:
        if candidate["replay"]["finishedAt"]:
            timeline.append(
                {
                    "date": candidate["replay"]["finishedAt"][:10],
                    "state": candidate["replay"]["status"],
                    "title": f"{candidate['title']} replay completed",
                    "detail": (
                        f"{candidate['replay']['completedDates']:,} dates and "
                        f"{candidate['replay']['summaryCells']:,} summary cells were verified."
                    ),
                }
            )
        decision_labels = {item["label"] for item in candidate["decisions"]}
        if decision_labels == {"SCREENING_REJECT"} and candidate["replay"]["finishedAt"]:
            timeline.append(
                {
                    "date": candidate["replay"]["finishedAt"][:10],
                    "state": "SCREENING_REJECT",
                    "title": f"{candidate['title']} rejected in both directions",
                    "detail": "LONG and SHORT both failed the preregistered conservative stability gates.",
                }
            )
        if candidate["validation"]["status"] == "RUNNING" and candidate["validation"]["updatedAt"]:
            timeline.append(
                {
                    "date": candidate["validation"]["updatedAt"][:10],
                    "state": "RUNNING",
                    "title": f"{candidate['title']} equivalence audit started",
                    "detail": "An independent uninterrupted replay is being compared with the resumed result.",
                }
            )

    published_at = datetime.now(UTC).isoformat()
    source_revision = (
        latest_source_revision["code_commit"] if latest_source_revision else campaign["code_commit"]
    )
    payload = {
        "metadata": {
            "schemaVersion": config.schema_version,
            "revision": revision,
            "dataAsOf": published_at[:10],
            "publishedAt": published_at,
            "sourceRevision": source_revision,
            "disclosurePolicyVersion": config.disclosure_policy_version,
        },
        "program": {
            "mode": config.program_mode,
            "policyState": config.policy_state,
            "maximumAuthority": config.maximum_authority,
            "backtestEligible": False,
            "paperEligible": False,
            "liveEligible": False,
            "disclosure": (
                "Only allowlisted aggregate evidence is published. Raw market rows, exact model "
                "surfaces, selected parameters, artifact locations, hashes, and internal errors remain private."
            ),
        },
        "campaign": {
            "key": campaign["campaign_key"],
            "name": campaign["name"],
            "status": campaign["status"],
            "stage": stage,
            "screeningAuthorized": screening_authorized,
            "researchEligible": research_eligible,
            "strategyVariantBudget": campaign["trial_budget"],
            "sealedHoldoutFinalistBudget": campaign["finalist_budget"],
            "summary": (
                f"Phase 1A is authorized for conservative screening only. {completed_replays} "
                f"candidate replay is complete, {rejected_candidates} candidate is screening-rejected, "
                f"and {pending_candidates} ordered candidate remains pending. No result grants "
                "Backtest, Paper, or Live authority."
            ),
        },
        "summary": {
            "families": len(families),
            "hypotheses": len(hypotheses),
            "observedPatterns": len(patterns),
            "discoverySlices": discovery_slices,
            "queryExposures": query_exposures,
            "runSpecs": run_specs,
            "runAttempts": int(attempt_stats["attempts"]),
            "succeededRuns": resolved_totals["SUCCEEDED"],
            "runningRuns": resolved_totals["RUNNING"],
            "failedRuns": resolved_totals["FAILED"],
            "reusedRuns": int(attempt_stats["reused"]),
            "outcomeCandidates": len(candidates),
            "evaluatedCandidates": evaluated_candidates,
            "screeningSurvivors": survivor_candidates,
            "screeningRejected": rejected_candidates,
            "pendingCandidates": pending_candidates,
            "blockedCandidates": blocked_candidates,
        },
        "dataQuality": {
            "datasetStatus": campaign["dataset_status"],
            "sourceFiles": source_files,
            "identifiedFiles": identified_files,
            "passedFiles": int(result_counts.get("PASS", 0)),
            "failedFiles": int(result_counts.get("FAIL", 0)),
            "rowGroups": int(observed.get("scanned_row_group_count", 0)),
            "eventRows": int(observed.get("scanned_row_count", 0)),
            "hardViolations": int(observed.get("hard_violation_count", 0)),
            "warningSymbols": int(
                provider_warning["observed"].get("partial_symbol_count", 0)
                if provider_warning
                else 0
            ),
            "coverageStart": _iso(campaign["expected_start_date"]),
            "coverageEnd": _iso(campaign["expected_end_date"]),
            "eligibleDays": int(campaign_counts["eligible_days"]),
            "ineligibleDays": int(campaign_counts["ineligible_days"]),
            "failedDates": [
                {"date": row["source_date"], "violations": int(row["violations"])}
                for row in failed_dates
            ],
        },
        "gates": gates,
        "families": families,
        "hypotheses": hypotheses,
        "patterns": sorted(patterns, key=lambda item: item["id"]),
        "runLedger": {
            "specs": run_specs,
            "attempts": int(attempt_stats["attempts"]),
            "reusedSuccesses": int(attempt_stats["reused"]),
            "byKind": run_ledger,
        },
        "outcomeCandidates": candidates,
        "timeline": sorted(timeline, key=lambda item: item["date"], reverse=True),
    }
    return payload
