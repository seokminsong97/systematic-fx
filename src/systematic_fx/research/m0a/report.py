"""Human-readable Markdown reporting for one completed M0a search epoch."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Final

from systematic_fx.research.m0a.evaluate import (
    SEARCH_DATA_RESULT,
    CandidateEvaluation,
    EpochEvaluation,
)
from systematic_fx.research.m0a.model import M0aDataError

REPORT_SCHEMA: Final = "systematic_fx.m0a_report.v1"
DEFAULT_EXECUTION_ASSUMPTIONS: Final = (
    "Long entry uses the next eligible ask; short entry uses the next eligible bid.",
    "The screening latency model applies the precommitted adverse entry tick assumption.",
    "Long exits are executable on bid; short exits are executable on ask.",
    "Passive take-profit requires trade-through; a mere touch is not a fill.",
    "Stops use conservative marketable execution and precommitted costs/slippage.",
    "One position is allowed at a time and occupancy ends at the candidate-specific exit_ts.",
    "Session policy is NO_CROSS_CLOSED_MARKET; roll positions never change instrument_id.",
)


class ReportError(M0aDataError):
    """Report metadata is incomplete or would overstate research authority."""


@dataclass(frozen=True, slots=True)
class EpochReportMetadata:
    epoch_id: str
    epoch_hash: str
    dataset_version: str
    dataset_hash: str
    feature_version: str
    label_version: str
    code_commit: str
    execution_model_version: str
    real_candidate_budget: int
    null_candidate_budget: int
    retry_count: int = 0
    roll_exclusion_count: int = 0
    session_exclusion_count: int = 0
    ambiguous_label_count: int = 0
    sealed_holdout_status: str = "UNTOUCHED_ACCESS_DENIED"
    candidate_registered_at: Mapping[str, str] | None = None
    execution_assumptions: Sequence[str] = DEFAULT_EXECUTION_ASSUMPTIONS

    def __post_init__(self) -> None:
        required = (
            self.epoch_id,
            self.epoch_hash,
            self.dataset_version,
            self.dataset_hash,
            self.feature_version,
            self.label_version,
            self.code_commit,
            self.execution_model_version,
        )
        if any(not isinstance(item, str) or not item for item in required):
            raise ReportError("epoch report identity fields must be non-empty strings")
        if len(self.epoch_hash) != 64 or len(self.dataset_hash) != 64:
            raise ReportError("epoch_hash and dataset_hash must be SHA-256 hex digests")
        counts = (
            self.real_candidate_budget,
            self.null_candidate_budget,
            self.retry_count,
            self.roll_exclusion_count,
            self.session_exclusion_count,
            self.ambiguous_label_count,
        )
        if any(isinstance(item, bool) or not isinstance(item, int) or item < 0 for item in counts):
            raise ReportError("report budget/count fields must be non-negative integers")
        if self.sealed_holdout_status != "UNTOUCHED_ACCESS_DENIED":
            raise ReportError("M0a report must show an untouched, inaccessible sealed holdout")
        if not self.execution_assumptions or any(not item for item in self.execution_assumptions):
            raise ReportError("execution assumptions must be non-empty")

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> EpochReportMetadata:
        registrations_value = value.get("candidate_registered_at")
        registrations: Mapping[str, str] | None
        if registrations_value is None:
            registrations = None
        elif isinstance(registrations_value, Mapping):
            registrations = {
                str(key): str(timestamp) for key, timestamp in registrations_value.items()
            }
        else:
            raise ReportError("candidate_registered_at must be a mapping")
        assumptions_value = value.get("execution_assumptions", DEFAULT_EXECUTION_ASSUMPTIONS)
        if isinstance(assumptions_value, str) or not isinstance(assumptions_value, Sequence):
            raise ReportError("execution_assumptions must be a sequence")
        return cls(
            epoch_id=str(value["epoch_id"]),
            epoch_hash=str(value["epoch_hash"]),
            dataset_version=str(value["dataset_version"]),
            dataset_hash=str(value["dataset_hash"]),
            feature_version=str(value["feature_version"]),
            label_version=str(value["label_version"]),
            code_commit=str(value["code_commit"]),
            execution_model_version=str(value["execution_model_version"]),
            real_candidate_budget=int(value["real_candidate_budget"]),
            null_candidate_budget=int(value["null_candidate_budget"]),
            retry_count=int(value.get("retry_count", 0)),
            roll_exclusion_count=int(value.get("roll_exclusion_count", 0)),
            session_exclusion_count=int(value.get("session_exclusion_count", 0)),
            ambiguous_label_count=int(value.get("ambiguous_label_count", 0)),
            sealed_holdout_status=str(
                value.get("sealed_holdout_status", "UNTOUCHED_ACCESS_DENIED")
            ),
            candidate_registered_at=registrations,
            execution_assumptions=tuple(str(item) for item in assumptions_value),
        )


def _metric_summary(candidate: CandidateEvaluation) -> list[str]:
    raw = candidate.raw_event_metrics
    flat = candidate.flat_only_metrics
    sequential = candidate.sequential_metrics
    stressed = candidate.stressed_cost_metrics
    return [
        f"- Raw event count: {raw.trade_count} eligible / {raw.signal_count} matching",
        f"- Flat-only trade count: {flat.trade_count}",
        f"- Sequential trade count: {sequential.trade_count}",
        (
            f"- TP / SL / timeout: {sequential.tp_first_count} / "
            f"{sequential.sl_first_count} / {sequential.timeout_count}"
        ),
        f"- TP-first probability (ppm): {sequential.tp_probability_ppm}",
        (
            f"- Sequential gross / cost / net PnL (ticks): "
            f"{sequential.gross_pnl_ticks} / {sequential.cost_ticks} / "
            f"{sequential.net_pnl_ticks}"
        ),
        f"- Sequential net EV (ticks): {sequential.as_dict()['net_ev_ticks']}",
        f"- Maximum drawdown (ticks): {sequential.maximum_drawdown_ticks}",
        f"- Active days: {sequential.active_days}",
        f"- Session distribution: {sequential.as_dict()['session_distribution']}",
        (
            f"- Stressed-cost net PnL / EV (ticks): {stressed.net_pnl_ticks} / "
            f"{stressed.as_dict()['net_ev_ticks']}"
        ),
    ]


def _fold_table(candidate: CandidateEvaluation) -> list[str]:
    lines = [
        (
            "| Fold | Role | Purge seconds | Calibration trades | Validation trades | "
            "Validation net PnL | Validation net EV |"
        ),
        "|---:|---|---:|---:|---:|---:|---:|",
    ]
    for fold in candidate.folds:
        validation = fold.validation_metrics
        lines.append(
            f"| {fold.fold_number} | {fold.result_role} | {fold.purge_seconds} | "
            f"{fold.calibration_metrics.trade_count} | {validation.trade_count} | "
            f"{validation.net_pnl_ticks} | {validation.as_dict()['net_ev_ticks']} |"
        )
    if not candidate.folds:
        lines.append("| - | SEARCH_DATA_EXPLORATORY | - | 0 | 0 | 0 | - |")
    return lines


def _control_table(candidate: CandidateEvaluation) -> list[str]:
    lines = [
        "| Control | Trades | Net PnL ticks | Net EV ticks | Candidate uplift ticks |",
        "|---|---:|---:|---:|---:|",
    ]
    for control in (candidate.circular_shift_control, candidate.matched_random_control):
        lines.append(
            f"| {control.method} | {control.metrics.trade_count} | "
            f"{control.metrics.net_pnl_ticks} | {control.metrics.as_dict()['net_ev_ticks']} | "
            f"{control.as_dict()['net_ev_uplift_ticks']} |"
        )
    return lines


def _candidate_table(
    evaluation: EpochEvaluation,
    registrations: Mapping[str, str],
) -> list[str]:
    lines = [
        "| Candidate hash | Family | Direction | Barrier | Status | registered_at |",
        "|---|---|---|---|---|---|",
    ]
    for result in evaluation.ranked:
        candidate = result.candidate
        registered_at = registrations.get(candidate.candidate_hash, "NOT_RECORDED")
        lines.append(
            f"| `{candidate.candidate_hash}` | {candidate.family_id} | "
            f"{candidate.direction.value} | {candidate.barrier.barrier_id} | "
            f"{result.status} | {registered_at} |"
        )
    if not evaluation.evaluations:
        lines.append("| - | - | - | - | NO_COMPLETED_CANDIDATE | NOT_RECORDED |")
    return lines


def render_markdown_report(
    evaluation: EpochEvaluation,
    metadata: EpochReportMetadata | Mapping[str, object],
) -> str:
    """Render a deterministic report containing the mandatory authority boundary."""

    if not isinstance(evaluation, EpochEvaluation):
        raise ReportError("evaluation must be an EpochEvaluation")
    context = (
        metadata
        if isinstance(metadata, EpochReportMetadata)
        else EpochReportMetadata.from_mapping(metadata)
    )
    if (
        not evaluation.sealed_holdout_untouched
        or evaluation.paper_eligible
        or evaluation.live_eligible
    ):
        raise ReportError("M0a evaluation attempts to exceed its research-only authority")
    registrations = context.candidate_registered_at or {}
    top = evaluation.top_candidate
    successful = len(evaluation.evaluations)
    budget_used = evaluation.real_experiments_attempted

    lines = [
        f"# M0a Research Epoch `{context.epoch_id}`",
        "",
        "> **Search-data result — Exploratory only.**",
        "> **Sealed holdout untouched.**",
        "> **Not paper eligible. Not live eligible.**",
        "",
        (
            "This report may describe an exploratory candidate or search-data survivor only. "
            "Every survivor is awaiting sealed holdout and awaiting forward evidence."
        ),
        "",
        "## Epoch and provenance",
        "",
        f"- Report schema: `{REPORT_SCHEMA}`",
        f"- Epoch ID: `{context.epoch_id}`",
        f"- Epoch hash: `{context.epoch_hash}`",
        f"- Dataset version / hash: `{context.dataset_version}` / `{context.dataset_hash}`",
        f"- Feature version: `{context.feature_version}`",
        f"- Label version: `{context.label_version}`",
        f"- Execution model version: `{context.execution_model_version}`",
        f"- Code commit: `{context.code_commit}`",
        f"- Random seed: `{evaluation.seed}`",
        "",
        "## Budget and execution status",
        "",
        f"- Real experiments attempted: {evaluation.real_experiments_attempted}",
        f"- Real candidate budget: {context.real_candidate_budget}",
        f"- Real budget used: {budget_used}",
        f"- Null experiments attempted: {evaluation.null_experiments_attempted}",
        f"- Null candidate budget: {context.null_candidate_budget}",
        f"- Null budget used: {evaluation.null_experiments_attempted}",
        f"- Completed candidate evaluations: {successful}",
        f"- Failures: {len(evaluation.failures)}",
        f"- Retries: {context.retry_count}",
        "",
        "## Candidate status",
        "",
        *_candidate_table(evaluation, registrations),
    ]

    if top is not None:
        lines.extend(
            [
                "",
                "## Top exploratory search-data result",
                "",
                f"- Candidate hash: `{top.candidate.candidate_hash}`",
                f"- Candidate status: `{top.status}`",
                (
                    "- candidate_registered_at: "
                    f"{registrations.get(top.candidate.candidate_hash, 'NOT_RECORDED')}"
                ),
                f"- Authority status: `{top.authority_status}`",
                *(_metric_summary(top)),
                "",
                "### Search-data walk-forward folds",
                "",
                *_fold_table(top),
                "",
                "### Null/control comparison",
                "",
                *_control_table(top),
            ]
        )

    lines.extend(
        [
            "",
            "## Execution assumptions",
            "",
            *(f"- {assumption}" for assumption in context.execution_assumptions),
            "",
            (
                "The passive trade-through rule is conservative but may still favor wider "
                "take-profit distances relative to narrower brackets; no MBO queue position "
                "is claimed."
            ),
            "",
            "## Exclusions and label diagnostics",
            "",
            f"- Roll/roll-guard exclusions: {context.roll_exclusion_count}",
            f"- Session-boundary exclusions: {context.session_exclusion_count}",
            f"- Ambiguous labels: {context.ambiguous_label_count}",
            "- Roll invariant: a trade never changes instrument_id after entry.",
            (
                "- Session invariant: eligibility is decided before the outcome under "
                "NO_CROSS_CLOSED_MARKET."
            ),
            "",
            "## Sealed holdout and promotion boundary",
            "",
            f"- Sealed holdout status: `{context.sealed_holdout_status}`",
            "- Sealed holdout untouched: true",
            "- Candidate classification: exploratory candidate / search-data survivor only",
            "- Awaiting sealed holdout: true",
            "- Awaiting forward evidence: true",
            "- Not paper eligible: true",
            "- Not live eligible: true",
        ]
    )
    if evaluation.failures:
        lines.extend(["", "## Failures", ""])
        lines.extend(
            f"- `{failure.candidate_hash}` — {failure.error_type}: {failure.error}"
            for failure in evaluation.failures
        )
    return "\n".join(lines) + "\n"


render_epoch_report = render_markdown_report


def _record_field(record: object, name: str, default: object = None) -> object:
    if isinstance(record, Mapping):
        return record.get(name, default)
    return getattr(record, name, default)


def _record_mapping(value: object, *, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ReportError(f"{label} must be a mapping")
    return value


def _durable_evaluation(record: object) -> Mapping[str, object]:
    value = _record_field(record, "evaluation", {})
    if value is None:
        return {}
    mapping = _record_mapping(value, label="durable candidate evaluation")
    # Accept either the evaluation_json payload or the outer immutable result
    # artifact document produced by LocalArtifactStore.
    nested = mapping.get("evaluation")
    return _record_mapping(nested, label="durable artifact evaluation") if nested else mapping


def _durable_metrics(evaluation: Mapping[str, object], key: str) -> Mapping[str, object]:
    return _record_mapping(evaluation.get(key, {}), label=f"durable {key}")


def _durable_candidate_payload(
    record: object, evaluation: Mapping[str, object]
) -> Mapping[str, object]:
    value = evaluation.get("candidate") or _record_field(record, "candidate_payload", {})
    return _record_mapping(value, label="durable candidate payload")


def _durable_retry_count(records: Sequence[object], *, fallback: int) -> int:
    """Derive retries from durable attempt cardinality when the loader supplies it."""

    observed = False
    retries = 0
    for record in records:
        attempt_count = _record_field(record, "attempt_count", None)
        if attempt_count is None:
            attempts = _record_field(record, "attempts", None)
            if isinstance(attempts, Sequence) and not isinstance(attempts, (str, bytes)):
                attempt_count = len(attempts)
        if attempt_count is None:
            attempt_metadata = _record_field(record, "attempt_metadata", None)
            if isinstance(attempt_metadata, Mapping):
                attempt_count = attempt_metadata.get("attempt_count")
                if attempt_count is None and isinstance(attempt_metadata.get("attempts"), Sequence):
                    attempt_count = len(attempt_metadata["attempts"])
        if attempt_count is None:
            continue
        if (
            isinstance(attempt_count, bool)
            or not isinstance(attempt_count, int)
            or attempt_count < 1
        ):
            raise ReportError("durable attempt_count must be a positive integer")
        observed = True
        retries += attempt_count - 1
    return retries if observed else fallback


def _metric_value(metrics: Mapping[str, object], key: str, default: object = 0) -> object:
    return metrics.get(key, default)


def _durable_fold_table(evaluation: Mapping[str, object]) -> list[str]:
    rows = evaluation.get("fold_metrics", [])
    if isinstance(rows, (str, bytes)) or not isinstance(rows, Sequence):
        raise ReportError("durable fold_metrics must be a sequence")
    lines = [
        (
            "| Fold | Role | Purge seconds | Calibration trades | Validation trades | "
            "Validation net PnL | Validation net EV |"
        ),
        "|---:|---|---:|---:|---:|---:|---:|",
    ]
    for value in rows:
        fold = _record_mapping(value, label="durable fold")
        calibration = _record_mapping(
            fold.get("calibration_metrics", {}), label="durable calibration metrics"
        )
        validation = _record_mapping(
            fold.get("validation_metrics", {}), label="durable validation metrics"
        )
        lines.append(
            f"| {fold.get('fold_number', '-')} | {fold.get('result_role', SEARCH_DATA_RESULT)} | "
            f"{fold.get('purge_seconds', '-')} | {calibration.get('trade_count', 0)} | "
            f"{validation.get('trade_count', 0)} | {validation.get('net_pnl_ticks', 0)} | "
            f"{validation.get('net_ev_ticks', None)} |"
        )
    if not rows:
        lines.append("| - | SEARCH_DATA_EXPLORATORY | - | 0 | 0 | 0 | - |")
    return lines


def _durable_control_table(evaluation: Mapping[str, object]) -> list[str]:
    values = (
        evaluation.get("circular_shift_control"),
        evaluation.get("matched_random_control"),
    )
    lines = [
        "| Control | Trades | Net PnL ticks | Net EV ticks | Candidate uplift ticks |",
        "|---|---:|---:|---:|---:|",
    ]
    present = 0
    for value in values:
        if value is None:
            continue
        control = _record_mapping(value, label="durable control")
        metrics = _record_mapping(control.get("metrics", {}), label="durable control metrics")
        lines.append(
            f"| {control.get('method', control.get('control_id', 'UNKNOWN'))} | "
            f"{metrics.get('trade_count', 0)} | {metrics.get('net_pnl_ticks', 0)} | "
            f"{metrics.get('net_ev_ticks', None)} | "
            f"{control.get('net_ev_uplift_ticks', None)} |"
        )
        present += 1
    if present == 0:
        lines.append("| NO_RECORDED_CONTROL | 0 | 0 | - | - |")
    return lines


def render_durable_markdown_report(
    epoch_record: Mapping[str, object] | object,
    candidate_records: Sequence[Mapping[str, object] | object],
    metadata: EpochReportMetadata | Mapping[str, object],
) -> str:
    """Render directly from verified ledger mappings after process restart.

    Each candidate record must expose ``candidate_sha256``, ``candidate_kind``,
    ``status``, ``registered_at``, ``candidate_payload``, and ``evaluation``.
    ``evaluation`` may be either the stored evaluation JSON or its outer result
    artifact.  This avoids reconstructing runtime dataclasses from durable JSON.
    """

    context = (
        metadata
        if isinstance(metadata, EpochReportMetadata)
        else EpochReportMetadata.from_mapping(metadata)
    )
    records = tuple(candidate_records)
    real_records = tuple(
        record for record in records if str(_record_field(record, "candidate_kind", "")) == "REAL"
    )
    null_records = tuple(
        record for record in records if str(_record_field(record, "candidate_kind", "")) == "NULL"
    )
    if len(real_records) > context.real_candidate_budget:
        raise ReportError("durable report real candidate count exceeds budget")
    if len(null_records) > context.null_candidate_budget:
        raise ReportError("durable report null candidate count exceeds budget")

    evaluated_real: list[tuple[object, Mapping[str, object]]] = []
    for record in real_records:
        evaluation = _durable_evaluation(record)
        if evaluation:
            evaluated_real.append((record, evaluation))
    evaluated_real.sort(
        key=lambda item: (
            str(_record_field(item[0], "status", "")) != "REGISTERED",
            -int(_durable_metrics(item[1], "sequential_metrics").get("net_pnl_ticks", 0)),
            str(_record_field(item[0], "candidate_sha256", "")),
        )
    )
    top = evaluated_real[0] if evaluated_real else None
    candidate_status_counts = _record_field(epoch_record, "candidate_status_counts", {})
    attempt_status_counts = _record_field(epoch_record, "attempt_status_counts", {})
    status_counts = (
        dict(candidate_status_counts) if isinstance(candidate_status_counts, Mapping) else {}
    )
    attempt_counts = (
        dict(attempt_status_counts) if isinstance(attempt_status_counts, Mapping) else {}
    )
    failure_count = int(status_counts.get("FAILED", 0)) + int(status_counts.get("CRASHED", 0))
    retry_count = _durable_retry_count(records, fallback=context.retry_count)

    lines = [
        f"# M0a Research Epoch `{context.epoch_id}`",
        "",
        "> **Search-data result — Exploratory only.**",
        "> **Sealed holdout untouched.**",
        "> **Not paper eligible. Not live eligible.**",
        "",
        "Every reported survivor is awaiting sealed holdout and awaiting forward evidence.",
        "",
        "## Epoch and provenance",
        "",
        f"- Report schema: `{REPORT_SCHEMA}`",
        f"- Epoch ID / hash: `{context.epoch_id}` / `{context.epoch_hash}`",
        f"- Dataset version / hash: `{context.dataset_version}` / `{context.dataset_hash}`",
        f"- Feature / label version: `{context.feature_version}` / `{context.label_version}`",
        f"- Execution model version: `{context.execution_model_version}`",
        f"- Code commit: `{context.code_commit}`",
        "",
        "## Durable budget and execution status",
        "",
        f"- Epoch status: {_record_field(epoch_record, 'status', 'UNKNOWN')}",
        (
            f"- Real experiments attempted / budget: {len(real_records)} / "
            f"{context.real_candidate_budget}"
        ),
        f"- Real budget used: {len(real_records)}",
        (
            f"- Null experiments attempted / budget: {len(null_records)} / "
            f"{context.null_candidate_budget}"
        ),
        f"- Null budget used: {len(null_records)}",
        f"- Candidate status counts: {status_counts}",
        f"- Attempt status counts: {attempt_counts}",
        f"- Failures: {failure_count}",
        f"- Retries: {retry_count}",
        "",
        "## Candidate status",
        "",
        "| Kind | Candidate hash | Family | Direction | Barrier | Status | registered_at |",
        "|---|---|---|---|---|---|---|",
    ]
    for record in records:
        evaluation = _durable_evaluation(record)
        candidate = _durable_candidate_payload(record, evaluation)
        barrier = candidate.get("barrier", {})
        barrier_id = barrier.get("barrier_id", "-") if isinstance(barrier, Mapping) else "-"
        registered_at = _record_field(record, "registered_at", None)
        if hasattr(registered_at, "isoformat"):
            registered_at = registered_at.isoformat()
        lines.append(
            f"| {_record_field(record, 'candidate_kind', '-')} | "
            f"`{_record_field(record, 'candidate_sha256', '-')}` | "
            f"{candidate.get('family_id', '-')} | {candidate.get('direction', '-')} | "
            f"{barrier_id} | {_record_field(record, 'status', '-')} | "
            f"{registered_at or 'NOT_REGISTERED'} |"
        )
    if not records:
        lines.append("| - | - | - | - | - | NO_CANDIDATE | NOT_REGISTERED |")

    failed_records = tuple(
        record
        for record in records
        if str(_record_field(record, "status", "")) in {"FAILED", "CRASHED"}
    )
    if failed_records:
        lines.extend(["", "## Failures", ""])
        for record in failed_records:
            lines.append(
                f"- `{_record_field(record, 'candidate_sha256', '-')}` — "
                f"{_record_field(record, 'error_class', 'UNKNOWN')}: "
                f"{_record_field(record, 'error_message', 'no durable error message')}"
            )

    if top is not None:
        record, evaluation = top
        raw = _durable_metrics(evaluation, "raw_event_metrics")
        flat = _durable_metrics(evaluation, "flat_only_metrics")
        sequential = _durable_metrics(evaluation, "sequential_metrics")
        stressed = _durable_metrics(evaluation, "stressed_cost_metrics")
        lines.extend(
            [
                "",
                "## Top exploratory search-data result",
                "",
                f"- Candidate hash: `{_record_field(record, 'candidate_sha256', '-')}`",
                f"- Candidate status: `{evaluation.get('status', _record_field(record, 'status'))}`",
                f"- candidate_registered_at: {_record_field(record, 'registered_at', None) or 'NOT_RECORDED'}",
                (
                    f"- Raw event count: {raw.get('trade_count', 0)} eligible / "
                    f"{raw.get('signal_count', 0)} matching"
                ),
                f"- Flat-only trade count: {flat.get('trade_count', 0)}",
                f"- Sequential trade count: {sequential.get('trade_count', 0)}",
                (
                    f"- TP / SL / timeout: {sequential.get('tp_first_count', 0)} / "
                    f"{sequential.get('sl_first_count', 0)} / "
                    f"{sequential.get('timeout_count', 0)}"
                ),
                f"- TP-first probability (ppm): {sequential.get('tp_probability_ppm', None)}",
                (
                    f"- Gross / cost / net PnL ticks: "
                    f"{sequential.get('gross_pnl_ticks', 0)} / "
                    f"{sequential.get('cost_ticks', 0)} / "
                    f"{sequential.get('net_pnl_ticks', 0)}"
                ),
                f"- Sequential net EV ticks: {sequential.get('net_ev_ticks', None)}",
                f"- Maximum drawdown ticks: {sequential.get('maximum_drawdown_ticks', 0)}",
                f"- Active days: {sequential.get('active_days', 0)}",
                f"- Session distribution: {sequential.get('session_distribution', [])}",
                (
                    f"- Stressed-cost net PnL / EV ticks: "
                    f"{stressed.get('net_pnl_ticks', 0)} / "
                    f"{stressed.get('net_ev_ticks', None)}"
                ),
                "",
                "### Search-data walk-forward folds",
                "",
                *_durable_fold_table(evaluation),
                "",
                "### Null/control comparison",
                "",
                *_durable_control_table(evaluation),
            ]
        )

    lines.extend(
        [
            "",
            "## Execution assumptions",
            "",
            *(f"- {assumption}" for assumption in context.execution_assumptions),
            "",
            (
                "The passive trade-through rule is conservative but may still favor wider "
                "take-profit distances; no MBO queue position is claimed."
            ),
            "",
            "## Exclusions and label diagnostics",
            "",
            f"- Roll/roll-guard exclusions: {context.roll_exclusion_count}",
            f"- Session-boundary exclusions: {context.session_exclusion_count}",
            f"- Ambiguous labels: {context.ambiguous_label_count}",
            "- A trade never changes instrument_id after entry.",
            "- Session eligibility is decided before outcomes under NO_CROSS_CLOSED_MARKET.",
            "",
            "## Sealed holdout and promotion boundary",
            "",
            f"- Sealed holdout status: `{context.sealed_holdout_status}`",
            "- Sealed holdout untouched: true",
            "- Candidate classification: exploratory candidate / search-data survivor only",
            "- Awaiting sealed holdout: true",
            "- Awaiting forward evidence: true",
            "- Not paper eligible: true",
            "- Not live eligible: true",
        ]
    )
    return "\n".join(lines) + "\n"
