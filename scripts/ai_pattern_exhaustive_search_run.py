"""Governed, resumable Search evaluation for all 518 eligible AI patterns.

Only the Discovery/Search partitions are addressable here.  The immutable
Batch 3 summaries for the original 12 members are imported without opening
their outcomes again.  The remaining 506 members are evaluated in 43 fixed
chunks, with one shared five-minute view and one streamed one-second pass per
chunk.  No chunk performs multiplicity correction; one exact 518-member BH
correction is produced only after every chunk is durably complete.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import stat
import tempfile
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from fractions import Fraction
from pathlib import Path
from typing import Final

from scripts.ai_pattern_exhaustive_search_config import (
    AI_PATTERN_EXHAUSTIVE_AUTHORITY,
    BATCH_COUNT,
    BATCH_SIZE,
    EXPECTED_INITIAL_SEARCH_MASKS_SHA256,
    EXPECTED_INITIAL_SEARCH_RESULT_SHA256,
    FINAL_BATCH_SIZE,
    INITIAL_FAMILY_COUNT,
    REMAINING_FAMILY_COUNT,
    SUPPORT_ELIGIBLE_FAMILY_COUNT,
    AIPatternExhaustiveConfig,
    load_ai_pattern_exhaustive_config,
)
from scripts.ai_pattern_holdout_config import load_ai_pattern_holdout_config
from scripts.ai_pattern_holdout_engine import (
    EvaluationSummary,
    ExecutionSpec,
    FrozenProposal,
    GroupSummary,
    StageCandidateSummary,
    StageEvaluationResult,
)
from scripts.ai_pattern_holdout_run import (
    HoldoutStagePlan,
    _build_evaluation_inputs,
    _load_stage_bars,
    _one_second_outcome_parts,
    _stage_groups,
    _stage_partitions,
)
from systematic_fx.research.ai_discovery_context import (
    EXPECTED_AI_DISCOVERY_CONTEXT_SHA256,
    load_ai_discovery_context,
    reopen_ai_discovery_context,
)
from systematic_fx.research.ai_pattern_config_v3 import (
    load_ai_pattern_discovery_config_v3,
)
from systematic_fx.research.ai_pattern_discovery import (
    AndRule,
    ArtifactIdentity,
    ImmutableArtifactError,
    ProposedPattern,
    _assess_pattern,
    context_from_ai_discovery_document,
    publish_canonical_artifact,
    verify_immutable_artifact,
)
from systematic_fx.research.ai_pattern_discovery_v2 import (
    V2_CANDIDATE_CATALOG_SHA256,
    deterministic_candidate_catalog_v2,
)
from systematic_fx.research.hypotheses import canonical_json_bytes, canonical_sha256

EXHAUSTIVE_RUN_SCHEMA: Final = "systematic_fx.ai_pattern_exhaustive_search_run.v1"
EXHAUSTIVE_EVENT_SCHEMA: Final = "systematic_fx.ai_pattern_exhaustive_search_event.v1"
EXHAUSTIVE_PLAN_SCHEMA: Final = "systematic_fx.ai_pattern_exhaustive_search_plan.v1"
EXHAUSTIVE_CANDIDATE_SCHEMA: Final = "systematic_fx.ai_pattern_exhaustive_search_candidate.v1"
DEFAULT_EXHAUSTIVE_ROOT: Final = Path("data/derived/bar_patterns/ai_pattern_exhaustive_search_v1")
INITIAL_SEARCH_ROOT: Final = Path("data/derived/bar_patterns/ai_pattern_holdout_v1/artifacts")
EXPECTED_REMAINING_ASSESSMENT_CATALOG_SHA256: Final = (
    "088c35d2b6781b74e058aa1eef4be8a87a7818e3a5b42d1bd000fb3883d36c3b"
)
EXPECTED_REMAINING_PATTERN_SHA_LIST_SHA256: Final = (
    "f34c5b2e6189136e758cc6f441622d6b2e417046f580b42a75bf367432aa77d3"
)
EXPECTED_BATCH_MANIFEST_SHA256: Final = (
    "022af03de649f829b5ae44f58c840bea05440bda36d7eeca9d5fc6d33fb0f322"
)
EXPECTED_ELIGIBLE_FAMILY_PATTERN_SHA256: Final = (
    "e269800244d62c346497dbbcdfdda540eb361f7273027f387fbc2efe27db4d59"
)
EXPECTED_SELECTED_PATTERN_ORDER_SHA256: Final = (
    "ad128cef2cb2ee5797cc85d987d3cf2145566ac397059fdef2e46f02551a95d0"
)
EXPECTED_SELECTED_PROPOSAL_ORDER_SHA256: Final = (
    "b33301df855fb4528044446cdf3e1f42b1f4007872bbc02fb68ed59387852956"
)
_WRITE_BITS: Final = stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH


class AIPatternExhaustiveSearchError(RuntimeError):
    """The exhaustive Search lifecycle or immutable evidence failed closed."""


class _ExhaustiveRunBusyError(OSError):
    """Another process owns the fixed exhaustive-Search mutation lock."""


@dataclass(frozen=True, slots=True)
class ExhaustiveCandidate:
    family_position: int
    evaluation_id: str
    pattern_sha256: str
    eligible_rank: int
    pattern: ProposedPattern
    support_rows: int
    session_support_count: int
    stability_ppm: int
    origin: str

    def __post_init__(self) -> None:
        if (
            isinstance(self.family_position, bool)
            or not isinstance(self.family_position, int)
            or not 1 <= self.family_position <= SUPPORT_ELIGIBLE_FAMILY_COUNT
            or isinstance(self.eligible_rank, bool)
            or not isinstance(self.eligible_rank, int)
            or not 1 <= self.eligible_rank <= SUPPORT_ELIGIBLE_FAMILY_COUNT
            or len(self.evaluation_id) != 64
            or any(character not in "0123456789abcdef" for character in self.evaluation_id)
            or self.pattern_sha256 != self.pattern.sha256
            or self.support_rows < 500
            or self.session_support_count < 80
            or not 0 <= self.stability_ppm <= 1_000_000
            or self.origin
            not in {
                "IMPORTED_BATCH3_EXACT_SUMMARY",
                "EXHAUSTIVE_DOMAIN_SEPARATED_V1",
            }
        ):
            raise AIPatternExhaustiveSearchError("exhaustive candidate identity differs")

    @property
    def selection_rank(self) -> int:
        return self.family_position

    @property
    def proposal_sha256(self) -> str:
        return self.evaluation_id

    @property
    def hypothesis_id(self) -> str:
        """Uniform scientific family key, independent of execution/null identity."""

        return self.pattern_sha256

    @property
    def direction(self) -> str:
        return self.pattern.direction

    @property
    def rule(self) -> AndRule:
        return self.pattern.rule

    def as_dict(self) -> dict[str, object]:
        return {
            "direction": self.pattern.direction,
            "eligible_rank": self.eligible_rank,
            "evaluation_id": self.evaluation_id,
            "family": self.pattern.family,
            "family_position": self.family_position,
            "hypothesis_id": self.hypothesis_id,
            "origin": self.origin,
            "pattern_sha256": self.pattern_sha256,
            "rule": self.pattern.rule.as_dict(),
            "session_support_count": self.session_support_count,
            "stability_ppm": self.stability_ppm,
            "support_rows": self.support_rows,
        }


@dataclass(frozen=True, slots=True)
class ExhaustiveBatch:
    batch_number: int
    batch_key: str
    batch_sha256: str
    members: tuple[ExhaustiveCandidate, ...]

    def __post_init__(self) -> None:
        expected_size = FINAL_BATCH_SIZE if self.batch_number == BATCH_COUNT else BATCH_SIZE
        if (
            self.batch_key != f"E{self.batch_number:03d}"
            or len(self.members) != expected_size
            or len({item.evaluation_id for item in self.members}) != len(self.members)
            or len(self.batch_sha256) != 64
            or any(character not in "0123456789abcdef" for character in self.batch_sha256)
        ):
            raise AIPatternExhaustiveSearchError("exhaustive batch shape differs")

    @property
    def member_ids(self) -> tuple[str, ...]:
        return tuple(item.evaluation_id for item in self.members)

    def as_dict(self) -> dict[str, object]:
        return {
            "batch_key": self.batch_key,
            "batch_number": self.batch_number,
            "batch_sha256": self.batch_sha256,
            "family_positions": [item.family_position for item in self.members],
            "member_ids": list(self.member_ids),
            "pattern_sha256s": [item.pattern_sha256 for item in self.members],
        }


@dataclass(frozen=True, slots=True)
class ExhaustiveSearchPlan:
    initial_members: tuple[ExhaustiveCandidate, ...]
    remaining_members: tuple[ExhaustiveCandidate, ...]
    batches: tuple[ExhaustiveBatch, ...]
    family_sha256: str
    remaining_assessment_catalog_sha256: str
    remaining_pattern_sha_list_sha256: str
    batch_manifest_sha256: str

    def __post_init__(self) -> None:
        members = self.members
        if (
            len(self.initial_members) != INITIAL_FAMILY_COUNT
            or len(self.remaining_members) != REMAINING_FAMILY_COUNT
            or len(members) != SUPPORT_ELIGIBLE_FAMILY_COUNT
            or tuple(item.family_position for item in members)
            != tuple(range(1, SUPPORT_ELIGIBLE_FAMILY_COUNT + 1))
            or len({item.evaluation_id for item in members}) != len(members)
            or len({item.hypothesis_id for item in members}) != len(members)
            or self.family_sha256 != canonical_sha256(list(self.family_ids))
            or len(self.batches) != BATCH_COUNT
            or tuple(item.batch_number for item in self.batches) != tuple(range(1, BATCH_COUNT + 1))
            or tuple(member for batch in self.batches for member in batch.members)
            != self.remaining_members
        ):
            raise AIPatternExhaustiveSearchError("exhaustive family plan is incomplete")

    @property
    def members(self) -> tuple[ExhaustiveCandidate, ...]:
        return tuple(
            sorted(
                (*self.initial_members, *self.remaining_members),
                key=lambda item: item.family_position,
            )
        )

    @property
    def family_ids(self) -> tuple[str, ...]:
        return tuple(item.hypothesis_id for item in self.members)

    @property
    def evaluation_ids(self) -> tuple[str, ...]:
        return tuple(item.evaluation_id for item in self.members)

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_schema": EXHAUSTIVE_PLAN_SCHEMA,
            "authority": AI_PATTERN_EXHAUSTIVE_AUTHORITY,
            "batch3_context_sha256": (
                "842539b17aa6a17b29ea125cc324f98d324ae1f5931cee47fa238dc2f6310637"
            ),
            "batch3_governed_request_sha256": (
                "17df16a432cd544c1ffde7fd43add6e20272c90d1e8358487ddf5f804b59303c"
            ),
            "batch_manifest_sha256": self.batch_manifest_sha256,
            "batches": [item.as_dict() for item in self.batches],
            "candidate_catalog_sha256": V2_CANDIDATE_CATALOG_SHA256,
            "family_count": len(self.members),
            "family_sha256": self.family_sha256,
            "hypothesis_id_policy": "UNIFORM_CANONICAL_PATTERN_SHA256",
            "initial_members": [
                {**item.as_dict(), "initial_selection_rank": rank}
                for rank, item in enumerate(self.initial_members, start=1)
            ],
            "multiplicity": {
                "maximum_finalists": 4,
                "method": "BENJAMINI_HOCHBERG",
                "q": {"denominator": 20, "numerator": 1},
                "timing": "AFTER_ALL_518_RAW_SUMMARIES_PRESENT",
            },
            "remaining_assessment_catalog_sha256": (self.remaining_assessment_catalog_sha256),
            "remaining_members": [item.as_dict() for item in self.remaining_members],
            "remaining_pattern_sha_list_sha256": self.remaining_pattern_sha_list_sha256,
            "scope": {
                "design_timing": (
                    "RETROSPECTIVE_EXPANSION_AFTER_INITIAL_12_SEARCH_RESULTS_OBSERVED"
                ),
                "embargo_opened": False,
                "fresh_preregistered_or_oos_claim": False,
                "holdout_opened": False,
                "search_only": True,
                "walk_forward_opened": False,
            },
        }

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.as_dict())


def _candidate_identity_document(
    *,
    pattern: ProposedPattern,
    pattern_sha256: str,
    eligible_rank: int,
    support_rows: int,
    session_support_count: int,
    stability_ppm: int,
) -> dict[str, object]:
    return {
        "artifact_schema": EXHAUSTIVE_CANDIDATE_SCHEMA,
        "batch3_context_sha256": (
            "842539b17aa6a17b29ea125cc324f98d324ae1f5931cee47fa238dc2f6310637"
        ),
        "batch3_governed_request_sha256": (
            "17df16a432cd544c1ffde7fd43add6e20272c90d1e8358487ddf5f804b59303c"
        ),
        "candidate_catalog_sha256": V2_CANDIDATE_CATALOG_SHA256,
        "direction": pattern.direction,
        "eligible_rank": eligible_rank,
        "family": pattern.family,
        "pattern_sha256": pattern_sha256,
        "rule": pattern.rule.as_dict(),
        "session_support_count": session_support_count,
        "stability_ppm": stability_ppm,
        "support_rows": support_rows,
    }


def _load_initial_pattern_mapping(project_root: Path) -> tuple[dict[str, object], ...]:
    path = project_root / (
        "data/derived/bar_patterns/ai_pattern_discovery_v3/artifacts/"
        "directional-proposal-batch-"
        "dfef5bad188f79af8fa63a6e74f8c9609df34778a9a050278f3740766d24ee4e.json"
    )
    document = _read_canonical_input(
        path,
        "dfef5bad188f79af8fa63a6e74f8c9609df34778a9a050278f3740766d24ee4e",
    )
    try:
        values = document["base_batch"]["proposals"]  # type: ignore[index]
    except (KeyError, TypeError) as error:
        raise AIPatternExhaustiveSearchError("Batch 3 proposal mapping is malformed") from error
    if not isinstance(values, list) or len(values) != INITIAL_FAMILY_COUNT:
        raise AIPatternExhaustiveSearchError("Batch 3 proposal mapping count differs")
    output: list[dict[str, object]] = []
    for expected_rank, wrapper in enumerate(values, start=1):
        if not isinstance(wrapper, dict) or set(wrapper) != {"proposal", "proposal_sha256"}:
            raise AIPatternExhaustiveSearchError("Batch 3 proposal wrapper differs")
        proposal = wrapper["proposal"]
        if not isinstance(proposal, dict) or proposal.get("selection_rank") != expected_rank:
            raise AIPatternExhaustiveSearchError("Batch 3 proposal rank differs")
        output.append(wrapper)
    return tuple(output)


def build_exhaustive_search_plan(project_root: Path | str) -> ExhaustiveSearchPlan:
    """Reconstruct the exact outcome-blind 518-family and 43-chunk plan."""

    root = _project_root(project_root)
    proposal_config = load_ai_pattern_discovery_config_v3(root)
    source_artifact = load_ai_discovery_context(root)
    source_document = reopen_ai_discovery_context(root, source_artifact)
    context = context_from_ai_discovery_document(
        proposal_config.request,
        source_document,
        expected_context_sha256=EXPECTED_AI_DISCOVERY_CONTEXT_SHA256,
    )
    if context.sha256 != ("842539b17aa6a17b29ea125cc324f98d324ae1f5931cee47fa238dc2f6310637"):
        raise AIPatternExhaustiveSearchError("Batch 3 compact context identity differs")
    catalog = deterministic_candidate_catalog_v2()
    assessed = tuple(_assess_pattern(context, pattern) for pattern in catalog)
    eligible = tuple(
        item
        for item in assessed
        if item.support_rows >= 500 and item.session_support_count >= 80 and item.stability_ppm >= 0
    )
    if len(eligible) != SUPPORT_ELIGIBLE_FAMILY_COUNT:
        raise AIPatternExhaustiveSearchError("support-eligible family count differs")
    ranked = tuple(
        sorted(
            eligible,
            key=lambda item: (-item.stability_ppm, -item.support_rows, item.pattern.sha256),
        )
    )
    rank_by_pattern = {item.pattern.sha256: rank for rank, item in enumerate(ranked, start=1)}
    assessment_by_pattern = {item.pattern.sha256: item for item in eligible}
    family_pattern_order = tuple(sorted(assessment_by_pattern))
    family_position_by_pattern = {
        pattern_sha256: position
        for position, pattern_sha256 in enumerate(family_pattern_order, start=1)
    }
    if canonical_sha256(list(family_pattern_order)) != EXPECTED_ELIGIBLE_FAMILY_PATTERN_SHA256:
        raise AIPatternExhaustiveSearchError("full 518 pattern family identity differs")
    initial_wrappers = _load_initial_pattern_mapping(root)
    initial_pattern_ids: set[str] = set()
    initial_rows: list[tuple[str, str, ProposedPattern]] = []
    for wrapper in initial_wrappers:
        proposal = wrapper["proposal"]
        if not isinstance(proposal, dict):  # pragma: no cover - checked above
            raise AIPatternExhaustiveSearchError("Batch 3 proposal is not an object")
        pattern = ProposedPattern(
            family=str(proposal["family"]),
            direction=str(proposal["direction"]),  # type: ignore[arg-type]
            rationale_code=str(proposal["rationale_code"]),
            rule=AndRule.from_dict(proposal["rule"]),
        )
        pattern_id = pattern.sha256
        evaluation_id = str(wrapper["proposal_sha256"])
        if pattern_id not in assessment_by_pattern or pattern_id in initial_pattern_ids:
            raise AIPatternExhaustiveSearchError("initial member is not uniquely eligible")
        initial_pattern_ids.add(pattern_id)
        initial_rows.append((evaluation_id, pattern_id, pattern))
    initial: list[ExhaustiveCandidate] = []
    for evaluation_id, pattern_id, pattern in initial_rows:
        assessment = assessment_by_pattern[pattern_id]
        initial.append(
            ExhaustiveCandidate(
                family_position_by_pattern[pattern_id],
                evaluation_id,
                pattern_id,
                rank_by_pattern[pattern_id],
                pattern,
                assessment.support_rows,
                assessment.session_support_count,
                assessment.stability_ppm,
                "IMPORTED_BATCH3_EXACT_SUMMARY",
            )
        )
    remaining_assessments = tuple(
        sorted(
            (item for item in eligible if item.pattern.sha256 not in initial_pattern_ids),
            key=lambda item: item.pattern.sha256,
        )
    )
    assessment_catalog = [
        {
            "direction": item.pattern.direction,
            "family": item.pattern.family,
            "pattern_sha256": item.pattern.sha256,
            "rule": item.pattern.rule.as_dict(),
            "session_support_count": item.session_support_count,
            "stability_ppm": item.stability_ppm,
            "support_rows": item.support_rows,
        }
        for item in remaining_assessments
    ]
    assessment_sha = canonical_sha256(assessment_catalog)
    pattern_list_sha = canonical_sha256([item.pattern.sha256 for item in remaining_assessments])
    if (
        len(remaining_assessments) != REMAINING_FAMILY_COUNT
        or assessment_sha != EXPECTED_REMAINING_ASSESSMENT_CATALOG_SHA256
        or pattern_list_sha != EXPECTED_REMAINING_PATTERN_SHA_LIST_SHA256
    ):
        raise AIPatternExhaustiveSearchError("remaining 506 catalog identity differs")
    if (
        canonical_sha256([item[1] for item in initial_rows])
        != EXPECTED_SELECTED_PATTERN_ORDER_SHA256
        or canonical_sha256([item[0] for item in initial_rows])
        != EXPECTED_SELECTED_PROPOSAL_ORDER_SHA256
    ):
        raise AIPatternExhaustiveSearchError("initial 12 mapping fingerprints differ")
    remaining: list[ExhaustiveCandidate] = []
    for item in remaining_assessments:
        identity_document = _candidate_identity_document(
            pattern=item.pattern,
            pattern_sha256=item.pattern.sha256,
            eligible_rank=rank_by_pattern[item.pattern.sha256],
            support_rows=item.support_rows,
            session_support_count=item.session_support_count,
            stability_ppm=item.stability_ppm,
        )
        remaining.append(
            ExhaustiveCandidate(
                family_position_by_pattern[item.pattern.sha256],
                canonical_sha256(identity_document),
                item.pattern.sha256,
                rank_by_pattern[item.pattern.sha256],
                item.pattern,
                item.support_rows,
                item.session_support_count,
                item.stability_ppm,
                "EXHAUSTIVE_DOMAIN_SEPARATED_V1",
            )
        )
    family_ids = family_pattern_order
    family_sha = canonical_sha256(list(family_ids))
    batches: list[ExhaustiveBatch] = []
    for offset in range(0, len(remaining), BATCH_SIZE):
        number = len(batches) + 1
        members = tuple(remaining[offset : offset + BATCH_SIZE])
        batch_key = f"E{number:03d}"
        batch_document = {
            "artifact_schema": "systematic_fx.ai_pattern_exhaustive_batch.v1",
            "batch_key": batch_key,
            "batch_number": number,
            "family_sha256": family_sha,
            "family_positions": [item.family_position for item in members],
            "member_ids": [item.evaluation_id for item in members],
            "pattern_sha256s": [item.pattern_sha256 for item in members],
        }
        batches.append(
            ExhaustiveBatch(number, batch_key, canonical_sha256(batch_document), members)
        )
    # The independent auditor hash is over exact 506 assessment-document slices,
    # not the domain-separated execution identities.
    batch_manifest_sha = canonical_sha256(
        [
            {
                "batch_index": offset // BATCH_SIZE + 1,
                "candidate_sha256s": [
                    item.pattern.sha256
                    for item in remaining_assessments[offset : offset + BATCH_SIZE]
                ],
            }
            for offset in range(0, len(remaining_assessments), BATCH_SIZE)
        ]
    )
    if batch_manifest_sha != EXPECTED_BATCH_MANIFEST_SHA256:
        raise AIPatternExhaustiveSearchError("43-batch manifest identity differs")
    return ExhaustiveSearchPlan(
        tuple(initial),
        tuple(remaining),
        tuple(batches),
        family_sha,
        assessment_sha,
        pattern_list_sha,
        batch_manifest_sha,
    )


def _project_root(value: Path | str) -> Path:
    requested = Path(value).expanduser()
    if requested.is_symlink():
        raise AIPatternExhaustiveSearchError("project root cannot be symbolic")
    root = requested.resolve(strict=True)
    if not root.is_dir() or not (root / "pyproject.toml").is_file():
        raise AIPatternExhaustiveSearchError("project root is not a systematic-fx checkout")
    return root


def _read_canonical_input(path: Path, expected_sha256: str) -> dict[str, object]:
    if path.is_symlink() or not path.is_file():
        raise AIPatternExhaustiveSearchError("immutable input is missing or symbolic")
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        before = os.fstat(descriptor)
        chunks: list[bytes] = []
        while chunk := os.read(descriptor, 1024 * 1024):
            chunks.append(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    lexical = path.stat(follow_symlinks=False)
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_mode & _WRITE_BITS
        or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        or (after.st_dev, after.st_ino, after.st_size)
        != (lexical.st_dev, lexical.st_ino, lexical.st_size)
    ):
        raise AIPatternExhaustiveSearchError("immutable input changed while opened")
    payload = b"".join(chunks)
    if hashlib.sha256(payload).hexdigest() != expected_sha256:
        raise AIPatternExhaustiveSearchError("immutable input SHA-256 differs")
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise AIPatternExhaustiveSearchError("immutable input is invalid JSON") from error
    if not isinstance(document, dict) or canonical_json_bytes(document) != payload:
        raise AIPatternExhaustiveSearchError("immutable input is not canonical JSON")
    return document


def _exact_dict(value: object, keys: set[str], *, label: str) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != keys:
        raise AIPatternExhaustiveSearchError(f"{label} schema differs")
    return value


def _sha256(value: object, *, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise AIPatternExhaustiveSearchError(f"{label} is not a SHA-256")
    return value


def _integer(value: object, *, label: str, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise AIPatternExhaustiveSearchError(f"{label} is not an integer")
    if minimum is not None and value < minimum:
        raise AIPatternExhaustiveSearchError(f"{label} is below its minimum")
    return value


def _boolean(value: object, *, label: str) -> bool:
    if not isinstance(value, bool):
        raise AIPatternExhaustiveSearchError(f"{label} is not a boolean")
    return value


def _fraction(value: object, *, label: str, optional: bool = False) -> Fraction | None:
    if value is None and optional:
        return None
    document = _exact_dict(value, {"denominator", "numerator"}, label=label)
    numerator = _integer(document["numerator"], label=f"{label}.numerator")
    denominator = _integer(document["denominator"], label=f"{label}.denominator", minimum=1)
    result = Fraction(numerator, denominator)
    if result.numerator != numerator or result.denominator != denominator:
        raise AIPatternExhaustiveSearchError(f"{label} is not a reduced fraction")
    return result


def _dated_counts(value: object, *, count_key: str, label: str) -> tuple[tuple[date, int], ...]:
    if not isinstance(value, list):
        raise AIPatternExhaustiveSearchError(f"{label} is not a list")
    output: list[tuple[date, int]] = []
    for row in value:
        item = _exact_dict(row, {"source_date", count_key}, label=label)
        try:
            source_date = date.fromisoformat(str(item["source_date"]))
        except ValueError as error:
            raise AIPatternExhaustiveSearchError(f"{label} date differs") from error
        if source_date.isoformat() != item["source_date"]:
            raise AIPatternExhaustiveSearchError(f"{label} date is not canonical")
        output.append((source_date, _integer(item[count_key], label=label)))
    result = tuple(output)
    if result != tuple(sorted(result)) or len(dict(result)) != len(result):
        raise AIPatternExhaustiveSearchError(f"{label} is duplicated or unordered")
    return result


_GROUP_SUMMARY_KEYS: Final = {
    "active_entry_day_count",
    "active_exit_day_count",
    "expected_value_ticks",
    "fully_loaded_net_pnl_ticks",
    "gross_pnl_ticks",
    "group_key",
    "net_gains_ticks",
    "net_losses_ticks",
    "profit_factor",
    "profit_factor_unbounded",
    "raw_signal_count",
    "signal_day_count",
    "trade_count",
}
_EVALUATION_SUMMARY_KEYS: Final = {
    "active_entry_day_count",
    "active_exit_day_count",
    "allocated_fixed_cost_ticks",
    "contract_count",
    "daily_net_ticks",
    "daily_signal_counts",
    "daily_trade_counts",
    "expected_value_ticks",
    "fully_loaded_net_pnl_ticks",
    "gross_pnl_ticks",
    "group_summaries",
    "maximum_drawdown_ticks",
    "median_signals_per_signal_day",
    "net_gains_ticks",
    "net_losses_ticks",
    "profit_factor",
    "profit_factor_unbounded",
    "raw_signal_count",
    "same_second_stop_first_count",
    "signal_day_count",
    "skipped_occupied_count",
    "stop_first_count",
    "take_profit_first_count",
    "timeout_count",
    "trade_count",
    "variable_cost_ticks",
}


def _decode_group_summary(value: object) -> GroupSummary:
    document = _exact_dict(value, _GROUP_SUMMARY_KEYS, label="group summary")
    result = GroupSummary(
        group_key=str(document["group_key"]),
        raw_signal_count=_integer(document["raw_signal_count"], label="raw_signal_count"),
        signal_day_count=_integer(document["signal_day_count"], label="signal_day_count"),
        trade_count=_integer(document["trade_count"], label="trade_count"),
        active_entry_day_count=_integer(
            document["active_entry_day_count"], label="active_entry_day_count"
        ),
        active_exit_day_count=_integer(
            document["active_exit_day_count"], label="active_exit_day_count"
        ),
        gross_pnl_ticks=_integer(document["gross_pnl_ticks"], label="gross_pnl_ticks"),
        fully_loaded_net_pnl_ticks=_integer(
            document["fully_loaded_net_pnl_ticks"], label="fully_loaded_net_pnl_ticks"
        ),
        net_gains_ticks=_integer(document["net_gains_ticks"], label="net_gains_ticks"),
        net_losses_ticks=_integer(document["net_losses_ticks"], label="net_losses_ticks"),
        expected_value_ticks=_fraction(
            document["expected_value_ticks"], label="expected_value_ticks", optional=True
        ),
        profit_factor=_fraction(document["profit_factor"], label="profit_factor", optional=True),
        profit_factor_unbounded=_boolean(
            document["profit_factor_unbounded"], label="profit_factor_unbounded"
        ),
    )
    if result.as_dict() != document:
        raise AIPatternExhaustiveSearchError("group summary does not round-trip exactly")
    return result


def _decode_evaluation_summary(value: object) -> EvaluationSummary:
    document = _exact_dict(value, _EVALUATION_SUMMARY_KEYS, label="evaluation summary")
    groups = document["group_summaries"]
    if not isinstance(groups, list):
        raise AIPatternExhaustiveSearchError("group summaries are not a list")
    integer_fields = (
        "raw_signal_count",
        "signal_day_count",
        "trade_count",
        "skipped_occupied_count",
        "active_entry_day_count",
        "active_exit_day_count",
        "contract_count",
        "take_profit_first_count",
        "stop_first_count",
        "timeout_count",
        "same_second_stop_first_count",
        "gross_pnl_ticks",
        "variable_cost_ticks",
        "allocated_fixed_cost_ticks",
        "fully_loaded_net_pnl_ticks",
        "maximum_drawdown_ticks",
        "net_gains_ticks",
        "net_losses_ticks",
    )
    parsed = {key: _integer(document[key], label=key) for key in integer_fields}
    result = EvaluationSummary(
        **parsed,
        median_signals_per_signal_day=_fraction(
            document["median_signals_per_signal_day"],
            label="median_signals_per_signal_day",
            optional=True,
        ),
        expected_value_ticks=_fraction(
            document["expected_value_ticks"], label="expected_value_ticks", optional=True
        ),
        profit_factor=_fraction(document["profit_factor"], label="profit_factor", optional=True),
        profit_factor_unbounded=_boolean(
            document["profit_factor_unbounded"], label="profit_factor_unbounded"
        ),
        daily_net_ticks=_dated_counts(
            document["daily_net_ticks"], count_key="ticks", label="daily_net_ticks"
        ),
        daily_trade_counts=_dated_counts(
            document["daily_trade_counts"],
            count_key="trade_count",
            label="daily_trade_counts",
        ),
        daily_signal_counts=_dated_counts(
            document["daily_signal_counts"],
            count_key="signal_count",
            label="daily_signal_counts",
        ),
        group_summaries=tuple(_decode_group_summary(item) for item in groups),
    )
    if result.as_dict() != document:
        raise AIPatternExhaustiveSearchError("evaluation summary does not round-trip exactly")
    return result


_STAGE_CANDIDATE_KEYS: Final = {
    "circular_shift",
    "conservative_p_value",
    "matched_random",
    "p_vs_circular_shift",
    "p_vs_matched_random",
    "p_vs_zero",
    "proposal_sha256",
    "real",
    "rule_support_count",
    "rule_support_daily_counts",
    "rule_support_day_count",
    "rule_support_group_counts",
}


def _decode_stage_candidate(value: object) -> StageCandidateSummary:
    document = _exact_dict(value, _STAGE_CANDIDATE_KEYS, label="stage candidate")
    group_counts = document["rule_support_group_counts"]
    if not isinstance(group_counts, list):
        raise AIPatternExhaustiveSearchError("support group counts are not a list")
    parsed_groups: list[tuple[str, int]] = []
    for raw in group_counts:
        row = _exact_dict(raw, {"group_key", "support_count"}, label="support group")
        parsed_groups.append(
            (str(row["group_key"]), _integer(row["support_count"], label="support_count"))
        )
    result = StageCandidateSummary(
        proposal_sha256=_sha256(document["proposal_sha256"], label="proposal_sha256"),
        real=_decode_evaluation_summary(document["real"]),
        circular_shift=_decode_evaluation_summary(document["circular_shift"]),
        matched_random=_decode_evaluation_summary(document["matched_random"]),
        p_vs_zero=_fraction(document["p_vs_zero"], label="p_vs_zero"),  # type: ignore[arg-type]
        p_vs_circular_shift=_fraction(document["p_vs_circular_shift"], label="p_vs_circular_shift"),  # type: ignore[arg-type]
        p_vs_matched_random=_fraction(document["p_vs_matched_random"], label="p_vs_matched_random"),  # type: ignore[arg-type]
        conservative_p_value=_fraction(
            document["conservative_p_value"], label="conservative_p_value"
        ),  # type: ignore[arg-type]
        rule_support_count=_integer(document["rule_support_count"], label="rule_support_count"),
        rule_support_day_count=_integer(
            document["rule_support_day_count"], label="rule_support_day_count"
        ),
        rule_support_daily_counts=_dated_counts(
            document["rule_support_daily_counts"],
            count_key="support_count",
            label="rule_support_daily_counts",
        ),
        rule_support_group_counts=tuple(parsed_groups),
    )
    if result.as_dict() != document:
        raise AIPatternExhaustiveSearchError("stage candidate does not round-trip exactly")
    from scripts.ai_pattern_holdout_engine import (
        exact_one_sided_sign_test,
        paired_daily_sign_test,
    )

    recomputed = (
        exact_one_sided_sign_test(value for _day, value in result.real.daily_net_ticks),
        paired_daily_sign_test(result.real.daily_net_ticks, result.circular_shift.daily_net_ticks),
        paired_daily_sign_test(result.real.daily_net_ticks, result.matched_random.daily_net_ticks),
    )
    if recomputed != (
        result.p_vs_zero,
        result.p_vs_circular_shift,
        result.p_vs_matched_random,
    ) or result.conservative_p_value != max(recomputed):
        raise AIPatternExhaustiveSearchError("stage candidate p-values do not replay")
    return result


def _initial_evidence(
    project_root: Path,
    plan: ExhaustiveSearchPlan,
) -> tuple[tuple[StageCandidateSummary, ...], dict[str, bool]]:
    masks_document = _read_canonical_input(
        project_root
        / INITIAL_SEARCH_ROOT
        / f"search-masks-{EXPECTED_INITIAL_SEARCH_MASKS_SHA256}.json",
        EXPECTED_INITIAL_SEARCH_MASKS_SHA256,
    )
    result_document = _read_canonical_input(
        project_root
        / INITIAL_SEARCH_ROOT
        / f"search-result-{EXPECTED_INITIAL_SEARCH_RESULT_SHA256}.json",
        EXPECTED_INITIAL_SEARCH_RESULT_SHA256,
    )
    for document, kind in ((masks_document, "MASKS"), (result_document, "RESULT")):
        if (
            document.get("artifact_schema") != "systematic_fx.ai_pattern_holdout_stage_artifact.v1"
            or document.get("authority") != AI_PATTERN_EXHAUSTIVE_AUTHORITY
            or document.get("config_semantic_sha256")
            != "035497ab4879409ff2fa118138e3f304a07a9e96fb45f1778da7521e4ecd71ef"
            or document.get("kind") != kind
            or document.get("stage") != "SEARCH"
        ):
            raise AIPatternExhaustiveSearchError("initial Search artifact lineage differs")
    try:
        mask_rows = masks_document["payload"]["payload"]["proposal_masks"]  # type: ignore[index]
        raw_result = result_document["payload"]["payload"]  # type: ignore[index]
    except (KeyError, TypeError) as error:
        raise AIPatternExhaustiveSearchError("initial Search payload differs") from error
    if not isinstance(mask_rows, list) or not isinstance(raw_result, dict):
        raise AIPatternExhaustiveSearchError("initial Search payload type differs")
    eligibility: dict[str, bool] = {}
    for row in mask_rows:
        if not isinstance(row, dict):
            raise AIPatternExhaustiveSearchError("initial mask row differs")
        identity = _sha256(row.get("proposal_sha256"), label="initial proposal id")
        eligible = row.get("sample_eligible")
        if not isinstance(eligible, bool) or identity in eligibility:
            raise AIPatternExhaustiveSearchError("initial mask eligibility differs")
        eligibility[identity] = eligible
    candidate_rows = raw_result.get("candidates")
    if not isinstance(candidate_rows, list):
        raise AIPatternExhaustiveSearchError("initial candidate summaries differ")
    candidates = tuple(_decode_stage_candidate(item) for item in candidate_rows)
    expected_ids = tuple(item.evaluation_id for item in plan.initial_members)
    by_id = {item.proposal_sha256: item for item in candidates}
    if set(by_id) != set(expected_ids) or set(eligibility) != set(expected_ids):
        raise AIPatternExhaustiveSearchError("initial Search family mapping differs")
    return tuple(by_id[item] for item in expected_ids), eligibility


def _mask_commitment(bundle: object) -> dict[str, object]:
    """Commit masks without retaining hundreds of megabytes of selected indexes."""

    from scripts.ai_pattern_holdout_engine import StageMaskBundle

    if not isinstance(bundle, StageMaskBundle):
        raise AIPatternExhaustiveSearchError("engine returned a non-mask bundle")

    def signal(mask: object | None) -> dict[str, object] | None:
        if mask is None:
            return None
        values = mask.values
        indexes = [index for index, selected in enumerate(values) if selected]
        return {
            "key": mask.key,
            "kind": mask.kind,
            "mask_values_sha256": canonical_sha256(list(values)),
            "null_seed_count": len(mask.null_seed_sha256s),
            "null_seed_sha256": canonical_sha256(list(mask.null_seed_sha256s)),
            "proposal_sha256": mask.proposal_sha256,
            "selected_index_sha256": canonical_sha256(indexes),
            "signal_count": len(indexes),
            "value_count": len(values),
        }

    rows = []
    for item in bundle.proposal_masks:
        rows.append(
            {
                "circular_shift": signal(item.circular_shift),
                "ineligibility_reason": item.ineligibility_reason,
                "matched_pairs_count": len(item.matched_pairs),
                "matched_pairs_sha256": canonical_sha256(
                    [pair.as_dict() for pair in item.matched_pairs]
                ),
                "matched_random": signal(item.matched_random),
                "proposal_sha256": item.proposal.proposal_sha256,
                "real": signal(item.real),
                "rule_support_count": item.rule_support_count,
                "rule_support_daily_counts": [
                    {"source_date": key.isoformat(), "support_count": count}
                    for key, count in item.rule_support_daily_counts
                ],
                "rule_support_day_count": item.rule_support_day_count,
                "rule_support_group_counts": [
                    {"group_key": key, "support_count": count}
                    for key, count in item.rule_support_group_counts
                ],
                "sample_eligible": item.sample_eligible,
                "selection_rank": item.proposal.selection_rank,
            }
        )
    return {
        "artifact_schema": "systematic_fx.ai_pattern_exhaustive_mask_commitment.v1",
        "eligible_position_count": sum(bundle.eligible_positions),
        "eligible_values_sha256": canonical_sha256(list(bundle.eligible_positions)),
        "five_minute_view_sha256": bundle.five_minute_view_sha256,
        "proposal_masks": rows,
        "stage_key": bundle.stage_key,
    }


@dataclass(frozen=True, slots=True)
class ExhaustiveLedgerEvent:
    sequence: int
    predecessor_sha256: str | None
    event_type: str
    request_sha256: str
    recorded_at_utc: str
    payload: Mapping[str, object]

    def as_dict(self) -> dict[str, object]:
        return {
            "artifact_schema": EXHAUSTIVE_EVENT_SCHEMA,
            "event_type": self.event_type,
            "payload": dict(self.payload),
            "predecessor_sha256": self.predecessor_sha256,
            "recorded_at_utc": self.recorded_at_utc,
            "request_sha256": self.request_sha256,
            "sequence": self.sequence,
        }

    @property
    def sha256(self) -> str:
        return canonical_sha256(self.as_dict())


_EVENT_TYPES: Final = {
    "PRECOMMITTED",
    "BATCH_MASKS_FROZEN",
    "ALL_MASKS_FROZEN",
    "BATCH_COMPLETED",
    "SEARCH_FAMILY_COMPLETED",
    "COMPLETED",
    "FAILED",
}


class ExhaustiveLedger:
    """Append-only mode-0444 predecessor-hashed lifecycle ledger."""

    def __init__(self, root: Path, *, create: bool) -> None:
        self.root = _safe_directory(root, create=create)
        self.events_root = _safe_directory(self.root / "events", create=create)
        self.staging_root = _safe_directory(self.root / "staging", create=create)

    def verify(self) -> tuple[ExhaustiveLedgerEvent, ...]:
        paths: dict[int, Path] = {}
        for path in self.events_root.iterdir():
            suffix = path.name.removeprefix("event-").removesuffix(".json")
            if (
                path.is_symlink()
                or not path.is_file()
                or path.name != f"event-{suffix}.json"
                or len(suffix) != 8
                or not suffix.isdigit()
                or path.stat().st_mode & _WRITE_BITS
            ):
                raise AIPatternExhaustiveSearchError("ledger contains an unsafe event")
            sequence = int(suffix)
            if sequence in paths:
                raise AIPatternExhaustiveSearchError("ledger sequence is duplicated")
            paths[sequence] = path
        output: list[ExhaustiveLedgerEvent] = []
        predecessor: str | None = None
        request_sha256: str | None = None
        for expected, sequence in enumerate(sorted(paths), start=1):
            if sequence != expected:
                raise AIPatternExhaustiveSearchError("ledger sequence is not contiguous")
            raw = paths[sequence].read_bytes()
            try:
                document = json.loads(raw)
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise AIPatternExhaustiveSearchError("ledger event is invalid JSON") from error
            document = _exact_dict(
                document,
                {
                    "artifact_schema",
                    "event_type",
                    "payload",
                    "predecessor_sha256",
                    "recorded_at_utc",
                    "request_sha256",
                    "sequence",
                },
                label="ledger event",
            )
            if (
                canonical_json_bytes(document) != raw
                or document["artifact_schema"] != EXHAUSTIVE_EVENT_SCHEMA
                or document["event_type"] not in _EVENT_TYPES
                or document["sequence"] != sequence
                or document["predecessor_sha256"] != predecessor
                or not isinstance(document["payload"], dict)
                or not isinstance(document["recorded_at_utc"], str)
                or not document["recorded_at_utc"].endswith("Z")
            ):
                raise AIPatternExhaustiveSearchError("ledger event content differs")
            event = ExhaustiveLedgerEvent(
                sequence,
                document["predecessor_sha256"],  # type: ignore[arg-type]
                str(document["event_type"]),
                _sha256(document["request_sha256"], label="request_sha256"),
                str(document["recorded_at_utc"]),
                document["payload"],  # type: ignore[arg-type]
            )
            if request_sha256 is None:
                request_sha256 = event.request_sha256
            if event.request_sha256 != request_sha256:
                raise AIPatternExhaustiveSearchError("ledger contains multiple requests")
            output.append(event)
            predecessor = event.sha256
        return tuple(output)

    def append(
        self,
        event_type: str,
        request_sha256: str,
        payload: Mapping[str, object],
    ) -> ExhaustiveLedgerEvent:
        if event_type not in _EVENT_TYPES:
            raise AIPatternExhaustiveSearchError("ledger event type differs")
        events = self.verify()
        event = ExhaustiveLedgerEvent(
            len(events) + 1,
            events[-1].sha256 if events else None,
            event_type,
            request_sha256,
            datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z"),
            payload,
        )
        destination = self.events_root / f"event-{event.sequence:08d}.json"
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".event-{event.sequence:08d}-",
            suffix=".tmp",
            dir=self.staging_root,
        )
        temporary = Path(temporary_name)
        try:
            _write_all(descriptor, canonical_json_bytes(event.as_dict()))
            os.fsync(descriptor)
            os.fchmod(descriptor, 0o444)
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = -1
            try:
                os.link(temporary, destination, follow_symlinks=False)
            except FileExistsError as error:
                raise _ExhaustiveRunBusyError(
                    "concurrent exhaustive ledger append; retry the fixed run"
                ) from error
            directory_descriptor = os.open(self.events_root, os.O_RDONLY)
            try:
                os.fsync(directory_descriptor)
            finally:
                os.close(directory_descriptor)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            temporary.unlink(missing_ok=True)
        if self.verify()[-1].sha256 != event.sha256:
            raise AIPatternExhaustiveSearchError("ledger append did not replay")
        return event


def _write_all(descriptor: int, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            raise AIPatternExhaustiveSearchError("immutable write made no progress")
        view = view[written:]


def _safe_directory(path: Path, *, create: bool) -> Path:
    if path.is_symlink():
        raise AIPatternExhaustiveSearchError("run directory cannot be symbolic")
    if create:
        path.mkdir(parents=True, exist_ok=True)
    try:
        resolved = path.resolve(strict=True)
    except FileNotFoundError as error:
        raise AIPatternExhaustiveSearchError("run directory does not exist") from error
    if not resolved.is_dir() or resolved != path.absolute():
        raise AIPatternExhaustiveSearchError("run directory is unsafe")
    return resolved


def _fixed_run_root(project_root: Path, *, create: bool) -> Path:
    current = project_root
    for part in DEFAULT_EXHAUSTIVE_ROOT.parts:
        current = current / part
        if current.is_symlink():
            raise AIPatternExhaustiveSearchError("run root has a symbolic component")
    return _safe_directory(project_root / DEFAULT_EXHAUSTIVE_ROOT, create=create)


@contextmanager
def _exclusive_run_lock(run_root: Path) -> Iterator[None]:
    """Keep every public mutation of the fixed run single-writer."""

    path = run_root / ".mutation.lock"
    flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    try:
        metadata = os.fstat(descriptor)
        visible = path.stat(follow_symlinks=False)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or (metadata.st_dev, metadata.st_ino) != (visible.st_dev, visible.st_ino)
        ):
            raise AIPatternExhaustiveSearchError("exhaustive mutation lock is unsafe")
        os.fchmod(descriptor, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise _ExhaustiveRunBusyError(
                "another exhaustive Search writer is active; retry later"
            ) from error
        try:
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


def _reconcile_artifact_publish_temps(artifacts_root: Path) -> None:
    """Remove only validated orphan staging leaves from the shared publisher."""

    removed = False
    for path in artifacts_root.iterdir():
        if not (path.name.startswith(".publish-") and path.name.endswith(".tmp")):
            continue
        metadata = path.lstat()
        if (
            path.is_symlink()
            or not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or metadata.st_nlink not in {1, 2}
        ):
            raise AIPatternExhaustiveSearchError("artifact publisher staging leaf is unsafe")
        if metadata.st_nlink == 2:
            companions = []
            for candidate in artifacts_root.iterdir():
                if candidate == path or candidate.is_symlink() or not candidate.is_file():
                    continue
                candidate_metadata = candidate.stat()
                if (candidate_metadata.st_dev, candidate_metadata.st_ino) == (
                    metadata.st_dev,
                    metadata.st_ino,
                ):
                    companions.append(candidate)
            if (
                len(companions) != 1
                or companions[0].suffix != ".json"
                or companions[0].stat().st_mode & _WRITE_BITS
            ):
                raise AIPatternExhaustiveSearchError(
                    "linked artifact publisher staging leaf is unsafe"
                )
        path.unlink()
        removed = True
    if removed:
        directory_descriptor = os.open(artifacts_root, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)


def _artifact_identity(value: object, *, artifact_type: str) -> ArtifactIdentity:
    try:
        identity = ArtifactIdentity.from_dict(value)
    except (TypeError, ValueError) as error:
        raise AIPatternExhaustiveSearchError("ledger artifact identity differs") from error
    if identity.artifact_type != artifact_type:
        raise AIPatternExhaustiveSearchError("ledger artifact role differs")
    return identity


def _publish(
    artifacts_root: Path,
    *,
    artifact_type: str,
    prefix: str,
    document: Mapping[str, object],
) -> ArtifactIdentity:
    try:
        return publish_canonical_artifact(
            artifacts_root,
            artifact_type=artifact_type,
            filename_prefix=prefix,
            document=document,
        )
    except ImmutableArtifactError as error:
        raise AIPatternExhaustiveSearchError(
            "immutable artifact publication failed closed"
        ) from error


def _request_document(
    config: AIPatternExhaustiveConfig,
    plan: ExhaustiveSearchPlan,
    plan_artifact: ArtifactIdentity,
) -> dict[str, object]:
    return {
        "artifact_schema": "systematic_fx.ai_pattern_exhaustive_search_request.v1",
        "authority": AI_PATTERN_EXHAUSTIVE_AUTHORITY,
        "config": config.as_dict(),
        "config_file_sha256": config.file_sha256,
        "config_semantic_sha256": config.semantic_sha256,
        "imported_search": {
            "masks_sha256": EXPECTED_INITIAL_SEARCH_MASKS_SHA256,
            "result_sha256": EXPECTED_INITIAL_SEARCH_RESULT_SHA256,
            "reuse_policy": "RAW_SUMMARIES_AND_MASK_ELIGIBILITY_ONLY_IGNORE_12_FAMILY_BH",
        },
        "limitations": [
            "UNSEALED_LOCAL_BAR_SCREENING_ONLY",
            "RETROSPECTIVE_EXPANSION_DECIDED_AFTER_INITIAL_12_SEARCH_RESULTS_OBSERVED",
            "NOT_FRESH_PREREGISTERED_OR_OUT_OF_SAMPLE_VALIDATION",
            "NO_PHYSICAL_HOLDOUT_ISOLATION",
            "NO_BID_ASK_FILL_PROOF",
            "NO_PAPER_LIVE_OR_PROMOTION_AUTHORITY",
        ],
        "plan_artifact": plan_artifact.as_dict(),
        "plan_sha256": plan.sha256,
        "scope": {
            "embargo_opened": False,
            "holdout_opened": False,
            "search_only": True,
            "walk_forward_opened": False,
        },
    }


def _batch_artifact_document(
    config: AIPatternExhaustiveConfig,
    plan: ExhaustiveSearchPlan,
    batch: ExhaustiveBatch,
    *,
    kind: str,
    payload: Mapping[str, object],
) -> dict[str, object]:
    return {
        "artifact_schema": "systematic_fx.ai_pattern_exhaustive_search_batch_artifact.v1",
        "authority": AI_PATTERN_EXHAUSTIVE_AUTHORITY,
        "batch": batch.as_dict(),
        "config_semantic_sha256": config.semantic_sha256,
        "kind": kind,
        "payload": dict(payload),
        "plan_sha256": plan.sha256,
        "scope": "SEARCH_ONLY",
    }


def _batch_summary(
    batch: ExhaustiveBatch,
    raw: StageEvaluationResult,
    eligibility: Mapping[str, bool],
) -> dict[str, object]:
    candidates = tuple(raw.candidates)
    p_values = tuple(item.conservative_p_value for item in candidates)
    net_values = tuple(item.real.fully_loaded_net_pnl_ticks for item in candidates)
    return {
        "batch_key": batch.batch_key,
        "batch_number": batch.batch_number,
        "candidate_count": len(batch.members),
        "economic_or_statistical_selection_performed": False,
        "evaluable_count": len(candidates),
        "hypothesis_ids": [item.hypothesis_id for item in batch.members],
        "maximum_net_ticks": max(net_values) if net_values else None,
        "maximum_p_star": (
            None
            if not p_values
            else {"denominator": max(p_values).denominator, "numerator": max(p_values).numerator}
        ),
        "minimum_net_ticks": min(net_values) if net_values else None,
        "minimum_p_star": (
            None
            if not p_values
            else {"denominator": min(p_values).denominator, "numerator": min(p_values).numerator}
        ),
        "sample_eligible_count": sum(eligibility.values()),
    }


def _batch_result_payload(
    batch: ExhaustiveBatch,
    raw: StageEvaluationResult,
    mask_commitment: Mapping[str, object],
) -> dict[str, object]:
    if (
        raw.stage_key != "SEARCH"
        or raw.classification != "RAW_BATCH_EVALUATED_NO_SELECTION"
        or raw.finalist_proposal_sha256s
        or raw.gate_decisions
        or raw.multiplicity_decisions
    ):
        raise AIPatternExhaustiveSearchError(
            "batch result attempted selection or multiplicity correction"
        )
    rows = mask_commitment.get("proposal_masks")
    if not isinstance(rows, list):
        raise AIPatternExhaustiveSearchError("mask commitment rows differ")
    eligibility = {
        _sha256(row.get("proposal_sha256"), label="mask proposal id"): row.get("sample_eligible")
        for row in rows
        if isinstance(row, dict)
    }
    if set(eligibility) != set(batch.member_ids) or any(
        not isinstance(value, bool) for value in eligibility.values()
    ):
        raise AIPatternExhaustiveSearchError("batch mask eligibility differs")
    return {
        "artifact_schema": "systematic_fx.ai_pattern_exhaustive_raw_batch_result.v1",
        "batch_summary": _batch_summary(batch, raw, eligibility),  # type: ignore[arg-type]
        "raw_result": {
            "candidates": [item.as_dict() for item in raw.candidates],
            "classification": raw.classification,
            "stage_key": raw.stage_key,
        },
        "sample_eligibility": [
            {"evaluation_id": identity, "sample_eligible": eligibility[identity]}
            for identity in batch.member_ids
        ],
    }


def _decode_raw_batch_result(
    value: object,
    batch: ExhaustiveBatch,
) -> tuple[StageEvaluationResult, dict[str, bool], dict[str, object]]:
    document = _exact_dict(
        value,
        {"artifact_schema", "batch_summary", "raw_result", "sample_eligibility"},
        label="raw batch result",
    )
    if document["artifact_schema"] != ("systematic_fx.ai_pattern_exhaustive_raw_batch_result.v1"):
        raise AIPatternExhaustiveSearchError("raw batch result schema differs")
    raw = _exact_dict(
        document["raw_result"],
        {"candidates", "classification", "stage_key"},
        label="engine raw result",
    )
    if (
        raw["stage_key"] != "SEARCH"
        or raw["classification"] != "RAW_BATCH_EVALUATED_NO_SELECTION"
        or not isinstance(raw["candidates"], list)
    ):
        raise AIPatternExhaustiveSearchError("per-batch selection was not prohibited")
    candidates = tuple(_decode_stage_candidate(item) for item in raw["candidates"])
    candidate_ids = tuple(item.proposal_sha256 for item in candidates)
    if len(set(candidate_ids)) != len(candidate_ids) or not set(candidate_ids).issubset(
        batch.member_ids
    ):
        raise AIPatternExhaustiveSearchError("batch result contains a duplicate or foreign member")
    result = StageEvaluationResult(
        "SEARCH",
        candidates,
        (),
        "RAW_BATCH_EVALUATED_NO_SELECTION",
    )
    eligibility_rows = document["sample_eligibility"]
    if not isinstance(eligibility_rows, list):
        raise AIPatternExhaustiveSearchError("batch eligibility is not a list")
    eligibility: dict[str, bool] = {}
    for raw_row in eligibility_rows:
        row = _exact_dict(
            raw_row,
            {"evaluation_id", "sample_eligible"},
            label="batch eligibility row",
        )
        identity = _sha256(row["evaluation_id"], label="evaluation_id")
        if not isinstance(row["sample_eligible"], bool) or identity in eligibility:
            raise AIPatternExhaustiveSearchError("batch eligibility row differs")
        eligibility[identity] = row["sample_eligible"]
    if tuple(eligibility) != batch.member_ids:
        raise AIPatternExhaustiveSearchError("batch eligibility order differs")
    expected_summary = _batch_summary(batch, result, eligibility)
    if document["batch_summary"] != expected_summary:
        raise AIPatternExhaustiveSearchError("batch summary does not replay")
    return result, eligibility, expected_summary


def _reopen_artifact(
    artifacts_root: Path,
    identity: ArtifactIdentity,
) -> dict[str, object]:
    try:
        payload = verify_immutable_artifact(artifacts_root, identity)
    except ImmutableArtifactError as error:
        raise AIPatternExhaustiveSearchError(
            "immutable artifact identity verification failed"
        ) from error
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise AIPatternExhaustiveSearchError("artifact is invalid JSON") from error
    if not isinstance(document, dict) or canonical_json_bytes(document) != payload:
        raise AIPatternExhaustiveSearchError("artifact is not canonical JSON")
    return document


def _verify_expected_artifact(
    artifacts_root: Path,
    identity: ArtifactIdentity,
    expected_bytes: bytes,
) -> None:
    try:
        verify_immutable_artifact(
            artifacts_root,
            identity,
            expected_bytes=expected_bytes,
        )
    except ImmutableArtifactError as error:
        raise AIPatternExhaustiveSearchError(
            "immutable artifact differs from exact replay"
        ) from error


def _verify_batch_artifact(
    document: object,
    config: AIPatternExhaustiveConfig,
    plan: ExhaustiveSearchPlan,
    batch: ExhaustiveBatch,
    *,
    kind: str,
) -> dict[str, object]:
    parsed = _exact_dict(
        document,
        {
            "artifact_schema",
            "authority",
            "batch",
            "config_semantic_sha256",
            "kind",
            "payload",
            "plan_sha256",
            "scope",
        },
        label="batch artifact",
    )
    if (
        parsed["artifact_schema"] != "systematic_fx.ai_pattern_exhaustive_search_batch_artifact.v1"
        or parsed["authority"] != AI_PATTERN_EXHAUSTIVE_AUTHORITY
        or parsed["batch"] != batch.as_dict()
        or parsed["config_semantic_sha256"] != config.semantic_sha256
        or parsed["kind"] != kind
        or parsed["plan_sha256"] != plan.sha256
        or parsed["scope"] != "SEARCH_ONLY"
        or not isinstance(parsed["payload"], dict)
    ):
        raise AIPatternExhaustiveSearchError("batch artifact lineage differs")
    return parsed["payload"]  # type: ignore[return-value]


@dataclass(frozen=True, slots=True)
class _VerifiedLedgerState:
    events: tuple[ExhaustiveLedgerEvent, ...]
    mask_artifacts: tuple[ArtifactIdentity, ...]
    result_artifacts: tuple[ArtifactIdentity, ...]
    family_artifact: ArtifactIdentity | None
    report_artifact: ArtifactIdentity | None
    failed: bool

    @property
    def masks_frozen(self) -> int:
        return len(self.mask_artifacts)

    @property
    def batches_completed(self) -> int:
        return len(self.result_artifacts)

    @property
    def all_masks_frozen(self) -> bool:
        return any(item.event_type == "ALL_MASKS_FROZEN" for item in self.events)


def _verify_ledger_state(
    ledger: ExhaustiveLedger,
    artifacts_root: Path,
    config: AIPatternExhaustiveConfig,
    plan: ExhaustiveSearchPlan,
) -> _VerifiedLedgerState:
    events = ledger.verify()
    if not events:
        return _VerifiedLedgerState((), (), (), None, None, False)
    prefix = [
        "PRECOMMITTED",
        *("BATCH_MASKS_FROZEN" for _ in plan.batches),
        "ALL_MASKS_FROZEN",
        *("BATCH_COMPLETED" for _ in plan.batches),
        "SEARCH_FAMILY_COMPLETED",
        "COMPLETED",
    ]
    observed = [item.event_type for item in events]
    failed = observed[-1] == "FAILED"
    comparable = observed[:-1] if failed else observed
    if comparable != prefix[: len(comparable)] or (failed and len(comparable) >= len(prefix)):
        raise AIPatternExhaustiveSearchError("ledger is not an exact lifecycle prefix")
    request_sha = events[0].request_sha256
    if any(item.request_sha256 != request_sha for item in events):
        raise AIPatternExhaustiveSearchError("ledger request identity differs")
    precommit = events[0]
    if set(precommit.payload) != {"plan_artifact", "plan_sha256", "request_artifact"}:
        raise AIPatternExhaustiveSearchError("precommit ledger payload differs")
    plan_identity = _artifact_identity(
        precommit.payload["plan_artifact"], artifact_type="AI_PATTERN_EXHAUSTIVE_SEARCH_PLAN"
    )
    request_identity = _artifact_identity(
        precommit.payload["request_artifact"],
        artifact_type="AI_PATTERN_EXHAUSTIVE_SEARCH_REQUEST",
    )
    if (
        request_identity.content_sha256 != request_sha
        or precommit.payload["plan_sha256"] != plan.sha256
        or _reopen_artifact(artifacts_root, plan_identity) != plan.as_dict()
        or _reopen_artifact(artifacts_root, request_identity)
        != _request_document(config, plan, plan_identity)
    ):
        raise AIPatternExhaustiveSearchError("precommit artifacts do not replay")
    referenced_artifact_leaves = {
        plan_identity.relative_uri,
        request_identity.relative_uri,
    }
    mask_artifacts: list[ArtifactIdentity] = []
    result_artifacts: list[ArtifactIdentity] = []
    cursor = 1
    for batch in plan.batches:
        if cursor >= len(comparable):
            break
        event = events[cursor]
        if set(event.payload) != {
            "batch_key",
            "batch_number",
            "member_ids",
            "masks_artifact",
        } or (
            event.payload["batch_key"],
            event.payload["batch_number"],
            event.payload["member_ids"],
        ) != (batch.batch_key, batch.batch_number, list(batch.member_ids)):
            raise AIPatternExhaustiveSearchError("mask ledger payload differs")
        identity = _artifact_identity(
            event.payload["masks_artifact"], artifact_type="AI_PATTERN_EXHAUSTIVE_SEARCH_MASKS"
        )
        payload = _verify_batch_artifact(
            _reopen_artifact(artifacts_root, identity),
            config,
            plan,
            batch,
            kind="MASKS",
        )
        if payload.get("artifact_schema") != (
            "systematic_fx.ai_pattern_exhaustive_mask_commitment.v1"
        ):
            raise AIPatternExhaustiveSearchError("mask commitment schema differs")
        mask_artifacts.append(identity)
        referenced_artifact_leaves.add(identity.relative_uri)
        cursor += 1
    if cursor < len(comparable):
        event = events[cursor]
        if event.event_type != "ALL_MASKS_FROZEN" or event.payload != {
            "batch_count": BATCH_COUNT,
            "batch_manifest_sha256": plan.batch_manifest_sha256,
            "first_new_one_second_bytes_opened": False,
        }:
            raise AIPatternExhaustiveSearchError("all-masks barrier differs")
        cursor += 1
    for batch in plan.batches:
        if cursor >= len(comparable):
            break
        event = events[cursor]
        if (
            event.event_type != "BATCH_COMPLETED"
            or set(event.payload)
            != {
                "batch_key",
                "batch_number",
                "batch_summary",
                "result_artifact",
            }
            or (event.payload["batch_key"], event.payload["batch_number"])
            != (
                batch.batch_key,
                batch.batch_number,
            )
        ):
            raise AIPatternExhaustiveSearchError("result ledger payload differs")
        identity = _artifact_identity(
            event.payload["result_artifact"],
            artifact_type="AI_PATTERN_EXHAUSTIVE_SEARCH_RESULT",
        )
        payload = _verify_batch_artifact(
            _reopen_artifact(artifacts_root, identity),
            config,
            plan,
            batch,
            kind="RESULT",
        )
        _raw, _eligibility, summary = _decode_raw_batch_result(payload, batch)
        if event.payload["batch_summary"] != summary:
            raise AIPatternExhaustiveSearchError("result ledger summary differs")
        result_artifacts.append(identity)
        referenced_artifact_leaves.add(identity.relative_uri)
        cursor += 1
    family_artifact: ArtifactIdentity | None = None
    report_artifact: ArtifactIdentity | None = None
    if cursor < len(comparable):
        event = events[cursor]
        if event.event_type != "SEARCH_FAMILY_COMPLETED" or set(event.payload) != {
            "classification",
            "family_artifact",
            "finalist_hypothesis_ids",
        }:
            raise AIPatternExhaustiveSearchError("family ledger payload differs")
        family_artifact = _artifact_identity(
            event.payload["family_artifact"],
            artifact_type="AI_PATTERN_EXHAUSTIVE_SEARCH_FAMILY_RESULT",
        )
        family_document = _reopen_artifact(artifacts_root, family_artifact)
        if event.payload != {
            "classification": family_document.get("classification"),
            "family_artifact": family_artifact.as_dict(),
            "finalist_hypothesis_ids": family_document.get("finalist_hypothesis_ids"),
        }:
            raise AIPatternExhaustiveSearchError("family event is not bound to its artifact")
        referenced_artifact_leaves.add(family_artifact.relative_uri)
        cursor += 1
    if cursor < len(comparable):
        event = events[cursor]
        if event.event_type != "COMPLETED" or set(event.payload) != {
            "final_status",
            "report_artifact",
        }:
            raise AIPatternExhaustiveSearchError("terminal ledger payload differs")
        report_artifact = _artifact_identity(
            event.payload["report_artifact"],
            artifact_type="AI_PATTERN_EXHAUSTIVE_SEARCH_REPORT",
        )
        report_document = _reopen_artifact(artifacts_root, report_artifact)
        if event.payload != {
            "final_status": report_document.get("final_status"),
            "report_artifact": report_artifact.as_dict(),
        }:
            raise AIPatternExhaustiveSearchError(
                "terminal event is not bound to its report artifact"
            )
        referenced_artifact_leaves.add(report_artifact.relative_uri)
        cursor += 1
    if cursor != len(comparable):
        raise AIPatternExhaustiveSearchError("ledger has trailing events")
    if failed:
        failure = events[-1]
        if set(failure.payload) != {"batch_key", "failure_code"}:
            raise AIPatternExhaustiveSearchError("failure ledger payload differs")
    if report_artifact is not None:
        observed_leaves: set[str] = set()
        for path in artifacts_root.iterdir():
            if path.is_symlink() or not path.is_file() or path.stat().st_mode & _WRITE_BITS:
                raise AIPatternExhaustiveSearchError(
                    "completed artifact directory contains an unsafe leaf"
                )
            observed_leaves.add(path.name)
        if observed_leaves != referenced_artifact_leaves:
            raise AIPatternExhaustiveSearchError(
                "completed artifact leaf set differs from the ledger closure"
            )
    return _VerifiedLedgerState(
        events,
        tuple(mask_artifacts),
        tuple(result_artifacts),
        family_artifact,
        report_artifact,
        failed,
    )


@dataclass(frozen=True, slots=True)
class ExhaustiveRunServices:
    load_search_plan: Callable[[Path], HoldoutStagePlan]
    load_five_minute_bars: Callable[[Path, HoldoutStagePlan], tuple[object, ...]]
    freeze_batch_masks: Callable[
        [
            Path,
            ExhaustiveBatch,
            HoldoutStagePlan,
            tuple[object, ...],
        ],
        object,
    ]
    evaluate_batch: Callable[
        [
            Path,
            ExhaustiveBatch,
            HoldoutStagePlan,
            tuple[object, ...],
            object,
        ],
        StageEvaluationResult,
    ]


def _load_search_plan_default(project_root: Path) -> HoldoutStagePlan:
    old_config = load_ai_pattern_holdout_config(project_root)
    inputs = _build_evaluation_inputs(project_root, old_config)
    if inputs.search_plan.stage_key != "SEARCH" or any(
        set(inputs.search_plan.data_dates) & set(plan.data_dates)
        for plan in (*inputs.walk_forward_plans, inputs.holdout_plan)
    ):
        raise AIPatternExhaustiveSearchError("Search allowlist overlaps a later stage")
    return inputs.search_plan


def _load_five_minute_bars_default(
    project_root: Path,
    search_plan: HoldoutStagePlan,
) -> tuple[object, ...]:
    return _load_stage_bars(project_root, _stage_partitions((search_plan,)), 300)


def _freeze_batch_masks_default(
    project_root: Path,
    batch: ExhaustiveBatch,
    search_plan: HoldoutStagePlan,
    five_minute_bars: tuple[object, ...],
) -> object:
    del project_root
    from scripts.ai_pattern_holdout_engine import build_stage_masks

    frozen = tuple(
        FrozenProposal(
            item.family_position,
            item.evaluation_id,
            item.pattern.direction,
            item.pattern.rule,
        )
        for item in batch.members
    )
    return build_stage_masks(
        "SEARCH",
        five_minute_bars,
        frozen,
        batch.member_ids,
        ExecutionSpec(),
        20_260_813,
        _stage_groups((search_plan,)),
    )


def _evaluate_batch_default(
    project_root: Path,
    batch: ExhaustiveBatch,
    search_plan: HoldoutStagePlan,
    five_minute_bars: tuple[object, ...],
    masks: object,
) -> StageEvaluationResult:
    from scripts.ai_pattern_holdout_engine import StageMaskBundle, evaluate_stage_parts

    if not isinstance(masks, StageMaskBundle):
        raise AIPatternExhaustiveSearchError("default evaluator received foreign masks")
    partitions = _stage_partitions((search_plan,))
    frozen = tuple(
        FrozenProposal(
            item.family_position,
            item.evaluation_id,
            item.pattern.direction,
            item.pattern.rule,
        )
        for item in batch.members
    )
    return evaluate_stage_parts(
        "SEARCH",
        five_minute_bars,
        _one_second_outcome_parts(project_root, partitions),
        masks,
        frozen,
        ExecutionSpec(),
        lambda _candidate: False,
        group_by_date=_stage_groups((search_plan,)),
        classification_pass="INVALID_PER_BATCH_SELECTION",
        classification_fail="RAW_BATCH_EVALUATED_NO_SELECTION",
    )


def _default_services() -> ExhaustiveRunServices:
    return ExhaustiveRunServices(
        _load_search_plan_default,
        _load_five_minute_bars_default,
        _freeze_batch_masks_default,
        _evaluate_batch_default,
    )


@dataclass(frozen=True, slots=True)
class _PresenceProposal:
    proposal_sha256: str


@dataclass(frozen=True, slots=True)
class _MaskPresence:
    proposal: _PresenceProposal
    sample_eligible: bool


@dataclass(frozen=True, slots=True)
class _AggregateMaskBundle:
    stage_key: str
    proposal_masks: tuple[_MaskPresence, ...]


def _result_from_artifact(
    artifacts_root: Path,
    identity: ArtifactIdentity,
    config: AIPatternExhaustiveConfig,
    plan: ExhaustiveSearchPlan,
    batch: ExhaustiveBatch,
) -> tuple[StageEvaluationResult, dict[str, bool], dict[str, object]]:
    payload = _verify_batch_artifact(
        _reopen_artifact(artifacts_root, identity),
        config,
        plan,
        batch,
        kind="RESULT",
    )
    return _decode_raw_batch_result(payload, batch)


def _aggregate_family_result(
    project_root: Path,
    artifacts_root: Path,
    config: AIPatternExhaustiveConfig,
    plan: ExhaustiveSearchPlan,
    result_artifacts: Sequence[ArtifactIdentity],
) -> StageEvaluationResult:
    """Import 12 + 506 raw summaries, then call the frozen selector exactly once."""

    if len(result_artifacts) != BATCH_COUNT:
        raise AIPatternExhaustiveSearchError("family correction requires all 43 results")
    initial_candidates, initial_eligibility = _initial_evidence(project_root, plan)
    member_by_evaluation = {item.evaluation_id: item for item in plan.members}
    summaries_by_hypothesis: dict[str, StageCandidateSummary] = {}
    eligibility_by_hypothesis: dict[str, bool] = {}

    def import_values(
        candidates: Sequence[StageCandidateSummary],
        eligibility: Mapping[str, bool],
        expected_ids: Sequence[str],
    ) -> None:
        candidate_by_id = {item.proposal_sha256: item for item in candidates}
        if not set(candidate_by_id).issubset(expected_ids) or set(eligibility) != set(expected_ids):
            raise AIPatternExhaustiveSearchError("raw family mapping differs")
        for evaluation_id in expected_ids:
            member = member_by_evaluation[evaluation_id]
            sample_eligible = eligibility[evaluation_id]
            candidate = candidate_by_id.get(evaluation_id)
            if sample_eligible != (candidate is not None):
                raise AIPatternExhaustiveSearchError(
                    "sample eligibility and raw candidate presence disagree"
                )
            if member.hypothesis_id in eligibility_by_hypothesis:
                raise AIPatternExhaustiveSearchError("hypothesis was imported twice")
            eligibility_by_hypothesis[member.hypothesis_id] = sample_eligible
            if candidate is not None:
                summaries_by_hypothesis[member.hypothesis_id] = replace(
                    candidate, proposal_sha256=member.hypothesis_id
                )

    import_values(
        initial_candidates,
        initial_eligibility,
        tuple(item.evaluation_id for item in plan.initial_members),
    )
    for batch, identity in zip(plan.batches, result_artifacts, strict=True):
        raw, eligibility, _summary = _result_from_artifact(
            artifacts_root, identity, config, plan, batch
        )
        import_values(raw.candidates, eligibility, batch.member_ids)
    if set(eligibility_by_hypothesis) != set(plan.family_ids):
        raise AIPatternExhaustiveSearchError("518-family eligibility is incomplete")
    raw_family = StageEvaluationResult(
        "SEARCH",
        tuple(
            summaries_by_hypothesis[key]
            for key in plan.family_ids
            if key in summaries_by_hypothesis
        ),
        (),
        "RAW_518_FAMILY_EVALUATED_NO_SELECTION",
    )
    mask_family = _AggregateMaskBundle(
        "SEARCH",
        tuple(
            _MaskPresence(_PresenceProposal(key), eligibility_by_hypothesis[key])
            for key in plan.family_ids
        ),
    )
    from scripts.ai_pattern_holdout_engine import select_stage_result

    selected = select_stage_result(
        "SEARCH",
        raw_family,
        mask_family,  # type: ignore[arg-type]
        plan.family_ids,
    )
    if len(selected.multiplicity_decisions) != SUPPORT_ELIGIBLE_FAMILY_COUNT:
        raise AIPatternExhaustiveSearchError("518-family BH width differs")
    return selected


def _family_result_document(
    config: AIPatternExhaustiveConfig,
    plan: ExhaustiveSearchPlan,
    selected: StageEvaluationResult,
    result_artifacts: Sequence[ArtifactIdentity],
) -> dict[str, object]:
    return {
        "artifact_schema": "systematic_fx.ai_pattern_exhaustive_search_family_result.v1",
        "authority": AI_PATTERN_EXHAUSTIVE_AUTHORITY,
        "batch_result_artifacts": [item.as_dict() for item in result_artifacts],
        "classification": selected.classification,
        "config_semantic_sha256": config.semantic_sha256,
        "family_count": SUPPORT_ELIGIBLE_FAMILY_COUNT,
        "family_sha256": plan.family_sha256,
        "finalist_hypothesis_ids": list(selected.finalist_proposal_sha256s),
        "gate_decisions": [item.as_dict() for item in selected.gate_decisions],
        "imported_initial_search": {
            "masks_sha256": EXPECTED_INITIAL_SEARCH_MASKS_SHA256,
            "result_sha256": EXPECTED_INITIAL_SEARCH_RESULT_SHA256,
        },
        "multiplicity_decisions": [item.as_dict() for item in selected.multiplicity_decisions],
        "multiplicity_method": "BENJAMINI_HOCHBERG_Q_1_OVER_20_ONCE_OVER_518",
        "plan_sha256": plan.sha256,
        "walk_forward_opened": False,
        "embargo_opened": False,
        "holdout_opened": False,
    }


def _report_document(
    config: AIPatternExhaustiveConfig,
    plan: ExhaustiveSearchPlan,
    family_artifact: ArtifactIdentity,
    selected: StageEvaluationResult,
) -> dict[str, object]:
    final_status = (
        "EXHAUSTIVE_SEARCH_FINALISTS_SELECTED"
        if selected.finalist_proposal_sha256s
        else "NO_EXHAUSTIVE_SEARCH_FINALISTS"
    )
    return {
        "artifact_schema": "systematic_fx.ai_pattern_exhaustive_search_report.v1",
        "authority": AI_PATTERN_EXHAUSTIVE_AUTHORITY,
        "batch_count": BATCH_COUNT,
        "config_semantic_sha256": config.semantic_sha256,
        "database_mutated": False,
        "family_artifact": family_artifact.as_dict(),
        "family_count": SUPPORT_ELIGIBLE_FAMILY_COUNT,
        "final_status": final_status,
        "finalist_hypothesis_ids": list(selected.finalist_proposal_sha256s),
        "limitations": [
            "UNSEALED_LOCAL_BAR_SCREENING_ONLY",
            "RETROSPECTIVE_EXPANSION_DECIDED_AFTER_INITIAL_12_SEARCH_RESULTS_OBSERVED",
            "NOT_FRESH_PREREGISTERED_OR_OUT_OF_SAMPLE_VALIDATION",
            "NO_PHYSICAL_HOLDOUT_ISOLATION",
            "NO_BID_ASK_FILL_PROOF",
            "NO_STRICT_BACKTEST_OR_PROMOTION_CLAIM",
        ],
        "network_accessed": False,
        "plan_sha256": plan.sha256,
        "scope": {
            "embargo_opened": False,
            "holdout_opened": False,
            "search_completed": True,
            "walk_forward_opened": False,
        },
    }


@dataclass(frozen=True, slots=True)
class AIPatternExhaustiveSearchRun:
    config: AIPatternExhaustiveConfig
    plan: ExhaustiveSearchPlan
    status: str
    masks_frozen: int
    batches_completed: int
    finalist_hypothesis_ids: tuple[str, ...]
    family_artifact: ArtifactIdentity | None
    report_artifact: ArtifactIdentity | None
    root: Path

    def as_dict(self) -> dict[str, object]:
        return {
            "authority": AI_PATTERN_EXHAUSTIVE_AUTHORITY,
            "batch_count": BATCH_COUNT,
            "batches_completed": self.batches_completed,
            "config_file_sha256": self.config.file_sha256,
            "config_semantic_sha256": self.config.semantic_sha256,
            "database_mutated": False,
            "family_artifact": (
                None if self.family_artifact is None else self.family_artifact.as_dict()
            ),
            "family_count": SUPPORT_ELIGIBLE_FAMILY_COUNT,
            "family_sha256": self.plan.family_sha256,
            "finalist_hypothesis_ids": list(self.finalist_hypothesis_ids),
            "holdout_opened": False,
            "masks_frozen": self.masks_frozen,
            "network_accessed": False,
            "plan_sha256": self.plan.sha256,
            "report_artifact": (
                None if self.report_artifact is None else self.report_artifact.as_dict()
            ),
            "schema": EXHAUSTIVE_RUN_SCHEMA,
            "status": self.status,
            "walk_forward_opened": False,
        }


def _status_from_state(state: _VerifiedLedgerState) -> str:
    if state.failed:
        return "FAILED"
    if state.report_artifact is not None:
        return "COMPLETED"
    if state.family_artifact is not None:
        return "SEARCH_FAMILY_COMPLETED"
    if state.batches_completed:
        return "BATCH_RESULTS_IN_PROGRESS"
    if state.all_masks_frozen:
        return "ALL_MASKS_FROZEN"
    if state.masks_frozen:
        return "MASK_FREEZE_IN_PROGRESS"
    return "PRECOMMITTED"


def _run_value(
    config: AIPatternExhaustiveConfig,
    plan: ExhaustiveSearchPlan,
    state: _VerifiedLedgerState,
    run_root: Path,
) -> AIPatternExhaustiveSearchRun:
    finalists: tuple[str, ...] = ()
    if state.family_artifact is not None:
        event = next(item for item in state.events if item.event_type == "SEARCH_FAMILY_COMPLETED")
        raw = event.payload["finalist_hypothesis_ids"]
        if not isinstance(raw, list):
            raise AIPatternExhaustiveSearchError("family finalist list differs")
        finalists = tuple(_sha256(item, label="finalist hypothesis") for item in raw)
    return AIPatternExhaustiveSearchRun(
        config,
        plan,
        _status_from_state(state),
        state.masks_frozen,
        state.batches_completed,
        finalists,
        state.family_artifact,
        state.report_artifact,
        run_root,
    )


def _ensure_precommit(
    project_root: Path,
    config: AIPatternExhaustiveConfig,
    plan: ExhaustiveSearchPlan,
    run_root: Path,
) -> tuple[ExhaustiveLedger, Path, _VerifiedLedgerState]:
    artifacts_root = _safe_directory(run_root / "artifacts", create=True)
    _reconcile_artifact_publish_temps(artifacts_root)
    ledger = ExhaustiveLedger(run_root / "ledger", create=True)
    state = _verify_ledger_state(ledger, artifacts_root, config, plan)
    if state.events:
        return ledger, artifacts_root, state
    # Verify the imported raw evidence before making the mixed-ID family durable.
    _initial_evidence(project_root, plan)
    plan_artifact = _publish(
        artifacts_root,
        artifact_type="AI_PATTERN_EXHAUSTIVE_SEARCH_PLAN",
        prefix="exhaustive-search-plan",
        document=plan.as_dict(),
    )
    request_document = _request_document(config, plan, plan_artifact)
    request_artifact = _publish(
        artifacts_root,
        artifact_type="AI_PATTERN_EXHAUSTIVE_SEARCH_REQUEST",
        prefix="exhaustive-search-request",
        document=request_document,
    )
    ledger.append(
        "PRECOMMITTED",
        request_artifact.content_sha256,
        {
            "plan_artifact": plan_artifact.as_dict(),
            "plan_sha256": plan.sha256,
            "request_artifact": request_artifact.as_dict(),
        },
    )
    return ledger, artifacts_root, _verify_ledger_state(ledger, artifacts_root, config, plan)


def _recover_unledgered_result(
    artifacts_root: Path,
    config: AIPatternExhaustiveConfig,
    plan: ExhaustiveSearchPlan,
    batch: ExhaustiveBatch,
) -> tuple[ArtifactIdentity, dict[str, object]] | None:
    prefix = f"batch-{batch.batch_key}-result-"
    paths = tuple(sorted(artifacts_root.glob(f"{prefix}*.json")))
    if not paths:
        return None
    if len(paths) != 1:
        raise AIPatternExhaustiveSearchError("multiple unledgered batch results exist")
    path = paths[0]
    digest = path.name.removeprefix(prefix).removesuffix(".json")
    identity = ArtifactIdentity(
        "AI_PATTERN_EXHAUSTIVE_SEARCH_RESULT",
        _sha256(digest, label="unledgered result SHA-256"),
        path.stat().st_size,
        path.name,
    )
    payload = _verify_batch_artifact(
        _reopen_artifact(artifacts_root, identity),
        config,
        plan,
        batch,
        kind="RESULT",
    )
    _raw, _eligibility, summary = _decode_raw_batch_result(payload, batch)
    return identity, summary


def _fixed_batch_json(summary: Mapping[str, object]) -> None:
    print(
        json.dumps(
            {"event": "BATCH_COMPLETED", **dict(summary)},
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ),
        flush=True,
    )


def _append_failure_if_possible(
    ledger: ExhaustiveLedger,
    request_sha256: str,
    *,
    error: Exception,
    batch_key: str | None,
) -> None:
    try:
        events = ledger.verify()
        if events and events[-1].event_type not in {"FAILED", "COMPLETED"}:
            ledger.append(
                "FAILED",
                request_sha256,
                {"batch_key": batch_key, "failure_code": type(error).__name__},
            )
    except (AIPatternExhaustiveSearchError, OSError, TypeError, ValueError):
        # A corrupt/unwritable ledger cannot safely accept a purported failure event.
        return


def _run_with_services(
    project_root: Path,
    config: AIPatternExhaustiveConfig,
    plan: ExhaustiveSearchPlan,
    run_root: Path,
    services: ExhaustiveRunServices,
    *,
    emit_batch_summaries: bool,
) -> AIPatternExhaustiveSearchRun:
    ledger, artifacts_root, state = _ensure_precommit(project_root, config, plan, run_root)
    if state.failed:
        raise AIPatternExhaustiveSearchError("exhaustive Search attempt is terminal FAILED")
    request_sha256 = state.events[0].request_sha256
    current_batch: str | None = None
    try:
        search_plan = services.load_search_plan(project_root)
        five_minute_bars: tuple[object, ...] | None = None
        mask_artifacts = list(state.mask_artifacts)
        # Freeze every remaining mask before the first new one-second loader call.
        for batch in plan.batches[state.masks_frozen :]:
            current_batch = batch.batch_key
            if five_minute_bars is None:
                five_minute_bars = services.load_five_minute_bars(project_root, search_plan)
            masks = services.freeze_batch_masks(project_root, batch, search_plan, five_minute_bars)
            commitment = _mask_commitment(masks)
            document = _batch_artifact_document(
                config, plan, batch, kind="MASKS", payload=commitment
            )
            identity = _publish(
                artifacts_root,
                artifact_type="AI_PATTERN_EXHAUSTIVE_SEARCH_MASKS",
                prefix=f"batch-{batch.batch_key}-masks",
                document=document,
            )
            ledger.append(
                "BATCH_MASKS_FROZEN",
                request_sha256,
                {
                    "batch_key": batch.batch_key,
                    "batch_number": batch.batch_number,
                    "member_ids": list(batch.member_ids),
                    "masks_artifact": identity.as_dict(),
                },
            )
            mask_artifacts.append(identity)
        if not state.all_masks_frozen:
            if len(mask_artifacts) != BATCH_COUNT:
                raise AIPatternExhaustiveSearchError("all-masks barrier is premature")
            ledger.append(
                "ALL_MASKS_FROZEN",
                request_sha256,
                {
                    "batch_count": BATCH_COUNT,
                    "batch_manifest_sha256": plan.batch_manifest_sha256,
                    "first_new_one_second_bytes_opened": False,
                },
            )
        result_artifacts = list(state.result_artifacts)
        for batch in plan.batches[state.batches_completed :]:
            current_batch = batch.batch_key
            if five_minute_bars is None:
                five_minute_bars = services.load_five_minute_bars(project_root, search_plan)
            masks = services.freeze_batch_masks(project_root, batch, search_plan, five_minute_bars)
            commitment = _mask_commitment(masks)
            expected_mask_document = _batch_artifact_document(
                config, plan, batch, kind="MASKS", payload=commitment
            )
            _verify_expected_artifact(
                artifacts_root,
                mask_artifacts[batch.batch_number - 1],
                canonical_json_bytes(expected_mask_document),
            )
            recovered = _recover_unledgered_result(artifacts_root, config, plan, batch)
            if recovered is None:
                raw = services.evaluate_batch(
                    project_root,
                    batch,
                    search_plan,
                    five_minute_bars,
                    masks,
                )
                payload = _batch_result_payload(batch, raw, commitment)
                _decoded, _eligibility, summary = _decode_raw_batch_result(payload, batch)
                document = _batch_artifact_document(
                    config, plan, batch, kind="RESULT", payload=payload
                )
                identity = _publish(
                    artifacts_root,
                    artifact_type="AI_PATTERN_EXHAUSTIVE_SEARCH_RESULT",
                    prefix=f"batch-{batch.batch_key}-result",
                    document=document,
                )
            else:
                identity, summary = recovered
            ledger.append(
                "BATCH_COMPLETED",
                request_sha256,
                {
                    "batch_key": batch.batch_key,
                    "batch_number": batch.batch_number,
                    "batch_summary": summary,
                    "result_artifact": identity.as_dict(),
                },
            )
            result_artifacts.append(identity)
            if emit_batch_summaries:
                _fixed_batch_json(summary)
        selected = _aggregate_family_result(
            project_root, artifacts_root, config, plan, result_artifacts
        )
        family_document = _family_result_document(config, plan, selected, result_artifacts)
        if state.family_artifact is None:
            family_artifact = _publish(
                artifacts_root,
                artifact_type="AI_PATTERN_EXHAUSTIVE_SEARCH_FAMILY_RESULT",
                prefix="exhaustive-search-family-result",
                document=family_document,
            )
            ledger.append(
                "SEARCH_FAMILY_COMPLETED",
                request_sha256,
                {
                    "classification": selected.classification,
                    "family_artifact": family_artifact.as_dict(),
                    "finalist_hypothesis_ids": list(selected.finalist_proposal_sha256s),
                },
            )
        else:
            family_artifact = state.family_artifact
            _verify_expected_artifact(
                artifacts_root,
                family_artifact,
                canonical_json_bytes(family_document),
            )
        report_document = _report_document(config, plan, family_artifact, selected)
        if state.report_artifact is None:
            report_artifact = _publish(
                artifacts_root,
                artifact_type="AI_PATTERN_EXHAUSTIVE_SEARCH_REPORT",
                prefix="exhaustive-search-report",
                document=report_document,
            )
            ledger.append(
                "COMPLETED",
                request_sha256,
                {
                    "final_status": report_document["final_status"],
                    "report_artifact": report_artifact.as_dict(),
                },
            )
        else:
            _verify_expected_artifact(
                artifacts_root,
                state.report_artifact,
                canonical_json_bytes(report_document),
            )
        final_state = _verify_ledger_state(ledger, artifacts_root, config, plan)
        return _run_value(config, plan, final_state, run_root)
    except OSError:
        # Disk/host interruption remains resumable; do not make it scientifically terminal.
        raise
    except Exception as error:
        _append_failure_if_possible(
            ledger,
            request_sha256,
            error=error,
            batch_key=current_batch,
        )
        raise


def precommit_ai_pattern_exhaustive_search(
    project_root: Path | str,
) -> AIPatternExhaustiveSearchRun:
    """Durably bind the exact 518 family and chunks without opening Search bars."""

    root = _project_root(project_root)
    config = load_ai_pattern_exhaustive_config(root)
    plan = build_exhaustive_search_plan(root)
    run_root = _fixed_run_root(root, create=True)
    with _exclusive_run_lock(run_root):
        _ledger, _artifacts, state = _ensure_precommit(root, config, plan, run_root)
    return _run_value(config, plan, state, run_root)


def run_ai_pattern_exhaustive_search(
    project_root: Path | str,
) -> AIPatternExhaustiveSearchRun:
    """Resume and complete exhaustive Search using only committed default services."""

    root = _project_root(project_root)
    config = load_ai_pattern_exhaustive_config(root)
    plan = build_exhaustive_search_plan(root)
    run_root = _fixed_run_root(root, create=True)
    with _exclusive_run_lock(run_root):
        return _run_with_services(
            root,
            config,
            plan,
            run_root,
            _default_services(),
            emit_batch_summaries=False,
        )


def _run_ai_pattern_exhaustive_search_for_cli(
    project_root: Path | str,
) -> AIPatternExhaustiveSearchRun:
    """Fixed CLI reporter; no caller-controlled callback or service injection."""

    root = _project_root(project_root)
    config = load_ai_pattern_exhaustive_config(root)
    plan = build_exhaustive_search_plan(root)
    run_root = _fixed_run_root(root, create=True)
    with _exclusive_run_lock(run_root):
        return _run_with_services(
            root,
            config,
            plan,
            run_root,
            _default_services(),
            emit_batch_summaries=True,
        )


def _replay_completed_batch_data_with_services(
    project_root: Path,
    config: AIPatternExhaustiveConfig,
    plan: ExhaustiveSearchPlan,
    state: _VerifiedLedgerState,
    artifacts_root: Path,
    services: ExhaustiveRunServices,
) -> None:
    """Recompute every recorded mask/result byte from the Search data allowlist."""

    if state.batches_completed and not state.all_masks_frozen:
        raise AIPatternExhaustiveSearchError("recorded results exist without the all-masks barrier")
    if not state.mask_artifacts:
        return
    search_plan = services.load_search_plan(project_root)
    five_minute_bars = services.load_five_minute_bars(project_root, search_plan)
    for index, (batch, identity) in enumerate(
        zip(plan.batches, state.mask_artifacts, strict=False)
    ):
        masks = services.freeze_batch_masks(project_root, batch, search_plan, five_minute_bars)
        commitment = _mask_commitment(masks)
        expected = _batch_artifact_document(config, plan, batch, kind="MASKS", payload=commitment)
        _verify_expected_artifact(
            artifacts_root,
            identity,
            canonical_json_bytes(expected),
        )
        if index >= state.batches_completed:
            continue
        result_identity = state.result_artifacts[index]
        raw = services.evaluate_batch(
            project_root,
            batch,
            search_plan,
            five_minute_bars,
            masks,
        )
        payload = _batch_result_payload(batch, raw, commitment)
        _decode_raw_batch_result(payload, batch)
        expected = _batch_artifact_document(config, plan, batch, kind="RESULT", payload=payload)
        _verify_expected_artifact(
            artifacts_root,
            result_identity,
            canonical_json_bytes(expected),
        )


def verify_ai_pattern_exhaustive_search(
    project_root: Path | str,
) -> AIPatternExhaustiveSearchRun:
    """Read-only replay of plan, ledger, imported summaries, BH, and reports."""

    root = _project_root(project_root)
    config = load_ai_pattern_exhaustive_config(root)
    plan = build_exhaustive_search_plan(root)
    run_root = _fixed_run_root(root, create=False)
    artifacts_root = _safe_directory(run_root / "artifacts", create=False)
    ledger = ExhaustiveLedger(run_root / "ledger", create=False)
    state = _verify_ledger_state(ledger, artifacts_root, config, plan)
    if not state.events:
        raise AIPatternExhaustiveSearchError("exhaustive Search has no precommit")
    _replay_completed_batch_data_with_services(
        root,
        config,
        plan,
        state,
        artifacts_root,
        _default_services(),
    )
    if state.family_artifact is not None:
        selected = _aggregate_family_result(
            root, artifacts_root, config, plan, state.result_artifacts
        )
        expected_family = _family_result_document(config, plan, selected, state.result_artifacts)
        _verify_expected_artifact(
            artifacts_root,
            state.family_artifact,
            canonical_json_bytes(expected_family),
        )
        family_event = next(
            item for item in state.events if item.event_type == "SEARCH_FAMILY_COMPLETED"
        )
        if family_event.payload != {
            "classification": selected.classification,
            "family_artifact": state.family_artifact.as_dict(),
            "finalist_hypothesis_ids": list(selected.finalist_proposal_sha256s),
        }:
            raise AIPatternExhaustiveSearchError("family ledger result does not replay")
        if state.report_artifact is not None:
            expected_report = _report_document(config, plan, state.family_artifact, selected)
            _verify_expected_artifact(
                artifacts_root,
                state.report_artifact,
                canonical_json_bytes(expected_report),
            )
    return _run_value(config, plan, state, run_root)


__all__ = [
    "DEFAULT_EXHAUSTIVE_ROOT",
    "AIPatternExhaustiveSearchError",
    "AIPatternExhaustiveSearchRun",
    "ExhaustiveBatch",
    "ExhaustiveCandidate",
    "ExhaustiveLedger",
    "ExhaustiveSearchPlan",
    "build_exhaustive_search_plan",
    "precommit_ai_pattern_exhaustive_search",
    "run_ai_pattern_exhaustive_search",
    "verify_ai_pattern_exhaustive_search",
]
