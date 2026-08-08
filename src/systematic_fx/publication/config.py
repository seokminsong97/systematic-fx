from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class FamilyPresentation:
    family_id: str
    title: str
    question: str
    description: str


@dataclass(frozen=True)
class OutcomeCandidatePresentation:
    candidate_id: str
    order: int
    predecessor_id: str | None


@dataclass(frozen=True)
class PublicationConfig:
    schema_version: str
    disclosure_policy_version: str
    campaign_key: str
    scope_key: str
    program_mode: str
    policy_state: str
    maximum_authority: str
    discovery_slice_target: int
    families: tuple[FamilyPresentation, ...]
    outcome_candidates: tuple[OutcomeCandidatePresentation, ...]


def load_publication_config(path: Path) -> PublicationConfig:
    with path.expanduser().resolve().open("rb") as handle:
        document: dict[str, Any] = tomllib.load(handle)
    publication = document["publication"]
    families = tuple(
        FamilyPresentation(
            family_id=item["id"],
            title=item["title"],
            question=item["question"],
            description=item["description"],
        )
        for item in document["families"]
    )
    candidates = tuple(
        OutcomeCandidatePresentation(
            candidate_id=item["id"],
            order=item["order"],
            predecessor_id=item.get("predecessor_id"),
        )
        for item in document["outcome_candidates"]
    )
    if {item.family_id for item in families} != {f"P{index}" for index in range(1, 7)}:
        raise ValueError("publication config must describe exactly families P1-P6")
    if [item.order for item in candidates] != list(range(1, len(candidates) + 1)):
        raise ValueError("outcome candidates must use contiguous order values starting at one")
    candidate_ids = {item.candidate_id for item in candidates}
    if len(candidate_ids) != len(candidates):
        raise ValueError("outcome candidate ids must be unique")
    for candidate in candidates:
        if candidate.predecessor_id is not None and candidate.predecessor_id not in candidate_ids:
            raise ValueError("outcome candidate predecessor must reference another candidate")
    return PublicationConfig(
        schema_version=publication["schema_version"],
        disclosure_policy_version=publication["disclosure_policy_version"],
        campaign_key=publication["campaign_key"],
        scope_key=publication["scope_key"],
        program_mode=publication["program_mode"],
        policy_state=publication["policy_state"],
        maximum_authority=publication["maximum_authority"],
        discovery_slice_target=publication["discovery_slice_target"],
        families=families,
        outcome_candidates=candidates,
    )
