"""Frozen scientific contract for the e2a month-end candidate.

This package is deliberately separate from the intraday candidate catalogs.  It
does not grant broker, paper, or live authority.  The historical handover has a
known event-selection/execution conflict, so the initial registration is
fail-closed while preserving the exact forward rule without retuning it.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Final, Literal

from systematic_fx.research.hypotheses import canonical_sha256

CAMPAIGN_ID: Final = "e2a_month_end_v1"
CANDIDATE_ID: Final = "e2a_last_eligible_weekday_london_1500_fade_mtd_hold_24h"
SCHEMA: Final = "systematic_fx.e2a_month_end_config.v1"
DATASET_MANIFEST_SHA256: Final = "e2c066ce4c8a97c4059dd2499f881300f905f4bab589240f87532d5cc49599dc"
DATASET_MANIFEST_RELATIVE_PATH: Final = Path(
    "data/derived/bar_patterns/trade_bar_dataset_manifest/"
    "identity_sha256=b0ecab04cdd3626d3c488f9108c8e9184f5dd610f51950ab7e7f74a5b7524297/"
    f"sha256={DATASET_MANIFEST_SHA256}.json"
)
HANDOVER_PROMPT_SHA256: Final = "4f06fa0837d7ff5a98ebf5c4cc0f0a0bfa37a57d71d0439610fe884b4206e949"
HANDOVER_SOURCE_ARTIFACT_SHA256S: Final = (
    (
        "data/handover_lab/e2a_trades/R1_2022-01_2023-07.parquet",
        "6cef0ddec33937dc657d8453c118ac23d1a4c866593dbecd6f8403310559329e",
    ),
    (
        "data/handover_lab/e2a_trades/R2_2023-09_2024-12.parquet",
        "d871162af1b026c4b5b348916109f2384d4480a3427f89838db45a2346994672",
    ),
    (
        "data/handover_lab/e2a_trades/R3_2025-01_2026-01.parquet",
        "45a974ef90de1fa5bc066f299c74507ef689aa3cbd6309240bed5108167694c6",
    ),
    (
        "data/handover_lab/precommits/precommit_ff_v1.json",
        "3bb8bab083f07f1b3fba84df502752d7e4258022615b466ebe17cea5a5b8baf9",
    ),
    (
        "data/handover_lab/precommits/precommit_final.json",
        "b1183757c4d035e540d8c9d27aa378f57899ec9b126a94576e92168312dca976",
    ),
    (
        "data/handover_lab/precommits/precommit_flow_v1.json",
        "47d0d6a6e04ede6bccc2d40915d3d4a5600a58cdf0e34a6112a1d8f4bc8199b8",
    ),
    (
        "data/handover_lab/precommits/precommit_lastdoors.json",
        "2b069f339482fcb4645d20c31ad506e7f2e06b4a34addf90d1799308f93f3f9a",
    ),
    (
        "data/handover_lab/precommits/precommit_val_round2.json",
        "2bdb9ab83f04b51d9021238636727a881569e4649ba1a0eac1bbded2a5fb3982",
    ),
    (
        "data/handover_lab/precommits/precommit_val_round3.json",
        "36840336cbc43b83ad9e7a3298fdde4fde88ce269a60f7f0b372edaa79567b93",
    ),
    (
        "data/handover_lab/precommits/precommit_wave3.json",
        "ff2bbfb07e39faeb66a4963a7766d5cb4a6fc5fc1a564842374670718082a9b3",
    ),
    (
        "data/handover_lab/verdicts/holdout_e2a.json",
        "f576491d42bd79b857811d45529cb3bedf91f8ba5560f80069d6a9f934d7f94a",
    ),
    (
        "data/handover_lab/verdicts/mechanism_diagnostics.json",
        "2ba0ce270e16ca7cb1bbd33a9243c602ddf102c2980d2db09b6ee37c3e5bab90",
    ),
)


class E2AConfigError(ValueError):
    """The single-candidate contract is incomplete or internally inconsistent."""


CampaignStatus = Literal["HISTORICAL_EVIDENCE_CONFLICT"]
ForwardStatus = Literal["PLANNED_NOT_ARMABLE"]


@dataclass(frozen=True, slots=True)
class HistoricalWindow:
    key: str
    role: str
    decision_start: date
    decision_end: date
    consumed: bool

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class E2AConfig:
    """One immutable candidate and its narrow low-frequency policy amendments."""

    schema: str
    campaign_id: str
    candidate_id: str
    candidate_count: int
    campaign_status: CampaignStatus
    forward_status: ForwardStatus
    instrument: str
    timezone: str
    decision_hour: int
    decision_minute: int
    entry_delay_seconds: int
    entry_wait_seconds: int
    holding_seconds: int
    maximum_stream_gap_seconds: int
    month_open_lookup_seconds: int
    month_open_staleness_seconds: int
    p15_staleness_seconds: int
    maximum_concurrent_positions: int
    take_profit: None
    stop_loss: None
    primary_cost_policy: str
    historical_comparison_debit_ticks: str
    stress_debit_ticks: tuple[int, int]
    minimum_forward_events: int
    minimum_wins: int
    alternative_net_ticks: int
    maximum_positive_concentration: str
    maximum_average_slippage_ticks_per_side: str
    forward_opportunity_start: date
    forward_opportunity_end: date
    missing_event_policy: str
    multiplicity_policy: str
    historical_windows: tuple[HistoricalWindow, ...]
    consumed_holdout_disclosure: str
    discovery_preregistered_disclosure: str
    closed_map_policy: str
    evidence_conflicts: tuple[str, ...]
    arm_blockers: tuple[str, ...]

    def __post_init__(self) -> None:
        if self.schema != SCHEMA:
            raise E2AConfigError("schema drifted")
        if self.campaign_id != CAMPAIGN_ID or self.candidate_id != CANDIDATE_ID:
            raise E2AConfigError("campaign or candidate identity drifted")
        if self.candidate_count != 1:
            raise E2AConfigError("e2a must remain a single-candidate campaign")
        if (self.instrument, self.timezone) != ("CME_6E", "Europe/London"):
            raise E2AConfigError("instrument or decision timezone drifted")
        if (self.decision_hour, self.decision_minute) != (15, 0):
            raise E2AConfigError("the London decision anchor may not be retuned")
        if (self.entry_delay_seconds, self.entry_wait_seconds) != (1, 3):
            raise E2AConfigError("entry timing may not be retuned")
        if self.holding_seconds != 86_400 or self.maximum_stream_gap_seconds != 345_600:
            raise E2AConfigError("fixed horizon or stream-gap boundary drifted")
        if (
            self.month_open_lookup_seconds,
            self.month_open_staleness_seconds,
            self.p15_staleness_seconds,
        ) != (60, 3_600, 1_800):
            raise E2AConfigError("signal price lookup policy drifted")
        if self.maximum_concurrent_positions != 1:
            raise E2AConfigError("e2a permits only one position")
        if self.take_profit is not None or self.stop_loss is not None:
            raise E2AConfigError("e2a may not acquire a TP/SL exit")
        if self.primary_cost_policy != "BBO_GROSS_PLUS_VERIFIED_ACTUAL_PER_FILL_FEES":
            raise E2AConfigError("the primary cost policy must not double-count BBO spread")
        if self.historical_comparison_debit_ticks != "1.5" or self.stress_debit_ticks != (
            14,
            18,
        ):
            raise E2AConfigError("historical comparison or stress debit drifted")
        if (
            self.minimum_forward_events,
            self.minimum_wins,
            self.alternative_net_ticks,
        ) != (12, 7, 120):
            raise E2AConfigError("forward event gates drifted")
        if self.maximum_positive_concentration != "0.50":
            raise E2AConfigError("positive-PnL concentration gate drifted")
        if self.maximum_average_slippage_ticks_per_side != "1.0":
            raise E2AConfigError("slippage gate drifted")
        if (self.forward_opportunity_start, self.forward_opportunity_end) != (
            date(2026, 8, 1),
            date(2027, 7, 31),
        ):
            raise E2AConfigError("forward opportunity horizon drifted")
        if self.missing_event_policy != "NO_AUTOMATIC_EXTENSION_INCONCLUSIVE_SUPPORT":
            raise E2AConfigError("missing forward events may not extend the frozen horizon")
        if self.multiplicity_policy != "SINGLE_CANDIDATE_NO_BH_NO_NEW_SIGNIFICANCE_CLAIM":
            raise E2AConfigError("e2a must not enter an intraday BH family")
        if len(self.historical_windows) != 4:
            raise E2AConfigError("historical disclosure must retain all four windows")
        if (
            not self.consumed_holdout_disclosure
            or not self.discovery_preregistered_disclosure
            or not self.closed_map_policy
        ):
            raise E2AConfigError("governance disclosures may not be blank")
        if not self.evidence_conflicts or not self.arm_blockers:
            raise E2AConfigError("the initial registration must remain fail-closed")

    def as_dict(self) -> dict[str, object]:
        value = asdict(self)
        value["historical_windows"] = [item.as_dict() for item in self.historical_windows]
        value["dataset_manifest_relative_path"] = str(DATASET_MANIFEST_RELATIVE_PATH)
        value["dataset_manifest_sha256"] = DATASET_MANIFEST_SHA256
        value["handover_prompt_sha256"] = HANDOVER_PROMPT_SHA256
        value["handover_source_artifact_sha256s"] = [
            {"relative_path": relative_path, "sha256": sha256}
            for relative_path, sha256 in HANDOVER_SOURCE_ARTIFACT_SHA256S
        ]
        return value

    @property
    def semantic_sha256(self) -> str:
        return canonical_sha256(self.as_dict())


def frozen_config() -> E2AConfig:
    """Return the exact candidate registration; there is no parameter surface."""

    return E2AConfig(
        schema=SCHEMA,
        campaign_id=CAMPAIGN_ID,
        candidate_id=CANDIDATE_ID,
        candidate_count=1,
        campaign_status="HISTORICAL_EVIDENCE_CONFLICT",
        forward_status="PLANNED_NOT_ARMABLE",
        instrument="CME_6E",
        timezone="Europe/London",
        decision_hour=15,
        decision_minute=0,
        entry_delay_seconds=1,
        entry_wait_seconds=3,
        holding_seconds=86_400,
        maximum_stream_gap_seconds=345_600,
        month_open_lookup_seconds=60,
        month_open_staleness_seconds=3_600,
        p15_staleness_seconds=1_800,
        maximum_concurrent_positions=1,
        take_profit=None,
        stop_loss=None,
        primary_cost_policy="BBO_GROSS_PLUS_VERIFIED_ACTUAL_PER_FILL_FEES",
        historical_comparison_debit_ticks="1.5",
        stress_debit_ticks=(14, 18),
        minimum_forward_events=12,
        minimum_wins=7,
        alternative_net_ticks=120,
        maximum_positive_concentration="0.50",
        maximum_average_slippage_ticks_per_side="1.0",
        forward_opportunity_start=date(2026, 8, 1),
        forward_opportunity_end=date(2027, 7, 31),
        missing_event_policy="NO_AUTOMATIC_EXTENSION_INCONCLUSIVE_SUPPORT",
        multiplicity_policy="SINGLE_CANDIDATE_NO_BH_NO_NEW_SIGNIFICANCE_CLAIM",
        historical_windows=(
            HistoricalWindow("R1", "DISCOVERY_CONSUMED", date(2022, 1, 1), date(2023, 7, 10), True),
            HistoricalWindow(
                "R2", "PREREGISTERED_CONSUMED", date(2023, 9, 1), date(2024, 12, 31), True
            ),
            HistoricalWindow(
                "R3", "PREREGISTERED_CONSUMED", date(2025, 1, 9), date(2026, 1, 20), True
            ),
            HistoricalWindow(
                "HO",
                "SEALED_HOLDOUT_OPENED_AND_CONSUMED",
                date(2026, 2, 16),
                date(2026, 7, 8),
                True,
            ),
        ),
        consumed_holdout_disclosure=(
            "The sealed holdout 2026-02-16..2026-07-08 was opened on 2026-08-20 as a "
            "one-shot single-candidate diagnostic for e2a (precommit "
            "precommits/precommit_final.json, sha "
            "b1183757c4d035e540d8c9d27aa378f57899ec9b126a94576e92168312dca976, "
            "hashed before "
            "opening). It is consumed for the month-end family and must not be presented "
            "as fresh OOS for it or for related calendar-anchor candidates. Result: 5 "
            "events, 4 wins, +42.5t (SUPPORTIVE label per frozen rule; n=5 carries no "
            "statistical claim)."
        ),
        discovery_preregistered_disclosure=(
            "2022-01..2023-07 was the discovery screen (69 candidates). 2023-09..2024-12 "
            "and 2025-01..2026-01 were preregistered validation windows. 2026-02..2026-07 "
            "was the one-shot opened and consumed holdout diagnostic. Every historical "
            "window is now in-sample for the month-end calendar-anchor family."
        ),
        closed_map_policy="NO_REMINING_OR_PARAMETER_TUNING_ON_THE_HANDOVER_CLOSED_MAP",
        evidence_conflicts=(
            "HANDOVER_47_ROWS_USE_MIXED_LEGACY_ADJACENCY_AND_CALENDAR_EVENT_RULES",
            "SECTION7_CALENDAR_RULE_ADDS_HISTORICAL_OPPORTUNITIES_NOT_PRESENT_IN_PARQUETS",
            "LEGACY_RAW_BBO_GRID_DID_NOT_ENFORCE_ITS_STATED_300_SECOND_STALENESS_CAP",
            "LEGACY_EXECUTION_ACCEPTED_LOCKED_BOOKS_AND_MULTI_HOUR_STALE_BOUNDARY_QUOTES",
            "R1_2022_03_31_DIRECTION_USED_MARCH_8_NOT_MARCH_1_AS_MONTH_OPEN_CONTEXT",
            "HANDOVER_DID_NOT_FREEZE_BOOK_RESET_AND_RECOVERY_SEMANTICS",
        ),
        arm_blockers=(
            "HISTORICAL_EVIDENCE_REPRODUCTION_NOT_IDENTICAL_UNDER_FROZEN_CALENDAR_RULE",
            "NO_LIVE_MARKET_DATA_ADAPTER",
            "NO_PAPER_BROKER_ORDER_OR_RECONCILIATION_ADAPTER",
            "NO_VERIFIED_ACTUAL_FEE_SCHEDULE",
            "NO_AUTHORITATIVE_FUTURE_TRADING_STATUS_OR_SCHEDULE_FEED",
            "NO_APPROVED_CAUSAL_FORWARD_POLICY_FOR_UNEXPECTED_QC_FAILURE_OR_96H_GAP",
            "NO_GOVERNED_E2A_BOOK_RESET_AND_RECOVERY_POLICY",
            "EXISTING_PAPER_POLICY_REQUIRES_TP_SL_OCO_WHILE_E2A_FORBIDS_TP_SL",
        ),
    )
