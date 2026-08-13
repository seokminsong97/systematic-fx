from __future__ import annotations

import os
from pathlib import Path

import pytest

from systematic_fx.data.cme_active_contract import (
    active_contract_mapping_as_of,
    load_active_contract_volume_manifest,
    materialize_active_contract_mapping_artifact,
    verify_active_contract_mapping_artifact,
)

PROJECT = Path(__file__).resolve().parents[2]
MANIFEST = PROJECT / "configs/data/cme_6e_active_contract_roll_context_v1.toml"


@pytest.mark.skipif(
    os.environ.get("SYSTEMATIC_FX_RUN_CME_ROLL_CONTEXT") != "1",
    reason="requires the exact local bounded CME roll-context raw allowlist",
)
def test_actual_previous_session_volume_mapping_switches_from_u2_to_z2() -> None:
    manifest = load_active_contract_volume_manifest(
        MANIFEST,
        allow_bounded_weekday_fallback=True,
    )
    artifact = materialize_active_contract_mapping_artifact(
        manifest,
        data_root=PROJECT / "data",
        verify_source_hashes=True,
    )
    mappings = artifact.mappings
    assert artifact.content_sha256 == (
        "3092fdb96e5aba7e64ac41f051f670c7ae8d969323d00766d3b94032256220a0"
    )
    assert [(item.trading_date.isoformat(), item.selected.raw_symbol) for item in mappings] == [
        ("2022-09-16", "6EU2"),
        ("2022-09-19", "6EZ2"),
    ]
    assert mappings[0].selected.trade_volume == 158_500
    assert mappings[1].selected.trade_volume == 224_580
    assert mappings[0].selection_available_ts_ns == 1_663_275_600_000_000_000
    assert mappings[1].selection_available_ts_ns == 1_663_362_000_000_000_000
    selected = active_contract_mapping_as_of(
        mappings,
        trading_date=mappings[1].trading_date,
        as_of_ts_ns=1_663_538_400_000_000_000,
    )
    assert selected.selected.raw_symbol == "6EZ2"
    verify_active_contract_mapping_artifact(
        artifact,
        manifest,
        data_root=PROJECT / "data",
        verify_source_hashes=False,
    )
