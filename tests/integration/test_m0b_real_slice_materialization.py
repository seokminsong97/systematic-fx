from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

import pytest

from systematic_fx.research.m0b import (
    load_materialized_real_slice,
    materialize_real_slice,
    verify_real_slice,
)

PROJECT = Path(__file__).resolve().parents[2]
CONFIG = PROJECT / "configs/research/m0b_real_slice_v1.toml"


@pytest.mark.skipif(
    os.environ.get("SYSTEMATIC_FX_RUN_M0B_REAL_SLICE") != "1",
    reason="requires the exact local 1.15 GB four-file raw allowlist",
)
def test_actual_bounded_raw_slice_is_reproducible_and_reopenable() -> None:
    with tempfile.TemporaryDirectory(prefix="systematic-fx-m0b-test-", dir="/private/tmp") as raw:
        staged = Path(raw)
        first = materialize_real_slice(CONFIG, data_root=PROJECT / "data", output_root=staged)
        second = materialize_real_slice(CONFIG, data_root=PROJECT / "data", output_root=staged)
        assert second == first
        assert first.config_hash == (
            "1f8b5bab8616ce82e1476a6444d5dd91e5903e93c2c1600143ba1e3290502422"
        )
        assert first.source_manifest.content_sha256 == (
            "549c2c62935955c600ad3b496595ca5cb47323ad9b9efcb1e8e6f60ae4947ef0"
        )
        assert (first.quote_manifest.row_count, first.quote_manifest.content_sha256) == (
            33_854,
            "6a13909d3f4844a30d8fe741119ef5dab2dceb7a2d20aa76cbf4ccef78405554",
        )
        assert (first.feature_manifest.row_count, first.feature_manifest.content_sha256) == (
            144,
            "90cc332da98672d641233e23b66d8b7cc5b60d943c1b2479df8522b130ecba62",
        )
        assert (first.label_manifest.row_count, first.label_manifest.content_sha256) == (
            7_776,
            "6c1f2df18eecaea8ec398b0ac44c4b2728333c15e23f1a1bbce7d38b6b145fb4",
        )
        assert first.sha256 == ("17f4ccdcb839c70bfdd95c9d00a2b37ca6d31fff89c34439a2adcaac4c32cf5f")

        build_path = staged / f"build-{first.sha256}.json"
        assert load_materialized_real_slice(build_path) == first
        verify_real_slice(
            first,
            CONFIG,
            data_root=PROJECT / "data",
            verify_source_bytes=False,
            staged_root=staged,
        )
        assert all(path.resolve().is_relative_to(staged.resolve()) for path in staged.iterdir())

        feature_path = staged / str(first.feature_manifest.relative_uri)
        features = [json.loads(line) for line in feature_path.read_text().splitlines()]
        assert all(
            row["context_30m_end_ns"] is None or row["context_30m_end_ns"] <= row["event_ts_ns"]
            for row in features
        )
        assert all(
            row["context_1h_end_ns"] is None or row["context_1h_end_ns"] <= row["event_ts_ns"]
            for row in features
        )
        assert any(row["contract_transition_context"] for row in features)
        assert all(not row["active_selection_proven"] for row in features)

        label_path = staged / str(first.label_manifest.relative_uri)
        labels = [json.loads(line) for line in label_path.read_text().splitlines()]
        quote_path = staged / str(first.quote_manifest.relative_uri)
        quotes = [json.loads(line) for line in quote_path.read_text().splitlines()]
        assert sum(row["event_count"] for row in quotes) == 1_416_660
        assert all(not row["entry_eligible"] for row in labels)
        assert any(row["invalid_reason"] == "WOULD_CROSS_SESSION_CLOSE" for row in labels)
        assert any(row["first_touch_type"] == "TP_FIRST" for row in labels)
        assert any(row["first_touch_type"] == "SL_FIRST" for row in labels)
        assert all(
            row["exit_ts_ns"] == row["event_ts_ns"] + row["max_hold_seconds"] * 1_000_000_000
            for row in labels
            if row["first_touch_type"] == "TIMEOUT"
        )
