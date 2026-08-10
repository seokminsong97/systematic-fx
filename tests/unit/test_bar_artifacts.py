from __future__ import annotations

import os
import stat
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from systematic_fx.research import bar_artifacts as bar_artifacts_module
from systematic_fx.research.bar_artifacts import (
    BAR_DATA_ARTIFACT_ROOT,
    BAR_PATTERN_ARTIFACT_ROOT,
    BarArtifactDescriptor,
    BarArtifactDriftError,
    BarArtifactError,
    arrow_schema_sha256,
    open_verified_bar_artifact,
    publish_bar_artifact_file,
    publish_bar_artifact_open_file,
    publish_bar_json_artifact,
    publish_bar_parquet_table,
    verify_published_bar_artifact,
)
from systematic_fx.research.hypotheses import canonical_sha256


def _descriptor(**overrides: object) -> BarArtifactDescriptor:
    values: dict[str, object] = {
        "artifact_key": "bar_pattern_discovery_v1:test:one",
        "artifact_type": "bar_test_result",
        "artifact_schema": "systematic_fx.bar_test_result.v1",
        "artifact_version": 1,
        "record_count": 2,
        "schema_sha256": "a" * 64,
        "source_manifest_sha256": "b" * 64,
        "logical_identity": {"candidate_key": "candidate_one", "split": "discovery"},
        "media_type": "application/json",
        "file_suffix": ".json",
        "root_kind": "bar_patterns",
    }
    values.update(overrides)
    return BarArtifactDescriptor(**values)  # type: ignore[arg-type]


def _document() -> dict[str, object]:
    return {
        "records": [{"id": 1}, {"id": 2}],
        "schema": "systematic_fx.bar_test_result.v1",
    }


def test_json_publication_binds_contract_and_bytes_and_is_idempotent(tmp_path: Path) -> None:
    descriptor = _descriptor()
    first = publish_bar_json_artifact(tmp_path, descriptor, _document())
    second = publish_bar_json_artifact(tmp_path, descriptor, _document())

    assert first == second
    assert first.path.is_relative_to(tmp_path / BAR_PATTERN_ARTIFACT_ROOT)
    assert first.path.parent.name == f"identity_sha256={descriptor.identity_sha256}"
    assert first.path.name == f"sha256={first.sha256}.json"
    assert stat.S_IMODE(first.path.stat().st_mode) & 0o222 == 0
    assert first.byte_size == len(first.path.read_bytes())
    assert first.database_metadata() == {
        **descriptor.identity_document(),
        "artifact_identity_sha256": descriptor.identity_sha256,
        "content_sha256": first.sha256,
    }
    verify_published_bar_artifact(tmp_path, first)


def test_same_bytes_with_different_count_have_distinct_contract_paths(tmp_path: Path) -> None:
    first = publish_bar_json_artifact(tmp_path, _descriptor(record_count=2), _document())
    second = publish_bar_json_artifact(tmp_path, _descriptor(record_count=3), _document())

    assert first.sha256 == second.sha256
    assert first.descriptor.identity_sha256 != second.descriptor.identity_sha256
    assert first.path != second.path


def test_json_schema_must_match_descriptor(tmp_path: Path) -> None:
    with pytest.raises(BarArtifactError, match="document schema"):
        publish_bar_json_artifact(
            tmp_path,
            _descriptor(),
            {"schema": "systematic_fx.some_other_result.v1"},
        )


def test_publication_rejects_a_symlinked_ancestor(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    (tmp_path / "data").symlink_to(outside, target_is_directory=True)

    with pytest.raises(BarArtifactError, match="unsafe|inaccessible"):
        publish_bar_json_artifact(tmp_path, _descriptor(), _document())


def test_open_verification_detects_leaf_swap_while_inode_is_held(tmp_path: Path) -> None:
    artifact = publish_bar_json_artifact(tmp_path, _descriptor(), _document())
    original_bytes = artifact.path.read_bytes()
    displaced = artifact.path.with_name("displaced.json")

    with (
        pytest.raises(BarArtifactDriftError, match="inode changed|no longer names"),
        open_verified_bar_artifact(tmp_path, artifact),
    ):
        artifact.path.rename(displaced)
        artifact.path.write_bytes(original_bytes)
        artifact.path.chmod(0o444)


def test_open_verification_detects_ancestor_swap_while_dirfds_are_held(
    tmp_path: Path,
) -> None:
    artifact = publish_bar_json_artifact(tmp_path, _descriptor(), _document())
    data_path = tmp_path / "data"
    displaced = tmp_path / "data-displaced"

    with (
        pytest.raises(BarArtifactDriftError, match="ancestor"),
        open_verified_bar_artifact(tmp_path, artifact),
    ):
        data_path.rename(displaced)
        data_path.mkdir()


def test_verification_rejects_writable_or_modified_content(tmp_path: Path) -> None:
    artifact = publish_bar_json_artifact(tmp_path, _descriptor(), _document())
    artifact.path.chmod(0o644)
    with pytest.raises(BarArtifactDriftError, match="write bits"):
        verify_published_bar_artifact(tmp_path, artifact)

    artifact.path.write_bytes(b"changed")
    artifact.path.chmod(0o444)
    with pytest.raises(BarArtifactDriftError, match="content differs"):
        verify_published_bar_artifact(tmp_path, artifact)


def test_parquet_publication_verifies_arrow_schema_and_row_count(tmp_path: Path) -> None:
    source = tmp_path / "source.parquet"
    table = pa.table({"price_ticks": pa.array([10, 11], type=pa.int64())})
    pq.write_table(table, source)
    schema_sha256 = arrow_schema_sha256(pq.ParquetFile(source).schema_arrow)
    descriptor = _descriptor(
        artifact_key="bar_pattern_discovery_v1:bars:test",
        artifact_type="selected_trade_bars",
        artifact_schema="systematic_fx.selected_trade_bars.v1",
        schema_sha256=schema_sha256,
        media_type="application/vnd.apache.parquet",
        file_suffix=".parquet",
        root_kind="bars",
    )

    artifact = publish_bar_artifact_file(tmp_path, descriptor, source.resolve())
    assert artifact.path.is_relative_to(tmp_path / BAR_DATA_ARTIFACT_ROOT)
    verify_published_bar_artifact(tmp_path, artifact)

    with pytest.raises(BarArtifactError, match="row count"):
        publish_bar_artifact_file(
            tmp_path,
            _descriptor(
                artifact_key="bar_pattern_discovery_v1:bars:wrong_count",
                artifact_type="selected_trade_bars",
                artifact_schema="systematic_fx.selected_trade_bars.v1",
                record_count=3,
                schema_sha256=schema_sha256,
                media_type="application/vnd.apache.parquet",
                file_suffix=".parquet",
                root_kind="bars",
            ),
            source.resolve(),
        )


def test_parquet_table_publication_keeps_one_anonymous_fd_through_publish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    table = pa.table({"price_ticks": pa.array([10, 11], type=pa.int64())})
    descriptor = _descriptor(
        artifact_key="bar_pattern_discovery_v1:evidence:test",
        artifact_type="bar_discovery_test_shard",
        artifact_schema="systematic_fx.bar_discovery_test_shard.v1",
        schema_sha256=arrow_schema_sha256(table.schema),
        media_type="application/vnd.apache.parquet",
        file_suffix=".parquet",
    )
    writer_descriptors: list[int] = []
    publisher_descriptors: list[int] = []
    original_write = bar_artifacts_module.pq.write_table
    original_publish = bar_artifacts_module._publish_from_descriptor

    def write_spy(value, destination, **kwargs):
        writer_descriptors.append(destination.fileno())
        return original_write(value, destination, **kwargs)

    def publish_spy(**kwargs):
        publisher_descriptors.append(kwargs["source_descriptor"])
        return original_publish(**kwargs)

    monkeypatch.setattr(bar_artifacts_module.pq, "write_table", write_spy)
    monkeypatch.setattr(bar_artifacts_module, "_publish_from_descriptor", publish_spy)

    artifact = publish_bar_parquet_table(tmp_path, descriptor, table)

    assert writer_descriptors == publisher_descriptors
    assert len(writer_descriptors) == 1
    assert pq.read_table(artifact.path).equals(table)
    verify_published_bar_artifact(tmp_path, artifact)


def test_parquet_table_publication_rejects_same_fd_tamper_before_copy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    table = pa.table({"price_ticks": pa.array([10, 11], type=pa.int64())})
    descriptor = _descriptor(
        artifact_key="bar_pattern_discovery_v1:evidence:tamper",
        artifact_type="bar_discovery_test_shard",
        artifact_schema="systematic_fx.bar_discovery_test_shard.v1",
        schema_sha256=arrow_schema_sha256(table.schema),
        media_type="application/vnd.apache.parquet",
        file_suffix=".parquet",
    )
    original_publish = bar_artifacts_module._publish_from_descriptor

    def tamper(**kwargs):
        os.pwrite(kwargs["source_descriptor"], b"X", 0)
        return original_publish(**kwargs)

    monkeypatch.setattr(bar_artifacts_module, "_publish_from_descriptor", tamper)

    with pytest.raises(BarArtifactDriftError, match="source artifact changed"):
        publish_bar_parquet_table(tmp_path, descriptor, table)


def test_open_file_publication_keeps_held_inode_across_path_swap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_path = tmp_path / "global-result.json"
    displaced_path = tmp_path / "held-original.json"
    original = b'{"schema":"systematic_fx.bar_test_result.v1"}\n'
    attacker = b'{"schema":"attacker"}\n'
    source_path.write_bytes(original)
    observed_descriptors: list[int] = []
    original_publish = bar_artifacts_module._publish_from_descriptor

    def publish_spy(**kwargs):
        observed_descriptors.append(kwargs["source_descriptor"])
        return original_publish(**kwargs)

    monkeypatch.setattr(bar_artifacts_module, "_publish_from_descriptor", publish_spy)
    with source_path.open("r+b") as held:
        held_descriptor = held.fileno()
        source_path.rename(displaced_path)
        source_path.write_bytes(attacker)
        artifact = publish_bar_artifact_open_file(tmp_path, _descriptor(), held)

    assert observed_descriptors == [held_descriptor]
    assert artifact.path.read_bytes() == original
    assert artifact.path.read_bytes() != source_path.read_bytes()
    verify_published_bar_artifact(tmp_path, artifact)


def test_open_file_publication_rejects_same_fd_tamper_before_copy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_publish = bar_artifacts_module._publish_from_descriptor

    def tamper(**kwargs):
        os.pwrite(kwargs["source_descriptor"], b"X", 0)
        return original_publish(**kwargs)

    monkeypatch.setattr(bar_artifacts_module, "_publish_from_descriptor", tamper)
    with tempfile.TemporaryFile(dir=tmp_path) as held:
        held.write(b'{"schema":"systematic_fx.bar_test_result.v1"}\n')
        with pytest.raises(BarArtifactDriftError, match="source artifact changed"):
            publish_bar_artifact_open_file(tmp_path, _descriptor(), held)


def test_descriptor_rejects_noncanonical_or_incomplete_identity() -> None:
    with pytest.raises(BarArtifactError, match="record_count"):
        _descriptor(record_count=-1)
    with pytest.raises(BarArtifactError, match="schema_sha256"):
        _descriptor(schema_sha256="not-a-hash")
    with pytest.raises(BarArtifactError, match="strict canonical JSON"):
        _descriptor(logical_identity={"binary_float": 0.25})

    descriptor = _descriptor()
    expected = canonical_sha256(descriptor.identity_document())
    assert descriptor.identity_sha256 == expected
    assert os.path.isabs(str((Path.cwd() / descriptor.relative_directory).resolve()))
