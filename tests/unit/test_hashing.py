import hashlib
import json
import os
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest import mock

from systematic_fx.data import hashing
from systematic_fx.data.hashing import HashManifestError, build_sha256_manifest


def _source_path(data_root: Path, day: date) -> Path:
    return (
        data_root
        / "mbp-10"
        / f"{day.year:04d}"
        / f"{day.month:02d}"
        / f"{day.day:02d}"
        / f"glbx-mdp3-{day:%Y%m%d}.mbp-10.parquet"
    )


def _write_source(data_root: Path, day: date, contents: bytes) -> Path:
    path = _source_path(data_root, day)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(contents)
    return path


def _footer_record(data_root: Path, day: date) -> dict[str, object]:
    path = _source_path(data_root, day)
    return {
        "file_size_bytes": path.stat().st_size,
        "path": path.relative_to(data_root / "mbp-10").as_posix(),
        "source_date": day.isoformat(),
    }


class HashManifestTest(unittest.TestCase):
    def test_builds_canonical_manifest_inside_data_and_reports_its_hash(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            later = _write_source(data_root, date(2024, 1, 3), b"later-source")
            earlier = _write_source(data_root, date(2024, 1, 2), b"earlier-source")
            original_contents = {path: path.read_bytes() for path in (earlier, later)}
            progress: list[hashing.HashProgress] = []

            report = build_sha256_manifest(
                data_root,
                chunk_size_bytes=2,
                progress_callback=progress.append,
            )

            expected_directory = data_root.resolve() / "derived" / "manifests"
            self.assertEqual(report.manifest_path.parent, expected_directory)
            self.assertEqual(report.checkpoint_path.parent, expected_directory)
            self.assertEqual(report.file_count, 2)
            self.assertEqual(report.hashed_file_count, 2)
            self.assertEqual(report.resumed_file_count, 0)
            self.assertEqual(report.total_source_bytes, len(b"earlier-sourcelater-source"))
            self.assertEqual(
                report.manifest_sha256,
                hashlib.sha256(report.manifest_path.read_bytes()).hexdigest(),
            )
            self.assertEqual(report.manifest_byte_size, report.manifest_path.stat().st_size)

            raw_lines = report.manifest_path.read_bytes().splitlines(keepends=True)
            records = [json.loads(line) for line in raw_lines]
            self.assertEqual(
                [record["source_date"] for record in records],
                ["2024-01-02", "2024-01-03"],
            )
            self.assertEqual(
                set(records[0]),
                {"byte_size", "relative_uri", "sha256", "source_date"},
            )
            for raw_line, record in zip(raw_lines, records, strict=True):
                canonical = (
                    json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n"
                ).encode()
                self.assertEqual(raw_line, canonical)
            self.assertEqual(records[0]["sha256"], hashlib.sha256(b"earlier-source").hexdigest())
            self.assertEqual(
                [event.status for event in progress],
                ["HASHED", "HASHED", "COMPLETE"],
            )
            self.assertEqual(progress[-1].bytes_processed, report.total_source_bytes)
            for path, contents in original_contents.items():
                self.assertEqual(path.read_bytes(), contents)

    def test_completed_checkpoint_makes_an_identical_rerun_fully_resumable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            _write_source(data_root, date(2024, 1, 2), b"one")
            _write_source(data_root, date(2024, 1, 3), b"two")
            first = build_sha256_manifest(data_root, chunk_size_bytes=1)
            first_manifest = first.manifest_path.read_bytes()
            progress: list[hashing.HashProgress] = []

            with mock.patch.object(
                hashing,
                "_hash_source",
                side_effect=AssertionError("resumed source was read"),
            ):
                second = build_sha256_manifest(
                    data_root,
                    chunk_size_bytes=1,
                    progress_callback=progress.append,
                )

            self.assertEqual(second.resumed_file_count, 2)
            self.assertEqual(second.hashed_file_count, 0)
            self.assertEqual(second.manifest_path.read_bytes(), first_manifest)
            self.assertEqual(
                [event.status for event in progress],
                ["RESUMED", "RESUMED", "COMPLETE"],
            )

    def test_callback_interruption_checkpoints_a_complete_prefix(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            _write_source(data_root, date(2024, 1, 2), b"one")
            _write_source(data_root, date(2024, 1, 3), b"two")

            def interrupt(progress: hashing.HashProgress) -> None:
                if progress.status == "HASHED":
                    raise RuntimeError("stop after a durable checkpoint")

            with self.assertRaisesRegex(RuntimeError, "durable checkpoint"):
                build_sha256_manifest(data_root, progress_callback=interrupt)

            manifest_directory = data_root / "derived" / "manifests"
            checkpoint = manifest_directory / "mbp10_source_sha256_v1.checkpoint.jsonl"
            final = manifest_directory / "mbp10_source_sha256_v1.jsonl"
            self.assertEqual(len(checkpoint.read_text().splitlines()), 1)
            self.assertFalse(final.exists())

            with mock.patch.object(
                hashing,
                "_hash_source",
                wraps=hashing._hash_source,
            ) as hash_source:
                report = build_sha256_manifest(data_root)

            self.assertEqual(hash_source.call_count, 1)
            self.assertEqual(report.resumed_file_count, 1)
            self.assertEqual(report.hashed_file_count, 1)

    def test_resume_rejects_same_size_source_identity_drift(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            first = _write_source(data_root, date(2024, 1, 2), b"abc")
            _write_source(data_root, date(2024, 1, 3), b"def")

            def interrupt(progress: hashing.HashProgress) -> None:
                if progress.status == "HASHED":
                    raise RuntimeError("interrupt")

            with self.assertRaises(RuntimeError):
                build_sha256_manifest(data_root, progress_callback=interrupt)

            old_stat = first.stat()
            first.write_bytes(b"xyz")
            os.utime(
                first,
                ns=(old_stat.st_atime_ns, old_stat.st_mtime_ns + 1_000_000_000),
            )

            with self.assertRaisesRegex(HashManifestError, "checkpoint identity drift"):
                build_sha256_manifest(data_root)

    def test_footer_manifest_is_verified_and_output_is_sorted_by_relative_uri(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            data_root = base / "data"
            _write_source(data_root, date(2024, 1, 2), b"a")
            _write_source(data_root, date(2024, 1, 3), b"bb")
            footer_manifest = base / "footer.jsonl"
            footer_manifest.write_text(
                "\n".join(
                    json.dumps(record)
                    for record in (
                        _footer_record(data_root, date(2024, 1, 3)),
                        _footer_record(data_root, date(2024, 1, 2)),
                    )
                )
                + "\n"
            )

            report = build_sha256_manifest(data_root, footer_manifest=footer_manifest)

            records = [json.loads(line) for line in report.manifest_path.read_text().splitlines()]
            self.assertEqual(
                [record["relative_uri"] for record in records],
                sorted(record["relative_uri"] for record in records),
            )

    def test_footer_manifest_rejects_duplicate_and_declared_size_drift(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            data_root = base / "data"
            _write_source(data_root, date(2024, 1, 2), b"abc")
            record = _footer_record(data_root, date(2024, 1, 2))
            footer_manifest = base / "footer.jsonl"
            footer_manifest.write_text(f"{json.dumps(record)}\n{json.dumps(record)}\n")

            with self.assertRaisesRegex(HashManifestError, "duplicate source"):
                build_sha256_manifest(data_root, footer_manifest=footer_manifest)

            record["file_size_bytes"] = 999
            footer_manifest.write_text(f"{json.dumps(record)}\n")
            with self.assertRaisesRegex(HashManifestError, "source size drift"):
                build_sha256_manifest(data_root, footer_manifest=footer_manifest)

    def test_rejects_path_traversal_and_output_escape(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            data_root = base / "data"
            _write_source(data_root, date(2024, 1, 2), b"abc")
            footer_manifest = base / "footer.jsonl"
            footer_manifest.write_text(
                json.dumps(
                    {
                        "file_size_bytes": 3,
                        "path": "../2024/01/02/glbx-mdp3-20240102.mbp-10.parquet",
                        "source_date": "2024-01-02",
                    }
                )
                + "\n"
            )

            with self.assertRaisesRegex(HashManifestError, "unsafe relative URI"):
                build_sha256_manifest(data_root, footer_manifest=footer_manifest)
            with self.assertRaisesRegex(HashManifestError, "one filename"):
                build_sha256_manifest(data_root, manifest_name="../escaped.jsonl")
            self.assertFalse((data_root / "escaped.jsonl").exists())

    def test_rejects_dataset_outside_data_root_and_source_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            data_root = base / "data"
            _write_source(data_root, date(2024, 1, 2), b"abc")
            outside = base / "outside"
            _write_source(outside, date(2024, 1, 3), b"outside")

            with self.assertRaisesRegex(HashManifestError, "inside data_root"):
                build_sha256_manifest(data_root, dataset_root=outside / "mbp-10")

            link = _source_path(data_root, date(2024, 1, 3))
            link.parent.mkdir(parents=True, exist_ok=True)
            try:
                link.symlink_to(_source_path(data_root, date(2024, 1, 2)))
            except OSError:
                self.skipTest("symbolic links are unavailable")
            with self.assertRaisesRegex(HashManifestError, "symbolic link"):
                build_sha256_manifest(data_root)


if __name__ == "__main__":
    unittest.main()
