from __future__ import annotations

import base64
import hashlib
import json
import os
import tempfile
import unittest
from pathlib import Path

from systematic_fx.research.provenance import (
    ProvenanceError,
    build_code_snapshot,
    dependency_lock_sha256,
    publish_code_snapshot,
    runtime_environment,
)

COMMIT = "a" * 40


def _workspace(root: Path) -> None:
    for directory in ("configs/x", "docs", "migrations", "src/systematic_fx"):
        (root / directory).mkdir(parents=True)
    (root / "Makefile").write_text("test:\n\ttrue\n", encoding="utf-8")
    (root / "pyproject.toml").write_text("[project]\nname='x'\n", encoding="utf-8")
    (root / "uv.lock").write_text("version = 1\n", encoding="utf-8")
    (root / "configs/x/a.toml").write_text("value = 1\n", encoding="utf-8")
    (root / "docs/a.md").write_text("policy\n", encoding="utf-8")
    (root / "migrations/0001.sql").write_text("SELECT 1;\n", encoding="utf-8")
    (root / "src/systematic_fx/a.py").write_text("VALUE = 1\n", encoding="utf-8")


class ProvenanceTests(unittest.TestCase):
    def test_snapshot_is_canonical_complete_and_changes_with_source_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _workspace(root)
            first = build_code_snapshot(root, code_commit=COMMIT)
            second = build_code_snapshot(root, code_commit=COMMIT)

            self.assertEqual(first.canonical_bytes, second.canonical_bytes)
            self.assertEqual(first.sha256, hashlib.sha256(first.canonical_bytes).hexdigest())
            payload = json.loads(first.canonical_bytes)
            self.assertEqual(payload["file_count"], 7)
            files = {item["relative_path"]: item for item in payload["files"]}
            restored = base64.b64decode(files["src/systematic_fx/a.py"]["content_base64"])
            self.assertEqual(restored, b"VALUE = 1\n")
            self.assertEqual(
                hashlib.sha256(restored).hexdigest(),
                files["src/systematic_fx/a.py"]["sha256"],
            )
            self.assertEqual(
                [item["relative_path"] for item in payload["files"]],
                sorted(item["relative_path"] for item in payload["files"]),
            )

            (root / "src/systematic_fx/a.py").write_text("VALUE = 2\n", encoding="utf-8")
            changed = build_code_snapshot(root, code_commit=COMMIT)
            self.assertNotEqual(changed.sha256, first.sha256)

    def test_snapshot_publication_is_under_derived_and_exactly_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _workspace(root)
            (root / "data/derived/manifests").mkdir(parents=True)
            snapshot = build_code_snapshot(root, code_commit=COMMIT)

            first = publish_code_snapshot(snapshot, data_root=root / "data")
            second = publish_code_snapshot(snapshot, data_root=root / "data")

            self.assertEqual(first.disposition, "CREATED")
            self.assertEqual(second.disposition, "REUSED")
            self.assertEqual(first.path, second.path)
            self.assertEqual(first.path.read_bytes(), snapshot.canonical_bytes)
            self.assertIn("data/derived/manifests/code_snapshot_v2", first.path.as_posix())

    def test_unsafe_snapshot_input_and_target_drift_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _workspace(root)
            (root / "data/derived/manifests").mkdir(parents=True)
            snapshot = build_code_snapshot(root, code_commit=COMMIT)
            report = publish_code_snapshot(snapshot, data_root=root / "data")
            os.chmod(report.path, 0o644)
            report.path.write_bytes(b"drift")
            with self.assertRaisesRegex(ProvenanceError, "content drift"):
                publish_code_snapshot(snapshot, data_root=root / "data")

            (root / "src/systematic_fx/a.py").unlink()
            (root / "src/systematic_fx/a.py").symlink_to(root / "uv.lock")
            with self.assertRaisesRegex(ProvenanceError, "symbolic link"):
                build_code_snapshot(root, code_commit=COMMIT)

    def test_dependency_and_runtime_environment_are_explicit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _workspace(root)
            expected = hashlib.sha256((root / "uv.lock").read_bytes()).hexdigest()
            self.assertEqual(dependency_lock_sha256(root), expected)

        environment = runtime_environment()
        self.assertEqual(environment["artifact_schema"], "systematic_fx.runtime_environment.v1")
        self.assertIn("pyarrow", environment["packages"])
        self.assertIn("PYTHONHASHSEED", environment["numeric_environment"])


if __name__ == "__main__":
    unittest.main()
