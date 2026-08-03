import tempfile
import unittest
from pathlib import Path

from systematic_fx.data.inventory import summarize_inventory


def _touch(root: Path, relative_path: str, contents: bytes = b"") -> None:
    target = root / relative_path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(contents)


class InventoryTest(unittest.TestCase):
    def test_inventory_summarizes_valid_daily_layout(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _touch(
                root,
                "2022/01/02/glbx-mdp3-20220102.mbp-10.parquet",
                contents=b"1234",
            )
            _touch(
                root,
                "2026/07/31/glbx-mdp3-20260731.mbp-10.parquet",
                contents=b"12",
            )

            summary = summarize_inventory(root)

            self.assertEqual(summary.file_count, 2)
            self.assertEqual(summary.total_bytes, 6)
            self.assertEqual(summary.first_source_date.isoformat(), "2022-01-02")
            self.assertEqual(summary.last_source_date.isoformat(), "2026-07-31")
            self.assertEqual(summary.invalid_layout_files, ())

    def test_inventory_reports_path_and_filename_date_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _touch(root, "2022/01/02/glbx-mdp3-20220103.mbp-10.parquet")

            summary = summarize_inventory(root)

            self.assertEqual(
                summary.invalid_layout_files,
                ("2022/01/02/glbx-mdp3-20220103.mbp-10.parquet",),
            )
