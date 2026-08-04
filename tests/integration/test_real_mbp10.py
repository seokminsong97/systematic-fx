import os
import unittest
from pathlib import Path

from systematic_fx.data.footer import read_mbp10_footer
from systematic_fx.data.smoke import smoke_check_parquet

EXPECTED_SCHEMA_FINGERPRINT = "57c7cc404aec87845b9e3872a4b2abcc651bd07858810324b4c9e3aa636ef5ea"


class RealMbp10IntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        configured = os.environ.get("SYSTEMATIC_FX_SMOKE_PARQUET")
        if not configured:
            raise unittest.SkipTest("SYSTEMATIC_FX_SMOKE_PARQUET is not set")
        cls.source_path = Path(configured).expanduser().resolve()

    def test_footer_matches_frozen_raw_contract(self) -> None:
        footer = read_mbp10_footer(self.source_path)

        self.assertEqual(footer.schema_fingerprint, EXPECTED_SCHEMA_FINGERPRINT)
        self.assertEqual(footer.contract.dataset, "GLBX.MDP3")
        self.assertEqual(footer.contract.schema_name, "mbp-10")
        self.assertGreater(footer.unique_instrument_count, 0)

    def test_first_row_group_has_no_structural_violations(self) -> None:
        result = smoke_check_parquet(self.source_path, max_row_groups=1)

        self.assertGreater(result.rows_scanned, 0)
        self.assertEqual(result.structural_violations, 0)
        self.assertTrue(result.passed)


if __name__ == "__main__":
    unittest.main()
