import unittest
from datetime import date

from systematic_fx.data.contracts import Mbp10ContractError
from systematic_fx.data.instruments import (
    InstrumentKind,
    classify_raw_symbol,
    parse_instrument_mappings,
)


class InstrumentMappingsTest(unittest.TestCase):
    def test_symbol_classifier_distinguishes_calendar_spreads(self) -> None:
        self.assertEqual(classify_raw_symbol("6EH7"), InstrumentKind.OUTRIGHT)
        self.assertEqual(
            classify_raw_symbol("6EU7-6EH7"),
            InstrumentKind.CALENDAR_SPREAD,
        )
        self.assertEqual(classify_raw_symbol("6EU7-CLZ7"), InstrumentKind.UNKNOWN)
        self.assertEqual(classify_raw_symbol("6E.FUT"), InstrumentKind.UNKNOWN)

    def test_parser_flattens_intervals_and_sorts_deterministically(self) -> None:
        payload = {
            "mappings": [
                {
                    "raw_symbol": "6EU7-6EH7",
                    "intervals": [{"start": "2026-07-31", "end": "2026-08-01", "symbol": "42"}],
                },
                {
                    "raw_symbol": "6EH7",
                    "intervals": [
                        {"start": "2026-07-31", "end": "2026-08-01", "symbol": "7"},
                        {"start": "2026-08-01", "end": "2026-08-02", "symbol": "8"},
                    ],
                },
            ]
        }

        mappings = parse_instrument_mappings(payload)

        self.assertEqual([mapping.instrument_id for mapping in mappings], [7, 42, 8])
        self.assertEqual(mappings[0].interval_start, date(2026, 7, 31))
        self.assertEqual(mappings[0].interval_end, date(2026, 8, 1))
        self.assertEqual(mappings[1].kind, InstrumentKind.CALENDAR_SPREAD)
        self.assertEqual(mappings[0].as_dict()["raw_symbol"], "6EH7")

    def test_parser_rejects_reversed_interval(self) -> None:
        payload = {
            "mappings": [
                {
                    "raw_symbol": "6EH7",
                    "intervals": [{"start": "2026-08-01", "end": "2026-07-31", "symbol": "7"}],
                }
            ]
        }

        with self.assertRaisesRegex(Mbp10ContractError, "end must be after"):
            parse_instrument_mappings(payload)

    def test_parser_rejects_instrument_id_outside_uint32(self) -> None:
        payload = {
            "mappings": [
                {
                    "raw_symbol": "6EH7",
                    "intervals": [
                        {
                            "start": "2026-07-31",
                            "end": "2026-08-01",
                            "symbol": str(2**32),
                        }
                    ],
                }
            ]
        }

        with self.assertRaisesRegex(Mbp10ContractError, "uint32 range"):
            parse_instrument_mappings(payload)


if __name__ == "__main__":
    unittest.main()
