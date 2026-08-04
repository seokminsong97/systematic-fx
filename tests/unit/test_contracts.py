import json
import unittest

import pyarrow as pa

from systematic_fx.data.contracts import (
    EXPECTED_COLUMN_COUNT,
    PRICE_SCALE,
    UNDEFINED_PRICE,
    Mbp10ContractError,
    compute_schema_fingerprint,
    expected_mbp10_schema,
    validate_mbp10_contract,
)


def _valid_metadata(**overrides: str) -> dict[bytes, bytes]:
    dbn = {
        "dataset": "GLBX.MDP3",
        "end": 1_704_153_600_000_000_000,
        "mappings": [],
        "schema": "mbp-10",
        "start": 1_704_067_200_000_000_000,
        "stype_out": "instrument_id",
        "version": 3,
    }
    values = {
        "dbn.dataset": "GLBX.MDP3",
        "dbn.metadata": json.dumps(dbn, sort_keys=True, separators=(",", ":")),
        "dbn.schema": "mbp-10",
        "dbn.version": "3",
        "mbo_mbp10.price_encoding": "fixed",
        "mbo_mbp10.price_scale": "1e-9",
        "mbo_mbp10.undefined_price": "9223372036854775807",
    }
    values.update(overrides)
    return {key.encode(): value.encode() for key, value in values.items()}


class Mbp10ContractTest(unittest.TestCase):
    def test_expected_schema_is_exactly_the_dbn_73_column_layout(self) -> None:
        schema = expected_mbp10_schema()

        self.assertEqual(len(schema), EXPECTED_COLUMN_COUNT)
        self.assertEqual(
            schema.names[:13],
            [
                "ts_recv",
                "ts_event",
                "rtype",
                "publisher_id",
                "instrument_id",
                "action",
                "side",
                "depth",
                "price",
                "size",
                "flags",
                "ts_in_delta",
                "sequence",
            ],
        )
        self.assertEqual(
            schema.names[-6:],
            [
                "bid_px_09",
                "ask_px_09",
                "bid_sz_09",
                "ask_sz_09",
                "bid_ct_09",
                "ask_ct_09",
            ],
        )
        self.assertEqual(schema.field("ts_recv").type, pa.timestamp("ns", tz="UTC"))
        self.assertEqual(schema.field("price").type, pa.int64())
        self.assertFalse(any(field.nullable for field in schema))

    def test_valid_contract_normalizes_fixed_price_metadata(self) -> None:
        contract = validate_mbp10_contract(expected_mbp10_schema(metadata=_valid_metadata()))

        self.assertEqual(contract.dataset, "GLBX.MDP3")
        self.assertEqual(contract.schema_name, "mbp-10")
        self.assertEqual(contract.price_scale, PRICE_SCALE)
        self.assertEqual(contract.undefined_price, UNDEFINED_PRICE)
        self.assertEqual(contract.as_dict()["price_scale"], "1e-9")

    def test_wrong_column_type_is_rejected(self) -> None:
        valid = expected_mbp10_schema(metadata=_valid_metadata())
        fields = list(valid)
        fields[8] = pa.field("price", pa.float64(), nullable=False)

        with self.assertRaisesRegex(Mbp10ContractError, "price.*int64"):
            validate_mbp10_contract(pa.schema(fields, metadata=valid.metadata))

    def test_noncanonical_price_scale_is_rejected(self) -> None:
        schema = expected_mbp10_schema(
            metadata=_valid_metadata(**{"mbo_mbp10.price_scale": "0.000000001"})
        )

        with self.assertRaisesRegex(Mbp10ContractError, "price_scale"):
            validate_mbp10_contract(schema)

    def test_embedded_dataset_must_match_arrow_metadata(self) -> None:
        metadata = _valid_metadata()
        embedded = json.loads(metadata[b"dbn.metadata"])
        embedded["dataset"] = "OTHER.DATASET"
        metadata[b"dbn.metadata"] = json.dumps(embedded).encode()

        with self.assertRaisesRegex(Mbp10ContractError, "dbn.metadata dataset"):
            validate_mbp10_contract(expected_mbp10_schema(metadata=metadata))

    def test_schema_fingerprint_excludes_per_day_mappings(self) -> None:
        first_metadata = _valid_metadata()
        second_metadata = _valid_metadata()
        embedded = json.loads(second_metadata[b"dbn.metadata"])
        embedded["mappings"] = [{"raw_symbol": "6EH7", "intervals": []}]
        second_metadata[b"dbn.metadata"] = json.dumps(embedded).encode()

        first = compute_schema_fingerprint(expected_mbp10_schema(metadata=first_metadata))
        second = compute_schema_fingerprint(expected_mbp10_schema(metadata=second_metadata))

        self.assertEqual(first, second)
        self.assertRegex(first, r"^[0-9a-f]{64}$")


if __name__ == "__main__":
    unittest.main()
