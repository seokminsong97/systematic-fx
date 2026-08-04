"""Exact Arrow and DBN metadata contract for raw GLBX.MDP3 MBP-10 files."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

import pyarrow as pa

DATASET_NAME = "GLBX.MDP3"
SCHEMA_NAME = "mbp-10"
PRICE_ENCODING = "fixed"
PRICE_SCALE_TEXT = "1e-9"
PRICE_SCALE = Decimal(PRICE_SCALE_TEXT)
UNDEFINED_PRICE = 2**63 - 1
EXPECTED_COLUMN_COUNT = 73


class Mbp10ContractError(ValueError):
    """Raised when a raw file does not satisfy the immutable MBP-10 contract."""


@dataclass(frozen=True)
class Mbp10ContractMetadata:
    """Normalized values recorded in a valid raw Parquet schema."""

    dataset: str
    schema_name: str
    dbn_version: int
    price_encoding: str
    price_scale: Decimal
    undefined_price: int

    def as_dict(self) -> dict[str, object]:
        return {
            "dataset": self.dataset,
            "dbn_version": self.dbn_version,
            "price_encoding": self.price_encoding,
            "price_scale": PRICE_SCALE_TEXT,
            "schema": self.schema_name,
            "undefined_price": self.undefined_price,
        }


def _mbp10_fields() -> list[pa.Field]:
    fields = [
        pa.field("ts_recv", pa.timestamp("ns", tz="UTC"), nullable=False),
        pa.field("ts_event", pa.timestamp("ns", tz="UTC"), nullable=False),
        pa.field("rtype", pa.uint8(), nullable=False),
        pa.field("publisher_id", pa.uint16(), nullable=False),
        pa.field("instrument_id", pa.uint32(), nullable=False),
        pa.field("action", pa.string(), nullable=False),
        pa.field("side", pa.string(), nullable=False),
        pa.field("depth", pa.uint8(), nullable=False),
        pa.field("price", pa.int64(), nullable=False),
        pa.field("size", pa.uint32(), nullable=False),
        pa.field("flags", pa.uint8(), nullable=False),
        pa.field("ts_in_delta", pa.int32(), nullable=False),
        pa.field("sequence", pa.uint32(), nullable=False),
    ]

    for level in range(10):
        suffix = f"{level:02d}"
        fields.extend(
            [
                pa.field(f"bid_px_{suffix}", pa.int64(), nullable=False),
                pa.field(f"ask_px_{suffix}", pa.int64(), nullable=False),
                pa.field(f"bid_sz_{suffix}", pa.uint32(), nullable=False),
                pa.field(f"ask_sz_{suffix}", pa.uint32(), nullable=False),
                pa.field(f"bid_ct_{suffix}", pa.uint32(), nullable=False),
                pa.field(f"ask_ct_{suffix}", pa.uint32(), nullable=False),
            ]
        )

    return fields


_EXPECTED_SCHEMA = pa.schema(_mbp10_fields())


def expected_mbp10_schema(*, metadata: Mapping[bytes, bytes] | None = None) -> pa.Schema:
    """Return the canonical 73-column schema, optionally with schema metadata."""

    return _EXPECTED_SCHEMA.with_metadata(metadata)


def decode_dbn_metadata(payload: bytes | str | Mapping[str, Any]) -> dict[str, Any]:
    """Decode the JSON value stored under ``dbn.metadata``."""

    if isinstance(payload, Mapping):
        decoded = dict(payload)
    else:
        if isinstance(payload, bytes):
            try:
                payload = payload.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise Mbp10ContractError("dbn.metadata is not valid UTF-8") from exc
        if not isinstance(payload, str):
            raise Mbp10ContractError("dbn.metadata must be JSON text or a mapping")
        try:
            decoded = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise Mbp10ContractError("dbn.metadata is not valid JSON") from exc

    if not isinstance(decoded, dict):
        raise Mbp10ContractError("dbn.metadata JSON root must be an object")
    return decoded


def _normalize_metadata(
    metadata: Mapping[object, object] | None,
) -> dict[str, str]:
    if metadata is None:
        raise Mbp10ContractError("Arrow schema metadata is missing")

    normalized: dict[str, str] = {}
    for raw_key, raw_value in metadata.items():
        if isinstance(raw_key, bytes):
            try:
                key = raw_key.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise Mbp10ContractError("schema metadata contains a non-UTF-8 key") from exc
        elif isinstance(raw_key, str):
            key = raw_key
        else:
            raise Mbp10ContractError("schema metadata keys must be bytes or strings")

        if isinstance(raw_value, bytes):
            try:
                value = raw_value.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise Mbp10ContractError(f"schema metadata value for {key!r} is not UTF-8") from exc
        elif isinstance(raw_value, str):
            value = raw_value
        else:
            raise Mbp10ContractError(f"schema metadata value for {key!r} must be bytes or a string")

        if key in normalized:
            raise Mbp10ContractError(f"duplicate schema metadata key after decoding: {key}")
        normalized[key] = value

    return normalized


def validate_mbp10_fields(schema: pa.Schema) -> None:
    """Validate exact column count, order, names, Arrow types, and nullability."""

    if not isinstance(schema, pa.Schema):
        raise TypeError("schema must be a pyarrow.Schema")
    if len(schema) != EXPECTED_COLUMN_COUNT:
        raise Mbp10ContractError(
            f"MBP-10 schema must contain exactly {EXPECTED_COLUMN_COUNT} columns; "
            f"found {len(schema)}"
        )

    for index, (actual, expected) in enumerate(zip(schema, _EXPECTED_SCHEMA)):
        if actual.name != expected.name:
            raise Mbp10ContractError(
                f"column {index} must be named {expected.name!r}; found {actual.name!r}"
            )
        if actual.type != expected.type:
            raise Mbp10ContractError(
                f"column {actual.name!r} must have type {expected.type}; found {actual.type}"
            )
        if actual.nullable != expected.nullable:
            raise Mbp10ContractError(
                f"column {actual.name!r} nullable must be {expected.nullable}; "
                f"found {actual.nullable}"
            )
        if actual.metadata != expected.metadata:
            raise Mbp10ContractError(f"column {actual.name!r} must not carry field-level metadata")


def validate_mbp10_metadata(
    metadata: Mapping[object, object] | None,
) -> Mbp10ContractMetadata:
    """Validate DBN identity and the fixed-width integer price encoding."""

    values = _normalize_metadata(metadata)
    required = {
        "dbn.dataset": DATASET_NAME,
        "dbn.schema": SCHEMA_NAME,
        "mbo_mbp10.price_encoding": PRICE_ENCODING,
        "mbo_mbp10.price_scale": PRICE_SCALE_TEXT,
        "mbo_mbp10.undefined_price": str(UNDEFINED_PRICE),
    }
    for key, expected in required.items():
        actual = values.get(key)
        if actual != expected:
            raise Mbp10ContractError(
                f"schema metadata {key!r} must be {expected!r}; found {actual!r}"
            )

    version_text = values.get("dbn.version")
    try:
        dbn_version = int(version_text) if version_text is not None else 0
    except ValueError as exc:
        raise Mbp10ContractError("schema metadata 'dbn.version' must be an integer") from exc
    if dbn_version <= 0:
        raise Mbp10ContractError("schema metadata 'dbn.version' must be positive")

    try:
        scale = Decimal(values["mbo_mbp10.price_scale"])
        undefined_price = int(values["mbo_mbp10.undefined_price"])
    except (InvalidOperation, ValueError) as exc:
        raise Mbp10ContractError("invalid fixed-price metadata") from exc

    payload = values.get("dbn.metadata")
    if payload is None:
        raise Mbp10ContractError("schema metadata 'dbn.metadata' is missing")
    dbn_metadata = decode_dbn_metadata(payload)
    if dbn_metadata.get("dataset") != DATASET_NAME:
        raise Mbp10ContractError(
            f"dbn.metadata dataset must be {DATASET_NAME!r}; found {dbn_metadata.get('dataset')!r}"
        )
    if dbn_metadata.get("schema") != SCHEMA_NAME:
        raise Mbp10ContractError(
            f"dbn.metadata schema must be {SCHEMA_NAME!r}; found {dbn_metadata.get('schema')!r}"
        )
    if dbn_metadata.get("version") != dbn_version:
        raise Mbp10ContractError("dbn.metadata version must match schema metadata 'dbn.version'")
    if dbn_metadata.get("stype_out") != "instrument_id":
        raise Mbp10ContractError("dbn.metadata stype_out must be 'instrument_id'")

    return Mbp10ContractMetadata(
        dataset=DATASET_NAME,
        schema_name=SCHEMA_NAME,
        dbn_version=dbn_version,
        price_encoding=PRICE_ENCODING,
        price_scale=scale,
        undefined_price=undefined_price,
    )


def validate_mbp10_contract(schema: pa.Schema) -> Mbp10ContractMetadata:
    """Validate the complete raw Arrow schema and return normalized metadata."""

    validate_mbp10_fields(schema)
    return validate_mbp10_metadata(schema.metadata)


def compute_schema_fingerprint(
    schema: pa.Schema,
    contract: Mbp10ContractMetadata | None = None,
) -> str:
    """Hash ordered fields and immutable DBN values, excluding daily metadata."""

    normalized_contract = contract or validate_mbp10_contract(schema)
    payload = {
        "contract": normalized_contract.as_dict(),
        "fields": [
            {
                "name": field.name,
                "nullable": field.nullable,
                "type": str(field.type),
            }
            for field in schema
        ],
    }
    canonical = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()
