import base64
import binascii
from dataclasses import dataclass
import json
from string import hexdigits

from freeze.manifest import ManifestEntry, canonical_json
from intake.device_profile import validate_frame_size
from intake.origin import Origin, parse_origin
from station_db import InputError, require_text


_FORMAT = 'waveform-v1'
_HEADER_FIELDS = {
    'format', 'publication_id', 'request_id', 'batch_id', 'record_id', 'origin',
    'batch_digest', 'entry', 'frame_b64',
}
_ENTRY_FIELDS = {'batch_id', 'position', 'record_id', 'origin', 'length', 'digest', 'peak'}


@dataclass(frozen=True, slots=True)
class Receipt:
    publication_id: str
    request_id: str
    batch_id: str
    record_id: str
    origin: Origin


@dataclass(frozen=True, slots=True)
class OrdinaryPackage:
    receipt: Receipt
    batch_digest: str
    entry: ManifestEntry
    frame: bytes


def encode_package(package: OrdinaryPackage) -> bytes:
    receipt = package.receipt
    return canonical_json({
        'format': _FORMAT,
        'publication_id': receipt.publication_id,
        'request_id': receipt.request_id,
        'batch_id': receipt.batch_id,
        'record_id': receipt.record_id,
        'origin': receipt.origin.text,
        'batch_digest': package.batch_digest,
        'entry': {'batch_id': package.entry.batch_id, **package.entry.evidence()},
        'frame_b64': base64.b64encode(package.frame).decode('ascii'),
    })


def _unique_fields(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise InputError(f'duplicate package field {key!r}')
        result[key] = value
    return result


def _object(value: object, fields: set[str], label: str) -> dict[str, object]:
    if not isinstance(value, dict) or value.keys() != fields:
        raise InputError(f'wrong fields in {label}')
    return value


def _digest(value: object, label: str) -> str:
    text = require_text(value, label)
    if len(text) != 64 or any(char not in hexdigits for char in text) or text != text.lower():
        raise InputError(f'{label} must be a lowercase SHA-256 digest')
    return text


def _nonnegative(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise InputError(f'{label} must be a nonnegative integer')
    return value


def _decode_entry(value: object) -> ManifestEntry:
    entry = _object(value, _ENTRY_FIELDS, 'sealed entry')
    length = _nonnegative(entry['length'], 'frame length')
    validate_frame_size(length)
    peak = _nonnegative(entry['peak'], 'peak')
    if peak > 32768:
        raise InputError('peak exceeds the instrument range')
    return ManifestEntry(
        batch_id=require_text(entry['batch_id'], 'entry batch id'),
        position=_nonnegative(entry['position'], 'position'),
        record_id=require_text(entry['record_id'], 'entry record id'),
        origin=parse_origin(entry['origin']),
        length=length,
        digest=_digest(entry['digest'], 'frame digest'),
        peak=peak,
    )


def decode_package(payload: bytes) -> OrdinaryPackage:
    if not isinstance(payload, bytes):
        raise InputError('an ordinary package must be supplied as bytes')
    try:
        document = json.loads(payload.decode('utf-8'), object_pairs_hook=_unique_fields)
    except (ValueError, RecursionError) as exc:
        raise InputError('invalid package JSON') from exc
    header = _object(document, _HEADER_FIELDS, 'package header')
    if header['format'] != _FORMAT:
        raise InputError('unknown package format')
    receipt = Receipt(
        publication_id=require_text(header['publication_id'], 'publication id'),
        request_id=require_text(header['request_id'], 'request id'),
        batch_id=require_text(header['batch_id'], 'batch id'),
        record_id=require_text(header['record_id'], 'record id'),
        origin=parse_origin(header['origin']),
    )
    encoded = require_text(header['frame_b64'], 'base64 frame')
    try:
        frame = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise InputError('invalid base64 waveform') from exc
    return OrdinaryPackage(
        receipt=receipt,
        batch_digest=_digest(header['batch_digest'], 'batch digest'),
        entry=_decode_entry(header['entry']),
        frame=frame,
    )
