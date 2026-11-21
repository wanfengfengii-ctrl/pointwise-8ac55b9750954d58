from sqlite3 import Connection

from freeze.manifest import ManifestEntry, SealedManifest
from freeze.seal_store import load_sealed_digest, load_sealed_entry, load_sealed_manifest
from station_db import NotFound, require_text


def manifest(db: Connection, batch_id: str) -> SealedManifest:
    batch_id = require_text(batch_id, 'batch id')
    sealed = load_sealed_manifest(db, batch_id)
    if sealed is None:
        raise NotFound(f'unknown sealed batch {batch_id!r}')
    return sealed


def get_sealed_entry(db: Connection, batch_id: str, record_id: str) -> ManifestEntry:
    batch_id = require_text(batch_id, 'batch id')
    record_id = require_text(record_id, 'record id')
    entry = load_sealed_entry(db, batch_id, record_id)
    if entry is None:
        raise NotFound(f'record {record_id!r} is absent in sealed batch {batch_id!r}')
    return entry


def sealed_digest(db: Connection, batch_id: str) -> str:
    digest = load_sealed_digest(db, batch_id)
    if digest is None:
        raise NotFound(f'unknown sealed batch {batch_id!r}')
    return digest
