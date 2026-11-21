from sqlite3 import Connection

from freeze.manifest import ManifestEntry, SealedManifest, frame_digest, manifest_digest
from freeze.peak import peak_abs
from freeze.seal_store import insert_sealed_manifest, load_sealed_manifest
from intake.frame_codec import decode_frame
from intake.raw_store import load_raw_batch
from station_db import NotFound, require_text, write_transaction


def seal(db: Connection, batch_id: str) -> SealedManifest:
    batch_id = require_text(batch_id, 'batch id')
    with write_transaction(db):
        previous = load_sealed_manifest(db, batch_id)
        if previous is not None:
            return previous
        batch = load_raw_batch(db, batch_id)
        if batch is None:
            raise NotFound(f'unknown imported batch {batch_id!r}')

        entries = []
        for position, raw in enumerate(batch.frames):
            samples = decode_frame(raw.frame)
            entries.append(ManifestEntry(
                batch_id=batch_id,
                position=position,
                record_id=raw.record_id,
                origin=raw.origin,
                length=len(raw.frame),
                digest=frame_digest(raw.frame),
                peak=peak_abs(samples),
            ))
        ordered = tuple(entries)
        sealed = SealedManifest(batch_id, manifest_digest(batch_id, ordered), ordered)
        insert_sealed_manifest(db, sealed)
        return sealed
