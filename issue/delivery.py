"""Shared first-issue mechanics for ordinary requests and reservation orders."""

from sqlite3 import Connection

from freeze.batch_query import get_sealed_entry, sealed_digest
from freeze.manifest import frame_digest
from intake.raw_store import load_raw_frame
from issue.consumption import claim_record
from issue.package import OrdinaryPackage, Receipt, encode_package
from issue.publication_store import Publication, allocate_publication_id, insert_publication
from station_db import IntegrityFailure


def deliver_record(db: Connection, order_id: str, batch_id: str, record_id: str) -> Receipt:
    """Claim one sealed original and persist its first publication and package."""
    entry = get_sealed_entry(db, batch_id, record_id)
    raw = load_raw_frame(db, batch_id, record_id)
    if raw is None or raw.record_id != entry.record_id or raw.origin != entry.origin:
        raise IntegrityFailure('source record identity or origin does not match the sealed entry')
    if (not isinstance(raw.frame, bytes) or len(raw.frame) != entry.length
            or frame_digest(raw.frame) != entry.digest):
        raise IntegrityFailure('source waveform does not match the sealed entry')

    claim_record(db, batch_id, record_id, order_id)
    sequence, publication_id = allocate_publication_id(db)
    result = Receipt(publication_id, order_id, batch_id, record_id, raw.origin)
    package = OrdinaryPackage(result, sealed_digest(db, batch_id), entry, raw.frame)
    insert_publication(db, sequence, Publication(result, encode_package(package)))
    return result
