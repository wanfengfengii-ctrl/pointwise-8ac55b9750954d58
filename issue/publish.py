from collections.abc import Callable
from sqlite3 import Connection

from freeze.batch_query import get_sealed_entry, sealed_digest
from freeze.manifest import frame_digest
from intake.raw_store import load_raw_frame
from issue.consumption import claim_record
from issue.package import OrdinaryPackage, Receipt, encode_package
from issue.publication_store import (
    Publication, allocate_publication_id, find_publication_by_request, insert_publication,
)
from request.request_query import require_request
from request.request_record import RequestStatus, mark_request_published
from station_db import IntegrityFailure, write_transaction


def publish(db: Connection, request_id: str, *, before_commit: Callable[[], None] | None = None) -> Receipt:
    with write_transaction(db):
        ordinary = require_request(db, request_id)
        previous = find_publication_by_request(db, ordinary.request_id)
        if previous is not None:
            return previous.receipt
        if ordinary.status != RequestStatus.PENDING:
            raise IntegrityFailure(f'published request {request_id!r} has no stored receipt')

        batch_id, record_id = ordinary.batch_id, ordinary.record_id
        entry = get_sealed_entry(db, batch_id, record_id)
        raw = load_raw_frame(db, batch_id, record_id)
        if raw is None or raw.record_id != entry.record_id or raw.origin != entry.origin:
            raise IntegrityFailure('source record identity or origin does not match the sealed entry')
        if (not isinstance(raw.frame, bytes) or len(raw.frame) != entry.length
                or frame_digest(raw.frame) != entry.digest):
            raise IntegrityFailure('source waveform does not match the sealed entry')

        claim_record(db, batch_id, record_id, ordinary.request_id)
        sequence, publication_id = allocate_publication_id(db)
        result = Receipt(publication_id, ordinary.request_id, batch_id, record_id, raw.origin)
        package = OrdinaryPackage(result, sealed_digest(db, batch_id), entry, raw.frame)
        insert_publication(db, sequence, Publication(result, encode_package(package)))
        mark_request_published(db, ordinary.request_id)
        if before_commit is not None:
            before_commit()
        return result
