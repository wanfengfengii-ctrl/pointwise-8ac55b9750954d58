from collections.abc import Callable
from sqlite3 import Connection

from freeze.batch_query import get_sealed_entry
from issue.delivery import deliver_record
from issue.package import Receipt
from issue.publication_store import find_publication_by_request
from request.feasibility import jointly_fulfillable
from request.request_query import require_request
from request.request_record import RequestStatus, mark_request_published
from request.reservation_query import available_originals, pending_candidates
from station_db import Conflict, IntegrityFailure, write_transaction


def publish(db: Connection, request_id: str, *, before_commit: Callable[[], None] | None = None) -> Receipt:
    with write_transaction(db):
        ordinary = require_request(db, request_id)
        previous = find_publication_by_request(db, ordinary.request_id)
        if previous is not None:
            return previous.receipt
        if ordinary.status != RequestStatus.PENDING:
            raise IntegrityFailure(f'published request {request_id!r} has no stored receipt')

        batch_id, record_id = ordinary.batch_id, ordinary.record_id
        get_sealed_entry(db, batch_id, record_id)

        # A directed request may claim an original only if every pending
        # reservation in the batch can still be fulfilled without it.
        pending = pending_candidates(db, batch_id)
        if pending and not jointly_fulfillable(pending, available_originals(db, batch_id) - {record_id}):
            raise Conflict(
                f'original {record_id!r} in batch {batch_id!r} is required by a pending reservation'
            )

        result = deliver_record(db, ordinary.request_id, batch_id, record_id)
        mark_request_published(db, ordinary.request_id)
        if before_commit is not None:
            before_commit()
        return result
