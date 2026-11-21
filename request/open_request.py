from sqlite3 import Connection

from request.eligibility import require_sealed_record
from request.request_query import find_request
from request.request_record import OrdinaryRequest, RequestStatus, insert_request
from station_db import Conflict, require_text, write_transaction


def request(db: Connection, batch_id: str, request_id: str, record_id: str) -> OrdinaryRequest:
    batch_id = require_text(batch_id, 'batch id')
    request_id = require_text(request_id, 'request id')
    record_id = require_text(record_id, 'record id')
    with write_transaction(db):
        previous = find_request(db, request_id)
        if previous is not None:
            if (previous.batch_id, previous.record_id) != (batch_id, record_id):
                raise Conflict(f'request {request_id!r} is already bound to another original')
            return previous

        entry = require_sealed_record(db, batch_id, record_id)
        pending = OrdinaryRequest(request_id, entry.batch_id, entry.record_id, RequestStatus.PENDING)
        insert_request(db, pending)
        return pending
