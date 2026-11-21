from collections.abc import Iterable
from sqlite3 import Connection

from freeze.batch_query import get_sealed_entry
from freeze.seal_store import load_sealed_manifest
from request.feasibility import jointly_fulfillable
from request.request_query import find_request
from request.reservation_query import available_originals, pending_candidates
from request.reservation_record import insert_reservation, load_reservation
from station_db import Conflict, InputError, NotFound, require_text, write_transaction

RESERVED = 'reserved'
UNAVAILABLE = 'unavailable'


def _candidate_ids(eligible_ids: object) -> frozenset[str]:
    if isinstance(eligible_ids, (str, bytes)) or not isinstance(eligible_ids, Iterable):
        raise InputError('eligible ids must be an iterable of record ids, not text')
    try:
        incoming = iter(eligible_ids)
    except TypeError as exc:
        raise InputError('eligible ids must be an iterable of record ids') from exc

    seen: set[str] = set()
    for value in incoming:
        seen.add(require_text(value, 'eligible record id'))
    if not seen:
        raise InputError('eligible ids must not be empty')
    return frozenset(seen)


def reserve(
    db: Connection, batch_id: str, order_id: str, eligible_ids: object,
) -> str:
    batch_id = require_text(batch_id, 'batch id')
    order_id = require_text(order_id, 'order id')
    candidates = _candidate_ids(eligible_ids)

    with write_transaction(db):
        existing = load_reservation(db, order_id)
        if existing is not None:
            if existing.batch_id != batch_id or existing.candidates != candidates:
                raise Conflict(f'reservation {order_id!r} is bound to another batch or candidate set')
            return RESERVED
        if find_request(db, order_id) is not None:
            raise Conflict(f'order id {order_id!r} is already used by an ordinary request')

        if load_sealed_manifest(db, batch_id) is None:
            raise NotFound(f'unknown sealed batch {batch_id!r}')
        for record_id in candidates:
            get_sealed_entry(db, batch_id, record_id)

        pending = pending_candidates(db, batch_id)
        pending[order_id] = candidates
        if not jointly_fulfillable(pending, available_originals(db, batch_id)):
            return UNAVAILABLE

        insert_reservation(db, order_id, batch_id, tuple(sorted(candidates)))
        return RESERVED
