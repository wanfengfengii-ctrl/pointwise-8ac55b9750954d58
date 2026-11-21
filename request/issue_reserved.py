from collections.abc import Callable
from sqlite3 import Connection

from issue.delivery import deliver_record
from issue.package import Receipt
from issue.publication_store import find_publication_by_request
from request.feasibility import jointly_fulfillable
from request.reservation_query import available_originals, pending_candidates
from request.reservation_record import (
    ReservationStatus, load_reservation, mark_reservation_published,
)
from station_db import IntegrityFailure, NotFound, require_text, write_transaction


def issue_reserved(
    db: Connection, batch_id: str, order_id: str, *,
    before_commit: Callable[[], None] | None = None,
) -> Receipt:
    batch_id = require_text(batch_id, 'batch id')
    order_id = require_text(order_id, 'order id')

    with write_transaction(db):
        reservation = load_reservation(db, order_id)
        # An order id with no reservation in this batch, or one that names an
        # ordinary request, is simply not a reserved order here.
        if reservation is None or reservation.batch_id != batch_id:
            raise NotFound(f'order {order_id!r} is not reserved in batch {batch_id!r}')

        previous = find_publication_by_request(db, order_id)
        if previous is not None:
            return previous.receipt
        if reservation.status != ReservationStatus.PENDING:
            raise IntegrityFailure(f'published reservation {order_id!r} has no stored receipt')

        others = pending_candidates(db, batch_id)
        others.pop(order_id, None)
        available = available_originals(db, batch_id)

        # Prefer the lowest record id, but never one whose removal would make a
        # remaining pending order unfillable.
        chosen = next(
            (
                record_id for record_id in sorted(reservation.candidates)
                if record_id in available
                and jointly_fulfillable(others, available - {record_id})
            ),
            None,
        )
        if chosen is None:
            raise IntegrityFailure(f'reservation {order_id!r} can no longer be fulfilled')

        result = deliver_record(db, order_id, batch_id, chosen)
        mark_reservation_published(db, order_id)
        if before_commit is not None:
            before_commit()
        return result
