from sqlite3 import Connection

from issue.consumption import consumed_record_ids
from request.reservation_record import pending_reservations


def available_originals(db: Connection, batch_id: str) -> frozenset[str]:
    sealed = {
        row[0] for row in db.execute(
            'SELECT record_id FROM sealed_entries WHERE batch_id = ?', (batch_id,)
        )
    }
    return frozenset(sealed - consumed_record_ids(db, batch_id))


def pending_candidates(db: Connection, batch_id: str) -> dict[str, frozenset[str]]:
    return {order.order_id: order.candidates for order in pending_reservations(db, batch_id)}
