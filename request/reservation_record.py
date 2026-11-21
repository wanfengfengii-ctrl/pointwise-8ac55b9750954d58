from dataclasses import dataclass
from enum import StrEnum
from sqlite3 import Connection


class ReservationStatus(StrEnum):
    PENDING = 'pending'
    PUBLISHED = 'published'


@dataclass(frozen=True, slots=True)
class ReservedOrder:
    order_id: str
    batch_id: str
    candidate_ids: tuple[str, ...]
    status: ReservationStatus

    @property
    def candidates(self) -> frozenset[str]:
        return frozenset(self.candidate_ids)


def initialize_reservation_schema(db: Connection) -> None:
    db.execute('''
        CREATE TABLE IF NOT EXISTS reserved_orders (
            order_id TEXT PRIMARY KEY NOT NULL,
            batch_id TEXT NOT NULL REFERENCES sealed_batches(batch_id),
            status TEXT NOT NULL CHECK (status IN ('pending', 'published'))
        )
    ''')
    db.execute('''
        CREATE TABLE IF NOT EXISTS reserved_candidates (
            order_id TEXT NOT NULL REFERENCES reserved_orders(order_id),
            batch_id TEXT NOT NULL,
            record_id TEXT NOT NULL,
            PRIMARY KEY (order_id, record_id),
            FOREIGN KEY (batch_id, record_id) REFERENCES sealed_entries(batch_id, record_id)
        )
    ''')


def insert_reservation(
    db: Connection, order_id: str, batch_id: str, candidate_ids: tuple[str, ...],
) -> None:
    db.execute(
        "INSERT INTO reserved_orders (order_id, batch_id, status) VALUES (?, ?, 'pending')",
        (order_id, batch_id),
    )
    db.executemany(
        'INSERT INTO reserved_candidates (order_id, batch_id, record_id) VALUES (?, ?, ?)',
        ((order_id, batch_id, record_id) for record_id in candidate_ids),
    )


def load_reservation(db: Connection, order_id: str) -> ReservedOrder | None:
    row = db.execute(
        'SELECT order_id, batch_id, status FROM reserved_orders WHERE order_id = ?',
        (order_id,),
    ).fetchone()
    if row is None:
        return None
    order_id, batch_id, status = row
    candidates = db.execute(
        'SELECT record_id FROM reserved_candidates WHERE order_id = ? ORDER BY record_id',
        (order_id,),
    )
    return ReservedOrder(order_id, batch_id, tuple(row[0] for row in candidates), ReservationStatus(status))


def pending_reservations(db: Connection, batch_id: str) -> tuple[ReservedOrder, ...]:
    rows = db.execute('''
        SELECT order_id FROM reserved_orders
        WHERE batch_id = ? AND status = 'pending' ORDER BY order_id
    ''', (batch_id,)).fetchall()
    return tuple(load_reservation(db, row[0]) for row in rows)


def mark_reservation_published(db: Connection, order_id: str) -> None:
    changed = db.execute('''
        UPDATE reserved_orders SET status = 'published'
        WHERE order_id = ? AND status = 'pending'
    ''', (order_id,))
    if changed.rowcount != 1:
        raise RuntimeError(f'reservation {order_id!r} is missing its pending state')
