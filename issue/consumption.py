from sqlite3 import Connection

from issue.schema import CONSUMED_RECORDS_DDL, migrate_exchange_schema
from station_db import Conflict


def initialize_consumption_schema(db: Connection) -> None:
    migrate_exchange_schema(db)
    db.execute(CONSUMED_RECORDS_DDL)


def claim_record(db: Connection, batch_id: str, record_id: str, request_id: str) -> None:
    inserted = db.execute('''
        INSERT INTO consumed_records (batch_id, record_id, request_id) VALUES (?, ?, ?)
        ON CONFLICT (batch_id, record_id) DO NOTHING
    ''', (batch_id, record_id, request_id))
    if inserted.rowcount != 1:
        raise Conflict(f'original {record_id!r} in batch {batch_id!r} is already published')


def is_consumed(db: Connection, batch_id: str, record_id: str) -> bool:
    row = db.execute(
        'SELECT 1 FROM consumed_records WHERE batch_id = ? AND record_id = ?',
        (batch_id, record_id),
    ).fetchone()
    return row is not None


def consumed_record_ids(db: Connection, batch_id: str) -> set[str]:
    rows = db.execute(
        'SELECT record_id FROM consumed_records WHERE batch_id = ?', (batch_id,)
    )
    return {row[0] for row in rows}
