from sqlite3 import Connection

from station_db import Conflict


def initialize_consumption_schema(db: Connection) -> None:
    db.execute('''
        CREATE TABLE IF NOT EXISTS consumed_records (
            batch_id TEXT NOT NULL,
            record_id TEXT NOT NULL,
            request_id TEXT NOT NULL REFERENCES ordinary_requests(request_id),
            PRIMARY KEY (batch_id, record_id),
            FOREIGN KEY (batch_id, record_id) REFERENCES sealed_entries(batch_id, record_id)
        )
    ''')


def claim_record(db: Connection, batch_id: str, record_id: str, request_id: str) -> None:
    inserted = db.execute('''
        INSERT INTO consumed_records (batch_id, record_id, request_id) VALUES (?, ?, ?)
        ON CONFLICT (batch_id, record_id) DO NOTHING
    ''', (batch_id, record_id, request_id))
    if inserted.rowcount != 1:
        raise Conflict(f'original {record_id!r} in batch {batch_id!r} is already published')
