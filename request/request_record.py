from dataclasses import dataclass
from enum import StrEnum
from sqlite3 import Connection

from station_db import IntegrityFailure


class RequestStatus(StrEnum):
    PENDING = 'pending'
    PUBLISHED = 'published'


@dataclass(frozen=True, slots=True)
class OrdinaryRequest:
    request_id: str
    batch_id: str
    record_id: str
    status: RequestStatus


def initialize_request_schema(db: Connection) -> None:
    db.execute('''
        CREATE TABLE IF NOT EXISTS ordinary_requests (
            request_id TEXT PRIMARY KEY NOT NULL,
            batch_id TEXT NOT NULL,
            record_id TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('pending', 'published')),
            FOREIGN KEY (batch_id, record_id) REFERENCES sealed_entries(batch_id, record_id)
        )
    ''')


def insert_request(db: Connection, request: OrdinaryRequest) -> None:
    db.execute('''
        INSERT INTO ordinary_requests (request_id, batch_id, record_id, status)
        VALUES (?, ?, ?, ?)
    ''', (request.request_id, request.batch_id, request.record_id, request.status.value))


def load_request(db: Connection, request_id: str) -> OrdinaryRequest | None:
    row = db.execute('''
        SELECT request_id, batch_id, record_id, status
        FROM ordinary_requests WHERE request_id = ?
    ''', (request_id,)).fetchone()
    if row is None:
        return None
    request_id, batch_id, record_id, status = row
    return OrdinaryRequest(request_id, batch_id, record_id, RequestStatus(status))


def mark_request_published(db: Connection, request_id: str) -> None:
    changed = db.execute('''
        UPDATE ordinary_requests SET status = ? WHERE request_id = ? AND status = ?
    ''', (RequestStatus.PUBLISHED.value, request_id, RequestStatus.PENDING.value))
    if changed.rowcount != 1:
        raise IntegrityFailure(f'request {request_id!r} is missing its pending state')
