from sqlite3 import Connection

from intake.origin import Origin
from receive.accepted_record import AcceptedRecord


def initialize_acceptance_schema(db: Connection) -> None:
    db.execute('''
        CREATE TABLE IF NOT EXISTS acceptances (
            sequence INTEGER PRIMARY KEY CHECK (sequence > 0),
            acceptance_id TEXT NOT NULL UNIQUE,
            publication_id TEXT NOT NULL UNIQUE,
            request_id TEXT NOT NULL,
            batch_id TEXT NOT NULL,
            record_id TEXT NOT NULL,
            origin TEXT NOT NULL,
            peak INTEGER NOT NULL CHECK (peak BETWEEN 0 AND 32768),
            package BLOB NOT NULL
        )
    ''')


def allocate_acceptance_id(db: Connection) -> tuple[int, str]:
    sequence = db.execute('SELECT COALESCE(MAX(sequence), 0) + 1 FROM acceptances').fetchone()[0]
    return sequence, f'A{sequence:08d}'


def insert_acceptance(db: Connection, sequence: int, record: AcceptedRecord) -> None:
    db.execute('''
        INSERT INTO acceptances (
            sequence, acceptance_id, publication_id, request_id, batch_id, record_id, origin, peak, package
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        sequence, record.acceptance_id, record.publication_id, record.request_id,
        record.batch_id, record.record_id, record.origin.text, record.peak, record.package,
    ))


_ACCEPTANCE_SELECT = '''
    SELECT acceptance_id, publication_id, request_id, batch_id, record_id, origin, peak, package
    FROM acceptances
'''


def _accepted_record(row: tuple | None) -> AcceptedRecord | None:
    if row is None:
        return None
    acceptance_id, publication_id, request_id, batch_id, record_id, origin, peak, package = row
    return AcceptedRecord(acceptance_id, publication_id, request_id, batch_id, record_id, Origin(origin), peak, package)


def find_acceptance_by_publication(db: Connection, publication_id: str) -> AcceptedRecord | None:
    row = db.execute(_ACCEPTANCE_SELECT + 'WHERE publication_id = ?', (publication_id,)).fetchone()
    return _accepted_record(row)


def load_acceptance(db: Connection, acceptance_id: str) -> AcceptedRecord | None:
    row = db.execute(_ACCEPTANCE_SELECT + 'WHERE acceptance_id = ?', (acceptance_id,)).fetchone()
    return _accepted_record(row)
