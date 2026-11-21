from dataclasses import dataclass
from sqlite3 import Connection

from intake.origin import Origin
from issue.package import Receipt
from issue.schema import PUBLICATIONS_DDL, migrate_exchange_schema


@dataclass(frozen=True, slots=True)
class Publication:
    receipt: Receipt
    package: bytes


def initialize_publication_schema(db: Connection) -> None:
    migrate_exchange_schema(db)
    db.execute(PUBLICATIONS_DDL)


def allocate_publication_id(db: Connection) -> tuple[int, str]:
    sequence = db.execute('SELECT COALESCE(MAX(sequence), 0) + 1 FROM publications').fetchone()[0]
    return sequence, f'P{sequence:08d}'


def insert_publication(db: Connection, sequence: int, publication: Publication) -> None:
    result = publication.receipt
    db.execute('''
        INSERT INTO publications (sequence, publication_id, request_id, batch_id, record_id, origin, package)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (
        sequence, result.publication_id, result.request_id, result.batch_id,
        result.record_id, result.origin.text, publication.package,
    ))


_PUBLICATION_SELECT = '''
    SELECT publication_id, request_id, batch_id, record_id, origin, package FROM publications
'''


def _publication(row: tuple | None) -> Publication | None:
    if row is None:
        return None
    publication_id, request_id, batch_id, record_id, origin, package = row
    return Publication(Receipt(publication_id, request_id, batch_id, record_id, Origin(origin)), package)


def find_publication_by_request(db: Connection, request_id: str) -> Publication | None:
    row = db.execute(_PUBLICATION_SELECT + 'WHERE request_id = ?', (request_id,)).fetchone()
    return _publication(row)


def load_publication(db: Connection, publication_id: str) -> Publication | None:
    row = db.execute(_PUBLICATION_SELECT + 'WHERE publication_id = ?', (publication_id,)).fetchone()
    return _publication(row)
