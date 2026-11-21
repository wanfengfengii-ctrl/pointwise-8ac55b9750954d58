"""Shared exchange-table schema and migration away from request-only identities.

Consumptions and publications used to require their ``request_id`` to name an
``ordinary_requests`` row.  Reservation orders now share that number space but
live in their own table, so the foreign key must be dropped while every other
constraint stays in place.
"""

from sqlite3 import Connection


CONSUMED_RECORDS_DDL = '''
    CREATE TABLE IF NOT EXISTS consumed_records (
        batch_id TEXT NOT NULL,
        record_id TEXT NOT NULL,
        request_id TEXT NOT NULL,
        PRIMARY KEY (batch_id, record_id),
        FOREIGN KEY (batch_id, record_id) REFERENCES sealed_entries(batch_id, record_id)
    )
'''

PUBLICATIONS_DDL = '''
    CREATE TABLE IF NOT EXISTS publications (
        sequence INTEGER PRIMARY KEY CHECK (sequence > 0),
        publication_id TEXT NOT NULL UNIQUE,
        request_id TEXT NOT NULL UNIQUE,
        batch_id TEXT NOT NULL,
        record_id TEXT NOT NULL,
        origin TEXT NOT NULL,
        package BLOB NOT NULL,
        FOREIGN KEY (batch_id, record_id) REFERENCES consumed_records(batch_id, record_id)
    )
'''

_LEGACY_PUBLICATIONS = '_publications_v1'
_LEGACY_CONSUMPTIONS = '_consumed_records_v1'


def _table_exists(db: Connection, table: str) -> bool:
    row = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)
    ).fetchone()
    return row is not None


def _foreign_targets(db: Connection, table: str) -> set[str]:
    return {row[2] for row in db.execute(f"PRAGMA foreign_key_list('{table}')")}


def _has_request_foreign_key(db: Connection, table: str) -> bool:
    return _table_exists(db, table) and 'ordinary_requests' in _foreign_targets(db, table)


def migrate_exchange_schema(db: Connection) -> None:
    """Drop legacy request-table foreign keys from exchange tables in place.

    Only the two foreign keys change; rows, columns, checks and the
    consumption-to-publication relationship are preserved.  Both tables are
    rebuilt together because renaming one side rewrites the other side's
    reference to it.
    """
    publications = _table_exists(db, 'publications')
    consumptions = _table_exists(db, 'consumed_records')
    legacy = (
        (publications and _has_request_foreign_key(db, 'publications'))
        or (consumptions and _has_request_foreign_key(db, 'consumed_records'))
    )
    if not legacy:
        return

    if publications:
        db.execute(f'ALTER TABLE publications RENAME TO {_LEGACY_PUBLICATIONS}')
    if consumptions:
        db.execute(f'ALTER TABLE consumed_records RENAME TO {_LEGACY_CONSUMPTIONS}')

    if consumptions:
        db.execute(CONSUMED_RECORDS_DDL)
        db.execute(f'''
            INSERT INTO consumed_records (batch_id, record_id, request_id)
            SELECT batch_id, record_id, request_id FROM {_LEGACY_CONSUMPTIONS}
        ''')
    if publications:
        db.execute(PUBLICATIONS_DDL)
        db.execute(f'''
            INSERT INTO publications (
                sequence, publication_id, request_id, batch_id, record_id, origin, package
            )
            SELECT sequence, publication_id, request_id, batch_id, record_id, origin, package
            FROM {_LEGACY_PUBLICATIONS}
        ''')

    # Drop the publications copy first so the legacy consumption table is no
    # longer referenced when it is removed.
    if publications:
        db.execute(f'DROP TABLE {_LEGACY_PUBLICATIONS}')
    if consumptions:
        db.execute(f'DROP TABLE {_LEGACY_CONSUMPTIONS}')
