from sqlite3 import Connection

from freeze.manifest import ManifestEntry, SealedManifest
from intake.origin import Origin


def initialize_seal_schema(db: Connection) -> None:
    db.execute('''
        CREATE TABLE IF NOT EXISTS sealed_batches (
            batch_id TEXT PRIMARY KEY NOT NULL REFERENCES raw_batches(batch_id),
            digest TEXT NOT NULL
        )
    ''')
    db.execute('''
        CREATE TABLE IF NOT EXISTS sealed_entries (
            batch_id TEXT NOT NULL REFERENCES sealed_batches(batch_id),
            position INTEGER NOT NULL CHECK (position >= 0),
            record_id TEXT NOT NULL,
            origin TEXT NOT NULL,
            length INTEGER NOT NULL CHECK (length > 0 AND length % 2 = 0),
            digest TEXT NOT NULL,
            peak INTEGER NOT NULL CHECK (peak BETWEEN 0 AND 32768),
            PRIMARY KEY (batch_id, record_id),
            UNIQUE (batch_id, position),
            FOREIGN KEY (batch_id, record_id) REFERENCES raw_frames(batch_id, record_id)
        )
    ''')


def insert_sealed_manifest(db: Connection, sealed: SealedManifest) -> None:
    db.execute('INSERT INTO sealed_batches (batch_id, digest) VALUES (?, ?)', (sealed.batch_id, sealed.digest))
    db.executemany('''
        INSERT INTO sealed_entries (batch_id, position, record_id, origin, length, digest, peak)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (
        (entry.batch_id, entry.position, entry.record_id, entry.origin.text, entry.length, entry.digest, entry.peak)
        for entry in sealed.entries
    ))


def _entry(row: tuple) -> ManifestEntry:
    batch_id, position, record_id, origin, length, digest, peak = row
    return ManifestEntry(batch_id, position, record_id, Origin(origin), length, digest, peak)


_ENTRY_SELECT = '''
    SELECT batch_id, position, record_id, origin, length, digest, peak
    FROM sealed_entries
'''


def load_sealed_manifest(db: Connection, batch_id: str) -> SealedManifest | None:
    header = db.execute('SELECT digest FROM sealed_batches WHERE batch_id = ?', (batch_id,)).fetchone()
    if header is None:
        return None
    rows = db.execute(_ENTRY_SELECT + 'WHERE batch_id = ? ORDER BY position', (batch_id,))
    return SealedManifest(batch_id, header[0], tuple(_entry(row) for row in rows))


def load_sealed_entry(db: Connection, batch_id: str, record_id: str) -> ManifestEntry | None:
    row = db.execute(_ENTRY_SELECT + 'WHERE batch_id = ? AND record_id = ?', (batch_id, record_id)).fetchone()
    return None if row is None else _entry(row)


def load_sealed_digest(db: Connection, batch_id: str) -> str | None:
    row = db.execute('SELECT digest FROM sealed_batches WHERE batch_id = ?', (batch_id,)).fetchone()
    return None if row is None else row[0]
