from dataclasses import dataclass
from sqlite3 import Connection

from intake.origin import Origin


@dataclass(frozen=True, slots=True)
class RawFrame:
    record_id: str
    origin: Origin
    frame: bytes


@dataclass(frozen=True, slots=True)
class RawBatch:
    batch_id: str
    frames: tuple[RawFrame, ...]


def initialize_raw_schema(db: Connection) -> None:
    db.execute('''
        CREATE TABLE IF NOT EXISTS raw_batches (
            batch_id TEXT PRIMARY KEY NOT NULL
        )
    ''')
    db.execute('''
        CREATE TABLE IF NOT EXISTS raw_frames (
            batch_id TEXT NOT NULL REFERENCES raw_batches(batch_id),
            position INTEGER NOT NULL CHECK (position >= 0),
            record_id TEXT NOT NULL,
            origin TEXT NOT NULL,
            frame BLOB NOT NULL,
            PRIMARY KEY (batch_id, record_id),
            UNIQUE (batch_id, position)
        )
    ''')


def load_raw_batch(db: Connection, batch_id: str) -> RawBatch | None:
    header = db.execute('SELECT batch_id FROM raw_batches WHERE batch_id = ?', (batch_id,)).fetchone()
    if header is None:
        return None
    frames = db.execute('''
        SELECT record_id, origin, frame FROM raw_frames
        WHERE batch_id = ? ORDER BY position
    ''', (batch_id,))
    return RawBatch(batch_id, tuple(RawFrame(record, Origin(origin), frame) for record, origin, frame in frames))


def load_raw_frame(db: Connection, batch_id: str, record_id: str) -> RawFrame | None:
    row = db.execute('''
        SELECT record_id, origin, frame FROM raw_frames
        WHERE batch_id = ? AND record_id = ?
    ''', (batch_id, record_id)).fetchone()
    if row is None:
        return None
    record, origin, frame = row
    return RawFrame(record, Origin(origin), frame)


def insert_raw_batch(db: Connection, batch: RawBatch) -> None:
    db.execute('INSERT INTO raw_batches (batch_id) VALUES (?)', (batch.batch_id,))
    db.executemany('''
        INSERT INTO raw_frames (batch_id, position, record_id, origin, frame)
        VALUES (?, ?, ?, ?, ?)
    ''', (
        (batch.batch_id, position, raw.record_id, raw.origin.text, raw.frame)
        for position, raw in enumerate(batch.frames)
    ))
