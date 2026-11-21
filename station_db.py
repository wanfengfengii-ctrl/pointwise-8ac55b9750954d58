from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
import sqlite3


class InputError(ValueError):
    """An input does not meet the station's format rules."""


class NotFound(LookupError):
    """An identity is absent in the required scope."""


class Conflict(ValueError):
    """An operation would change a binding or consume a claimed original."""


class IntegrityFailure(ValueError):
    """The bytes or origin disagree with their recorded evidence."""


def require_text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise InputError(f'{label} must be nonblank text')
    try:
        value.encode('utf-8')
    except UnicodeEncodeError as exc:
        raise InputError(f'{label} must be valid UTF-8 text') from exc
    return value


def connect(path: str | Path) -> sqlite3.Connection:
    db = sqlite3.connect(path, isolation_level=None)
    try:
        db.execute('PRAGMA foreign_keys = ON')
    except BaseException:
        db.close()
        raise
    return db


@contextmanager
def write_transaction(db: sqlite3.Connection) -> Iterator[None]:
    # Take the write lock before reading state used to decide a write.
    db.execute('BEGIN IMMEDIATE')
    try:
        yield
        db.commit()
    except BaseException:
        db.rollback()
        raise
