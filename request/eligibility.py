from sqlite3 import Connection

from freeze.batch_query import get_sealed_entry
from freeze.manifest import ManifestEntry


def require_sealed_record(db: Connection, batch_id: str, record_id: str) -> ManifestEntry:
    return get_sealed_entry(db, batch_id, record_id)
