from sqlite3 import Connection

from receive.acceptance_store import load_acceptance
from receive.accepted_record import AcceptedRecord
from station_db import NotFound, require_text


def accepted(db: Connection, acceptance_id: str) -> AcceptedRecord:
    acceptance_id = require_text(acceptance_id, 'acceptance id')
    record = load_acceptance(db, acceptance_id)
    if record is None:
        raise NotFound(f'unknown acceptance {acceptance_id!r}')
    return record
