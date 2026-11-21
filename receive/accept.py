from sqlite3 import Connection

from receive.acceptance_store import allocate_acceptance_id, find_acceptance_by_publication, insert_acceptance
from receive.accepted_record import AcceptedRecord, build_accepted_record
from receive.verify import verify_package
from station_db import Conflict, write_transaction


def accept(db: Connection, payload: bytes) -> AcceptedRecord:
    verified = verify_package(payload)
    publication_id = verified.package.receipt.publication_id
    with write_transaction(db):
        previous = find_acceptance_by_publication(db, publication_id)
        if previous is not None:
            if previous.package != verified.payload:
                raise Conflict(f'publication {publication_id!r} is already bound to another package')
            return previous

        sequence, acceptance_id = allocate_acceptance_id(db)
        record = build_accepted_record(acceptance_id, verified)
        insert_acceptance(db, sequence, record)
        return record
