from collections.abc import Iterable
from sqlite3 import Connection

from intake.device_profile import validate_frame_size
from intake.origin import parse_origin
from intake.raw_store import RawBatch, RawFrame, insert_raw_batch, load_raw_batch
from station_db import Conflict, InputError, require_text, write_transaction


def ingest(db: Connection, batch_id: str, frames: Iterable[tuple[str, str, bytes]]) -> RawBatch:
    batch_id = require_text(batch_id, 'batch id')
    try:
        incoming = iter(frames)
    except TypeError as exc:
        raise InputError('frames must be an ordered iterable') from exc

    validated = []
    seen = set()
    for position, value in enumerate(incoming):
        if not isinstance(value, (tuple, list)) or len(value) != 3:
            raise InputError(f'frame {position} must give record id, origin and bytes')
        record_id, origin, frame = value
        record_id = require_text(record_id, 'record id')
        origin = parse_origin(origin)
        if record_id in seen:
            raise InputError(f'duplicate record id {record_id!r} in batch {batch_id!r}')
        if not isinstance(frame, bytes):
            raise InputError(f'frame {record_id!r} must be bytes')
        validate_frame_size(len(frame))
        validated.append(RawFrame(record_id, origin, frame))
        seen.add(record_id)

    batch = RawBatch(batch_id, tuple(validated))
    with write_transaction(db):
        previous = load_raw_batch(db, batch_id)
        if previous is not None:
            if previous != batch:
                raise Conflict(f'batch {batch_id!r} is already bound to different frames')
            return previous
        insert_raw_batch(db, batch)
        return batch
