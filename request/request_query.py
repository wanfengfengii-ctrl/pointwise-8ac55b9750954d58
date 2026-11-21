from sqlite3 import Connection

from request.request_record import OrdinaryRequest, load_request
from station_db import NotFound, require_text


def find_request(db: Connection, request_id: str) -> OrdinaryRequest | None:
    return load_request(db, require_text(request_id, 'request id'))


def require_request(db: Connection, request_id: str) -> OrdinaryRequest:
    request = find_request(db, request_id)
    if request is None:
        raise NotFound(f'unknown request {request_id!r}')
    return request
