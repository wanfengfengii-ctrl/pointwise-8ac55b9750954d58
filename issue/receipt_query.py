from sqlite3 import Connection

from issue.publication_store import Publication, load_publication
from station_db import NotFound, require_text


def receipt(db: Connection, publication_id: str) -> Publication:
    publication_id = require_text(publication_id, 'publication id')
    result = load_publication(db, publication_id)
    if result is None:
        raise NotFound(f'unknown publication {publication_id!r}')
    return result
