from dataclasses import dataclass

from station_db import require_text


@dataclass(frozen=True, slots=True)
class Origin:
    text: str

    def __post_init__(self) -> None:
        require_text(self.text, 'origin')


def parse_origin(value: object) -> Origin:
    return Origin(require_text(value, 'origin'))
