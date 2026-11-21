from dataclasses import dataclass
from hashlib import sha256
import json

from intake.origin import Origin


@dataclass(frozen=True, slots=True)
class ManifestEntry:
    batch_id: str
    position: int
    record_id: str
    origin: Origin
    length: int
    digest: str
    peak: int

    def evidence(self) -> dict[str, str | int]:
        return {
            'position': self.position,
            'record_id': self.record_id,
            'origin': self.origin.text,
            'length': self.length,
            'digest': self.digest,
            'peak': self.peak,
        }


@dataclass(frozen=True, slots=True)
class SealedManifest:
    batch_id: str
    digest: str
    entries: tuple[ManifestEntry, ...]


def canonical_json(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode('utf-8')


def frame_digest(frame: bytes) -> str:
    return sha256(frame).hexdigest()


def manifest_digest(batch_id: str, entries: tuple[ManifestEntry, ...]) -> str:
    payload = {'batch_id': batch_id, 'entries': [entry.evidence() for entry in entries]}
    return sha256(canonical_json(payload)).hexdigest()
