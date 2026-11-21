from dataclasses import dataclass

from freeze.manifest import frame_digest
from freeze.peak import peak_abs
from intake.frame_codec import decode_frame
from issue.package import OrdinaryPackage, decode_package
from station_db import IntegrityFailure


@dataclass(frozen=True, slots=True)
class VerifiedPackage:
    package: OrdinaryPackage
    payload: bytes
    peak: int


def verify_package(payload: bytes) -> VerifiedPackage:
    package = decode_package(payload)
    receipt, entry = package.receipt, package.entry
    if (receipt.batch_id, receipt.record_id, receipt.origin) != (entry.batch_id, entry.record_id, entry.origin):
        raise IntegrityFailure('package identity does not match the sealed entry')
    if len(package.frame) != entry.length:
        raise IntegrityFailure('frame length does not match the sealed entry')
    if frame_digest(package.frame) != entry.digest:
        raise IntegrityFailure('frame digest does not match the sealed entry')
    peak = peak_abs(decode_frame(package.frame))
    if peak != entry.peak:
        raise IntegrityFailure('recomputed peak does not match the sealed entry')
    return VerifiedPackage(package, payload, peak)
