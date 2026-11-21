from dataclasses import dataclass

from intake.origin import Origin
from receive.verify import VerifiedPackage


@dataclass(frozen=True, slots=True)
class AcceptedRecord:
    acceptance_id: str
    publication_id: str
    request_id: str
    batch_id: str
    record_id: str
    origin: Origin
    peak: int
    package: bytes


def build_accepted_record(acceptance_id: str, verified: VerifiedPackage) -> AcceptedRecord:
    receipt = verified.package.receipt
    return AcceptedRecord(
        acceptance_id=acceptance_id,
        publication_id=receipt.publication_id,
        request_id=receipt.request_id,
        batch_id=receipt.batch_id,
        record_id=receipt.record_id,
        origin=receipt.origin,
        peak=verified.peak,
        package=verified.payload,
    )
