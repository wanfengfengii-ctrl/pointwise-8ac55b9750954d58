from contextlib import closing
import sqlite3

from issue.package import decode_package
from issue.publication_store import find_publication_by_request
from request.request_query import require_request
from request.request_record import RequestStatus
from tests import StationTestCase
from wave_station import (
    Conflict, IntegrityFailure, NotFound, connect, ingest, manifest, publish, receipt, request, seal,
)


def _seal_one(source, batch_id, record_id='R01', origin='scope CH1'):
    ingest(source, batch_id, [(record_id, origin, bytes.fromhex('00800c00'))])
    return seal(source, batch_id)


def _signed_count(source):
    return source.execute('SELECT COUNT(*) FROM publications').fetchone()[0]


def _claimed_count(source):
    return source.execute('SELECT COUNT(*) FROM consumed_records').fetchone()[0]


class IssueTests(StationTestCase):
    def test_cross_batch_request_is_rejected(self):
        with closing(connect(self.source_path)) as source:
            a = _seal_one(source, 'A', 'A-ONLY')
            b = _seal_one(source, 'B', 'B-ONLY')
            with self.assertRaises(NotFound):
                request(source, 'A', 'Q', 'B-ONLY')
            with self.assertRaises(NotFound):
                require_request(source, 'Q')
            self.assertEqual(manifest(source, 'A'), a)
            self.assertEqual(manifest(source, 'B'), b)

    def test_request_replay_keeps_original_binding_and_state(self):
        with closing(connect(self.source_path)) as source:
            ingest(source, 'A', [('R01', 'CH1', b'\x01\x00'), ('R02', 'CH2', b'\x02\x00')])
            seal(source, 'A')
            _seal_one(source, 'B')
            original = request(source, 'A', 'Q', 'R01')
            self.assertEqual(original.status, RequestStatus.PENDING)
            self.assertEqual(request(source, 'A', 'Q', 'R01'), original)
            for batch_id, record_id in [('A', 'R02'), ('B', 'R01')]:
                with self.subTest(batch=batch_id, record=record_id), self.assertRaises(Conflict):
                    request(source, batch_id, 'Q', record_id)
            self.assertEqual(require_request(source, 'Q'), original)
            publish(source, 'Q')
            self.assertEqual(request(source, 'A', 'Q', 'R01').status, RequestStatus.PUBLISHED)

    def test_same_original_is_claimed_once_and_successful_request_replays(self):
        with closing(connect(self.source_path)) as source:
            _seal_one(source, 'BATCH')
            request(source, 'BATCH', 'Q1', 'R01')
            request(source, 'BATCH', 'Q2', 'R01')
            winner = publish(source, 'Q1')
            with self.assertRaises(Conflict):
                publish(source, 'Q2')
            self.assertEqual(publish(source, 'Q1'), winner)
            self.assertEqual(receipt(source, winner.publication_id).receipt, winner)
            self.assertEqual(require_request(source, 'Q2').status, RequestStatus.PENDING)
            self.assertIsNone(find_publication_by_request(source, 'Q2'))
            self.assertEqual((_claimed_count(source), _signed_count(source)), (1, 1))
            with self.assertRaises(NotFound):
                publish(source, 'UNKNOWN')

    def test_cross_batch_original_ids_can_each_be_claimed(self):
        with closing(connect(self.source_path)) as source:
            _seal_one(source, 'A', origin='left scope')
            _seal_one(source, 'B', origin='right scope')
            request(source, 'A', 'Q1', 'R01')
            request(source, 'B', 'Q2', 'R01')
            left, right = publish(source, 'Q1'), publish(source, 'Q2')
            self.assertEqual((left.record_id, right.record_id), ('R01', 'R01'))
            self.assertEqual((left.batch_id, right.batch_id), ('A', 'B'))
            self.assertEqual((left.origin.text, right.origin.text), ('left scope', 'right scope'))
            self.assertNotEqual(left.publication_id, right.publication_id)
            self.assertEqual((_claimed_count(source), _signed_count(source)), (2, 2))

    def test_source_mismatch_rejects_without_claim_or_receipt(self):
        with closing(connect(self.source_path)) as source:
            for batch_id, column, replacement in [
                ('BAD-BYTES', 'frame', bytes.fromhex('00800d00')),
                ('BAD-ORIGIN', 'origin', 'different channel'),
            ]:
                with self.subTest(column=column):
                    frozen = _seal_one(source, batch_id)
                    request(source, batch_id, batch_id, 'R01')
                    # Change the test database after sealing to simulate damaged source storage.
                    statement = {
                        'frame': 'UPDATE raw_frames SET frame = ? WHERE batch_id = ?',
                        'origin': 'UPDATE raw_frames SET origin = ? WHERE batch_id = ?',
                    }[column]
                    source.execute(statement, (replacement, batch_id))
                    with self.assertRaises(IntegrityFailure):
                        publish(source, batch_id)
                    self.assertEqual(manifest(source, batch_id), frozen)
                    self.assertEqual(require_request(source, batch_id).status, RequestStatus.PENDING)
                    self.assertIsNone(find_publication_by_request(source, batch_id))
                    self.assertEqual((_claimed_count(source), _signed_count(source)), (0, 0))

    def test_reopen_receipt_returns_frozen_package(self):
        with closing(connect(self.source_path)) as source:
            frozen = _seal_one(source, 'BATCH')
            request(source, 'BATCH', 'R', 'R01')
            signed = publish(source, 'R')
            first = receipt(source, signed.publication_id)
        with closing(connect(self.source_path)) as source:
            self.assertEqual(publish(source, 'R'), signed)
            reopened = receipt(source, signed.publication_id)
        self.assertEqual(reopened, first)
        decoded = decode_package(reopened.package)
        self.assertEqual(decoded.receipt, signed)
        self.assertEqual(decoded.entry, frozen.entries[0])
        self.assertEqual(decoded.batch_digest, frozen.digest)
        self.assertEqual(decoded.frame, bytes.fromhex('00800c00'))

    def test_precommit_failure_keeps_retry_available_after_reopen(self):
        with closing(connect(self.source_path)) as source:
            _seal_one(source, 'BATCH')
            original = request(source, 'BATCH', 'RETRY', 'R01')
            request(source, 'BATCH', 'OTHER', 'R01')
            attempted = []

            def fail_at_commit():
                self.assertEqual((_claimed_count(source), _signed_count(source)), (1, 1))
                self.assertEqual(require_request(source, 'RETRY').status, RequestStatus.PUBLISHED)
                attempted.append(find_publication_by_request(source, 'RETRY').receipt.publication_id)
                raise sqlite3.OperationalError('controlled precommit failure')

            with self.assertRaisesRegex(sqlite3.OperationalError, 'controlled precommit failure'):
                publish(source, 'RETRY', before_commit=fail_at_commit)
            self.assertEqual(len(attempted), 1)

        with closing(connect(self.source_path)) as source:
            self.assertEqual((_claimed_count(source), _signed_count(source)), (0, 0))
            self.assertEqual(require_request(source, 'RETRY'), original)
            with self.assertRaises(NotFound):
                receipt(source, attempted[0])
            signed = publish(source, 'RETRY')
            with self.assertRaises(Conflict):
                publish(source, 'OTHER')
            self.assertIsNone(find_publication_by_request(source, 'OTHER'))
            self.assertEqual(publish(source, 'RETRY'), signed)
            self.assertEqual(receipt(source, signed.publication_id).receipt, signed)
            self.assertEqual((_claimed_count(source), _signed_count(source)), (1, 1))
