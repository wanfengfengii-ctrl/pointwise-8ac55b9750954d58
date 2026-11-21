from contextlib import closing
import json
from pathlib import Path
import sqlite3

from issue.package import decode_package
from issue.publication_store import find_publication_by_request
from request.request_query import require_request
from request.request_record import RequestStatus
from request.reservation_record import load_reservation, pending_reservations
from tests import StationTestCase
from wave_station import (
    Conflict, InputError, IntegrityFailure, NotFound, accept, accepted, connect, ingest,
    initialize_demo, initialize_receiver, initialize_source, issue_reserved, manifest, publish,
    read_frame_fixture, receipt, request, reserve, seal,
)


_TWINS = 'TWINS'


def _load_cases() -> dict:
    return json.loads((Path(__file__).resolve().parent.parent / 'examples' / 'reservation_cases.json')
                      .read_text(encoding='utf-8'))


def _seal_twins(source):
    cases = _load_cases()
    ingest(source, cases['batch_id'], read_frame_fixture(cases['frame_fixture']))
    return seal(source, cases['batch_id'])


def _pending_ids(source, batch_id):
    return [order.order_id for order in pending_reservations(source, batch_id)]


class ReservationTests(StationTestCase):
    def test_pre_reservation_schema_is_migrated_in_place(self):
        # Build a database with the original (request-linked) exchange tables,
        # then confirm initialization drops only the request foreign keys.
        old_path = self.directory / 'old.sqlite3'
        raw = sqlite3.connect(old_path, isolation_level=None)
        try:
            raw.execute('PRAGMA foreign_keys = ON')
            raw.executescript('''
                CREATE TABLE raw_batches (batch_id TEXT PRIMARY KEY NOT NULL);
                CREATE TABLE raw_frames (
                    batch_id TEXT NOT NULL REFERENCES raw_batches(batch_id),
                    position INTEGER NOT NULL CHECK (position >= 0), record_id TEXT NOT NULL,
                    origin TEXT NOT NULL, frame BLOB NOT NULL,
                    PRIMARY KEY (batch_id, record_id), UNIQUE (batch_id, position));
                CREATE TABLE sealed_batches (
                    batch_id TEXT PRIMARY KEY NOT NULL REFERENCES raw_batches(batch_id),
                    digest TEXT NOT NULL);
                CREATE TABLE sealed_entries (
                    batch_id TEXT NOT NULL REFERENCES sealed_batches(batch_id),
                    position INTEGER NOT NULL CHECK (position >= 0), record_id TEXT NOT NULL,
                    origin TEXT NOT NULL, length INTEGER NOT NULL,
                    digest TEXT NOT NULL, peak INTEGER NOT NULL,
                    PRIMARY KEY (batch_id, record_id), UNIQUE (batch_id, position),
                    FOREIGN KEY (batch_id, record_id) REFERENCES raw_frames(batch_id, record_id));
                CREATE TABLE ordinary_requests (
                    request_id TEXT PRIMARY KEY NOT NULL, batch_id TEXT NOT NULL,
                    record_id TEXT NOT NULL, status TEXT NOT NULL,
                    FOREIGN KEY (batch_id, record_id) REFERENCES sealed_entries(batch_id, record_id));
                CREATE TABLE consumed_records (
                    batch_id TEXT NOT NULL, record_id TEXT NOT NULL,
                    request_id TEXT NOT NULL REFERENCES ordinary_requests(request_id),
                    PRIMARY KEY (batch_id, record_id),
                    FOREIGN KEY (batch_id, record_id) REFERENCES sealed_entries(batch_id, record_id));
                CREATE TABLE publications (
                    sequence INTEGER PRIMARY KEY CHECK (sequence > 0),
                    publication_id TEXT NOT NULL UNIQUE,
                    request_id TEXT NOT NULL UNIQUE REFERENCES ordinary_requests(request_id),
                    batch_id TEXT NOT NULL, record_id TEXT NOT NULL, origin TEXT NOT NULL,
                    package BLOB NOT NULL,
                    FOREIGN KEY (batch_id, record_id) REFERENCES consumed_records(batch_id, record_id));
            ''')
            raw.executescript((Path(__file__).resolve().parent.parent / 'examples' / 'v1.sql')
                              .read_text(encoding='utf-8'))
        finally:
            raw.close()

        with closing(connect(old_path)) as source:
            initialize_source(source)
            self.assertEqual(receipt(source, 'P00000001').receipt.record_id, 'L01')
            self.assertEqual(source.execute('PRAGMA integrity_check').fetchone()[0], 'ok')
            self.assertEqual(source.execute('PRAGMA foreign_key_check').fetchall(), [])
            publication_targets = {row[2] for row in source.execute("PRAGMA foreign_key_list('publications')")}
            consumption_targets = {row[2] for row in source.execute("PRAGMA foreign_key_list('consumed_records')")}
            self.assertNotIn('ordinary_requests', publication_targets)
            self.assertNotIn('ordinary_requests', consumption_targets)
            self.assertIn('consumed_records', publication_targets)
            self.assertIn('sealed_entries', consumption_targets)
            # A reservation may now occupy the shared number space without a request row.
            self.assertEqual(reserve(source, 'LEGACY', 'REUSE', ['L01']), 'unavailable')

    def test_wide_and_narrow_linkage_survives_reopen_and_reissue(self):
        cases = _load_cases()
        with closing(connect(self.source_path)) as source:
            _seal_twins(source)
            reserve(source, _TWINS, 'W', cases['orders']['W'])
            reserve(source, _TWINS, 'N', cases['orders']['N'])
            wide = issue_reserved(source, _TWINS, 'W')
            wide_package = receipt(source, wide.publication_id)
            narrow = issue_reserved(source, _TWINS, 'N')
            narrow_package = receipt(source, narrow.publication_id)
        with closing(connect(self.receiver_path)) as receiver:
            wide_accepted = accept(receiver, wide_package.package)
            narrow_accepted = accept(receiver, narrow_package.package)
            self.assertEqual((wide_accepted.record_id, narrow_accepted.record_id), ('T02', 'T09'))
            self.assertEqual((wide_accepted.origin.text, narrow_accepted.origin.text),
                             ('R11 台架测道 7', 'R11 台架测道 4'))
        # Reopen: reissue by the original publication id returns the frozen package.
        with closing(connect(self.source_path)) as source:
            self.assertEqual(receipt(source, wide.publication_id), wide_package)
            self.assertEqual(receipt(source, narrow.publication_id), narrow_package)
            self.assertEqual(issue_reserved(source, _TWINS, 'W'), wide)
            self.assertEqual(issue_reserved(source, _TWINS, 'N'), narrow)
        with closing(connect(self.receiver_path)) as receiver:
            self.assertEqual(accepted(receiver, wide_accepted.acceptance_id), wide_accepted)
            self.assertEqual(accept(receiver, wide_package.package), wide_accepted)

    def test_wide_and_narrow_orders_fulfill_in_either_order(self):
        cases = _load_cases()
        for first, second in (('W', 'N'), ('N', 'W')):
            with self.subTest(first=first):
                with closing(connect(self.source_path)) as source:
                    _seal_twins(source)
                    self.assertEqual(reserve(source, _TWINS, 'W', cases['orders']['W']), 'reserved')
                    self.assertEqual(reserve(source, _TWINS, 'N', cases['orders']['N']), 'reserved')
                    issued = {
                        first: issue_reserved(source, _TWINS, first),
                        second: issue_reserved(source, _TWINS, second),
                    }
                    self.assertEqual(issued['W'].record_id, 'T02')
                    self.assertEqual(issued['N'].record_id, 'T09')
                    wide = receipt(source, issued['W'].publication_id)
                    narrow = receipt(source, issued['N'].publication_id)
                    self.assertEqual(wide.receipt.origin.text, 'R11 台架测道 7')
                    self.assertEqual(narrow.receipt.origin.text, 'R11 台架测道 4')
                    self.assertEqual(decode_package(wide.package).frame, bytes.fromhex('faff0400'))
                    self.assertEqual(decode_package(narrow.package).frame, bytes.fromhex('faff0400'))
                    self.assertEqual(_pending_ids(source, _TWINS), [])

                # The receiver independently accepts both distinct originals.
                self.receiver_path.unlink(missing_ok=True)
                with closing(connect(self.receiver_path)) as receiver:
                    initialize_receiver(receiver)
                    wide_a = accept(receiver, wide.package)
                    narrow_a = accept(receiver, narrow.package)
                    self.assertEqual((wide_a.record_id, narrow_a.record_id), ('T02', 'T09'))
                    self.assertEqual((wide_a.origin.text, narrow_a.origin.text),
                                     ('R11 台架测道 7', 'R11 台架测道 4'))
                    self.assertEqual((wide_a.peak, narrow_a.peak), (6, 6))
                    self.assertNotEqual(wide_a.acceptance_id, narrow_a.acceptance_id)
                # Reset the source for the next iteration.
                self.source_path.unlink()
                with closing(connect(self.source_path)) as source:
                    initialize_source(source)

    def test_third_order_is_unavailable_and_not_registered(self):
        cases = _load_cases()
        with closing(connect(self.source_path)) as source:
            _seal_twins(source)
            reserve(source, _TWINS, 'W', cases['orders']['W'])
            reserve(source, _TWINS, 'N', cases['orders']['N'])
            self.assertEqual(reserve(source, _TWINS, 'T', cases['orders']['T']), 'unavailable')
            self.assertIsNone(load_reservation(source, 'T'))
            self.assertEqual(_pending_ids(source, _TWINS), ['N', 'W'])
            with self.assertRaises(NotFound):
                issue_reserved(source, _TWINS, 'T')

    def test_reservation_replay_after_signature_returns_reserved(self):
        cases = _load_cases()
        with closing(connect(self.source_path)) as source:
            _seal_twins(source)
            reserve(source, _TWINS, 'W', cases['orders']['W'])
            reserve(source, _TWINS, 'N', cases['orders']['N'])
            first = issue_reserved(source, _TWINS, 'W')
            self.assertEqual(issue_reserved(source, _TWINS, 'W'), first)
            self.assertEqual(receipt(source, first.publication_id).receipt, first)
            # Same order id, same set, different order and duplicates still reuses.
            self.assertEqual(reserve(source, _TWINS, 'W', ['T02', 'T09', 'T09']), 'reserved')
            self.assertEqual(reserve(source, _TWINS, 'N', ['T09']), 'reserved')

    def test_candidate_set_or_batch_rebinding_conflicts(self):
        cases = _load_cases()
        with closing(connect(self.source_path)) as source:
            _seal_twins(source)
            reserve(source, _TWINS, 'W', cases['orders']['W'])
            with self.assertRaises(Conflict):
                reserve(source, _TWINS, 'W', ['T09'])
            with self.assertRaises(Conflict):
                reserve(source, _TWINS, 'W', ['T09', 'T02', 'extra'])
        # A second sealed batch with the same original ids cannot reuse the order id.
        with closing(connect(self.source_path)) as source:
            ingest(source, 'OTHER', [('T09', 'other CH', b'\x01\x00'), ('T02', 'other CH2', b'\x02\x00')])
            seal(source, 'OTHER')
            with self.assertRaises(Conflict):
                reserve(source, 'OTHER', 'W', ['T09', 'T02'])

    def test_invalid_candidates_are_rejected(self):
        cases = _load_cases()
        with closing(connect(self.source_path)) as source:
            _seal_twins(source)
            for bad in ('T09', b'T09', [], iter(()), ['T09', 'ABSENT'], ['T09', '']):
                with self.subTest(candidates=bad):
                    with self.assertRaises((InputError, NotFound)):
                        reserve(source, _TWINS, 'BAD', bad)
                    self.assertIsNone(load_reservation(source, 'BAD'))
            with self.assertRaises(NotFound):
                reserve(source, 'NO-SUCH-BATCH', 'BAD', ['T09'])
            self.assertIsNone(load_reservation(source, 'BAD'))

    def test_direct_request_may_claim_only_when_reservations_survive(self):
        cases = _load_cases()
        with closing(connect(self.source_path)) as source:
            _seal_twins(source)
            reserve(source, _TWINS, 'W', cases['orders']['W'])
            reserve(source, _TWINS, 'N', cases['orders']['N'])
            request(source, _TWINS, 'D-A', cases['direct_requests']['D-A'])
            with self.assertRaises(Conflict):
                publish(source, 'D-A')
            self.assertEqual(require_request(source, 'D-A').status, RequestStatus.PENDING)
            self.assertIsNone(find_publication_by_request(source, 'D-A'))
            wide = issue_reserved(source, _TWINS, 'W')
            self.assertEqual(wide.record_id, 'T02')
            # With W satisfied and N still holding T09, D-A (pointed at T09) stays blocked.
            with self.assertRaises(Conflict):
                publish(source, 'D-A')
            self.assertEqual(issue_reserved(source, _TWINS, 'N').record_id, 'T09')
            # T09 is now consumed, so the directed request can no longer claim it either.
            with self.assertRaises(Conflict):
                publish(source, 'D-A')

    def test_direct_request_allowed_when_its_original_is_not_needed(self):
        cases = _load_cases()
        with closing(connect(self.source_path)) as source:
            _seal_twins(source)
            reserve(source, _TWINS, 'W', cases['orders']['W'])
            # Only W is pending; it can fall back to T09, so D-B may take T02.
            request(source, _TWINS, 'D-B', cases['direct_requests']['D-B'])
            self.assertEqual(publish(source, 'D-B').record_id, 'T02')
            self.assertEqual(issue_reserved(source, _TWINS, 'W').record_id, 'T09')

    def test_two_direct_requests_both_blocked_when_two_reservations_pending(self):
        cases = _load_cases()
        with closing(connect(self.source_path)) as source:
            _seal_twins(source)
            reserve(source, _TWINS, 'W', cases['orders']['W'])
            reserve(source, _TWINS, 'N', cases['orders']['N'])
            request(source, _TWINS, 'D-A', cases['direct_requests']['D-A'])
            request(source, _TWINS, 'D-B', cases['direct_requests']['D-B'])
            for order in ('D-A', 'D-B'):
                with self.subTest(order=order), self.assertRaises(Conflict):
                    publish(source, order)
            self.assertEqual(_pending_ids(source, _TWINS), ['N', 'W'])
            self.assertEqual(issue_reserved(source, _TWINS, 'W').record_id, 'T02')
            self.assertEqual(issue_reserved(source, _TWINS, 'N').record_id, 'T09')

    def test_direct_request_allowed_when_only_one_order_pending(self):
        cases = _load_cases()
        with closing(connect(self.source_path)) as source:
            _seal_twins(source)
            reserve(source, _TWINS, 'W', cases['orders']['W'])
            request(source, _TWINS, 'D-A', cases['direct_requests']['D-A'])
            signed = publish(source, 'D-A')
            self.assertEqual(signed.record_id, 'T09')
            self.assertEqual(issue_reserved(source, _TWINS, 'W').record_id, 'T02')

    def test_order_number_space_is_shared_between_kinds(self):
        cases = _load_cases()
        with closing(connect(self.source_path)) as source:
            _seal_twins(source)
            request(source, _TWINS, 'SHARED', 'T09')
            with self.assertRaises(Conflict):
                reserve(source, _TWINS, 'SHARED', ['T09', 'T02'])
            self.assertIsNone(load_reservation(source, 'SHARED'))
            self.assertEqual(require_request(source, 'SHARED').record_id, 'T09')
            # The other direction: an order id used by a reservation cannot become a request.
            reserve(source, _TWINS, 'RES', ['T09'])
            with self.assertRaises(Conflict):
                request(source, _TWINS, 'RES', 'T02')
            with self.assertRaises(NotFound):
                require_request(source, 'RES')

    def test_issue_reserved_unknown_or_ordinary_order_is_not_found(self):
        cases = _load_cases()
        with closing(connect(self.source_path)) as source:
            _seal_twins(source)
            request(source, _TWINS, 'ORD', 'T09')
            with self.assertRaises(NotFound):
                issue_reserved(source, _TWINS, 'ORD')
            with self.assertRaises(NotFound):
                issue_reserved(source, _TWINS, 'MISSING')

    def test_precommit_failure_preserves_commitment_and_retry(self):
        cases = _load_cases()
        attempted = []
        with closing(connect(self.source_path)) as source:
            _seal_twins(source)
            reserve(source, _TWINS, 'W', cases['orders']['W'])
            reserve(source, _TWINS, 'N', cases['orders']['N'])

            def fail_at_commit():
                attempted.append(find_publication_by_request(source, 'W').receipt.publication_id)
                raise sqlite3.OperationalError('controlled precommit failure')

            with self.assertRaisesRegex(sqlite3.OperationalError, 'controlled precommit failure'):
                issue_reserved(source, _TWINS, 'W', before_commit=fail_at_commit)
            self.assertEqual(len(attempted), 1)

        with closing(connect(self.source_path)) as source:
            self.assertIsNone(find_publication_by_request(source, 'W'))
            with self.assertRaises(NotFound):
                receipt(source, attempted[0])
            self.assertEqual(_pending_ids(source, _TWINS), ['N', 'W'])
            self.assertEqual(source.execute('SELECT COUNT(*) FROM consumed_records').fetchone()[0], 0)
            self.assertEqual(source.execute('SELECT COUNT(*) FROM publications').fetchone()[0], 0)
            wide = issue_reserved(source, _TWINS, 'W')
            narrow = issue_reserved(source, _TWINS, 'N')
            self.assertEqual((wide.record_id, narrow.record_id), ('T02', 'T09'))

    def test_reopen_allows_finishing_pending_and_replaying_signed(self):
        cases = _load_cases()
        with closing(connect(self.source_path)) as source:
            _seal_twins(source)
            reserve(source, _TWINS, 'W', cases['orders']['W'])
            reserve(source, _TWINS, 'N', cases['orders']['N'])
            wide = issue_reserved(source, _TWINS, 'W')
            first_package = receipt(source, wide.publication_id)
        with closing(connect(self.source_path)) as source:
            self.assertEqual(issue_reserved(source, _TWINS, 'W'), wide)
            self.assertEqual(receipt(source, wide.publication_id), first_package)
            narrow = issue_reserved(source, _TWINS, 'N')
            self.assertEqual(narrow.record_id, 'T09')
        with closing(connect(self.source_path)) as source:
            reloaded = receipt(source, wide.publication_id)
        self.assertEqual(reloaded, first_package)

    def test_reservation_survives_reinitialization(self):
        directory = self.directory / 'demo'
        source_path, _ = initialize_demo(directory)
        cases = _load_cases()
        with closing(connect(source_path)) as source:
            ingest(source, _TWINS, read_frame_fixture(cases['frame_fixture']))
            seal(source, _TWINS)
            reserve(source, _TWINS, 'W', cases['orders']['W'])
            reserve(source, _TWINS, 'N', cases['orders']['N'])
            issued = issue_reserved(source, _TWINS, 'W')
        initialize_demo(directory)
        with closing(connect(source_path)) as source:
            self.assertEqual(_pending_ids(source, _TWINS), ['N'])
            self.assertEqual(issue_reserved(source, _TWINS, 'W'), issued)
            self.assertEqual(issue_reserved(source, _TWINS, 'N').record_id, 'T09')

    def test_legacy_snapshot_keeps_publications_and_accepts_new_kinds(self):
        directory = self.directory / 'v1'
        source_path, receiver_path = initialize_demo(directory, legacy=True)
        with closing(connect(source_path)) as source:
            old = receipt(source, 'P00000001')
            self.assertEqual(publish(source, 'legacy-request'), old.receipt)
            reserve(source, 'LEGACY', 'LEG-R', ['L01'])
            # L01 is already consumed, so a reservation for it is unavailable.
            self.assertIsNone(load_reservation(source, 'LEG-R'))
            ingest(source, _TWINS, read_frame_fixture('twins.json'))
            seal(source, _TWINS)
            reserve(source, _TWINS, 'W', ['T09', 'T02'])
            reserve(source, _TWINS, 'N', ['T09'])
            self.assertEqual(issue_reserved(source, _TWINS, 'W').record_id, 'T02')
        with closing(connect(receiver_path)) as receiver:
            self.assertEqual(accept(receiver, old.package).peak, 8)
