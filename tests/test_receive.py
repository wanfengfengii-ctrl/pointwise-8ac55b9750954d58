from contextlib import closing
from dataclasses import replace
from hashlib import sha256
import json

from issue.package import decode_package, encode_package
from receive.verify import verify_package
from tests import StationTestCase
from wave_station import (
    Conflict, InputError, IntegrityFailure, NotFound, accept, accepted, connect,
    ingest, initialize_demo, initialize_source, publish, read_frame_fixture, receipt, request, seal,
)


_WAVE = bytes.fromhex('00800c00')


def _ship(db, batch_id, record_id, origin, frame=_WAVE):
    ingest(db, batch_id, [(record_id, origin, frame)])
    seal(db, batch_id)
    request(db, batch_id, batch_id, record_id)
    return receipt(db, publish(db, batch_id).publication_id)


def _count(receiver):
    return receiver.execute('SELECT COUNT(*) FROM acceptances').fetchone()[0]


class ReceiveTests(StationTestCase):
    def _original_package(self):
        with closing(connect(self.source_path)) as source:
            return _ship(source, 'BATCH', 'R1', 'test rig CH1').package

    def test_equal_bodies_from_different_originals_are_received_separately(self):
        frames = read_frame_fixture('twins.json')
        with closing(connect(self.source_path)) as source:
            ingest(source, 'TWINS', frames)
            seal(source, 'TWINS')
            publications = []
            for record_id, _, _ in frames:
                request(source, 'TWINS', record_id, record_id)
                signed = publish(source, record_id)
                publications.append(receipt(source, signed.publication_id))
        with closing(connect(self.receiver_path)) as receiver:
            records = [accept(receiver, shipment.package) for shipment in publications]
            self.assertEqual(_count(receiver), 2)
        self.assertNotEqual(records[0].acceptance_id, records[1].acceptance_id)
        self.assertEqual([record.record_id for record in records], ['T09', 'T02'])
        self.assertEqual([record.origin.text for record in records], ['R11 台架测道 4', 'R11 台架测道 7'])
        self.assertEqual([record.peak for record in records], [6, 6])
        with closing(connect(self.receiver_path)) as receiver:
            for shipment, record in zip(publications, records):
                self.assertEqual(accepted(receiver, record.acceptance_id), record)
                self.assertEqual(accept(receiver, shipment.package), record)
                self.assertEqual(decode_package(record.package).frame, bytes.fromhex('faff0400'))
                self.assertEqual(record.publication_id, shipment.receipt.publication_id)
            self.assertEqual(_count(receiver), 2)

    def test_reopen_same_package_returns_first_acceptance(self):
        payload = self._original_package()
        with closing(connect(self.receiver_path)) as receiver:
            first = accept(receiver, payload)
        with closing(connect(self.receiver_path)) as receiver:
            self.assertEqual(accept(receiver, payload), first)
            self.assertEqual(accepted(receiver, first.acceptance_id), first)
            self.assertEqual(_count(receiver), 1)
        self.assertEqual(first.package, payload)
        self.assertEqual(first.peak, 32768)

    def test_same_publication_with_different_valid_package_conflicts(self):
        first_payload = self._original_package()
        with closing(connect(self.directory / 'another-source.sqlite3')) as source:
            initialize_source(source)
            other = _ship(source, 'SECOND', 'R2', 'test rig CH2', b'\x07\x00')
        first_verified = verify_package(first_payload)
        other_verified = verify_package(other.package)
        self.assertEqual(first_verified.package.receipt.publication_id, other.receipt.publication_id)
        self.assertEqual((first_verified.peak, other_verified.peak), (32768, 7))
        with closing(connect(self.receiver_path)) as receiver:
            first = accept(receiver, first_payload)
            with self.assertRaises(Conflict):
                accept(receiver, other.package)
            self.assertEqual(accepted(receiver, first.acceptance_id), first)
            self.assertEqual(_count(receiver), 1)

    def test_initialization_keeps_user_publication_and_acceptance(self):
        directory = self.directory / 'demo'
        source_path, receiver_path = initialize_demo(directory)
        with closing(connect(source_path)) as source:
            first = _ship(source, 'USER-BATCH', 'USER-R', 'user measurement CH4', b'\xfb\xff')
        with closing(connect(receiver_path)) as receiver:
            first_acceptance = accept(receiver, first.package)
        self.assertEqual(initialize_demo(directory), (source_path, receiver_path))
        with closing(connect(source_path)) as source:
            self.assertEqual(receipt(source, first.receipt.publication_id), first)
        with closing(connect(receiver_path)) as receiver:
            self.assertEqual(accepted(receiver, first_acceptance.acceptance_id), first_acceptance)
            self.assertEqual(first_acceptance.peak, 5)
            self.assertEqual(_count(receiver), 1)

    def test_unknown_history_ids_are_rejected(self):
        with closing(connect(self.source_path)) as source:
            with self.assertRaises(NotFound):
                receipt(source, 'P99999999')
        with closing(connect(self.receiver_path)) as receiver:
            with self.assertRaises(NotFound):
                accepted(receiver, 'A99999999')

    def test_same_length_same_peak_damage_is_rejected_by_digest(self):
        original = decode_package(self._original_package())
        changed = bytes.fromhex('00800d00')
        self.assertEqual(len(original.frame), len(changed))
        self.assertEqual(int.from_bytes(changed[:2], 'little', signed=True), -32768)
        self.assertEqual(int.from_bytes(changed[2:], 'little', signed=True), 13)
        tampered = encode_package(replace(original, frame=changed))
        with closing(connect(self.receiver_path)) as receiver:
            with self.assertRaisesRegex(IntegrityFailure, 'frame digest'):
                accept(receiver, tampered)
            self.assertEqual(_count(receiver), 0)

    def test_bad_format_is_rejected_without_acceptance(self):
        payload = self._original_package()
        missing = json.loads(payload)
        del missing['entry']
        bad_base64 = json.loads(payload)
        bad_base64['frame_b64'] = '%%%%'
        with closing(connect(self.receiver_path)) as receiver:
            for bad in (b'\xff', json.dumps(missing).encode(), json.dumps(bad_base64).encode()):
                with self.subTest(payload=bad), self.assertRaises(InputError):
                    accept(receiver, bad)
                self.assertEqual(_count(receiver), 0)

    def test_header_conflicting_with_sealed_entry_is_rejected(self):
        original = decode_package(self._original_package())
        with closing(connect(self.receiver_path)) as receiver:
            for field, wrong in [('batch_id', 'OTHER'), ('record_id', 'OTHER'), ('origin', 'other CH')]:
                with self.subTest(field=field):
                    header = json.loads(encode_package(original))
                    header[field] = wrong
                    with self.assertRaisesRegex(IntegrityFailure, 'package identity'):
                        accept(receiver, json.dumps(header).encode())
                    self.assertEqual(_count(receiver), 0)

    def test_wrong_peak_with_valid_digest_is_rejected(self):
        original = decode_package(self._original_package())
        self.assertEqual(original.entry.digest, sha256(_WAVE).hexdigest())
        self.assertEqual(original.entry.length, 4)
        wrong_peak = replace(original.entry, peak=12)
        contradictory = encode_package(replace(original, entry=wrong_peak))
        with closing(connect(self.receiver_path)) as receiver:
            with self.assertRaisesRegex(IntegrityFailure, 'recomputed peak'):
                accept(receiver, contradictory)
            self.assertEqual(_count(receiver), 0)
            self.assertEqual(accept(receiver, encode_package(original)).peak, 32768)
