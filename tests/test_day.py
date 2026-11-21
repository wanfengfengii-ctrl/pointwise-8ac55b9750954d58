from contextlib import closing

from issue.package import decode_package
from tests import StationTestCase
from wave_station import (
    Conflict, accept, accepted, connect, ingest, initialize_demo, manifest, publish, receipt, request,
    run_shift, seal,
)


class DayTests(StationTestCase):
    def test_full_shift_uses_peak_to_select_and_receive_original(self):
        clearance, publication, verified = run_shift(self.directory / 'shift')
        self.assertEqual([(entry.record_id, entry.peak) for entry in clearance.entries], [('R20', 32768), ('R03', 256)])
        self.assertEqual(publication.receipt.request_id, 'DAY-REVIEW-01')
        self.assertEqual(publication.receipt.record_id, 'R20')
        self.assertEqual(verified.record_id, 'R20')
        self.assertEqual(verified.origin.text, 'R04 台架白班测道 2')
        self.assertEqual(verified.peak, 32768)
        self.assertEqual(verified.package, publication.package)
        self.assertEqual(decode_package(verified.package).frame.hex(), '00800c00')

    def test_existing_v1_snapshot_opens_and_keeps_old_receipt(self):
        directory = self.directory / 'v1'
        source_path, receiver_path = initialize_demo(directory, legacy=True)
        with closing(connect(source_path)) as source:
            first = receipt(source, 'P00000001')
            self.assertEqual(first.receipt, publish(source, 'legacy-request'))
            self.assertEqual(first.receipt.record_id, 'L01')
            self.assertEqual(first.receipt.origin.text, 'R02 archived CH1')
            self.assertEqual(manifest(source, 'LEGACY').entries[0].peak, 8)
            self.assertEqual(decode_package(first.package).frame.hex(), 'f8ff0400')
            request(source, 'LEGACY', 'other', 'L01')
            with self.assertRaises(Conflict):
                publish(source, 'other')
            ingest(source, 'ADDED', [('N01', 'new fixture', b'\x03\x00')])
            seal(source, 'ADDED')
            request(source, 'ADDED', 'new-request', 'N01')
            self.assertEqual(publish(source, 'new-request').publication_id, 'P00000002')
        with closing(connect(receiver_path)) as receiver:
            verified = accept(receiver, first.package)
            self.assertEqual(verified.peak, 8)
        initialize_demo(directory, legacy=True)
        with closing(connect(source_path)) as source:
            self.assertEqual(receipt(source, 'P00000001'), first)
            self.assertEqual(receipt(source, 'P00000002').receipt.record_id, 'N01')
        with closing(connect(receiver_path)) as receiver:
            self.assertEqual(accepted(receiver, verified.acceptance_id), verified)
