from contextlib import closing
from hashlib import sha256

from intake.frame_codec import decode_frame
from intake.raw_store import load_raw_batch
from tests import StationTestCase
from wave_station import (
    Conflict, InputError, NotFound, connect, ingest, initialize_demo, manifest, read_frame_fixture, seal,
)


class BatchTests(StationTestCase):
    def test_signed_extreme_and_capture_order(self):
        frames = read_frame_fixture('shift.json')
        with closing(connect(self.source_path)) as source:
            ingest(source, 'SHIFT', frames)
            frozen = seal(source, 'SHIFT')
            self.assertEqual(manifest(source, 'SHIFT'), frozen)
        self.assertEqual(decode_frame(frames[0][2]), (-32768, 12))
        self.assertEqual(decode_frame(frames[1][2]), (-3, 256))
        self.assertEqual([(row.position, row.record_id, row.peak, row.origin.text) for row in frozen.entries], [
            (0, 'R20', 32768, 'R04 台架白班测道 2'),
            (1, 'R03', 256, 'R04 台架白班测道 5'),
        ])
        self.assertEqual([row.length for row in frozen.entries], [4, 4])
        self.assertEqual(frozen.entries[0].digest, sha256(bytes.fromhex('00800c00')).hexdigest())

    def test_raw_replay_compares_order_origin_and_bytes(self):
        frames = read_frame_fixture('shift.json')
        first, second = frames
        variations = [
            (second, first),
            ((first[0], 'changed origin', first[2]), second),
            ((first[0], first[1], bytes.fromhex('00800d00')), second),
        ]
        with closing(connect(self.source_path)) as source:
            original = ingest(source, 'REPLAY', iter(frames))
            self.assertEqual(ingest(source, 'REPLAY', frames), original)
            for varied in variations:
                with self.subTest(frames=varied), self.assertRaises(Conflict):
                    ingest(source, 'REPLAY', varied)
            self.assertEqual(load_raw_batch(source, 'REPLAY'), original)
            self.assertEqual([entry.record_id for entry in seal(source, 'REPLAY').entries], ['R20', 'R03'])

    def test_manifest_order_after_reopen(self):
        with closing(connect(self.source_path)) as source:
            ingest(source, 'ORDER', read_frame_fixture('shift.json'))
            first = seal(source, 'ORDER')
        with closing(connect(self.source_path)) as source:
            reopened = manifest(source, 'ORDER')
            self.assertEqual(seal(source, 'ORDER'), first)
        self.assertEqual(reopened, first)
        self.assertEqual([(row.record_id, row.origin.text) for row in reopened.entries], [
            ('R20', 'R04 台架白班测道 2'), ('R03', 'R04 台架白班测道 5'),
        ])

    def test_reinitialization_preserves_user_batches(self):
        directory = self.directory / 'demo'
        source_path, receiver_path = initialize_demo(directory)
        frames = [('CUSTOM', ' 用户台架  ', b'\x05\x00')]
        with closing(connect(source_path)) as source:
            sample = manifest(source, 'R04-DAY')
            original = ingest(source, 'USER', frames)
            frozen = seal(source, 'USER')
        self.assertEqual(initialize_demo(directory), (source_path, receiver_path))
        with closing(connect(source_path)) as source:
            self.assertEqual(ingest(source, 'USER', frames), original)
            self.assertEqual(manifest(source, 'USER'), frozen)
            self.assertEqual(manifest(source, 'R04-DAY'), sample)
            self.assertEqual(frozen.entries[0].origin.text, ' 用户台架  ')

    def test_truncated_tail_rejects_whole_batch(self):
        frames = [('good', 'CH1', b'\x00\x00'), ('tail', 'CH2', b'\x01')]
        with closing(connect(self.source_path)) as source:
            with self.assertRaisesRegex(InputError, 'frame length'):
                ingest(source, 'TRUNCATED', frames)
            self.assertIsNone(load_raw_batch(source, 'TRUNCATED'))
            with self.assertRaises(NotFound):
                seal(source, 'TRUNCATED')

    def test_bad_frame_fields_reject_whole_batch(self):
        variants = {
            'zero length': ('R2', 'CH2', b''),
            'duplicate': ('R1', 'CH2', b'\x02\x00'),
            'empty origin': ('R2', ' ', b'\x02\x00'),
            'empty id': ('', 'CH2', b'\x02\x00'),
            'absent bytes': ('R2', 'CH2', None),
        }
        with closing(connect(self.source_path)) as source:
            for label, invalid in variants.items():
                with self.subTest(case=label):
                    with self.assertRaises(InputError):
                        ingest(source, 'INVALID', [('R1', 'CH1', b'\x01\x00'), invalid])
                    self.assertIsNone(load_raw_batch(source, 'INVALID'))
                    with self.assertRaises(NotFound):
                        seal(source, 'INVALID')

    def test_empty_batch_seals_with_stable_digest(self):
        digest = sha256(b'{"batch_id":"EMPTY","entries":[]}').hexdigest()
        with closing(connect(self.source_path)) as source:
            self.assertEqual(ingest(source, 'EMPTY', ()).frames, ())
            self.assertEqual(seal(source, 'EMPTY').digest, digest)
        with closing(connect(self.source_path)) as source:
            frozen = manifest(source, 'EMPTY')
            self.assertEqual(frozen.entries, ())
            self.assertEqual(frozen.digest, digest)
            self.assertEqual(seal(source, 'EMPTY'), frozen)

    def test_unknown_import_and_manifest(self):
        with closing(connect(self.source_path)) as source:
            with self.assertRaises(NotFound):
                seal(source, 'UNKNOWN')
            with self.assertRaises(NotFound):
                manifest(source, 'UNKNOWN')
            ingest(source, 'UNSEALED', ())
            with self.assertRaises(NotFound):
                manifest(source, 'UNSEALED')
            seal(source, 'UNSEALED')
            self.assertEqual(manifest(source, 'UNSEALED').entries, ())
