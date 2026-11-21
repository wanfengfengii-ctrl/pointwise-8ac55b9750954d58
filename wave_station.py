import argparse
from contextlib import closing
import json
import os
from pathlib import Path
from sqlite3 import Connection
from tempfile import TemporaryDirectory

from freeze.batch_query import manifest as manifest
from freeze.manifest import SealedManifest
from freeze.seal import seal as seal
from freeze.seal_store import initialize_seal_schema
from intake.ingest import ingest as ingest
from intake.raw_store import initialize_raw_schema
from issue.consumption import initialize_consumption_schema
from issue.package import decode_package
from issue.publication_store import Publication, initialize_publication_schema
from issue.publish import publish as publish
from issue.receipt_query import receipt as receipt
from receive.accept import accept as accept
from receive.acceptance_query import accepted as accepted
from receive.acceptance_store import initialize_acceptance_schema
from receive.accepted_record import AcceptedRecord
from request.open_request import request as request
from request.request_record import initialize_request_schema
from station_db import Conflict as Conflict
from station_db import InputError as InputError
from station_db import IntegrityFailure as IntegrityFailure
from station_db import NotFound as NotFound
from station_db import connect as connect
from station_db import write_transaction


_EXAMPLES = Path(__file__).resolve().parent / 'examples'
_SHIFT_BATCH = 'R04-DAY'


def initialize_source(db: Connection) -> None:
    with write_transaction(db):
        initialize_raw_schema(db)
        initialize_seal_schema(db)
        initialize_request_schema(db)
        initialize_consumption_schema(db)
        initialize_publication_schema(db)


def initialize_receiver(db: Connection) -> None:
    with write_transaction(db):
        initialize_acceptance_schema(db)


def read_frame_fixture(name: str) -> tuple[tuple[str, str, bytes], ...]:
    rows = json.loads((_EXAMPLES / name).read_text(encoding='utf-8'))
    return tuple((row['record_id'], row['origin'], bytes.fromhex(row['frame_hex'])) for row in rows)


def _create_demo_source(path: Path, legacy: bool) -> None:
    # Build a complete sample before linking it into place. Concurrent initialization
    # can reuse the winning database without replacing its contents.
    with TemporaryDirectory(prefix='.wave-init-', dir=path.parent) as staging:
        staged = Path(staging) / path.name
        with closing(connect(staged)) as source:
            initialize_source(source)
            if legacy:
                source.executescript((_EXAMPLES / 'v1.sql').read_text(encoding='utf-8'))
            else:
                ingest(source, _SHIFT_BATCH, read_frame_fixture('shift.json'))
                seal(source, _SHIFT_BATCH)
        try:
            os.link(staged, path)
        except FileExistsError:
            pass


def initialize_demo(directory: Path, *, legacy: bool = False) -> tuple[Path, Path]:
    directory.mkdir(parents=True, exist_ok=True)
    source_path = directory / 'source.sqlite3'
    receiver_path = directory / 'receiver.sqlite3'
    if not source_path.exists():
        _create_demo_source(source_path, legacy)
    with closing(connect(source_path)) as source:
        initialize_source(source)
    with closing(connect(receiver_path)) as receiver:
        initialize_receiver(receiver)
    return source_path, receiver_path


def run_shift(directory: Path) -> tuple[SealedManifest, Publication, AcceptedRecord]:
    source_path, receiver_path = initialize_demo(directory)
    with closing(connect(source_path)) as source:
        clearance = manifest(source, _SHIFT_BATCH)
        selected = max(clearance.entries, key=lambda row: row.peak)
        request(source, clearance.batch_id, 'DAY-REVIEW-01', selected.record_id)
        signed = publish(source, 'DAY-REVIEW-01')
        dispatched = receipt(source, signed.publication_id)

    with closing(connect(receiver_path)) as receiver:
        collected = accept(receiver, dispatched.package)

    with closing(connect(source_path)) as source:
        archived = receipt(source, signed.publication_id)
    with closing(connect(receiver_path)) as receiver:
        checked = accepted(receiver, collected.acceptance_id)
    if archived != dispatched or checked != collected:
        raise IntegrityFailure('closed-station receipt replay changed the delivered content')
    return clearance, archived, checked


def _demo() -> None:
    with TemporaryDirectory(prefix='wave-station-demo-') as directory:
        clearance, archived, checked = run_shift(Path(directory))
    input_rows = [
        {'record_id': record_id, 'origin': origin, 'frame_hex': frame.hex()}
        for record_id, origin, frame in read_frame_fixture('shift.json')
    ]
    expected = [
        ['R20', 32768, 'R04 台架白班测道 2'],
        ['R03', 256, 'R04 台架白班测道 5'],
    ]
    actual = [[entry.record_id, entry.peak, entry.origin.text] for entry in clearance.entries]
    downstream_expected = {
        'selected': 'R20', 'received': 'R20', 'origin': 'R04 台架白班测道 2',
        'peak': 32768, 'frame_hex': '00800c00', 'archived_package_matches': True,
    }
    downstream_actual = {
        'selected': archived.receipt.record_id,
        'received': checked.record_id,
        'origin': checked.origin.text,
        'peak': checked.peak,
        'frame_hex': decode_package(checked.package).frame.hex(),
        'archived_package_matches': archived.package == checked.package,
    }
    if actual != expected or downstream_actual != downstream_expected:
        raise IntegrityFailure('shift results differ from the independently checked example')
    print('POINTWISE_SCENARIO ' + json.dumps({
        'case_id': 'V1',
        'input': {'batch_id': clearance.batch_id, 'frames': input_rows},
        'expected': expected,
        'actual': actual,
        'downstream_expected': downstream_expected,
        'downstream_actual': downstream_actual,
        'code_paths': [
            'intake/ingest.py', 'intake/frame_codec.py', 'freeze/peak.py', 'freeze/seal.py',
            'request/open_request.py', 'issue/publish.py', 'issue/receipt_query.py',
            'receive/verify.py', 'receive/accept.py', 'receive/acceptance_query.py',
        ],
        'oracle': '手算两个小端样例为 [-32768, 12] 和 [-3, 256]，比较绝对振幅并核对首行原号、出处和原始十六进制字节。',
    }, ensure_ascii=False, separators=(',', ':')))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='砺川原始波形交换站')
    commands = parser.add_subparsers(dest='command', required=True)
    commands.add_parser('demo', help='运行并核对完整交班示例')
    demo_database = commands.add_parser('init-demo', help='初始化演示库')
    demo_database.add_argument('directory', type=Path)
    legacy_database = commands.add_parser('restore-v1', help='物化已签发的 V1 样例库')
    legacy_database.add_argument('directory', type=Path)
    args = parser.parse_args(argv)
    if args.command == 'demo':
        _demo()
    else:
        source, receiver = initialize_demo(args.directory, legacy=args.command == 'restore-v1')
        print(source)
        print(receiver)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
