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
from request.issue_reserved import issue_reserved as issue_reserved
from request.request_record import initialize_request_schema
from request.reserve import reserve as reserve
from request.reservation_record import initialize_reservation_schema
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
        initialize_reservation_schema(db)
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


def _load_reservation_cases() -> dict:
    return json.loads((_EXAMPLES / 'reservation_cases.json').read_text(encoding='utf-8'))


def _seal_twins(source) -> SealedManifest:
    cases = _load_reservation_cases()
    ingest(source, cases['batch_id'], read_frame_fixture(cases['frame_fixture']))
    return seal(source, cases['batch_id'])


def run_reservations(directory: Path) -> dict[str, object]:
    """Reserve a wide and narrow order, issue wide first, then replay after reopen."""
    directory.mkdir(parents=True, exist_ok=True)
    source_path = directory / 'source.sqlite3'
    receiver_path = directory / 'receiver.sqlite3'
    with closing(connect(source_path)) as source:
        initialize_source(source)
        _seal_twins(source)
        cases = _load_reservation_cases()
        wide_admitted = reserve(source, cases['batch_id'], 'W', cases['orders']['W'])
        narrow_admitted = reserve(source, cases['batch_id'], 'N', cases['orders']['N'])
        narrow_second = reserve(source, cases['batch_id'], 'N', cases['orders']['N'])
        third = reserve(source, cases['batch_id'], 'T', cases['orders']['T'])
        wide = issue_reserved(source, cases['batch_id'], 'W')
        wide_package = receipt(source, wide.publication_id)
        narrow = issue_reserved(source, cases['batch_id'], 'N')
        narrow_package = receipt(source, narrow.publication_id)
        wide_replay = issue_reserved(source, cases['batch_id'], 'W')

    with closing(connect(receiver_path)) as receiver:
        initialize_receiver(receiver)
        wide_accepted = accept(receiver, wide_package.package)
        narrow_accepted = accept(receiver, narrow_package.package)

    # Reopen: replay the signed order and reissue the archived original by id.
    with closing(connect(source_path)) as source:
        wide_after_reopen = issue_reserved(source, cases['batch_id'], 'W')
        archived_wide = receipt(source, wide.publication_id)
        archived_narrow = receipt(source, narrow.publication_id)
    with closing(connect(receiver_path)) as receiver:
        reaccepted = accept(receiver, archived_wide.package)
        rechecked = accepted(receiver, wide_accepted.acceptance_id)

    return {
        'admitted': [wide_admitted, narrow_admitted, narrow_second, third],
        'receipts': {
            'W': (wide.record_id, wide.origin.text, wide.publication_id),
            'N': (narrow.record_id, narrow.origin.text, narrow.publication_id),
        },
        'received': {
            'W': (wide_accepted.record_id, wide_accepted.origin.text, wide_accepted.peak),
            'N': (narrow_accepted.record_id, narrow_accepted.origin.text, narrow_accepted.peak),
        },
        'frames': (decode_package(wide_package.package).frame.hex(),
                   decode_package(narrow_package.package).frame.hex()),
        'wide_replay_stable': wide_replay == wide,
        'reopen_replay_stable': wide_after_reopen == wide,
        'archived_matches': archived_wide == wide_package and archived_narrow == narrow_package,
        'reaccept_matches': reaccepted == wide_accepted and rechecked == wide_accepted,
    }


def run_direct_contention(directory: Path) -> dict[str, object]:
    """One pending reservation lets a directed request take the spare original;
    two pending reservations block both directed first issues."""
    directory.mkdir(parents=True, exist_ok=True)
    cases = _load_reservation_cases()
    batch = cases['batch_id']

    # Only W reserved: D-A (T09) may be signed, then W takes the remaining T02.
    solo_path = directory / 'solo.sqlite3'
    with closing(connect(solo_path)) as source:
        initialize_source(source)
        _seal_twins(source)
        reserve(source, batch, 'W', cases['orders']['W'])
        request(source, batch, 'D-A', cases['direct_requests']['D-A'])
        direct = publish(source, 'D-A')
        wide = issue_reserved(source, batch, 'W')
    solo = {'direct': (direct.record_id, direct.origin.text),
            'wide': (wide.record_id, wide.origin.text)}

    # Both W and N reserved: both directed first issues are refused.
    blocked_path = directory / 'blocked.sqlite3'
    with closing(connect(blocked_path)) as source:
        initialize_source(source)
        _seal_twins(source)
        reserve(source, batch, 'W', cases['orders']['W'])
        reserve(source, batch, 'N', cases['orders']['N'])
        request(source, batch, 'D-A', cases['direct_requests']['D-A'])
        request(source, batch, 'D-B', cases['direct_requests']['D-B'])
        blocked = []
        for order in ('D-A', 'D-B'):
            try:
                publish(source, order)
            except Conflict:
                blocked.append(order)
        wide = issue_reserved(source, batch, 'W')
        narrow = issue_reserved(source, batch, 'N')
    return {
        'solo': solo,
        'blocked_orders': blocked,
        'still_fulfilled': {'W': wide.record_id, 'N': narrow.record_id},
    }


def _print_scenario(payload: dict) -> None:
    print('POINTWISE_SCENARIO ' + json.dumps(payload, ensure_ascii=False, separators=(',', ':')))


def _demo() -> None:
    with TemporaryDirectory(prefix='wave-station-demo-') as directory:
        clearance, archived, checked = run_shift(Path(directory) / 'shift')
        reservation_results = run_reservations(Path(directory) / 'reservations')
        contention_results = run_direct_contention(Path(directory) / 'contention')
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

    reservation_expected = {
        'admitted': ['reserved', 'reserved', 'reserved', 'unavailable'],
        'receipts': {'W': ['T02', 'R11 台架测道 7', 'P00000001'],
                     'N': ['T09', 'R11 台架测道 4', 'P00000002']},
        'received': {'W': ['T02', 'R11 台架测道 7', 6], 'N': ['T09', 'R11 台架测道 4', 6]},
    }
    reservation_actual = {
        'admitted': reservation_results['admitted'],
        'receipts': {k: list(v) for k, v in reservation_results['receipts'].items()},
        'received': {k: list(v) for k, v in reservation_results['received'].items()},
    }
    if (reservation_actual != reservation_expected
            or not reservation_results['wide_replay_stable']
            or not reservation_results['reopen_replay_stable']
            or not reservation_results['archived_matches']
            or not reservation_results['reaccept_matches']
            or reservation_results['frames'] != ('faff0400', 'faff0400')):
        raise IntegrityFailure('reservation results differ from the independently checked example')

    contention_expected = {
        'solo': {'direct': ['T09', 'R11 台架测道 4'], 'wide': ['T02', 'R11 台架测道 7']},
        'blocked_orders': ['D-A', 'D-B'],
        'still_fulfilled': {'W': 'T02', 'N': 'T09'},
    }
    contention_actual = {
        'solo': {k: list(v) for k, v in contention_results['solo'].items()},
        'blocked_orders': contention_results['blocked_orders'],
        'still_fulfilled': contention_results['still_fulfilled'],
    }
    if contention_actual != contention_expected:
        raise IntegrityFailure('direct-entry contention differs from the expected outcome')

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
    _print_scenario({
        'case_id': 'RESERVATION-WIDE-FIRST',
        'admitted': reservation_actual['admitted'],
        'expected': reservation_expected,
        'actual': reservation_actual,
        'replay': {
            'wide_replay_stable': reservation_results['wide_replay_stable'],
            'reopen_replay_stable': reservation_results['reopen_replay_stable'],
            'archived_matches': reservation_results['archived_matches'],
            'reaccept_matches': reservation_results['reaccept_matches'],
        },
        'code_paths': [
            'request/reserve.py', 'request/feasibility.py', 'request/reservation_query.py',
            'request/issue_reserved.py', 'issue/delivery.py', 'issue/publish.py',
            'issue/receipt_query.py', 'receive/accept.py',
        ],
        'oracle': 'W 候选 {T09,T02}、N 候选 {T09}；先签宽单 W 仍必须领 T02，N 领 T09，两件同字节异原号异出处；重开按签发号复领同一件。',
    })
    _print_scenario({
        'case_id': 'DIRECT-CONTENTION',
        'expected': contention_expected,
        'actual': contention_actual,
        'oracle': '仅 W 预约时 D-A 可先领 T09，W 余领 T02；W、N 都预约时 D-A、D-B 首签均拒绝，两预约仍各领 T02、T09。',
    })


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
