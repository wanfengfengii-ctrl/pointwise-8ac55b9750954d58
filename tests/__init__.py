"""Shared setup for tests that reopen the source and receiver databases."""

from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from wave_station import connect, initialize_receiver, initialize_source


class StationTestCase(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory(prefix='wave-station-test-')
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.source_path = self.directory / 'source.sqlite3'
        self.receiver_path = self.directory / 'receiver.sqlite3'
        with closing(connect(self.source_path)) as source:
            initialize_source(source)
        with closing(connect(self.receiver_path)) as receiver:
            initialize_receiver(receiver)
