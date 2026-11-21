import struct

from intake.device_profile import SAMPLE_FORMAT, validate_frame_size
from station_db import InputError


def decode_frame(frame: bytes) -> tuple[int, ...]:
    if not isinstance(frame, bytes):
        raise InputError('waveform frame must be bytes')
    validate_frame_size(len(frame))
    return tuple(sample for (sample,) in struct.iter_unpack(SAMPLE_FORMAT, frame))
