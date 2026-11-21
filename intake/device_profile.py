from station_db import InputError


SAMPLE_FORMAT = '<h'
SAMPLE_WIDTH_BYTES = 2


def validate_frame_size(length: int) -> None:
    if type(length) is not int or length <= 0 or length % SAMPLE_WIDTH_BYTES:
        raise InputError('frame length must be a positive multiple of two bytes')
