from collections.abc import Iterable


def peak_abs(samples: Iterable[int]) -> int:
    """Return the maximum absolute amplitude in integer instrument counts."""
    return max(abs(sample) for sample in samples)
