"""Joint-fulfilability checks shared by reservation admission and issuing.

An order is fulfilled by assigning it one distinct available original drawn
from its candidate set.  Because several orders may compete for the same
original, checking a whole batch is a bipartite matching (system of distinct
representatives) problem; a greedy check would reject valid combinations.
"""

from collections.abc import Iterable, Mapping

RecordId = str


def _assignable(
    order_candidates: Mapping[str, frozenset[str]],
    order_ids: Iterable[str],
    available: frozenset[str],
) -> bool:
    owner: dict[str, str] = {}

    def augment(order_id: str, visited: set[str]) -> bool:
        for record_id in sorted(order_candidates[order_id]):
            if record_id not in available or record_id in visited:
                continue
            visited.add(record_id)
            current = owner.get(record_id)
            if current is None or augment(current, visited):
                owner[record_id] = order_id
                return True
        return False

    for order_id in sorted(order_ids):
        if not augment(order_id, set()):
            return False
    return True


def jointly_fulfillable(
    candidates: Mapping[str, frozenset[str]], available: frozenset[str],
) -> bool:
    """Return whether every order can take a distinct original from ``available``."""
    return _assignable(candidates, candidates.keys(), available)


def fulfillable_after(
    candidates: Mapping[str, frozenset[str]],
    available: frozenset[str],
    taken: str,
) -> bool:
    """Return whether every order stays jointly fulfillable if ``taken`` is used."""
    if taken not in available:
        return False
    return _assignable(candidates, candidates.keys(), available - {taken})
