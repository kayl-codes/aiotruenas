"""Shared pure computational helpers used across TrueNAS domain endpoints.

Ported from ``coordinator.py`` (both the Bronze-fork and Prod-HACS variants,
which are identical in this region) — the small, self-contained value
computations that multiple future ``TrueNASState`` endpoint methods will
reuse: vdev/topology error aggregation, netdata (reporting) graph mean
extraction, median, and defensive int coercion. Kept separate from
``_normalize.py`` since these are plain computations, not part of the
declarative ``parse_api`` mapping engine.
"""

from __future__ import annotations

from typing import Any


def _stat_name_similar(a: str, b: str) -> bool:
    """Return True if two stat graph names look like near-misses of each other."""
    a_l, b_l = a.lower(), b.lower()
    if a_l == b_l:
        return False
    if a_l.replace("_", "") == b_l.replace("_", ""):
        return True
    if (
        a_l.startswith(b_l)
        or a_l.endswith(b_l)
        or b_l.startswith(a_l)
        or b_l.endswith(a_l)
    ):
        return True
    return abs(len(a_l) - len(b_l)) <= 2 and a_l[:3] == b_l[:3]


def _median(values: list[float]) -> float:
    """Return the median of a non-empty list of numbers."""
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    mid = n // 2
    if n % 2 == 1:
        return sorted_vals[mid]
    return (sorted_vals[mid - 1] + sorted_vals[mid]) / 2


def _as_int(value: Any) -> int:
    """Return value as an int, or 0 if it is not an integer.

    ``bool`` is a subclass of ``int`` in Python but is explicitly rejected here:
    a JSON ``true``/``false`` is not a valid error-count value.
    """
    return value if isinstance(value, int) and not isinstance(value, bool) else 0


def _to_int(value: Any, default: int = 0) -> int:
    """Parse value into an int (also from strings like "48"), else default.

    Booleans are rejected (see ``_as_int``) rather than parsed to 0/1.
    """
    if isinstance(value, bool):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _accumulate_vdev_errors(vdev: Any, totals: dict[str, int]) -> None:
    """Recursively accumulate leaf-device error counts into totals.

    Only leaf vdevs (those without children) are counted, so the error totals
    of parent vdevs (e.g. mirrors) are not added on top of their disks.
    """
    if not isinstance(vdev, dict):
        return

    children = vdev.get("children")
    if isinstance(children, list) and children:
        for child in children:
            _accumulate_vdev_errors(child, totals)
        return

    stats = vdev.get("stats")
    if isinstance(stats, dict):
        totals["read"] += _as_int(stats.get("read_errors"))
        totals["write"] += _as_int(stats.get("write_errors"))
        totals["checksum"] += _as_int(stats.get("checksum_errors"))


def _aggregate_topology_errors(topology: Any) -> tuple[int, int, int]:
    """Sum read/write/checksum errors across all leaf vdevs of a pool topology."""
    totals = {"read": 0, "write": 0, "checksum": 0}
    if not isinstance(topology, dict):
        return 0, 0, 0

    # Categories: data, log, cache, spare, special, dedup.
    for category in topology.values():
        if isinstance(category, list):
            for vdev in category:
                _accumulate_vdev_errors(vdev, totals)

    return totals["read"], totals["write"], totals["checksum"]


def _netdata_mean_value(graph_data: Any) -> float | None:
    """Extract mean value from a netdata graph response.

    Defensive parsing: handles missing/malformed structure by returning None.
    """
    if not isinstance(graph_data, list) or not graph_data:
        return None

    item = graph_data[0]
    if not isinstance(item, dict):
        return None

    aggregations = item.get("aggregations")
    if not isinstance(aggregations, dict):
        return None

    mean = aggregations.get("mean", {})
    if not isinstance(mean, dict):
        return None

    values = [
        v
        for v in mean.values()
        if isinstance(v, (int, float)) and not isinstance(v, bool)
    ]
    return round(sum(values) / len(values), 2) if values else None


def _arc_value(graph_data: Any) -> float | None:
    """Return the mean value of a single-metric ARC netdata graph, if present."""
    return _netdata_mean_value(graph_data)


def _ups_value(graph_data: Any) -> float | None:
    """Return the mean value of a single-metric UPS netdata graph, if present."""
    return _netdata_mean_value(graph_data)
