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

import re
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
    except (TypeError, ValueError, OverflowError):
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


def _cpuset_size(cpuset: Any) -> int:
    """Return the number of CPUs in a cpuset string such as ``"0-3,6"``.

    Malformed segments are skipped so a partially valid cpuset still yields
    the count of its valid CPUs.
    """
    if not isinstance(cpuset, str) or not cpuset.strip():
        return 0
    count = 0
    for part in cpuset.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            if "-" in part:
                start, end = (int(p) for p in part.split("-", 1))
                count += max(0, end - start + 1)
            else:
                int(part)
                count += 1
        except ValueError:
            continue
    return count


def _first_ipv4(aliases: Any) -> str:
    """Return the first IPv4 address from a virt instance alias list, else 'unknown'."""
    if isinstance(aliases, list):
        for alias in aliases:
            if isinstance(alias, dict) and alias.get("type") == "INET":
                addr = alias.get("address")
                if isinstance(addr, str) and addr:
                    return addr
    return "unknown"


def _disk_temps_from_graph_data(graph_data: list[Any]) -> dict[str, float]:
    """Extract a per-disk median temperature from a netdata disk-temp graph response.

    Each entry carries a disk "identifier" and an "aggregations/mean" map of
    per-series readings; values outside the 0-100 degC sane range are
    discarded before taking the median, to reduce the impact of transient
    spikes/outliers.
    """
    temps: dict[str, float] = {}
    for entry in graph_data:
        if not isinstance(entry, dict):
            continue
        identifier = entry.get("identifier")
        if not identifier:
            continue
        mean = entry.get("aggregations", {})
        mean = mean.get("mean") if isinstance(mean, dict) else None
        if not isinstance(mean, dict):
            continue
        if valid_means := [
            v
            for v in mean.values()
            if isinstance(v, (int, float))
            and not isinstance(v, bool)
            and 0.0 <= v <= 100.0
        ]:
            temps[str(identifier)] = _median(valid_means)
    return temps


def _netdata_named_means(graph_data: Any, names: tuple[str, ...]) -> dict[str, float]:
    """Extract named per-series mean values from a netdata graph response.

    Unlike ``_netdata_mean_value`` (which averages all series into one
    scalar, for single-metric graphs), this looks up each of ``names``
    individually by its "legend" entry -- for multi-series graphs like
    "load" (shortterm/midterm/longterm) or "memory" (available). A name
    missing from a malformed/missing response is simply absent from the
    result (rather than defaulted to 0.0), so the caller can tell that apart
    from a legitimately-reported zero and leave its previous cached value
    untouched instead of resetting it.
    """
    if not isinstance(graph_data, list) or not graph_data:
        return {}
    item = graph_data[0]
    if not isinstance(item, dict):
        return {}
    legend = item.get("legend")
    aggregations = item.get("aggregations")
    if not isinstance(legend, list) or not isinstance(aggregations, dict):
        return {}
    mean = aggregations.get("mean")
    result: dict[str, float] = {}
    for name in names:
        if name not in legend:
            continue
        value = mean.get(name) if isinstance(mean, dict) else None
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            result[name] = float(value)
    return result


def _netdata_max_mean(graph_data: Any) -> float | None:
    """Return the highest per-series mean value in a netdata graph response.

    Used for CPU temperature, where each series is one core/sensor and the
    hottest one is the value of interest -- unlike ``_netdata_mean_value``,
    which averages series together.
    """
    if not isinstance(graph_data, list) or not graph_data:
        return None
    item = graph_data[0]
    if not isinstance(item, dict):
        return None
    aggregations = item.get("aggregations")
    mean = aggregations.get("mean") if isinstance(aggregations, dict) else None
    if not isinstance(mean, dict):
        return None
    valid_means = [
        v
        for v in mean.values()
        if isinstance(v, (int, float)) and not isinstance(v, bool)
    ]
    return round(max(valid_means), 2) if valid_means else None


# Netdata reports interface throughput in kilobits/s; TrueNAS's own UI (and
# the coordinator original) shows it in KiB/s. 1000 / 8192 converts between
# the two (1000 bits/kilobit, 8192 bits/KiB).
_KILOBITS_TO_KIBIBYTES = 1000 / 8192
_NETDATA_INTERFACE_RENAME = {"received": "rx", "sent": "tx"}


def _interface_item_throughput(item: dict[str, Any]) -> dict[str, float]:
    """Return one netdata "interface" graph item's rx/tx throughput (KiB/s).

    Returns only the keys with a valid numeric reading; a malformed/missing
    legend or aggregations, or an individual missing/invalid series, is
    simply absent from the result -- so a caller applying this via ``dict.
    update()`` leaves the previously cached value for that key untouched
    instead of resetting it to zero.
    """
    legend = item.get("legend")
    aggregations = item.get("aggregations")
    mean = aggregations.get("mean") if isinstance(aggregations, dict) else None
    if not isinstance(legend, list) or not isinstance(mean, dict):
        return {}

    throughput: dict[str, float] = {}
    for raw_name, short_name in _NETDATA_INTERFACE_RENAME.items():
        if raw_name not in legend and short_name not in legend:
            continue
        value = next(
            (
                candidate
                for candidate in (mean.get(raw_name), mean.get(short_name))
                if isinstance(candidate, (int, float))
                and not isinstance(candidate, bool)
            ),
            None,
        )
        if value is not None:
            throughput[short_name] = round(value * _KILOBITS_TO_KIBIBYTES, 2)
    return throughput


def _netdata_interface_throughput(graph_data: Any) -> dict[str, dict[str, float]]:
    """Extract per-interface rx/tx throughput (KiB/s) from a netdata
    "interface" graph response.

    Each response item covers one interface, identified by its
    "identifier". Netdata's raw legend/mean keys ("received"/"sent") are
    renamed to "rx"/"tx". Returns an empty dict for a missing/malformed
    response (RPC failure).
    """
    if not isinstance(graph_data, list):
        return {}
    result: dict[str, dict[str, float]] = {}
    for item in graph_data:
        if not isinstance(item, dict):
            continue
        identifier = item.get("identifier")
        if isinstance(identifier, str) and identifier:
            result[identifier] = _interface_item_throughput(item)
    return result


_VIRTUAL_MANUFACTURERS = {"QEMU", "VMware, Inc.", "Microsoft Corporation", "Xen"}
_VIRTUAL_PRODUCTS = {"VirtualBox", "Virtual Machine"}


def _is_virtual_machine(manufacturer: Any, product: Any) -> bool:
    """Return True if system.info's manufacturer/product indicates a VM."""
    return manufacturer in _VIRTUAL_MANUFACTURERS or product in _VIRTUAL_PRODUCTS


def _stable_uptime_epoch(
    previous_epoch: Any, uptime_seconds: float, now_epoch: int, tolerance: int = 300
) -> int:
    """Return a boot-time epoch derived from ``uptime_seconds``, damped against jitter.

    Only adopts the newly computed epoch (``now_epoch - uptime_seconds``) if
    it differs from ``previous_epoch`` by more than ``tolerance`` seconds --
    small per-poll timing variance would otherwise make an uptime sensor
    jump around by a few seconds on every refresh.
    """
    new_epoch = int(now_epoch - uptime_seconds)
    previous = previous_epoch if isinstance(previous_epoch, (int, float)) else 0
    return new_epoch if abs(new_epoch - previous) > tolerance else int(previous)


def _parse_version_tuple(version_str: Any) -> tuple[int, int]:
    """Parse (major, minor) from a TrueNAS version string, e.g. "TrueNAS-25.10.0".

    Returns (0, 0) on missing/unparsable input. Bounded quantifiers
    ({1,9}) avoid unbounded backtracking (Sonar S5852); version components
    never have that many digits.
    """
    if not isinstance(version_str, str):
        return (0, 0)
    clean_version = version_str.replace("TrueNAS-", "").replace("SCALE-", "")
    if match := re.search(r"(\d{1,9})\.(\d{1,9})", clean_version):
        return int(match[1]), int(match[2])
    return (0, 0)
