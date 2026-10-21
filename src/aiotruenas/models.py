"""Typed, immutable snapshots of TrueNAS system data.

Returned by :class:`aiotruenas.system.TrueNASSystem`. Every field TrueNAS did
not deliver (missing, ``null``, blank or of an unexpected type) is ``None`` --
never a placeholder such as ``0``, ``"unknown"`` or ``False`` -- so a consumer
can tell "no data" apart from a real reading. Values are passed through in
TrueNAS's own units (bytes, seconds, kilobits/s, percent, degrees Celsius);
no unit conversion happens here.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType


@dataclass(frozen=True, slots=True, kw_only=True)
class SystemInfo:
    """Static and slowly changing system facts from ``system.info``."""

    hostname: str | None
    version: str | None
    manufacturer: str | None
    product: str | None
    serial: str | None
    #: Installed physical memory in bytes.
    physical_memory: int | None
    #: Boot time (UTC), from ``system.info``'s ``boottime``.
    boot_time: datetime | None
    uptime_seconds: float | None
    #: 1/5/15-minute load averages, from ``system.info``'s ``loadavg``.
    load_1m: float | None
    load_5m: float | None
    load_15m: float | None
    #: Whether the system runs virtualized, by a heuristic over manufacturer/
    #: product: ``True`` for a recognised hypervisor, ``False`` for any other
    #: reported hardware (including an unrecognised hypervisor), ``None`` if
    #: neither is reported.
    is_virtual: bool | None


@dataclass(frozen=True, slots=True, kw_only=True)
class NetworkInterface:
    """One network interface from ``interface.query``."""

    id: str
    name: str | None
    description: str | None
    mtu: int | None
    #: Raw TrueNAS link state, e.g. ``"LINK_STATE_UP"``.
    link_state: str | None
    #: ``True`` for ``"LINK_STATE_UP"``, ``False`` for ``"LINK_STATE_DOWN"``,
    #: ``None`` for any other or a missing state.
    link_up: bool | None
    active_media_type: str | None
    active_media_subtype: str | None
    link_address: str | None


@dataclass(frozen=True, slots=True, kw_only=True)
class InterfaceTraffic:
    """Current throughput of one network interface, in kilobits per second."""

    rx_kbps: float | None
    tx_kbps: float | None


@dataclass(frozen=True, slots=True, kw_only=True)
class SystemStats:
    """Current CPU/memory/ARC/network readings from ``reporting.netdata_graph``.

    Each value is the mean over the queried time window.
    """

    cpu_usage_percent: float | None
    #: Hottest CPU core/sensor in degrees Celsius; ``None`` on a VM (not
    #: queried) or when no sensor reports a value.
    cpu_temperature: float | None
    #: Available memory in bytes.
    memory_available: int | None
    #: Used memory in percent of physical memory; ``None`` unless the
    #: physical memory size was known (see ``TrueNASSystem.get_system_stats``).
    memory_usage_percent: float | None
    #: ZFS ARC size in bytes.
    arc_size: int | None
    #: Throughput per interface, keyed by interface id (``NetworkInterface.id``);
    #: a read-only copy of whatever mapping was passed in.
    interfaces: Mapping[str, InterfaceTraffic]

    def __post_init__(self) -> None:
        # frozen=True blocks normal assignment; copy so the snapshot can't be
        # changed through the caller's original dict either.
        object.__setattr__(self, "interfaces", MappingProxyType(dict(self.interfaces)))
