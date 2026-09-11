"""Normalized, cached TrueNAS domain state built on top of a TrueNASClient.

``TrueNASState`` composes a :class:`~aiotruenas.client.TrueNASClient` (rather
than subclassing it, keeping the client itself a thin transport layer) and
exposes one ``async def get_<endpoint>()`` method per TrueNAS RPC endpoint.
Each method queries the endpoint, normalizes the response via
``domain._normalize.parse_api`` and the field specs in ``domain._specs``, and
caches the result in ``self.ds[<endpoint>]`` -- the same dict-keyed-by-id
shape historically produced by consumer integrations' own
``apiparser.py``/``coordinator.py``. Exceptions are the netdata-graph-backed
endpoints (``arc``, ``ups``), ``system_info`` (a flat singleton -- there is
only ever one system), and the hand-aggregated ``alerts``/``update``/``smb``
endpoints, none of which have a natural object id and instead cache a flat
dict.
"""

from __future__ import annotations

import asyncio
import copy
from collections.abc import Callable, Hashable, Mapping
from datetime import UTC, datetime
from logging import getLogger
from typing import Any, TypedDict, cast

from ..client import TrueNASClient
from ..exceptions import TrueNASError
from ._helpers import (
    _aggregate_topology_errors,
    _arc_value,
    _cpuset_size,
    _disk_temps_from_graph_data,
    _find_disk_temp_graph_name,
    _first_ipv4,
    _has_disk_temp_entries,
    _has_netdata_series_entry,
    _is_finite_number,
    _is_virtual_machine,
    _netdata_interface_throughput,
    _netdata_max_mean,
    _netdata_named_means,
    _parse_version_tuple,
    _stable_uptime_epoch,
    _to_int,
    _ups_value,
)
from ._normalize import get_uid, parse_api
from ._specs import (
    _APP_ENSURE_VALS,
    _APP_VALS,
    _CERTIFICATE_VALS,
    _CLOUDSYNC_VALS,
    _CONTAINER_ENSURE_VALS,
    _CONTAINER_V26_ENSURE_VALS,
    _CONTAINER_V26_VALS,
    _CONTAINER_VALS,
    _CRONJOB_ENSURE_VALS,
    _CRONJOB_VALS,
    _DATASET_VALS,
    _DIRECTORYSERVICES_ENSURE_VALS,
    _DIRECTORYSERVICES_VALS,
    _DISK_ENSURE_VALS,
    _DISK_VALS,
    _INTERFACE_ENSURE_VALS,
    _INTERFACE_VALS,
    _POOL_ENSURE_VALS,
    _POOL_VALS,
    _REPLICATION_VALS,
    _RSYNC_VALS,
    _SCRUB_VALS,
    _SERVICE_ENSURE_VALS,
    _SERVICE_VALS,
    _SNAPSHOTTASK_VALS,
    _SYSTEMINFO_ENSURE_VALS,
    _SYSTEMINFO_VALS,
    _VM_ENSURE_VALS,
    _VM_VALS,
)

_LOGGER = getLogger(__name__)

_EndpointMap = dict[Hashable, dict[str, Any]]
#: get_arc() shape: one entry per known metric, None where currently unavailable.
_ArcMap = dict[str, float | None]
#: get_ups() shape: one entry per currently-discovered UPS graph. A graph
#: that fails or returns an unusable reading keeps its previous value here
#: (see ups_stale_graphs) rather than being omitted; a graph no longer
#: discovered at all is dropped. Never None -- a metric with no prior
#: reading and a failing/unusable current one is simply absent.
_UpsMap = dict[str, float]
#: get_alerts() shape: aggregated counters/messages, no natural object id.
_AlertsMap = dict[str, Any]
#: get_update() shape: pending-update status fields, no natural object id.
_UpdateMap = dict[str, Any]
#: get_smb() shape: single connection-count reading, no natural object id.
_SmbMap = dict[str, int]
#: get_systeminfo()/get_systemstats() shape: flat singleton system fields
#: (hostname, uptime, CPU/load/memory/ARC-size readings), no natural object
#: id -- there is only ever one system.
_SystemInfoMap = dict[str, Any]
#: Public shape of the ``ds`` property: a plain mapping (rather than the
#: TypedDict used internally) so consumers can index it with a runtime
#: string, e.g. when iterating over endpoint names.
_PublicStateMap = dict[
    str,
    _EndpointMap
    | _ArcMap
    | _UpsMap
    | _AlertsMap
    | _UpdateMap
    | _SmbMap
    | _SystemInfoMap,
]


class _StateMap(TypedDict):
    """Per-key shape of ``self._ds``: id-keyed maps, except the flat
    ``arc``/``ups``/``alerts``/``update``/``smb``/``system_info`` entries,
    which have no natural object id.
    """

    pool: _EndpointMap
    dataset: _EndpointMap
    cloudsync: _EndpointMap
    replication: _EndpointMap
    rsynctask: _EndpointMap
    snapshottask: _EndpointMap
    cronjob: _EndpointMap
    service: _EndpointMap
    vm: _EndpointMap
    container: _EndpointMap
    app: _EndpointMap
    certificate: _EndpointMap
    directoryservices: _EndpointMap
    interface: _EndpointMap
    disk: _EndpointMap
    scrub: _EndpointMap
    arc: _ArcMap
    ups: _UpsMap
    alerts: _AlertsMap
    update: _UpdateMap
    smb: _SmbMap
    system_info: _SystemInfoMap


# Maps a netdata graph name (``reporting.netdata_graphs``) to its ds["arc"] field.
_ARC_GRAPHS: dict[str, str] = {
    "demanddatahitpercentage": "data_hit_percent",
    "demandmetadatahitpercentage": "metadata_hit_percent",
    "l2architpercentage": "l2_hit_percent",
}

# Maps a netdata graph name (``reporting.netdata_graphs``) to its ds["ups"] field.
_UPS_GRAPHS: dict[str, str] = {
    "upscharge": "battery_charge",
    "upsruntime": "runtime_seconds",
    "upsload": "load",
    "upsvoltage": "voltage",
    "upscurrent": "current",
    "upsfrequency": "frequency",
    "upstemperature": "temperature",
}

# Netdata graphs queried by get_systemstats(), each independently (matching
# get_arc()/get_ups()'s per-graph query pattern) rather than the original
# coordinator's single combined multi-graph batch. "interface" is queried
# separately (see get_systemstats()) since it enriches ds["interface"]
# rather than ds["system_info"], and only when that map is non-empty.
_SYSTEMSTATS_GRAPHS: tuple[str, ...] = ("load", "cpu", "cputemp", "memory", "arcsize")

# RPC method queried by get_systeminfo() and, as a lazy fallback, by
# _detect_version()/_detect_virtual() -- a shared constant avoids the
# duplicated-literal finding (SonarQube S1192) across those three call sites.
_SYSTEM_INFO_METHOD = "system.info"

# RPC method used to fetch a single netdata graph's data points, queried by
# get_arc(), _query_ups_graphs(), and the disk-temperature/interface-throughput
# refresh paths -- a shared constant avoids the duplicated-literal finding
# (SonarQube S1192) across those five call sites.
_NETDATA_GRAPH_METHOD = "reporting.netdata_graph"

# _note_fallback_outcome() keys, one per cached-fallback code path. Extracted
# as constants (rather than inline literals at each call site) to avoid the
# duplicated-literal finding (SonarQube S1192) -- several of these keys are
# already referenced 3+ times per file -- and because a typo in a duplicated
# literal would silently misroute a failing/recovered pair to a dead key that
# is never checked, defeating the point of the fallback-tracking mechanism.
_KEY_POOL_QUERY = "pool_query"
_KEY_UPS_NETDATA_GRAPHS = "ups_netdata_graphs"
_KEY_DIRECTORYSERVICES_CONFIG = "directoryservices_config"
_KEY_DIRECTORYSERVICES_STATUS = "directoryservices_status"
_KEY_ALERT_LIST = "alert_list"
_KEY_SMB_STATUS = "smb_status"
_KEY_DISK_TEMPERATURE_UPDATE_UNEXPECTED = "disk_temperature_update_unexpected"
_KEY_DISK_TEMP_NETDATA = "disk_temp_netdata"
_KEY_DISK_TEMP_FALLBACK = "disk_temp_fallback"
_KEY_DETECT_VIRTUAL = "detect_virtual"
# Shared between _detect_virtual() and get_systeminfo(), the two call sites
# that populate self._is_virtual from the same 'system.info'
# 'system_manufacturer'/'system_product' fields -- see _VERSION_DETECT_WARNING
# below for why this is a shared constant rather than inline literals.
_VIRTUAL_DETECT_WARNING = (
    "'system.info' response missing or unusable manufacturer/product fields "
    "needed to detect virtualization status: %s"
)
_VIRTUAL_DETECT_RECOVERED = "Virtualization status detection recovered"
_KEY_DETECT_VERSION = "detect_version"
_KEY_INTERFACE_THROUGHPUT = "interface_throughput"
# Shared between _detect_version() and get_systeminfo(), the two call sites
# that populate self._version from the same 'system.info' 'version' field --
# a shared constant avoids the duplicated-literal finding (SonarQube S1192)
# and keeps the warning text consistent regardless of which site first
# observes a missing/unparsable version string.
_VERSION_DETECT_WARNING = (
    "Failed to detect TrueNAS version from 'system.info' (missing/"
    "unparsable 'version' field: %s); version-gated endpoints (e.g. "
    "get_container()) will use legacy behavior until this recovers"
)
_VERSION_DETECT_RECOVERED = "TrueNAS version detection recovered"
_KEY_SYSTEM_INFO = "system_info"
_SYSTEM_INFO_MALFORMED_WARNING = "Malformed 'system.info' response: %s"
_SYSTEM_INFO_RECOVERED = "'system.info' response recovered"
_KEY_DATASET_QUERY = "dataset_query"
_KEY_INTERFACE_QUERY = "interface_query"
_KEY_SCRUB_QUERY = "scrub_query"
_KEY_SERVICE_QUERY = "service_query"
_KEY_VM_QUERY = "vm_query"
_KEY_BOOT_POOL = "boot_pool"
_KEY_POOL_CAPACITY_DATASET = "pool_capacity_dataset"
# Separate from _KEY_DATASET_QUERY on purpose: that key tracks
# 'pool.dataset.query's own raw outcome (set by _compute_dataset(), which
# assumes its result will be published) and can legitimately flip back to
# "recovered" every poll while 'pool.query' stays malformed -- reusing it in
# get_pool()'s early-return branch below would re-warn on every single poll
# of a persistent 'pool.query' outage instead of once per transition,
# breaking _note_fallback_outcome()'s "warn only on the failing transition"
# contract. This key instead tracks whether "dataset" was actually
# (re-)published this refresh -- by *either* get_pool() (whose early-return
# branch sets it) or get_dataset() (which clears it on every call,
# regardless of whether the underlying 'pool.dataset.query' itself
# succeeded -- _KEY_DATASET_QUERY tracks that outcome separately -- since
# get_dataset() is the only other call site that ever writes
# self._ds["dataset"]).
#
# Both get_pool() and get_dataset() write self._fallback_failing[this key]
# directly rather than through _note_fallback_outcome(): a real consumer
# (e.g. a coordinator's poll loop) calls both every single poll, in a fixed
# get_dataset()-then-get_pool() order, so this key's own value flips on
# every poll of a persistent 'pool.query' outage regardless of whether the
# underlying condition ever actually changes -- it is not a meaningful
# "transition" to warn or recover-log on. The warning/recovery log lines
# for this condition are instead driven entirely by _KEY_POOL_QUERY's own
# transition (checked explicitly in get_pool()), which does reflect the
# real, once-per-outage event.
#
# Known over-reporting tradeoff: get_pool()'s early-return branch sets this
# key unconditionally, without checking whether a get_dataset() call already
# republished "dataset" earlier in the same poll (the real coordinator order)
# -- so "dataset" can show up in stale_endpoints for the whole 'pool.query'
# outage even though the data it points at is, in fact, current. Accepted
# rather than chased further: distinguishing "truly never republished" from
# "republished moments ago by a sibling call" would need its own generation
# counter across two call sites, for a signal no current consumer reads yet
# (see stale_endpoints's own docstring for the equivalent, deliberate
# get_systeminfo() gap).
_KEY_DATASET_NOT_PUBLISHED = "dataset_not_published"
# Prefix for get_ups()'s dynamic per-graph fallback keys (e.g.
# "ups_graph:upscharge") -- a module-level constant rather than a fixed
# _KEY_* value since the graph name is only known at runtime, but still
# extracted so the prefix can't drift between where it's built and where
# it's matched.
_UPS_GRAPH_KEY_PREFIX = "ups_graph:"
# Prefix for get_systemstats()'s dynamic per-graph fallback keys (e.g.
# "systemstat:cpu") -- like _UPS_GRAPH_KEY_PREFIX, a runtime-built key, but
# extracted so the prefix can't drift between where it is built
# (_refresh_systemstat_graphs()) and _NON_ENDPOINT_FALLBACK_KEY_PREFIXES,
# where it is listed as an intentional non-endpoint key for stale_endpoints.
_SYSTEMSTAT_KEY_PREFIX = "systemstat:"

# Maps a _note_fallback_outcome() key that guards an endpoint's *primary*
# RPC result to that endpoint's public ``ds`` name, so ``stale_endpoints``
# can report -- per endpoint -- when the last refresh served the previous
# cached snapshot instead of a fresh reading.
#
# Only *primary-result* keys belong here. A key guarding best-effort
# enrichment layered onto an otherwise-fresh primary result (disk
# temperatures, interface throughput, the systemstats netdata graphs, a
# single one of get_ups()'s several per-graph queries) or an internal
# capability detection (TrueNAS version, virtualization) is listed in
# _NON_ENDPOINT_FALLBACK_KEYS / _NON_ENDPOINT_FALLBACK_KEY_PREFIXES instead
# and deliberately does *not* flag its endpoint: on hardware that never
# reports the optional metric (a disk with no temp sensor, a NUT driver
# that never yields "upscurrent", a dmidecode-less container) that path
# fails on every poll forever, which would pin the whole endpoint -- and
# every entity a consumer derives from it -- to a permanent stale state
# even though the primary result is perfectly fresh. That staleness stays
# visible through the finer ``systemstats_stale_graphs`` /
# ``ups_stale_graphs`` (which are field-based -- they only fire when a
# value that *did* exist went stale) only. A test asserts every _KEY_* /
# prefix constant is classified exactly once, so a renamed or newly added
# key can't silently fall through.
#
# get_ups()'s *discovery* call (_KEY_UPS_NETDATA_GRAPHS) is the exception:
# it gates every UPS field at once, so a failed discovery is a genuine
# primary-result failure for the endpoint and stays mapped here.
_FALLBACK_KEY_ENDPOINTS: dict[str, str] = {
    _KEY_POOL_QUERY: "pool",
    _KEY_BOOT_POOL: "pool",
    # Pool capacity (available/total/usage/size/allocated) is derived from
    # the pool's root dataset (see _apply_pool_capacity()); when
    # 'pool.dataset.query' itself falls back to its cached snapshot, those
    # fields are stale even though 'pool.query' may have refreshed cleanly,
    # so this key flags "pool" too -- a field-level fallback like
    # _KEY_DIRECTORYSERVICES_STATUS, just sourced from a different RPC call.
    _KEY_POOL_CAPACITY_DATASET: "pool",
    _KEY_DATASET_QUERY: "dataset",
    _KEY_DATASET_NOT_PUBLISHED: "dataset",
    _KEY_UPS_NETDATA_GRAPHS: "ups",
    _KEY_DIRECTORYSERVICES_CONFIG: "directoryservices",
    _KEY_DIRECTORYSERVICES_STATUS: "directoryservices",
    _KEY_ALERT_LIST: "alerts",
    _KEY_SMB_STATUS: "smb",
    _KEY_SYSTEM_INFO: "system_info",
    _KEY_INTERFACE_QUERY: "interface",
    _KEY_SCRUB_QUERY: "scrub",
    _KEY_SERVICE_QUERY: "service",
    _KEY_VM_QUERY: "vm",
}

# _note_fallback_outcome() keys/prefixes that intentionally map to *no*
# endpoint -- see _FALLBACK_KEY_ENDPOINTS above for the rationale. Kept as
# explicit sets (rather than "anything not in the map") so the completeness
# test can tell a deliberate omission from a forgotten one.
_NON_ENDPOINT_FALLBACK_KEYS: frozenset[str] = frozenset(
    {
        _KEY_DISK_TEMPERATURE_UPDATE_UNEXPECTED,
        _KEY_DISK_TEMP_NETDATA,
        _KEY_DISK_TEMP_FALLBACK,
        _KEY_INTERFACE_THROUGHPUT,
        _KEY_DETECT_VIRTUAL,
        _KEY_DETECT_VERSION,
    }
)
_NON_ENDPOINT_FALLBACK_KEY_PREFIXES: frozenset[str] = frozenset(
    {_SYSTEMSTAT_KEY_PREFIX, _UPS_GRAPH_KEY_PREFIX}
)


def _apply_cputemp_stat(raw: Any, info: dict[str, Any]) -> bool:
    temp = _netdata_max_mean(raw)
    if temp is None:
        return False
    info["cpu_temperature"] = temp
    return True


def _apply_load_stat(raw: Any, info: dict[str, Any]) -> bool:
    means = _netdata_named_means(raw, ("shortterm", "midterm", "longterm"))
    for series, field in (
        ("shortterm", "load_shortterm"),
        ("midterm", "load_midterm"),
        ("longterm", "load_longterm"),
    ):
        if series in means:
            info[field] = round(means[series], 2)
    return bool(means)


def _apply_cpu_stat(raw: Any, info: dict[str, Any]) -> bool:
    means = _netdata_named_means(raw, ("cpu",))
    if "cpu" not in means:
        return False
    info["cpu_usage"] = round(means["cpu"], 2)
    return True


def _apply_arcsize_stat(raw: Any, info: dict[str, Any]) -> bool:
    means = _netdata_named_means(raw, ("size",))
    if "size" not in means:
        return False
    info["cache_size-arc_value"] = round(means["size"], 2)
    return True


def _apply_memory_stat(raw: Any, info: dict[str, Any]) -> bool:
    means = _netdata_named_means(raw, ("available",))
    if "available" not in means:
        return False
    info["memory-free_value"] = round(means["available"], 2)
    total = info.get("memory-total_value", 0)
    if isinstance(total, (int, float)) and not isinstance(total, bool) and total > 0:
        info["memory-usage_percent"] = round(100 * (total - means["available"]) / total)
    return True


# Dispatch table for _apply_systemstat(): keeps that method's cognitive
# complexity low by moving each graph's field-mapping logic into its own
# small, independently-testable function (SonarQube S3776).
_SYSTEMSTAT_HANDLERS: dict[str, Callable[[Any, dict[str, Any]], bool]] = {
    "cputemp": _apply_cputemp_stat,
    "load": _apply_load_stat,
    "cpu": _apply_cpu_stat,
    "arcsize": _apply_arcsize_stat,
    "memory": _apply_memory_stat,
}

# Maps a service.query "service" id to its human-friendly display name, used
# as a fallback when the API's own "name" field is missing/"unknown".
_SERVICE_DISPLAY_NAMES: dict[str, str] = {
    "afp": "AFP",
    "cifs": "SMB",
    "dynamicdns": "Dynamic DNS",
    "ftp": "FTP",
    "iscsitarget": "iSCSI",
    "lldp": "LLDP",
    "nfs": "NFS",
    "openvpn_client": "OpenVPN Client",
    "openvpn_server": "OpenVPN Server",
    "rsync": "Rsync",
    "s3": "S3",
    "snmp": "SNMP",
    "ssh": "SSH",
    "tftp": "TFTP",
    "ups": "UPS",
    "webdav": "WebDAV",
}


def _is_valid_pool_entry(entry: Any) -> bool:
    """Return True if entry is a dict with a usable (hashable) "guid"."""
    return get_uid(entry, "guid", None, None, None) is not None


class TrueNASState:
    """Normalized TrueNAS domain state, refreshed one endpoint at a time.

    Refreshes are serialized by an internal lock: concurrent ``get_*`` calls
    (e.g. via ``asyncio.gather``) would otherwise interleave and read/write
    each other's intermediate state (most notably ``get_pool()``, which
    depends on a freshly-fetched dataset snapshot to derive pool capacity).
    """

    def __init__(self, client: TrueNASClient) -> None:
        self._client = client
        self._lock = asyncio.Lock()
        self._ds: _StateMap = {
            "pool": {},
            "dataset": {},
            "cloudsync": {},
            "replication": {},
            "rsynctask": {},
            "snapshottask": {},
            "cronjob": {},
            "service": {},
            "vm": {},
            "container": {},
            "app": {},
            "certificate": {},
            "directoryservices": {},
            "interface": {},
            "disk": {},
            "scrub": {},
            "arc": {},
            "ups": {},
            "alerts": {
                "count": 0,
                "messages": [],
                "critical": 0,
                "warning": 0,
                "info": 0,
                "disk_issues": False,
                "uuids": [],
            },
            "update": self._no_update_pending(),
            "smb": {"connections": 0},
            "system_info": {
                "version": "unknown",
                "hostname": "unknown",
                "uptime_seconds": 0,
                "system_serial": "unknown",
                "system_product": "unknown",
                "system_manufacturer": "unknown",
                "physmem": 0,
                "uptimeEpoch": 0,
                "cpu_temperature": None,
                "cpu_usage": 0.0,
                "load_shortterm": 0.0,
                "load_midterm": 0.0,
                "load_longterm": 0.0,
                "cache_size-arc_value": 0.0,
                "memory-free_value": 0.0,
                "memory-total_value": 0.0,
                "memory-usage_percent": 0,
            },
        }
        # Cached (major, minor) TrueNAS version, used by get_container() to
        # pick the right query API; detected lazily on first use (see
        # _detect_version()) since it cannot change without an appliance
        # reboot, which drops the underlying connection. get_systeminfo()
        # also populates this from its own system.info call, sparing
        # _detect_version() a redundant one once it has run.
        self._version: tuple[int, int] | None = None
        # Cached netdata graph name reporting per-disk temperatures, used by
        # get_disk(); "" once discovery has run but found none, None until
        # discovery has run at all (see _disk_temps_from_netdata()).
        self._disk_temp_graph: str | None = None
        # Per-fallback-path failing state, keyed by an arbitrary identifier
        # (e.g. "disk_temp_fallback", "ups_netdata_graphs") -- lets
        # _note_fallback_outcome() warn once on a path's failing transition
        # and once on its recovery instead of every poll for the duration of
        # an outage. A key absent (or False) means that path is not
        # currently failing.
        self._fallback_failing: dict[str, bool] = {}
        # Whether the connected system is virtualized; used by
        # get_systemstats() to skip the CPU-temperature graph, which has no
        # physical sensor on a VM. Populated by get_systeminfo(), or lazily
        # detected on first use by _detect_virtual() if get_systemstats() is
        # called before get_systeminfo() ever has been -- None means "not
        # yet known" (see _detect_virtual()), distinct from a confirmed False.
        self._is_virtual: bool | None = None
        # Names of get_systemstats()'s netdata graphs that failed on the most
        # recent call, leaving their field(s) at the previous value instead
        # of a fresh reading -- see systemstats_stale_graphs.
        self._systemstats_stale_graphs: frozenset[str] = frozenset()
        # Names of get_ups()'s netdata graphs that failed on the most recent
        # call, leaving their field at the previous value instead of a fresh
        # reading -- see ups_stale_graphs.
        self._ups_stale_graphs: frozenset[str] = frozenset()
        # The ds["pool"] key of the boot-pool entry, once _add_boot_pool()
        # has successfully merged one in -- lets a later malformed/empty
        # boot.get_state() response re-carry over that specific cached
        # entry (see _add_boot_pool()) instead of silently dropping it from
        # ds["pool"], the way pool.query's own removed-pool guids can't be
        # told apart from a boot-pool that merely failed to refresh this
        # poll. None until the first successful merge.
        # Typed Hashable rather than str: parse_api()/get_uid() never coerce
        # a guid to str, so a hashable-but-non-str guid (e.g. an int) is
        # technically possible and would be stored as-is.
        self._boot_pool_uid: Hashable | None = None

    @property
    def ds(self) -> _PublicStateMap:
        """Normalized state, keyed by endpoint name then by object id/guid.

        The ``arc``, ``ups``, ``alerts``, ``update``, ``smb``, and
        ``system_info`` endpoints have no natural object id and are keyed by
        endpoint name only, holding a flat dict.

        Typed as a plain mapping rather than the ``TypedDict`` used
        internally, so it can be indexed with a runtime string (e.g. when
        iterating over endpoint names) under static type checking.
        """
        return cast(_PublicStateMap, self._ds)

    @property
    def systemstats_stale_graphs(self) -> frozenset[str]:
        """Names of ``get_systemstats()``'s netdata graphs that failed on the
        most recent call, leaving their field(s) at the previous value
        instead of a fresh reading. Empty after a fully fresh refresh, and
        also empty before ``get_systemstats()`` has ever been called.

        A non-empty result does not mean ``get_systemstats()`` failed -- it
        still returns the current (partially stale) ``ds["system_info"]`` --
        only that some of its fields may be outdated, e.g. because a netdata
        RPC failed transiently or the connection dropped mid-refresh.
        """
        return self._systemstats_stale_graphs

    @property
    def ups_stale_graphs(self) -> frozenset[str]:
        """Names of ``get_ups()``'s netdata graphs that failed on the most
        recent call, leaving their field at the previous value instead of a
        fresh reading. Empty after a fully fresh refresh, when no UPS graphs
        are discovered at all, and before ``get_ups()`` has ever been called.

        A non-empty result does not mean ``get_ups()`` failed -- it still
        returns the current (partially stale) ``ds["ups"]`` -- only that some
        of its fields may be outdated, e.g. because a netdata RPC failed
        transiently. When ``get_ups()`` returns early due to a failed or
        malformed graph *discovery* call, every graph whose field is still
        present in the returned (unrefreshed) snapshot is reported as stale
        here, since nothing was refreshed this poll -- not just the graphs
        that were already flagged stale before that call.

        A graph that fails but has never once produced a reading is *not*
        listed here -- its field is absent from ``ds["ups"]`` entirely rather
        than outdated, so there is no stale value to warn about. An empty
        result therefore means "no field is outdated", not "nothing failed"
        -- unlike ``systemstats_stale_graphs``, whose fields are all seeded at
        construction time and so are always present to begin with.
        """
        return self._ups_stale_graphs

    @property
    def stale_endpoints(self) -> frozenset[str]:
        """``ds`` endpoint names whose most recent refresh used a cached value
        for at least one primary or field-level result instead of a fresh one.

        One-way guarantee: if ``e`` is in the result, the most recent refresh
        attempt for ``e`` could not freshly fetch at least one primary or
        field-level result for ``e`` and carried the corresponding prior
        value over -- not necessarily the whole ``ds[e]`` snapshot left
        unchanged: ``get_directoryservices()`` and ``get_ups()`` still
        refresh every other field from the fresh response and only carry
        over the one piece that failed when the failure is field-level: a
        whole-endpoint failure such as ``get_ups()``'s graph-discovery call
        erroring out leaves ``ds[e]`` untouched -- unwritten, not merely
        unchanged -- for that refresh attempt. The converse does not
        hold in full -- a primary ``get_*`` that fails by *raising* is not
        reflected here (the caller's own error handling already sees it), and
        so are the pre-existing silent-fallback paths that this property does
        not yet map. The reachable names are a subset of ``ds``'s keys --
        ``"pool"``, ``"dataset"``, ``"ups"``, ``"directoryservices"``,
        ``"alerts"``, ``"smb"``, ``"system_info"``, ``"interface"``,
        ``"scrub"``, ``"service"``, ``"vm"``.

        ``"pool"`` is also reported when its capacity figures (available/
        total/usage/size/allocated), derived from the pool's root dataset --
        see ``_apply_pool_capacity()`` -- were computed from a stale
        ``"dataset"`` snapshot, even if ``pool.query`` itself refreshed
        cleanly: a field-level fallback sourced from a different endpoint's
        RPC call, only flagged when at least one pool actually has a root
        dataset to depend on (a boot-pool-only refresh never triggers it).
        Likewise, a malformed/empty ``boot.get_state()`` response no longer
        silently drops the boot-pool from ``ds["pool"]`` when a previous
        entry exists to carry over -- and ``"pool"`` is reported stale for
        that refresh. A response with nothing to carry over (``boot.get_
        state`` has never once succeeded) leaves ``"pool"`` unreported --
        same rationale as ``ups_stale_graphs``: there is no previous value
        to call stale, and permanently pinning ``"pool"`` for a
        boot.get_state that never once works would falsely mark every pool
        entity unavailable even while ``pool.query`` itself stays fresh.

        ``"dataset"`` has one known exception to the one-way guarantee
        above: a malformed ``pool.query`` response always (re-)flags
        ``"dataset"`` too (see :meth:`get_pool`'s malformed-``pool.query``
        branch, and ``_KEY_DATASET_NOT_PUBLISHED``'s own module-level
        comment), even if a direct
        :meth:`get_dataset` call already republished a fully fresh dataset
        map earlier in the very same refresh cycle -- a real consumer
        (e.g. a coordinator's poll loop) that calls both every cycle would
        see ``"dataset"`` reported stale for the whole ``pool.query``
        outage regardless, even though the data it points at is in fact
        current. Distinguishing that case would need its own cross-call
        tracking for a signal no current consumer reads yet, so it is left
        as a deliberate over-report rather than a silent gap.

        Best-effort *enrichment* layered onto an otherwise-fresh primary
        result does **not** count: disk temperatures (``"disk"`` stays out --
        ``disk.query`` is the primary result and raises on a real failure),
        interface throughput specifically (rx/tx, enriched by
        ``get_systemstats()``'s netdata queries onto an already-fresh
        ``ds["interface"]`` -- distinct from ``interface.query`` itself,
        which *is* covered), the systemstats netdata graphs (CPU/load/
        memory/ARC-size -- they enrich an already-fresh ``ds["system_info"]``),
        and a single one of ``get_ups()``'s per-graph queries failing while
        discovery still succeeds. Nor does internal capability detection
        (TrueNAS version / virtualization). Those stay observable only through
        :attr:`systemstats_stale_graphs` / :attr:`ups_stale_graphs` -- the
        finer, per-graph counterparts this sits above. Mapping them to an
        endpoint here would pin that endpoint (and every entity a consumer
        derives from it) to a permanent stale state on hardware that simply
        never reports the optional metric.

        ``"ups"`` is the one place the two layers meet: it is reported here
        both when the graph *discovery* call fails (a genuine all-or-nothing
        primary failure) and when :attr:`ups_stale_graphs` is non-empty. The
        latter is safe to fold in because that set is field-based and
        self-clearing -- it only lists graphs whose value *did* exist and
        went stale, never a graph that has failed since the first poll.

        Exists for a consumer (e.g. a Home Assistant coordinator applying the
        ``entity-unavailable`` / ``log-when-unavailable`` quality-scale
        rules): the ``get_*`` methods for these endpoints do **not** raise
        when the primary RPC errors (``get_smb()`` / ``get_ups()``) or
        returns a malformed payload -- they log once via
        :meth:`_note_fallback_outcome` and return the previous snapshot -- so
        this property is the only signal that the data went stale.
        ``get_systeminfo()`` is narrower here than the other five: it only
        treats a *non-dict* ``system.info`` response as malformed, so a
        structurally-empty ``{}`` response is accepted as-is and will not be
        reflected in ``stale_endpoints`` -- a known pre-existing gap, not
        introduced by this property.

        ``"arc"`` is deliberately absent: :meth:`get_arc` does not swallow a
        failure into a cached value -- a ``TrueNASError`` propagates to the
        caller, and a malformed-but-non-raising response writes ``None`` for
        the affected field rather than serving a stale one.
        """
        stale: set[str] = {
            endpoint
            for key, failing in self._fallback_failing.items()
            if failing and (endpoint := self._fallback_endpoint(key)) is not None
        }
        if self._ups_stale_graphs:
            stale.add("ups")
        return frozenset(stale)

    @staticmethod
    def _fallback_endpoint(key: str) -> str | None:
        """Resolve a ``self._fallback_failing`` key to its ``ds`` endpoint name.

        Returns ``None`` for a key that maps to no endpoint -- both an
        unrecognized key and a deliberately non-endpoint one (enrichment /
        capability detection, see ``_NON_ENDPOINT_FALLBACK_KEYS`` /
        ``_NON_ENDPOINT_FALLBACK_KEY_PREFIXES``). A test asserts every known
        ``_KEY_*`` / prefix constant is classified on purpose either way.
        """
        return _FALLBACK_KEY_ENDPOINTS.get(key)

    async def get_dataset(self) -> _EndpointMap:
        """Refresh and return normalized ZFS datasets (``pool.dataset.query``)."""
        async with self._lock:
            self._ds["dataset"] = await self._compute_dataset()
            # This call just published a dataset map directly, so any
            # earlier _KEY_DATASET_NOT_PUBLISHED (set by get_pool()'s
            # early-return branch) no longer applies -- cleared directly,
            # bypassing _note_fallback_outcome()'s own log-on-transition
            # tracking, for the reasons given at that key's own definition
            # above.
            self._fallback_failing.pop(_KEY_DATASET_NOT_PUBLISHED, None)
            return self._ds["dataset"]

    async def _compute_dataset(self) -> _EndpointMap:
        """Return a freshly computed dataset map, without publishing it.

        Starts from a copy of the current dataset cache (rather than an empty
        dict) so a ``None``/malformed ``pool.dataset.query`` response makes
        ``parse_api()`` preserve the previous snapshot instead of collapsing
        it to empty; a genuine, non-empty response still ends up containing
        only its own entries, since ``parse_api()`` prunes anything absent
        from it.

        Notes the outcome under ``_KEY_DATASET_QUERY`` (mapped to the
        ``"dataset"`` endpoint) so a malformed/failed response is reflected
        in ``stale_endpoints`` -- this call site had no such tracking before,
        unlike every other primary RPC result in this module.

        Caller must hold ``self._lock`` and is responsible for publishing the
        result to ``self._ds["dataset"]``.
        """
        raw = await self._client.call("pool.dataset.query")
        self._note_primary_query_outcome(_KEY_DATASET_QUERY, raw, "pool.dataset.query")
        return parse_api(
            data=copy.deepcopy(self._ds["dataset"]),
            source=raw,
            key="id",
            vals=_DATASET_VALS,
        )

    async def get_pool(self) -> _EndpointMap:
        """Refresh and return normalized pools (``pool.query`` + boot-pool).

        Refreshes datasets first: a pool's usable capacity is derived from its
        root dataset's available/used figures (matching the TrueNAS WebUI),
        which requires up-to-date dataset data.

        Both the dataset and pool maps are built up on local (deep-copied)
        snapshots and only published to ``self._ds`` once the primary
        ``pool.query`` step has succeeded, so a malformed response there
        leaves the previous, fully-consistent snapshot of both endpoints in
        place instead of a dataset/pool pair that no longer agree with each
        other. The boot-pool lookup (``_add_boot_pool()``) is handled more
        leniently: a malformed/empty ``boot.get_state()`` response there
        does not block publishing -- if a previously cached boot-pool entry
        exists, it carries that entry over instead (see
        ``_add_boot_pool()``) and flags ``"pool"`` in ``stale_endpoints`` to
        reflect the carry-over; if none exists yet (``boot.get_state`` has
        never once succeeded), the boot-pool is simply left out and nothing
        is flagged. Either way this method still publishes the resulting
        snapshot normally.
        """
        async with self._lock:
            datasets = await self._compute_dataset()

            raw_pools = await self._client.call("pool.query")
            if not isinstance(raw_pools, list) or (
                raw_pools and not any(_is_valid_pool_entry(p) for p in raw_pools)
            ):
                # A malformed pool.query response -- not a list at all (e.g.
                # None), or a non-empty list containing no usable pool
                # entries (e.g. [None] or [{}]) -- cannot be trusted to
                # reflect the current pool set; leave the previous
                # dataset/pool snapshot in place rather than pairing it with
                # a freshly refreshed dataset map from this same call. A
                # genuinely empty list ([]) is not malformed -- it means
                # there are no pools left -- so it falls through normally.
                self._note_fallback_outcome(
                    _KEY_POOL_QUERY,
                    failed=True,
                    warning=(
                        "Malformed 'pool.query' response: %s -- 'dataset' "
                        "not refreshed either"
                    ),
                    reason=raw_pools,
                )
                # self._ds["dataset"] is never reassigned below on this path,
                # so "dataset" must be (re-)flagged stale here too -- see
                # _KEY_DATASET_NOT_PUBLISHED's own comment above for why
                # this is written directly rather than through a second
                # _note_fallback_outcome() call.
                self._fallback_failing[_KEY_DATASET_NOT_PUBLISHED] = True
                return self._ds["pool"]
            self._note_fallback_outcome(
                _KEY_POOL_QUERY,
                failed=False,
                recovered="'pool.query' recovered; 'dataset' recovered with it",
            )
            self._fallback_failing.pop(_KEY_DATASET_NOT_PUBLISHED, None)

            pools = parse_api(
                data=copy.deepcopy(self._ds["pool"]),
                source=raw_pools,
                key="guid",
                vals=_POOL_VALS,
                ensure_vals=_POOL_ENSURE_VALS,
            )
            self._apply_pool_errors(pools, raw_pools)
            pools = await self._add_boot_pool(pools)

            # Build a lookup of datasets by their mountpoint so a pool's
            # free/total space can be derived from its root dataset. Matching
            # the pool "path" against the dataset "mountpoint" (e.g.
            # "/mnt/tank") is the primary and most reliable method; the
            # dataset id (which equals the pool name for a root dataset) is
            # used only as a fallback.
            dataset_by_mountpoint: dict[str, dict[str, Any]] = {
                dataset["mountpoint"]: dataset
                for dataset in datasets.values()
                if isinstance(dataset.get("mountpoint"), str)
                and dataset["mountpoint"] not in ("", "unknown")
            }

            # Whether this refresh's dataset map is itself the previous
            # cached snapshot (pool.dataset.query fell back) rather than a
            # fresh one -- checked once, outside the loop, since it applies
            # equally to every pool that resolves a root dataset below.
            dataset_query_stale = self._fallback_failing.get(_KEY_DATASET_QUERY, False)
            stale_capacity_uids: list[str] = []

            for uid, vals in pools.items():
                # A malformed "path"/"name" (e.g. a list, from a corrupted
                # API response) is unhashable and would raise TypeError from
                # dict.get() below.
                path = vals.get("path")
                root_dataset = (
                    dataset_by_mountpoint.get(path) if isinstance(path, str) else None
                )
                if root_dataset is None:
                    name = vals.get("name")
                    if isinstance(name, Hashable):
                        root_dataset = datasets.get(name)

                if root_dataset is not None and dataset_query_stale:
                    stale_capacity_uids.append(str(uid))

                self._apply_pool_capacity(pools, uid, vals, root_dataset)

                # pool.query reports fragmentation as a percentage string
                # (e.g. "48").
                pools[uid]["fragmentation"] = _to_int(vals.get("fragmentation"))

            # Flag "pool" itself only when at least one pool actually used a
            # stale root dataset for its capacity figures -- e.g. a
            # boot-pool-only refresh has no root dataset to begin with, so a
            # stale dataset snapshot never actually reached any pool's
            # capacity that poll and shouldn't be reported as if it had.
            self._note_fallback_outcome(
                _KEY_POOL_CAPACITY_DATASET,
                failed=bool(stale_capacity_uids),
                warning=(
                    "Pool capacity computed from a stale 'pool.dataset.query' "
                    "snapshot for pool(s): %s"
                ),
                recovered="Pool capacity dataset dependency recovered",
                reason=stale_capacity_uids,
            )

            self._ds["dataset"] = datasets
            self._ds["pool"] = pools
            return pools

    async def _add_boot_pool(self, pools: _EndpointMap) -> _EndpointMap:
        """Return ``pools`` with the boot-pool merged in, if present.

        ``pool.query`` does not include the boot-pool; ``boot.get_state``
        reports it with the same top-level shape (name/status/healthy/scan/
        size/allocated/free/fragmentation), so it is parsed with the same
        field mapping. It has no root dataset, so the capacity falls back to
        the pool's own free/size (handled in ``_apply_pool_capacity``).

        A malformed/empty response carries over the previously cached
        boot-pool entry (tracked via ``self._boot_pool_uid``), if one
        exists, instead of silently omitting it from ``pools`` -- unlike
        every other primary RPC result in this module, this call site used
        to drop its entity outright on failure rather than keeping the last
        known snapshot, and noted no fallback outcome at all. Notes
        ``_KEY_BOOT_POOL`` (mapped to the ``"pool"`` endpoint) only when a
        carry-over actually happens -- not on every malformed response --
        matching ``stale_endpoints``'s own "carried the corresponding prior
        value over" contract and ``ups_stale_graphs``'s precedent that a
        value which has never once existed has nothing stale to report (a
        ``boot.get_state`` that has never once succeeded, e.g. early in this
        object's lifetime, would otherwise pin ``"pool"`` permanently stale
        even while ``pool.query`` itself keeps refreshing cleanly).

        The ``guid`` used as ``self._boot_pool_uid`` -- and as the merge key
        below -- is resolved via :func:`get_uid`, the same resolver
        :func:`parse_api` itself uses internally, so a response that would
        leave ``parse_api()`` unable to find a usable uid (an unhashable
        ``guid``/derived ``"name"``, or an explicit ``None`` one -- e.g.
        ``isinstance(None, Hashable)`` is ``True``, so a bare ``Hashable``
        check alone would miss it) is treated identically here, rather than
        being merged as a bogus "success" or stored into
        ``self._boot_pool_uid`` and breaking every later carry-over attempt.
        """
        raw_boot = await self._client.call("boot.get_state")
        if isinstance(raw_boot, dict) and raw_boot:
            # boot.get_state carries no guid/id; use the pool name as a
            # stable key.
            raw_boot.setdefault("guid", raw_boot.get("name", "boot-pool"))
            raw_boot.setdefault("id", raw_boot.get("name", "boot-pool"))
        guid = get_uid(raw_boot, "guid", None, None, None)

        if guid is None:
            carried_over = False
            if self._boot_pool_uid is not None:
                cached = self._ds["pool"].get(self._boot_pool_uid)
                if cached is not None:
                    pools[self._boot_pool_uid] = copy.deepcopy(cached)
                    carried_over = True
            if carried_over:
                self._note_fallback_outcome(
                    _KEY_BOOT_POOL,
                    failed=True,
                    warning="Malformed or empty 'boot.get_state' response: %s",
                    reason=raw_boot,
                )
            return pools

        self._boot_pool_uid = guid
        pools = parse_api(
            data=pools,
            source=raw_boot,
            key="guid",
            vals=_POOL_VALS,
            ensure_vals=_POOL_ENSURE_VALS,
            prune=False,
        )
        self._apply_pool_errors(pools, [raw_boot])
        self._note_fallback_outcome(
            _KEY_BOOT_POOL, failed=False, recovered="'boot.get_state' recovered"
        )
        return pools

    def _apply_pool_capacity(
        self,
        pools: _EndpointMap,
        uid: Hashable,
        vals: dict[str, Any],
        root_dataset: dict[str, Any] | None,
    ) -> None:
        """Set available/total/usage (and size/allocated) for a single pool.

        Prefers the root dataset's available/used values (matching the
        figures shown in the TrueNAS UI) and falls back to the pool's own
        free/size fields when no root dataset is available (e.g. boot-pool).

        When the root dataset is used, size/allocated are overwritten with
        the usable figures too, so they match the UI for parity layouts
        (raidz) instead of the raw pool.query capacity that counts parity
        disks.
        """
        # _to_int() also doubles as safety here: a malformed non-numeric
        # value (str, list, dict, ...) would otherwise either raise from the
        # arithmetic below or, worse, silently produce nonsense via string
        # concatenation instead of addition.
        if root_dataset:
            available = _to_int(root_dataset.get("available"))
            used = _to_int(root_dataset.get("used"))
            total = available + used
            pools[uid]["size"] = total
            pools[uid]["allocated"] = used
        else:
            available = _to_int(vals.get("free"))
            total = _to_int(vals.get("size")) or (
                _to_int(vals.get("allocated")) + available
            )

        pools[uid]["available"] = available
        pools[uid]["total"] = total
        pools[uid]["usage"] = (
            round((total - available) / total * 100) if total > 0 else 0
        )

    def _apply_pool_errors(self, pools: _EndpointMap, raw_pools: Any) -> None:
        """Aggregate read/write/checksum errors from each pool's topology."""
        if not isinstance(raw_pools, list):
            return

        for raw_pool in raw_pools:
            if not isinstance(raw_pool, dict):
                continue
            uid = raw_pool.get("guid")
            # A malformed guid (e.g. a list/dict) is unhashable and would
            # raise TypeError on the membership check below.
            if not isinstance(uid, Hashable) or uid not in pools:
                continue

            read, write, checksum = _aggregate_topology_errors(raw_pool.get("topology"))
            pool = pools[uid]
            pool["read_errors"] = read
            pool["write_errors"] = write
            pool["checksum_errors"] = checksum
            pool["errors"] = read + write + checksum

    async def get_cloudsync(self) -> _EndpointMap:
        """Refresh and return normalized cloud-sync tasks (``cloudsync.query``)."""
        async with self._lock:
            self._ds["cloudsync"] = parse_api(
                data=self._ds["cloudsync"],
                source=await self._client.call("cloudsync.query"),
                key="id",
                vals=_CLOUDSYNC_VALS,
            )
            return self._ds["cloudsync"]

    async def get_replication(self) -> _EndpointMap:
        """Refresh and return normalized replication tasks (``replication.query``).

        Prefers the persistent task state (``state/state``, what the TrueNAS
        WebUI shows) over the last job's state, falling back to the job state
        only when the task state is missing/unknown; the fallback-only
        ``job_state`` field is dropped afterwards so it doesn't leak out as a
        stray attribute.
        """
        async with self._lock:
            self._ds["replication"] = parse_api(
                data=self._ds["replication"],
                source=await self._client.call("replication.query"),
                key="id",
                vals=_REPLICATION_VALS,
            )
            for vals in self._ds["replication"].values():
                # A malformed persistent state (e.g. an explicit null at
                # "state/state") resolves to None rather than being absent, so
                # .get("state", "unknown") returns None and skips the fallback
                # below unless the non-string case is checked for explicitly.
                if not isinstance(vals.get("state"), str) or vals["state"] == "unknown":
                    vals["state"] = vals.get("job_state", "unknown")
                vals.pop("job_state", None)
            return self._ds["replication"]

    async def get_rsync(self) -> _EndpointMap:
        """Refresh and return normalized rsync tasks (``rsynctask.query``)."""
        async with self._lock:
            self._ds["rsynctask"] = parse_api(
                data=self._ds["rsynctask"],
                source=await self._client.call("rsynctask.query"),
                key="id",
                vals=_RSYNC_VALS,
            )
            return self._ds["rsynctask"]

    async def get_snapshottask(self) -> _EndpointMap:
        """Refresh and return snapshot tasks (``pool.snapshottask.query``)."""
        async with self._lock:
            self._ds["snapshottask"] = parse_api(
                data=self._ds["snapshottask"],
                source=await self._client.call("pool.snapshottask.query"),
                key="id",
                vals=_SNAPSHOTTASK_VALS,
            )
            return self._ds["snapshottask"]

    async def get_cronjob(self) -> _EndpointMap:
        """Refresh and return normalized cron jobs (``cronjob.query``).

        Derives a human-friendly ``display_name``: the description, falling
        back to the command, falling back to a generic "Cronjob <id>" label
        for jobs with neither -- matching the TrueNAS WebUI's own fallback.
        """
        async with self._lock:
            self._ds["cronjob"] = parse_api(
                data=self._ds["cronjob"],
                source=await self._client.call("cronjob.query"),
                key="id",
                vals=_CRONJOB_VALS,
                ensure_vals=_CRONJOB_ENSURE_VALS,
            )
            for uid, vals in self._ds["cronjob"].items():
                # A malformed API response can leave a non-string value in
                # "description"/"command" (from_entry() only coerces bool-typed
                # specs, not str-typed ones), which would raise AttributeError
                # from .strip() below.
                description = vals.get("description")
                description = (
                    description.strip() if isinstance(description, str) else ""
                )
                command = vals.get("command")
                command = command.strip() if isinstance(command, str) else ""
                vals["display_name"] = description or command or f"Cronjob {uid}"
            return self._ds["cronjob"]

    async def get_arc(self) -> dict[str, float | None]:
        """Refresh and return ZFS ARC hit-ratio percentages from netdata graphs.

        Unlike the other endpoints, this is a flat set of scalar readings
        (``reporting.netdata_graph``) rather than a collection keyed by id.
        """
        async with self._lock:
            report_epoch = int(datetime.now(UTC).replace(microsecond=0).timestamp())
            graph_query = {
                "start": report_epoch - 300,
                "end": report_epoch,
                "aggregate": True,
            }
            arc: dict[str, float | None] = {}
            for graph_name, field_name in _ARC_GRAPHS.items():
                graph_data = await self._client.call(
                    _NETDATA_GRAPH_METHOD, [graph_name, graph_query]
                )
                arc[field_name] = _arc_value(graph_data)
            self._ds["arc"] = arc
            return arc

    def _all_cached_ups_graphs(self) -> frozenset[str]:
        """Names of every UPS graph whose field is currently present in
        ``ds["ups"]``, regardless of ``get_ups()``'s per-poll discovery.

        Used by ``get_ups()``'s discovery-failure paths: when discovery
        itself fails, nothing was refreshed this poll, so every graph
        backing a field still in the (unrefreshed) snapshot must be reported
        as stale -- not just the ones already flagged stale before that
        call. ``_UPS_GRAPHS`` is injective (each graph maps to a distinct
        field), so this reverse lookup is unambiguous.
        """
        return frozenset(
            name for name, field in _UPS_GRAPHS.items() if field in self._ds["ups"]
        )

    def _apply_ups_graph_result(self, graph_name: str, result: Any) -> float | None:
        """Turn one UPS netdata graph's ``asyncio.gather`` result into a value.

        ``None`` on failure or an unusable payload; the two unusable-payload
        cases are handled differently (see below), and the hard-failure
        (raised ``TrueNASError``) case is always recorded via
        ``_note_fallback_outcome()`` before returning, so the caller only
        needs to decide what ``None`` means for ``ups_stale_graphs`` (see
        ``get_ups()``).

        A structurally recognized but empty response (a recognizable
        per-series entry, e.g. matching ``{"name": ..., "identifier": ...}``,
        with no samples/aggregations yet, or an empty top-level list) is
        *not* treated as a failure worth warning about: some UPS/NUT drivers
        simply never report a given metric for a particular device (observed
        for "upscurrent" -- kayl-codes/homeassistant-truenas#142), mirroring
        the disk-temperature fallback's handling of the analogous
        empty-sampling-window shape (kayl-codes/homeassistant-truenas#139,
        see ``_disk_temps_from_netdata()``). Only logged at DEBUG, and any
        failing flag left over from an earlier *real* failure is cleared
        silently (no recovery log -- nothing was actually confirmed working
        this poll). A non-empty response with no recognizable series entry
        at all is a different case -- a malformed payload -- and does follow
        the real-failure path (warn once, set the flag).

        The recognizability check is scoped to ``result[:1]`` only, matching
        ``_ups_value()`` -> ``_netdata_mean_value()``'s own single-entry
        (``graph_data[0]``) parse scope -- unlike the disk-temp graph, a UPS
        netdata graph is single-series, so only the first entry is ever
        actually consulted for a value. Scanning the *whole* list here (as
        ``_has_netdata_series_entry()`` does by default) would let a
        malformed first entry alongside an unrelated well-formed later one
        get misclassified as "recognized but empty", silently clearing a
        failing flag that should stay set.
        """
        key = f"{_UPS_GRAPH_KEY_PREFIX}{graph_name}"
        if isinstance(result, TrueNASError):
            self._note_fallback_outcome(
                key,
                failed=True,
                warning=f"Failed to query '{graph_name}' UPS netdata graph: %s",
                reason=result,
            )
            return None
        if isinstance(result, BaseException):
            raise result
        value = _ups_value(result)
        if value is None:
            if isinstance(result, list) and (
                not result or _has_netdata_series_entry(result[:1])
            ):
                self._fallback_failing.pop(key, None)
                _LOGGER.debug(
                    "'%s' UPS netdata graph returned no usable reading this "
                    "poll (keeping previous value, if any): %s",
                    graph_name,
                    result,
                )
                return None
            self._note_fallback_outcome(
                key,
                failed=True,
                warning=f"Malformed '{graph_name}' UPS netdata graph response: %s",
                reason=result,
            )
            return None
        self._note_fallback_outcome(
            key,
            failed=False,
            recovered=f"'{graph_name}' UPS netdata graph query recovered",
        )
        return value

    async def _query_ups_graphs(
        self, graph_names: list[str], graph_query: dict[str, Any]
    ) -> dict[str, float | None]:
        """Query all given UPS netdata graphs concurrently.

        Mirrors ``_refresh_systemstat_graphs()``'s concurrent-fetch pattern so
        one slow/unresponsive UPS graph cannot delay the others. Maps each
        graph name to ``_apply_ups_graph_result()``'s outcome.
        """
        results = await asyncio.gather(
            *(
                self._client.call(_NETDATA_GRAPH_METHOD, [graph_name, graph_query])
                for graph_name in graph_names
            ),
            return_exceptions=True,
        )
        return {
            graph_name: self._apply_ups_graph_result(graph_name, result)
            for graph_name, result in zip(graph_names, results, strict=True)
        }

    async def get_ups(self) -> dict[str, float]:
        """Refresh and return UPS readings from netdata graphs, if a UPS is present.

        Discovers which UPS graphs TrueNAS currently exposes
        (``reporting.netdata_graphs``) on every call rather than caching the
        result, so a UPS attached or removed at runtime is picked up without
        needing a restart. Returns an empty dict when no UPS graphs exist; on
        a failed discovery call, the previous reading is preserved instead
        (retried on the next call). A graph that is still discovered but
        fails, or returns an unusable reading, on this particular call also
        keeps its previous field value rather than dropping it -- see
        ``ups_stale_graphs`` for which graphs (if any) are stale this way.
        """
        async with self._lock:
            try:
                graphs = await self._client.call("reporting.netdata_graphs")
            except TrueNASError as err:
                self._note_fallback_outcome(
                    _KEY_UPS_NETDATA_GRAPHS,
                    failed=True,
                    warning="Failed to discover UPS netdata graphs: %s",
                    reason=err,
                )
                # Nothing was refreshed this poll -- every field currently in
                # the returned snapshot is stale, not just the previously
                # tracked ones, or a caller would see a full (but frozen)
                # snapshot alongside an empty ups_stale_graphs and mistake it
                # for a fresh one.
                self._ups_stale_graphs = self._all_cached_ups_graphs()
                return self._ds["ups"]
            if not isinstance(graphs, list):
                self._note_fallback_outcome(
                    _KEY_UPS_NETDATA_GRAPHS,
                    failed=True,
                    warning="Malformed 'reporting.netdata_graphs' response: %s",
                    reason=graphs,
                )
                self._ups_stale_graphs = self._all_cached_ups_graphs()
                return self._ds["ups"]
            self._note_fallback_outcome(
                _KEY_UPS_NETDATA_GRAPHS,
                failed=False,
                recovered="UPS netdata graph discovery recovered",
            )

            available = {
                name
                for graph in graphs
                if isinstance(graph, dict)
                and (name := str(graph.get("name", ""))) in _UPS_GRAPHS
            }
            # A UPS graph that no longer appears in this poll's discovery
            # wasn't queried at all this time -- clear its failing flag
            # instead of leaving it stuck forever (it would otherwise never
            # warn again once the graph comes back and fails a second time),
            # mirroring how the other cached-fallback paths in this module
            # clear their key when that path wasn't consulted this poll.
            current_graph_keys = {
                f"{_UPS_GRAPH_KEY_PREFIX}{name}" for name in available
            }
            for stale_key in [
                key
                for key in self._fallback_failing
                if key.startswith(_UPS_GRAPH_KEY_PREFIX)
                and key not in current_graph_keys
            ]:
                self._fallback_failing.pop(stale_key, None)
            if not available:
                self._ds["ups"] = {}
                self._ups_stale_graphs = frozenset()
                return self._ds["ups"]

            report_epoch = int(datetime.now(UTC).replace(microsecond=0).timestamp())
            graph_query = {
                "start": report_epoch - 90,
                "end": report_epoch - 30,
                "aggregate": True,
            }
            # Seed from the previous snapshot (restricted to graphs still
            # discovered this poll) rather than starting empty -- a graph
            # that fails or returns an unusable reading below should leave
            # its field at the previous value, matching
            # _refresh_systemstat_graphs()/_refresh_interface_throughput(),
            # not silently drop it from the result.
            ups: dict[str, float] = {
                field: self._ds["ups"][field]
                for graph_name in available
                if (field := _UPS_GRAPHS[graph_name]) in self._ds["ups"]
            }
            stale: set[str] = set()
            graph_values = await self._query_ups_graphs(list(available), graph_query)
            for graph_name, value in graph_values.items():
                field = _UPS_GRAPHS[graph_name]
                if value is None:
                    # One graph failing (or returning an unusable reading)
                    # shouldn't drop the others -- leave this graph's field
                    # at its previous (seeded) value, same as
                    # _refresh_systemstat_graphs()/_refresh_interface_
                    # throughput() do for the analogous case. Only report it
                    # as stale if a previous value actually exists to be
                    # stale -- a graph that has never once succeeded has no
                    # field in `ups` at all, so flagging it here would make
                    # ups_stale_graphs name a field that is entirely absent
                    # from the result.
                    if field in ups:
                        stale.add(graph_name)
                    continue
                ups[field] = value
            self._ds["ups"] = ups
            self._ups_stale_graphs = frozenset(stale)
            return ups

    async def get_service(self) -> _EndpointMap:
        """Refresh and return normalized services (``service.query``).

        Derives ``running`` from the service state and a ``display_name``
        that falls back to a known human-friendly label (``_SERVICE_DISPLAY_
        NAMES``) when the API's own "name" field is missing/"unknown". A
        malformed/failed response is tracked under ``_KEY_SERVICE_QUERY``
        (mapped to the ``"service"`` endpoint) -- see
        ``_note_primary_query_outcome``.
        """
        async with self._lock:
            raw = await self._client.call("service.query")
            self._note_primary_query_outcome(_KEY_SERVICE_QUERY, raw, "service.query")
            self._ds["service"] = parse_api(
                data=self._ds["service"],
                source=raw,
                key="id",
                vals=_SERVICE_VALS,
                ensure_vals=_SERVICE_ENSURE_VALS,
            )
            for vals in self._ds["service"].values():
                vals["running"] = vals["state"] == "RUNNING"
                name = vals.get("name")
                if not name or name == "unknown":
                    name = _SERVICE_DISPLAY_NAMES.get(
                        vals.get("service"), vals.get("service", "unknown")
                    )
                vals["display_name"] = name
            return self._ds["service"]

    async def get_vm(self) -> _EndpointMap:
        """Refresh and return normalized VMs (``vm.query``).

        A malformed/failed response is tracked under ``_KEY_VM_QUERY``
        (mapped to the ``"vm"`` endpoint) -- see
        ``_note_primary_query_outcome``.
        """
        async with self._lock:
            raw = await self._client.call("vm.query")
            self._note_primary_query_outcome(_KEY_VM_QUERY, raw, "vm.query")
            self._ds["vm"] = parse_api(
                data=self._ds["vm"],
                source=raw,
                key="id",
                vals=_VM_VALS,
                ensure_vals=_VM_ENSURE_VALS,
            )
            for vals in self._ds["vm"].values():
                # Only substitute 0 for a null memory value (e.g. some
                # instance types report None), which would raise a TypeError
                # on division; other invalid types should still surface.
                memory = vals.get("memory")
                if memory is None:
                    memory = 0
                vals["memory"] = round(memory / 1024)
                vals["running"] = vals["status"] == "RUNNING"
            return self._ds["vm"]

    async def _detect_version(self) -> tuple[int, int]:
        """Return the cached (major, minor) TrueNAS version, detecting it on
        first use via ``system.info``.

        The version cannot change without a full appliance reboot, which
        drops the underlying WebSocket connection, so a single successful
        detection is reused for the lifetime of this ``TrueNASState``. A
        malformed (non-dict) ``system.info`` response and a valid response
        with a missing/unparsable ``version`` field both warn once (via
        ``_KEY_DETECT_VERSION``) and are retried on the next call, rather
        than silently defaulting ``get_container()`` to legacy behavior.

        Deliberately does **not** touch ``_KEY_SYSTEM_INFO`` -- the key
        ``get_systeminfo()`` uses for its own primary-refresh tracking, which
        ``stale_endpoints`` maps to the ``"system_info"`` endpoint. This
        method can run (via ``get_container()``) before ``get_systeminfo()``
        has ever refreshed ``ds["system_info"]``, or long after it last did,
        so its own success/failure here says nothing about whether that
        endpoint's last refresh was fresh or fell back to cache; sharing the
        key would let this method's own RPC call flag (or silently clear)
        ``"system_info"`` in ``stale_endpoints`` for a refresh it never
        performed. Mirrors ``_detect_virtual()``/``_apply_virtual_detection()``,
        which likewise only ever touch their own ``_KEY_DETECT_VIRTUAL``.
        """
        if self._version is not None:
            return self._version
        raw = await self._client.call(_SYSTEM_INFO_METHOD)
        if not isinstance(raw, dict):
            self._note_fallback_outcome(
                _KEY_DETECT_VERSION,
                failed=True,
                warning=(
                    "Malformed 'system.info' response while detecting "
                    "TrueNAS version: %s"
                ),
                reason=raw,
            )
            return (0, 0)
        version = _parse_version_tuple(raw.get("version"))
        if version != (0, 0):
            self._version = version
            self._note_fallback_outcome(
                _KEY_DETECT_VERSION, failed=False, recovered=_VERSION_DETECT_RECOVERED
            )
        else:
            self._note_fallback_outcome(
                _KEY_DETECT_VERSION,
                failed=True,
                warning=_VERSION_DETECT_WARNING,
                reason=raw.get("version"),
            )
        return version

    def _apply_virtual_detection(self, raw: Mapping[str, Any]) -> None:
        """Update the cached virtualization flag from a ``system.info`` payload.

        Shared by ``get_systeminfo()`` and ``_detect_virtual()``. Reads
        ``system_manufacturer``/``system_product`` straight from ``raw``, not
        a ``parse_api()``-normalized dict: that would default an absent field
        to the string "unknown", making "field absent" and "server reported
        unknown" indistinguishable and risking a permanently wrong "not
        virtual" default from an incomplete response. Requiring at least one
        field to actually be a non-blank string (not just present) also
        rejects a dmidecode-less container reporting null or "" for both.

        Only warns while ``self._is_virtual`` is not yet cached -- an
        already-cached value (from a prior successful poll, here or via the
        other caller) stays valid and keeps gating ``get_systemstats()``
        correctly, so a later poll's response lacking usable fields is not
        actually the failure the warning text describes. A *good* response is
        always safe to (re-)cache, since hardware/hypervisor identity cannot
        actually change for the lifetime of a running system.
        """
        manufacturer = raw.get("system_manufacturer")
        product = raw.get("system_product")
        manufacturer_usable = isinstance(manufacturer, str) and manufacturer.strip()
        product_usable = isinstance(product, str) and product.strip()
        if manufacturer_usable or product_usable:
            self._is_virtual = _is_virtual_machine(
                manufacturer.strip() if manufacturer_usable else manufacturer,
                product.strip() if product_usable else product,
            )
            self._note_fallback_outcome(
                _KEY_DETECT_VIRTUAL, failed=False, recovered=_VIRTUAL_DETECT_RECOVERED
            )
        elif self._is_virtual is None:
            self._note_fallback_outcome(
                _KEY_DETECT_VIRTUAL,
                failed=True,
                warning=_VIRTUAL_DETECT_WARNING,
                reason=(manufacturer, product),
            )

    async def _detect_virtual(self) -> bool:
        """Return whether the system is virtualized, detecting it on first use.

        Mirrors ``_detect_version()``'s lazy, cached-on-first-use pattern, so
        ``get_systemstats()`` correctly skips the ``cputemp`` graph even when
        called before ``get_systeminfo()`` has ever populated
        ``self._is_virtual``. Like ``_detect_version()``, a malformed
        (non-dict) response is not cached -- caching the resulting "not
        virtual" default from missing manufacturer/product fields would
        otherwise permanently and silently misremember a virtualized system
        as physical for the lifetime of this ``TrueNASState``.

        Owns its own ``_note_fallback_outcome`` tracking (rather than leaving
        it to ``get_systemstats()``) so both a raised error and a malformed
        response are warned about -- not just the former.
        """
        if self._is_virtual is not None:
            return self._is_virtual
        try:
            raw = await self._client.call(_SYSTEM_INFO_METHOD)
        except TrueNASError as err:
            self._note_fallback_outcome(
                _KEY_DETECT_VIRTUAL,
                failed=True,
                warning="Failed to detect virtualization status: %s",
                reason=err,
            )
            return False
        if not isinstance(raw, dict):
            self._note_fallback_outcome(
                _KEY_DETECT_VIRTUAL,
                failed=True,
                warning=(
                    "Malformed 'system.info' response while detecting "
                    "virtualization: %s"
                ),
                reason=raw,
            )
            return False
        self._apply_virtual_detection(raw)
        return bool(self._is_virtual)

    async def get_container(self) -> _EndpointMap:
        """Refresh and return normalized containers.

        Dispatches to ``container.query`` (LXC, TrueNAS 26.0+) or
        ``virt.instance.query`` (legacy Incus) depending on the connected
        TrueNAS version (see ``_detect_version()``). On the legacy API, only
        CONTAINER-type instances are surfaced -- VM-type Incus instances are
        covered by ``get_vm()``.
        """
        async with self._lock:
            if await self._detect_version() >= (26, 0):
                self._ds["container"] = await self._compute_container_v26()
            else:
                self._ds["container"] = await self._compute_container_legacy()
            return self._ds["container"]

    async def _compute_container_legacy(self) -> _EndpointMap:
        """Return containers via ``virt.instance.query`` (pre-TrueNAS-26.0).

        Caller must hold ``self._lock``.
        """
        raw_instances = await self._client.call("virt.instance.query")
        instances = raw_instances if isinstance(raw_instances, list) else []
        containers = [
            instance
            for instance in instances
            if isinstance(instance, dict) and instance.get("type") == "CONTAINER"
        ]

        result = parse_api(
            data=self._ds["container"],
            source=containers,
            key="id",
            vals=_CONTAINER_VALS,
            ensure_vals=_CONTAINER_ENSURE_VALS,
        )
        for vals in result.values():
            # cpu is reported as a string (e.g. "1") and may be null;
            # normalize to an int so the attribute is numeric like memory.
            vals["cpu"] = _to_int(vals.get("cpu"))
            # Container memory is reported in bytes and may be null; show MiB.
            memory = vals.get("memory")
            if not isinstance(memory, (int, float)):
                memory = 0
            vals["memory"] = round(memory / 1048576)
            vals["running"] = vals.get("status") == "RUNNING"
            vals["ip_address"] = _first_ipv4(vals.get("aliases"))
        return result

    async def _compute_container_v26(self) -> _EndpointMap:
        """Return LXC containers via ``container.query`` (TrueNAS 26.0+).

        The entry carries no memory, image or IP information and its status
        is nested (``status/state``); the resulting record keeps the same
        keys as the legacy Incus path so callers see an unchanged shape.

        Caller must hold ``self._lock``.
        """
        raw_containers = await self._client.call("container.query")
        containers = raw_containers if isinstance(raw_containers, list) else []

        result = parse_api(
            data=self._ds["container"],
            source=containers,
            key="id",
            vals=_CONTAINER_V26_VALS,
            ensure_vals=_CONTAINER_V26_ENSURE_VALS,
        )
        for vals in result.values():
            vals["type"] = "CONTAINER"
            vals["cpu"] = _cpuset_size(vals.pop("cpuset", None))
            vals["memory"] = 0
            vals["aliases"] = []
            vals["ip_address"] = "unknown"
            if not vals.get("image"):
                vals["image"] = "unknown"
            vals["running"] = vals.get("status") == "RUNNING"
        return result

    async def get_app(self) -> _EndpointMap:
        """Refresh and return normalized apps (``app.query``).

        Derives ``running`` from the app state and ``update_available`` from
        either a catalog chart upgrade (``upgrade_available``) or, for custom/
        compose apps only, an available container image update -- a
        chart-up-to-date catalog app with a newer image digest should not
        show a phantom update.

        Update-job tracking (``update_jobid``/``update_progress``/...) is
        left to the caller: polling an app's upgrade job is tied to a
        consumer's own HA update-entity handling, not TrueNAS normalization.
        """
        async with self._lock:
            self._ds["app"] = parse_api(
                data=self._ds["app"],
                source=await self._client.call("app.query"),
                key="id",
                vals=_APP_VALS,
                ensure_vals=_APP_ENSURE_VALS,
            )
            for vals in self._ds["app"].values():
                vals["running"] = vals["state"] == "RUNNING"
                vals["update_available"] = bool(vals.get("update_available")) or (
                    bool(vals.get("custom_app"))
                    and bool(vals.get("image_updates_available"))
                )
            return self._ds["app"]

    async def get_certificates(self) -> _EndpointMap:
        """Refresh and return normalized certificates (``certificate.query``).

        Keyed by "name" rather than "id": a manual certificate renewal/
        reissue deletes the old database row and creates a new one with a
        fresh id but the same (database-unique) name, so "name" is the
        stable identity across a renewal.

        Derives ``days_until_expiry`` from the parsed ``until`` timestamp.
        """
        async with self._lock:
            self._ds["certificate"] = parse_api(
                data=self._ds["certificate"],
                source=await self._client.call("certificate.query"),
                key="name",
                vals=_CERTIFICATE_VALS,
            )
            now = datetime.now(UTC)
            for vals in self._ds["certificate"].values():
                until = vals.get("until")
                vals["days_until_expiry"] = (
                    max(0, (until - now).days) if isinstance(until, datetime) else None
                )
            return self._ds["certificate"]

    async def get_directoryservices(self) -> _EndpointMap:
        """Refresh and return directory-service status (AD/LDAP/IPA).

        Uses the unified ``directoryservices`` API (TrueNAS 25.04+):
        ``directoryservices.config`` carries the service type/domain/options,
        ``directoryservices.status`` carries the live state (HEALTHY/FAULTED/
        ...). Both are merged into a single source row before normalizing,
        since there is only ever one row (a real object id is not provided by
        the API). Returns an empty map when no directory service is
        configured/enabled -- querying "status" would be meaningless then.

        Unlike the coordinator method this replaces, gating on whether the
        feature is "monitored" is an HA options-flow concern for the caller,
        not TrueNAS normalization -- this always queries and normalizes.
        """
        async with self._lock:
            config = await self._client.call("directoryservices.config")
            if not isinstance(config, dict):
                # Malformed/failed config refresh: keep the last known
                # snapshot instead of dropping it like a legitimate
                # disabled/unconfigured service would.
                self._note_fallback_outcome(
                    _KEY_DIRECTORYSERVICES_CONFIG,
                    failed=True,
                    warning="Malformed 'directoryservices.config' response: %s",
                    reason=config,
                )
                return self._ds["directoryservices"]
            self._note_fallback_outcome(
                _KEY_DIRECTORYSERVICES_CONFIG,
                failed=False,
                recovered="'directoryservices.config' recovered",
            )
            if not config.get("service_type") or not config.get("enable"):
                self._ds["directoryservices"] = {}
                # The status endpoint wasn't consulted this poll (service
                # disabled/unconfigured) -- clear a stuck failing flag rather
                # than leaving it stuck True forever, or a later status
                # failure would stay silently unwarned. Silent: nothing ran
                # this poll, so there is nothing to report as "recovered".
                self._fallback_failing.pop(_KEY_DIRECTORYSERVICES_STATUS, None)
                return self._ds["directoryservices"]

            raw_status = await self._client.call("directoryservices.status")
            status_field = (
                raw_status.get("status") if isinstance(raw_status, dict) else None
            )
            if isinstance(status_field, str) and status_field:
                status_val = status_field
                status_msg = raw_status.get("status_msg")
                self._note_fallback_outcome(
                    _KEY_DIRECTORYSERVICES_STATUS,
                    failed=False,
                    recovered="'directoryservices.status' recovered",
                )
            else:
                # Malformed/failed status refresh (non-dict, missing
                # "status", or a non-string/empty value): keep the last
                # known status/health instead of falsely reporting
                # "unhealthy".
                previous = self._ds["directoryservices"].get(1, {})
                status_val = previous.get("status", "unknown")
                status_msg = previous.get("status_msg")
                self._note_fallback_outcome(
                    _KEY_DIRECTORYSERVICES_STATUS,
                    failed=True,
                    warning="Malformed 'directoryservices.status' response: %s",
                    reason=raw_status,
                )

            merged = dict(config)
            merged["id"] = 1
            merged["status"] = status_val
            merged["status_msg"] = status_msg

            self._ds["directoryservices"] = parse_api(
                data={},
                source=[merged],
                key="id",
                vals=_DIRECTORYSERVICES_VALS,
                ensure_vals=_DIRECTORYSERVICES_ENSURE_VALS,
            )
            for vals in self._ds["directoryservices"].values():
                vals["healthy"] = vals.get("status") == "HEALTHY"
            return self._ds["directoryservices"]

    async def get_alerts(self) -> _AlertsMap:
        """Refresh and return aggregated alert counters (``alert.list``).

        Unlike the other endpoints, this has no natural object id and is not
        run through ``parse_api()`` -- the entire result is derived by hand:
        dismissed alerts are excluded, counts are aggregated by ``level``,
        and ``disk_issues`` is a heuristic match on ``klass``/``title``
        substrings (disk/pool/smart) flagging disk-related alerts
        specifically.
        """
        async with self._lock:
            raw = await self._client.call("alert.list")
            if not isinstance(raw, list):
                self._note_fallback_outcome(
                    _KEY_ALERT_LIST,
                    failed=True,
                    warning="Malformed 'alert.list' response: %s",
                    reason=raw,
                )
                return self._ds["alerts"]

            usable = [
                alert for alert in raw if isinstance(alert, dict) and alert.get("uuid")
            ]
            if raw and not usable:
                # A non-empty response with no usable (uuid-bearing) entries
                # is just as untrustworthy as a malformed/failed query --
                # keep the previous snapshot instead of publishing a false
                # "0 alerts" result. An empty list is exempt: that
                # legitimately means "no alerts left".
                self._note_fallback_outcome(
                    _KEY_ALERT_LIST,
                    failed=True,
                    warning="'alert.list' response had no usable entries: %s",
                    reason=raw,
                )
                return self._ds["alerts"]
            self._note_fallback_outcome(
                _KEY_ALERT_LIST, failed=False, recovered="'alert.list' recovered"
            )

            active = [alert for alert in usable if not alert.get("dismissed", False)]

            disk_issues = False
            for alert in active:
                klass = str(alert.get("klass", "")).lower()
                title = str(alert.get("title", "")).lower()
                if any(
                    term in klass or term in title for term in ("disk", "pool", "smart")
                ):
                    disk_issues = True
                    break

            self._ds["alerts"] = {
                "count": len(active),
                "messages": [
                    alert.get("formatted", "Unknown alert") for alert in active
                ],
                "critical": sum(alert.get("level") == "CRITICAL" for alert in active),
                "warning": sum(alert.get("level") == "WARNING" for alert in active),
                "info": sum(alert.get("level") == "INFO" for alert in active),
                "disk_issues": disk_issues,
                "uuids": [alert.get("uuid") for alert in active if alert.get("uuid")],
            }
            return self._ds["alerts"]

    async def get_interface(self) -> _EndpointMap:
        """Refresh and return normalized network interfaces (``interface.query``).

        Derives a boolean ``link_up`` from the link state. Live rx/tx
        throughput is out of scope (see ``_INTERFACE_VALS``'s docstring in
        ``_specs.py``) -- ``rx``/``tx`` default to 0. A malformed/failed
        response is tracked under ``_KEY_INTERFACE_QUERY`` (mapped to the
        ``"interface"`` endpoint) -- see ``_note_primary_query_outcome``.
        """
        async with self._lock:
            raw = await self._client.call("interface.query")
            self._note_primary_query_outcome(
                _KEY_INTERFACE_QUERY, raw, "interface.query"
            )
            self._ds["interface"] = parse_api(
                data=self._ds["interface"],
                source=raw,
                key="id",
                vals=_INTERFACE_VALS,
                ensure_vals=_INTERFACE_ENSURE_VALS,
            )
            for vals in self._ds["interface"].values():
                vals["link_up"] = vals.get("link_state") == "LINK_STATE_UP"
            return self._ds["interface"]

    async def get_scrub(self) -> _EndpointMap:
        """Refresh and return normalized pool scrub tasks (``pool.scrub.query``).

        A malformed/failed response is tracked under ``_KEY_SCRUB_QUERY``
        (mapped to the ``"scrub"`` endpoint) -- see
        ``_note_primary_query_outcome``.
        """
        async with self._lock:
            raw = await self._client.call("pool.scrub.query")
            self._note_primary_query_outcome(_KEY_SCRUB_QUERY, raw, "pool.scrub.query")
            self._ds["scrub"] = parse_api(
                data=self._ds["scrub"],
                source=raw,
                key="id",
                vals=_SCRUB_VALS,
            )
            return self._ds["scrub"]

    async def get_smb(self) -> _SmbMap:
        """Refresh and return the active SMB connection count (``smb.status``).

        Flat dict (single ``connections`` key), matching the ``arc``/``ups``
        endpoints' shape. A malformed/failed response keeps the previous
        count rather than reporting a false "0 connections".
        """
        async with self._lock:
            try:
                raw = await self._client.call("smb.status")
            except TrueNASError as err:
                self._note_fallback_outcome(
                    _KEY_SMB_STATUS,
                    failed=True,
                    warning="Failed to update SMB connection count: %s",
                    reason=err,
                )
                return self._ds["smb"]
            if isinstance(raw, list):
                self._ds["smb"] = {"connections": len(raw)}
            elif isinstance(raw, dict) and isinstance(raw.get("sessions"), list):
                self._ds["smb"] = {"connections": len(raw["sessions"])}
            else:
                self._note_fallback_outcome(
                    _KEY_SMB_STATUS,
                    failed=True,
                    warning="Malformed 'smb.status' response: %s",
                    reason=raw,
                )
                return self._ds["smb"]
            self._note_fallback_outcome(
                _KEY_SMB_STATUS, failed=False, recovered="SMB status recovered"
            )
            return self._ds["smb"]

    @staticmethod
    def _no_update_pending() -> _UpdateMap:
        """Return the "no update pending" resting state for ``get_update()``."""
        return {
            "update_available": False,
            "update_state": "IDLE",
            "update_version": "up-to-date",
            "update_date": None,
            "update_profile": None,
            "update_train": None,
            "update_filename": None,
        }

    async def get_update(self) -> _UpdateMap:
        """Refresh and return the pending-update status (``update.status``).

        Flat dict, no natural object id, matching the ``arc``/``ups``/
        ``alerts`` endpoints' shape. Resets to "no update pending" whenever
        the response is malformed or carries no new version, rather than
        reporting a phantom update; progress-tracking of a running install
        job is left to the caller via the generic ``call(..., job=True)``
        polling (``core.get_jobs``), not TrueNAS normalization.
        """
        async with self._lock:
            raw = await self._client.call("update.status")
            status = raw.get("status") if isinstance(raw, dict) else None
            new_version = (
                status.get("new_version") if isinstance(status, dict) else None
            )
            if not isinstance(new_version, dict) or not new_version.get("version"):
                self._ds["update"] = self._no_update_pending()
                return self._ds["update"]

            manifest = new_version.get("manifest")
            manifest = manifest if isinstance(manifest, dict) else {}
            state = status.get("state") or status.get("status")
            self._ds["update"] = {
                "update_available": True,
                "update_state": state if isinstance(state, str) else "unknown",
                "update_version": new_version["version"],
                "update_date": manifest.get("date"),
                "update_profile": manifest.get("profile"),
                "update_train": manifest.get("train"),
                "update_filename": manifest.get("filename"),
            }
            return self._ds["update"]

    async def get_disk(self) -> _EndpointMap:
        """Refresh and return normalized disks (``disk.query``), enriched
        with per-disk temperature readings.

        Temperatures are primarily sourced from netdata's disk-temperature
        graph (auto-discovered once and cached for the lifetime of this
        ``TrueNASState``); disks it doesn't cover fall back to the
        ``disk.temperatures`` RPC. Both enrichment paths are best-effort: a
        failure leaves ``temperature`` at its previous value (or the
        ``None`` default for a disk seen for the first time) rather than
        failing the whole refresh, since ``disk.query`` itself is the
        primary, required result.
        """
        async with self._lock:
            self._ds["disk"] = parse_api(
                data=self._ds["disk"],
                source=await self._client.call("disk.query"),
                key="identifier",
                vals=_DISK_VALS,
                ensure_vals=_DISK_ENSURE_VALS,
            )
            try:
                await self._update_disk_temperatures()
            except TrueNASError as err:
                # Both inner enrichment paths (netdata and the
                # disk.temperatures fallback) already catch their own
                # TrueNASError internally, so reaching here means something
                # unexpected slipped through -- worth a warning even though
                # disk.query itself (the primary, required result) is
                # unaffected.
                self._note_fallback_outcome(
                    _KEY_DISK_TEMPERATURE_UPDATE_UNEXPECTED,
                    failed=True,
                    warning="Unexpected error updating disk temperatures: %s",
                    reason=err,
                )
            else:
                self._note_fallback_outcome(
                    _KEY_DISK_TEMPERATURE_UPDATE_UNEXPECTED,
                    failed=False,
                    recovered="Disk temperature update recovered",
                )
            return self._ds["disk"]

    async def _update_disk_temperatures(self) -> None:
        """Enrich ``self._ds["disk"]`` with temperatures.

        Caller must hold ``self._lock``. A failed netdata query is caught
        inside ``_disk_temps_from_netdata()`` (rather than left to propagate
        to ``get_disk()``'s own try/except) so the ``disk.temperatures``
        fallback below still runs for every disk instead of being skipped
        entirely.

        Once netdata has produced a reading for a disk, that disk's
        ``temperature`` is never ``None`` again, so gating the fallback on
        ``temperature is None`` alone would only ever cover a disk on its
        very first poll. If netdata later stops covering that disk (e.g. its
        graph disappears, a name-mapping mismatch, or the query starts
        failing), the stale reading would then persist indefinitely instead
        of being refreshed. Falling back for every disk this poll's netdata
        read did not actually refresh -- not just ones with no reading at
        all -- keeps the value moving in that case too.
        """
        netdata_temps = await self._disk_temps_from_netdata()

        refreshed: set[Hashable] = set()
        if netdata_temps:
            disk_map = self._build_disk_name_map()
            for name, temp in netdata_temps.items():
                if (uid := disk_map.get(name)) is not None:
                    self._ds["disk"][uid]["temperature"] = round(temp, 2)
                    refreshed.add(uid)

        if fallback_uids := [uid for uid in self._ds["disk"] if uid not in refreshed]:
            await self._fallback_disk_temperatures(fallback_uids)
        else:
            # netdata alone covered every disk this poll -- the fallback
            # wasn't even consulted, so a prior failure can no longer be
            # confirmed. Clear it rather than leaving it stuck True forever,
            # or a later fallback failure would stay silently unwarned. This
            # reset is intentionally silent (no recovery log) -- the fallback
            # itself never ran this poll, so there is nothing to report it
            # having recovered from.
            self._fallback_failing.pop(_KEY_DISK_TEMP_FALLBACK, None)

    def _build_disk_name_map(self) -> dict[str, Hashable]:
        """Map each disk's identifier/devname/name to its ``self._ds["disk"]`` uid."""
        disk_map: dict[str, Hashable] = {}
        for uid, vals in self._ds["disk"].items():
            for key in (vals.get("identifier"), vals.get("devname"), vals.get("name")):
                if isinstance(key, str) and key:
                    if key not in disk_map:
                        disk_map[key] = uid
                    elif disk_map[key] != uid:
                        _LOGGER.debug(
                            "Disk mapping collision: key '%s' resolves to "
                            "both %s and %s",
                            key,
                            disk_map[key],
                            uid,
                        )
        return disk_map

    async def _disk_temps_from_netdata(self) -> dict[str, float] | None:
        """Return per-disk temperatures from the netdata disk-temp graph, if any.

        Returns ``None`` when no disk-temp graph is configured at all, when
        graph discovery or the graph query itself fails or returns a
        malformed payload (a real failure -- warned on the failing
        transition, mirroring ``get_ups()``'s handling of a failed/malformed
        discovery), and when the query succeeded but yielded no usable
        reading for any disk. A failed or malformed discovery leaves
        ``self._disk_temp_graph`` at ``None`` so discovery is retried on the
        next call instead of being cached as "no graph found".

        Unlike those, "RPC succeeded and returned recognizable per-disk
        series, but every disk's samples/aggregations came back empty" is
        *not* treated as a failure worth warning about:
        TrueNAS's netdata backend legitimately has nothing to report for a
        disk yet right after a service restart (samples haven't accumulated
        for that poll's window), and some collectors (observed with NVMe
        SMART-temp probes) simply sample slower than this library's polling
        interval, so an empty window here and there is expected, not an
        outage -- see kayl-codes/homeassistant-truenas#139. Warning on every
        such poll was pure noise, especially since ``disk.temperatures`` (see
        ``_update_disk_temperatures()``) already covers any disk this leaves
        unrefreshed. Only logged at DEBUG for troubleshooting, and any
        failing flag left over from an earlier *real* failure is cleared
        silently (no recovery log -- nothing was actually confirmed working
        this poll, mirroring the fallback-uid case in
        ``_update_disk_temperatures()``) rather than left stuck, or a later
        real failure would stay silently unwarned forever. A *non-empty*
        response that contains no recognizable per-disk series at all is a
        different case -- a malformed payload, not an empty window -- and
        does follow the real-failure path (warn once, set the flag).
        Either way, returning ``None`` here (never raising) is what lets
        the caller,
        ``_update_disk_temperatures()``, still fall back to
        ``disk.temperatures`` for every disk.
        """
        if self._disk_temp_graph is None:
            try:
                graphs = await self._client.call("reporting.netdata_graphs")
            except TrueNASError as err:
                self._note_fallback_outcome(
                    _KEY_DISK_TEMP_NETDATA,
                    failed=True,
                    warning="Failed to discover disk-temp netdata graph: %s",
                    reason=err,
                )
                return None
            if not isinstance(graphs, list):
                self._note_fallback_outcome(
                    _KEY_DISK_TEMP_NETDATA,
                    failed=True,
                    warning="Malformed 'reporting.netdata_graphs' response: %s",
                    reason=graphs,
                )
                return None
            self._disk_temp_graph = _find_disk_temp_graph_name(graphs)
        if not self._disk_temp_graph:
            # Legitimate "no disk-temp graph configured" -- not a failure,
            # but still clears any failing flag left over from an earlier
            # discovery error (idempotent once already cleared), so a stuck
            # flag doesn't survive a discovery that has since succeeded.
            self._note_fallback_outcome(
                _KEY_DISK_TEMP_NETDATA,
                failed=False,
                recovered="Disk-temp netdata discovery recovered: no matching graph",
            )
            return None

        report_epoch = int(datetime.now(UTC).replace(microsecond=0).timestamp())
        try:
            graph_data = await self._client.call(
                _NETDATA_GRAPH_METHOD,
                [
                    self._disk_temp_graph,
                    {
                        "start": report_epoch - 90,
                        "end": report_epoch - 30,
                        "aggregate": True,
                    },
                ],
            )
        except TrueNASError as err:
            self._note_fallback_outcome(
                _KEY_DISK_TEMP_NETDATA,
                failed=True,
                warning="Failed to update disk temperatures from netdata: %s",
                reason=err,
            )
            return None
        if not isinstance(graph_data, list):
            self._note_fallback_outcome(
                _KEY_DISK_TEMP_NETDATA,
                failed=True,
                warning="Malformed disk-temp netdata graph response: %s",
                reason=graph_data,
            )
            return None
        temps = _disk_temps_from_graph_data(graph_data)
        if not temps:
            if graph_data and not _has_disk_temp_entries(graph_data):
                # Non-empty response we could not recognize as per-disk
                # series at all -- a malformed payload, not the expected
                # "samples not accumulated yet" empty window handled below
                # (an empty list *is* that window and stays out of here).
                # Follow the real-failure path so a stuck fallback value
                # never goes silently unwarned.
                self._note_fallback_outcome(
                    _KEY_DISK_TEMP_NETDATA,
                    failed=True,
                    warning=(
                        "Malformed disk-temp netdata graph response "
                        "(no disk entries): %s"
                    ),
                    reason=graph_data,
                )
                return None
            # Not a failing transition (see docstring): the RPC itself
            # succeeded, so whatever earlier real failure set this flag no
            # longer applies. Clear it silently (no recovery log -- nothing
            # was actually confirmed recovered this poll) rather than
            # leaving it stuck True, or a later real failure would stay
            # unwarned forever. Mirrors the same reasoning in
            # _update_disk_temperatures() for _KEY_DISK_TEMP_FALLBACK.
            self._fallback_failing.pop(_KEY_DISK_TEMP_NETDATA, None)
            _LOGGER.debug(
                "Disk-temp netdata graph returned no usable reading this "
                "poll (falling back to 'disk.temperatures'): %s",
                graph_data,
            )
            return None
        self._note_fallback_outcome(
            _KEY_DISK_TEMP_NETDATA,
            failed=False,
            recovered="Disk temperatures from netdata recovered",
        )
        return temps

    def _note_fallback_outcome(
        self,
        key: str,
        *,
        failed: bool,
        warning: str = "",
        recovered: str = "",
        reason: object = None,
    ) -> None:
        """Track a cached-fallback code path's failing/recovered state.

        ``key`` identifies the path in ``self._fallback_failing`` (e.g.
        "disk_temp_fallback", "ups_netdata_graphs") -- each path tracks its
        own transition independently. ``failed`` decides the transition
        explicitly -- it is not inferred from ``reason``, since a legitimate
        falsy/None result can be indistinguishable from "no failure" for some
        callers. Warns only on the failing transition and logs recovery at
        debug, so a persistent outage does not re-warn every poll.

        Originally introduced for the ``disk.temperatures`` fallback
        (kayl-codes/homeassistant-truenas#131); generalized to cover every
        other cached-fallback code path in this module so a stuck cached
        value is never silently unwarned, regardless of endpoint.

        ``warning``/``recovered`` default to "" so a caller can omit whichever
        of the two never applies to it (a ``failed=True`` site has no use for
        ``recovered``, and vice versa) -- but a caller that forgets the one
        that *does* apply must not end up formatting "" against ``reason``
        via ``%``, which raises ``TypeError`` deep inside logging's deferred
        formatting and is silently swallowed by ``Handler.handleError()``.
        Falling back to a generic message keyed by ``key`` keeps that
        mistake visible instead of silently dropping the log line.
        """
        if failed:
            if not self._fallback_failing.get(key, False):
                if warning:
                    _LOGGER.warning(warning, reason)
                else:
                    _LOGGER.warning("Fallback path '%s' failed: %s", key, reason)
                self._fallback_failing[key] = True
        elif self._fallback_failing.pop(key, False):
            if recovered:
                _LOGGER.debug(recovered)
            else:
                _LOGGER.debug("Fallback path '%s' recovered", key)

    def _note_primary_query_outcome(
        self, key: str, raw: Any, method: str, *, id_field: str = "id"
    ) -> None:
        """Track whether ``raw`` (the result of calling RPC ``method``) is usable.

        Shared by every primary ``get_*`` call site that hands its raw RPC
        result straight to :func:`parse_api` (keyed on ``id_field``, "id" for
        every current caller) without its own bespoke validation.
        ``parse_api()`` already falls back to the cached snapshot on its own
        in two cases -- a ``None``/non-list/non-dict ``source``, and a
        non-empty list/dict none of whose entries resolve to a usable
        ``id_field`` uid (e.g. ``[{}]``, ``[None]``: ``parse_api()``'s own
        pruning guard leaves ``data`` completely untouched rather than
        wiping it, since ``seen_uids`` stays empty) -- this makes both
        fallbacks *visible* via :meth:`_note_fallback_outcome`/
        ``stale_endpoints`` instead of silently keeping the previous value
        with no tracked failing flag, the gap that originally motivated
        ``stale_endpoints`` for the other primary results in this module.
        Reuses :func:`get_uid` (the same resolver ``parse_api()`` itself
        calls) so this mirrors its real fallback trigger exactly rather than
        approximating it.

        An empty list is not malformed -- it is ``parse_api()``'s legitimate
        "nothing left" signal (it prunes ``data`` to ``{}``) -- so it counts
        as usable here too.

        Not used by call sites with their own dedicated validation (e.g.
        ``get_pool()``'s usable-entry scan, ``_add_boot_pool()``'s
        empty-dict/unhashable-guid checks) -- those already note their own,
        more specific outcome.
        """
        if isinstance(raw, dict):
            entries: list[Any] | None = [raw]
        elif isinstance(raw, list):
            entries = raw
        else:
            entries = None

        usable = entries is not None and (
            not entries
            or any(
                get_uid(entry, id_field, None, None, None) is not None
                for entry in entries
            )
        )

        if not usable:
            self._note_fallback_outcome(
                key,
                failed=True,
                warning=f"Malformed {method!r} response: %s",
                reason=raw,
            )
        else:
            self._note_fallback_outcome(
                key, failed=False, recovered=f"{method!r} recovered"
            )

    async def _fallback_disk_temperatures(self, stale_uids: list[Hashable]) -> None:
        """Fetch temperatures for ``stale_uids`` via ``disk.temperatures``.

        Every ``stale_uids`` entry is a disk netdata did not refresh this
        poll (see ``_update_disk_temperatures``), so a failure here -- an
        RPC error just as much as a malformed response -- always leaves at
        least one disk's temperature stuck; it is warned about regardless
        of whether netdata covered other disks fine. The warning only fires
        on the failing transition (and recovery is logged at debug) so a
        persistent outage does not re-warn every poll.
        """
        disk_names = [
            name
            for uid in stale_uids
            if isinstance(name := self._ds["disk"].get(uid, {}).get("name"), str)
            and name != "unknown"
        ]
        warning = "Failed to update disk temperatures from API 'disk.temperatures': %s"
        recovered = "Disk temperatures from API 'disk.temperatures' recovered"
        try:
            temps = await self._client.call("disk.temperatures", [disk_names])
        except TrueNASError as err:
            self._note_fallback_outcome(
                _KEY_DISK_TEMP_FALLBACK, failed=True, warning=warning, reason=err
            )
            return
        if not isinstance(temps, dict):
            self._note_fallback_outcome(
                _KEY_DISK_TEMP_FALLBACK, failed=True, warning=warning, reason=temps
            )
            return
        self._note_fallback_outcome(
            _KEY_DISK_TEMP_FALLBACK, failed=False, recovered=recovered
        )

        for uid in stale_uids:
            vals = self._ds["disk"][uid]
            candidates = (vals.get("identifier"), vals.get("devname"), vals.get("name"))
            matched = next(
                (
                    temps[key]
                    for key in candidates
                    if isinstance(key, str) and key in temps
                ),
                None,
            )
            if matched is None:
                _LOGGER.debug(
                    "No matching temperature entry in 'disk.temperatures' "
                    "for disk uid=%s (candidates: %s)",
                    uid,
                    [key for key in candidates if isinstance(key, str)],
                )
            elif isinstance(matched, (int, float)) and not isinstance(matched, bool):
                vals["temperature"] = matched
            else:
                _LOGGER.debug(
                    "Invalid temperature value %r for disk uid=%s", matched, uid
                )

    async def get_systeminfo(self) -> _SystemInfoMap:
        """Refresh and return system info (``system.info``).

        Flat singleton dict, no natural object id, matching the ``arc``/
        ``ups``/``alerts``/``update``/``smb`` endpoints' shape.

        Derives ``uptimeEpoch`` from ``uptime_seconds``, damped against small
        per-poll timing jitter (see ``_stable_uptime_epoch()``). Also caches
        whether the system is virtualized (used by ``get_systemstats()`` to
        skip the CPU-temperature graph -- no physical sensor to report on a
        VM) and the parsed ``(major, minor)`` version, sparing
        ``get_container()``'s own ``_detect_version()`` a redundant
        ``system.info`` call once this has already run. A malformed
        (non-dict) ``system.info`` response warns on its failing transition
        regardless of whether a version is already cached -- not just before
        the first successful poll -- under its own key (``_KEY_SYSTEM_INFO``,
        deliberately *not* shared with ``_detect_version()``'s identical
        guard -- see that method's docstring for why), since a ``system.info``
        call going from valid to malformed mid-lifetime would otherwise leave
        every other endpoint (uptime, memory, hostname, ...) silently frozen
        on stale data with no signal. A
        missing/unparsable ``version`` field within an otherwise-valid
        response warns once, while no version is yet cached (via
        ``_note_fallback_outcome()``, shared with ``_detect_version()``'s own
        parse of the same field), rather than silently leaving version-gated
        endpoints on legacy behavior.

        CPU/load/memory/ARC-size stats and interface throughput are not part
        of ``system.info`` itself; ``ensure_vals`` only guarantees their keys
        exist with a resting default until ``get_systemstats()`` -- a much
        larger, independent netdata-graph query -- fills them in.

        Update-status and SMB-connection-count fields the original
        coordinator also stored on ``ds["system_info"]`` are intentionally
        not duplicated here: they are already covered by the dedicated,
        better-normalized ``get_update()``/``get_smb()`` endpoints.
        """
        async with self._lock:
            raw = await self._client.call(_SYSTEM_INFO_METHOD)
            if not isinstance(raw, dict):
                self._note_fallback_outcome(
                    _KEY_SYSTEM_INFO,
                    failed=True,
                    warning=_SYSTEM_INFO_MALFORMED_WARNING,
                    reason=raw,
                )
                return self._ds["system_info"]
            self._note_fallback_outcome(
                _KEY_SYSTEM_INFO, failed=False, recovered=_SYSTEM_INFO_RECOVERED
            )
            self._ds["system_info"] = parse_api(
                data=self._ds["system_info"],
                source=raw,
                vals=_SYSTEMINFO_VALS,
                ensure_vals=_SYSTEMINFO_ENSURE_VALS,
            )
            info = self._ds["system_info"]

            version = _parse_version_tuple(info.get("version"))
            if version != (0, 0):
                self._version = version
                self._note_fallback_outcome(
                    _KEY_DETECT_VERSION,
                    failed=False,
                    recovered=_VERSION_DETECT_RECOVERED,
                )
            elif self._version is None:
                # Only warn while no version is cached yet -- an already-cached
                # version (from a prior successful poll) stays valid and keeps
                # gating get_container() correctly, so a later poll's bad
                # 'version' field here is not actually the failure the warning
                # text describes.
                self._note_fallback_outcome(
                    _KEY_DETECT_VERSION,
                    failed=True,
                    warning=_VERSION_DETECT_WARNING,
                    reason=raw.get("version"),
                )
            self._apply_virtual_detection(raw)

            physmem = info.get("physmem")
            if _is_finite_number(physmem) and physmem > 0:
                info["memory-total_value"] = physmem

            uptime_seconds = info.get("uptime_seconds")
            if (
                isinstance(uptime_seconds, (int, float))
                and not isinstance(uptime_seconds, bool)
                and uptime_seconds >= 0
            ):
                now_epoch = int(datetime.now(UTC).replace(microsecond=0).timestamp())
                info["uptimeEpoch"] = _stable_uptime_epoch(
                    info.get("uptimeEpoch"), uptime_seconds, now_epoch
                )
            return info

    async def get_systemstats(self) -> _SystemInfoMap:
        """Refresh CPU/load/memory/ARC-size stats and interface throughput
        from netdata graphs (``reporting.netdata_graph``).

        Enriches ``ds["system_info"]`` (the primary target, also returned)
        and ``ds["interface"]`` (rx/tx, a side effect -- see
        ``_INTERFACE_VALS``'s docstring in ``_specs.py``).

        Unlike the original coordinator's single combined multi-graph batch,
        each graph is queried and applied independently and best-effort: a
        failed/malformed individual graph leaves its field(s) at their
        previous value rather than resetting to zero, matching this
        library's other endpoints. CPU temperature is skipped entirely on a
        virtualized system (see ``get_systeminfo()``) -- no physical sensor
        to report; a failure detecting that (see ``_detect_virtual()``) is
        treated as non-virtual so it does not abort the graph queries below.
        Interface throughput is skipped entirely if ``get_interface()``
        hasn't populated ``ds["interface"]`` yet (nothing to attribute it to).

        A graph that fails is recorded in ``systemstats_stale_graphs`` (named
        ``"interface"`` for the throughput enrichment) so a caller can tell
        a partially-stale result apart from a fully fresh one -- this covers
        both an outright RPC failure and an RPC that succeeds but returns a
        malformed/empty payload with no usable reading.
        """
        async with self._lock:
            report_epoch = int(datetime.now(UTC).replace(microsecond=0).timestamp())
            graph_query = {
                "start": report_epoch - 90,
                "end": report_epoch - 30,
                "aggregate": True,
            }
            info = self._ds["system_info"]
            # _detect_virtual() owns its own failing/recovered tracking (both
            # a raised error and a malformed response), so a failure here is
            # already warned about -- just treat it as non-virtual so it does
            # not abort the graph queries below.
            is_virtual = await self._detect_virtual()

            stale = await self._refresh_systemstat_graphs(
                graph_query, info, is_virtual=is_virtual
            )
            stale |= await self._refresh_interface_throughput(graph_query)

            self._systemstats_stale_graphs = frozenset(stale)
            return info

    async def _refresh_systemstat_graphs(
        self, graph_query: dict[str, Any], info: dict[str, Any], *, is_virtual: bool
    ) -> set[str]:
        """Query each netdata stats graph in ``_SYSTEMSTATS_GRAPHS`` and apply it.

        All graphs are fetched concurrently so a single slow/unresponsive
        graph cannot delay the others -- see ``get_systemstats()``'s
        staleness contract for how failures are surfaced instead of raising.

        Returns the set of graph names that failed or yielded no usable
        reading.
        """
        graph_names = [
            name for name in _SYSTEMSTATS_GRAPHS if name != "cputemp" or not is_virtual
        ]
        results = await asyncio.gather(
            *(
                self._client.call(_NETDATA_GRAPH_METHOD, [graph_name, graph_query])
                for graph_name in graph_names
            ),
            return_exceptions=True,
        )
        stale: set[str] = set()
        for graph_name, result in zip(graph_names, results, strict=True):
            key = f"{_SYSTEMSTAT_KEY_PREFIX}{graph_name}"
            if isinstance(result, TrueNASError):
                stale.add(graph_name)
                self._note_fallback_outcome(
                    key,
                    failed=True,
                    warning=f"Failed to update '{graph_name}' systemstat graph: %s",
                    reason=result,
                )
                continue
            if isinstance(result, BaseException):
                raise result
            if self._apply_systemstat(graph_name, result, info):
                self._note_fallback_outcome(
                    key,
                    failed=False,
                    recovered=f"'{graph_name}' systemstat graph recovered",
                )
            else:
                stale.add(graph_name)
                self._note_fallback_outcome(
                    key,
                    failed=True,
                    warning=(
                        f"'{graph_name}' systemstat graph returned no usable "
                        "reading: %s"
                    ),
                    reason=result,
                )
        return stale

    async def _refresh_interface_throughput(
        self, graph_query: dict[str, Any]
    ) -> set[str]:
        """Enrich ``ds["interface"]`` with rx/tx throughput, if populated.

        Returns ``{"interface"}`` if the graph failed or yielded no usable
        reading, an empty set otherwise -- see ``get_systemstats()``.
        """
        if not self._ds["interface"]:
            # The throughput graph wasn't consulted this poll (no interfaces
            # populated yet) -- clear a stuck failing flag rather than
            # leaving it stuck True forever, or a later throughput failure
            # would stay silently unwarned. Silent: nothing ran this poll,
            # so there is nothing to report as "recovered".
            self._fallback_failing.pop(_KEY_INTERFACE_THROUGHPUT, None)
            return set()
        try:
            raw_interface = await self._client.call(
                _NETDATA_GRAPH_METHOD, ["interface", graph_query]
            )
        except TrueNASError as err:
            self._note_fallback_outcome(
                _KEY_INTERFACE_THROUGHPUT,
                failed=True,
                warning="Failed to update interface throughput: %s",
                reason=err,
            )
            return {"interface"}
        throughput_by_id = _netdata_interface_throughput(raw_interface)
        applied = False
        for identifier, throughput in throughput_by_id.items():
            if identifier in self._ds["interface"] and throughput:
                self._ds["interface"][identifier].update(throughput)
                applied = True
        if applied:
            self._note_fallback_outcome(
                _KEY_INTERFACE_THROUGHPUT,
                failed=False,
                recovered="Interface throughput recovered",
            )
            return set()
        self._note_fallback_outcome(
            _KEY_INTERFACE_THROUGHPUT,
            failed=True,
            warning="'interface' netdata graph returned no usable reading: %s",
            reason=raw_interface,
        )
        return {"interface"}

    def _apply_systemstat(
        self, graph_name: str, raw: Any, info: dict[str, Any]
    ) -> bool:
        """Apply one netdata graph reading onto ``info`` (``ds["system_info"]``).

        Returns whether a usable reading was actually applied, so a raw
        response that is present but malformed/empty (RPC succeeded, but
        the payload has no valid data) can be reported as stale too, not
        just an outright RPC failure -- see ``systemstats_stale_graphs``.
        """
        handler = _SYSTEMSTAT_HANDLERS.get(graph_name)
        return handler(raw, info) if handler is not None else True
