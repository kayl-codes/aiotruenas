"""Normalized, cached TrueNAS domain state built on top of a TrueNASClient.

``TrueNASState`` composes a :class:`~aiotruenas.client.TrueNASClient` (rather
than subclassing it, keeping the client itself a thin transport layer) and
exposes one ``async def get_<endpoint>()`` method per TrueNAS RPC endpoint.
Each method queries the endpoint, normalizes the response via
``domain._normalize.parse_api`` and the field specs in ``domain._specs``, and
caches the result in ``self.ds[<endpoint>]`` -- the same dict-keyed-by-id
shape historically produced by consumer integrations' own
``apiparser.py``/``coordinator.py``. Exceptions are the netdata-graph-backed
endpoints (``arc``, ``ups``), which have no natural object id and instead
cache a flat dict of scalar readings.
"""

from __future__ import annotations

import asyncio
import copy
from collections.abc import Hashable
from datetime import UTC, datetime
from typing import Any, TypedDict, cast

from ..client import TrueNASClient
from ..exceptions import TrueNASError
from ._helpers import (
    _aggregate_topology_errors,
    _arc_value,
    _cpuset_size,
    _first_ipv4,
    _parse_version_tuple,
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
    _POOL_ENSURE_VALS,
    _POOL_VALS,
    _REPLICATION_VALS,
    _RSYNC_VALS,
    _SERVICE_ENSURE_VALS,
    _SERVICE_VALS,
    _SNAPSHOTTASK_VALS,
    _VM_ENSURE_VALS,
    _VM_VALS,
)

_EndpointMap = dict[Hashable, dict[str, Any]]
#: get_arc() shape: one entry per known metric, None where currently unavailable.
_ArcMap = dict[str, float | None]
#: get_ups() shape: only metrics currently reporting a value (never None --
#: missing/no-UPS metrics are omitted rather than included as None).
_UpsMap = dict[str, float]
#: get_alerts() shape: aggregated counters/messages, no natural object id.
_AlertsMap = dict[str, Any]
#: Public shape of the ``ds`` property: a plain mapping (rather than the
#: TypedDict used internally) so consumers can index it with a runtime
#: string, e.g. when iterating over endpoint names.
_PublicStateMap = dict[str, _EndpointMap | _ArcMap | _UpsMap | _AlertsMap]


class _StateMap(TypedDict):
    """Per-key shape of ``self._ds``: id-keyed maps, except the flat scalar
    readings of ``arc``/``ups``, which have no natural object id.
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
    arc: _ArcMap
    ups: _UpsMap
    alerts: _AlertsMap


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
        }
        # Cached (major, minor) TrueNAS version, used by get_container() to
        # pick the right query API; detected lazily on first use (see
        # _detect_version()) since it cannot change without an appliance
        # reboot, which drops the underlying connection.
        self._version: tuple[int, int] | None = None

    @property
    def ds(self) -> _PublicStateMap:
        """Normalized state, keyed by endpoint name then by object id/guid.

        The ``arc`` and ``ups`` endpoints have no natural object id and are
        keyed by endpoint name only, holding a flat dict of scalar readings.

        Typed as a plain mapping rather than the ``TypedDict`` used
        internally, so it can be indexed with a runtime string (e.g. when
        iterating over endpoint names) under static type checking.
        """
        return cast(_PublicStateMap, self._ds)

    async def get_dataset(self) -> _EndpointMap:
        """Refresh and return normalized ZFS datasets (``pool.dataset.query``)."""
        async with self._lock:
            self._ds["dataset"] = await self._compute_dataset()
            return self._ds["dataset"]

    async def _compute_dataset(self) -> _EndpointMap:
        """Return a freshly computed dataset map, without publishing it.

        Starts from a copy of the current dataset cache (rather than an empty
        dict) so a ``None``/malformed ``pool.dataset.query`` response makes
        ``parse_api()`` preserve the previous snapshot instead of collapsing
        it to empty; a genuine, non-empty response still ends up containing
        only its own entries, since ``parse_api()`` prunes anything absent
        from it.

        Caller must hold ``self._lock`` and is responsible for publishing the
        result to ``self._ds["dataset"]``.
        """
        return parse_api(
            data=copy.deepcopy(self._ds["dataset"]),
            source=await self._client.call("pool.dataset.query"),
            key="id",
            vals=_DATASET_VALS,
        )

    async def get_pool(self) -> _EndpointMap:
        """Refresh and return normalized pools (``pool.query`` + boot-pool).

        Refreshes datasets first: a pool's usable capacity is derived from its
        root dataset's available/used figures (matching the TrueNAS WebUI),
        which requires up-to-date dataset data.

        Both the dataset and pool maps are built up on local (deep-copied)
        snapshots and only published to ``self._ds`` once every step --
        including the fallible boot-pool lookup -- has succeeded, so a
        failure partway through leaves the previous, fully-consistent
        snapshot of both endpoints in place instead of a dataset/pool pair
        that no longer agree with each other.
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
                return self._ds["pool"]

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

                self._apply_pool_capacity(pools, uid, vals, root_dataset)

                # pool.query reports fragmentation as a percentage string
                # (e.g. "48").
                pools[uid]["fragmentation"] = _to_int(vals.get("fragmentation"))

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
        """
        raw_boot = await self._client.call("boot.get_state")
        if not isinstance(raw_boot, dict) or not raw_boot:
            return pools

        # boot.get_state carries no guid/id; use the pool name as a stable key.
        raw_boot.setdefault("guid", raw_boot.get("name", "boot-pool"))
        raw_boot.setdefault("id", raw_boot.get("name", "boot-pool"))
        pools = parse_api(
            data=pools,
            source=raw_boot,
            key="guid",
            vals=_POOL_VALS,
            ensure_vals=_POOL_ENSURE_VALS,
            prune=False,
        )
        self._apply_pool_errors(pools, [raw_boot])
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
                    "reporting.netdata_graph", [graph_name, graph_query]
                )
                arc[field_name] = _arc_value(graph_data)
            self._ds["arc"] = arc
            return arc

    async def get_ups(self) -> dict[str, float]:
        """Refresh and return UPS readings from netdata graphs, if a UPS is present.

        Discovers which UPS graphs TrueNAS currently exposes
        (``reporting.netdata_graphs``) on every call rather than caching the
        result, so a UPS attached or removed at runtime is picked up without
        needing a restart. Returns an empty dict when no UPS graphs exist; on
        a failed discovery call, the previous reading is preserved instead
        (retried on the next call).
        """
        async with self._lock:
            try:
                graphs = await self._client.call("reporting.netdata_graphs")
            except TrueNASError:
                return self._ds["ups"]
            if not isinstance(graphs, list):
                return self._ds["ups"]

            available = {
                name
                for graph in graphs
                if isinstance(graph, dict)
                and (name := str(graph.get("name", ""))) in _UPS_GRAPHS
            }
            if not available:
                self._ds["ups"] = {}
                return self._ds["ups"]

            report_epoch = int(datetime.now(UTC).replace(microsecond=0).timestamp())
            graph_query = {
                "start": report_epoch - 90,
                "end": report_epoch - 30,
                "aggregate": True,
            }
            ups: dict[str, float] = {}
            for graph_name in available:
                graph_data = await self._client.call(
                    "reporting.netdata_graph", [graph_name, graph_query]
                )
                value = _ups_value(graph_data)
                if value is not None:
                    ups[_UPS_GRAPHS[graph_name]] = value
            self._ds["ups"] = ups
            return ups

    async def get_service(self) -> _EndpointMap:
        """Refresh and return normalized services (``service.query``).

        Derives ``running`` from the service state and a ``display_name``
        that falls back to a known human-friendly label (``_SERVICE_DISPLAY_
        NAMES``) when the API's own "name" field is missing/"unknown".
        """
        async with self._lock:
            self._ds["service"] = parse_api(
                data=self._ds["service"],
                source=await self._client.call("service.query"),
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
        """Refresh and return normalized VMs (``vm.query``)."""
        async with self._lock:
            self._ds["vm"] = parse_api(
                data=self._ds["vm"],
                source=await self._client.call("vm.query"),
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
        failed/unparsable detection is not cached and is retried on the next
        call.
        """
        if self._version is not None:
            return self._version
        raw = await self._client.call("system.info")
        version_str = raw.get("version") if isinstance(raw, dict) else None
        version = _parse_version_tuple(version_str)
        if version != (0, 0):
            self._version = version
        return version

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
                data={},
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
            if (
                not isinstance(config, dict)
                or not config.get("service_type")
                or not config.get("enable")
            ):
                self._ds["directoryservices"] = {}
                return self._ds["directoryservices"]

            raw_status = await self._client.call("directoryservices.status")
            status = raw_status if isinstance(raw_status, dict) else {}

            merged = dict(config)
            merged["status"] = status.get("status", "unknown")
            merged["status_msg"] = status.get("status_msg")

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
                return self._ds["alerts"]

            active = [
                alert
                for alert in raw
                if isinstance(alert, dict) and not alert.get("dismissed", False)
            ]

            disk_issues = False
            for alert in active:
                klass = str(alert.get("klass", "")).lower()
                title = str(alert.get("title", "")).lower()
                if "disk" in klass or "pool" in klass or "smart" in title:
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
