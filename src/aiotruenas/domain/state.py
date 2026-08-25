"""Normalized, cached TrueNAS domain state built on top of a TrueNASClient.

``TrueNASState`` composes a :class:`~aiotruenas.client.TrueNASClient` (rather
than subclassing it, keeping the client itself a thin transport layer) and
exposes one ``async def get_<endpoint>()`` method per TrueNAS RPC endpoint.
Each method queries the endpoint, normalizes the response via
``domain._normalize.parse_api`` and the field specs in ``domain._specs``, and
caches the result in ``self.ds[<endpoint>]`` -- the same dict-keyed-by-id
shape historically produced by consumer integrations' own
``apiparser.py``/``coordinator.py``.
"""

from __future__ import annotations

import asyncio
import copy
from collections.abc import Hashable
from typing import Any

from ..client import TrueNASClient
from ._helpers import _aggregate_topology_errors, _to_int
from ._normalize import get_uid, parse_api
from ._specs import _CLOUDSYNC_VALS, _DATASET_VALS, _POOL_ENSURE_VALS, _POOL_VALS

_EndpointMap = dict[Hashable, dict[str, Any]]


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
        self._ds: dict[str, _EndpointMap] = {
            "pool": {},
            "dataset": {},
            "cloudsync": {},
        }

    @property
    def ds(self) -> dict[str, _EndpointMap]:
        """Normalized state, keyed by endpoint name then by object id/guid."""
        return self._ds

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
                root_dataset = dataset_by_mountpoint.get(vals.get("path"))
                if root_dataset is None:
                    root_dataset = datasets.get(vals.get("name"))

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
        if root_dataset:
            # Use "or 0" so a null value (not just a missing key) is handled.
            available = root_dataset.get("available") or 0
            used = root_dataset.get("used") or 0
            total = available + used
            pools[uid]["size"] = total
            pools[uid]["allocated"] = used
        else:
            available = vals.get("free") or 0
            total = vals.get("size") or (
                (vals.get("allocated") or 0) + (vals.get("free") or 0)
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
