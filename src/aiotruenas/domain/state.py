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

from typing import Any

from ..client import TrueNASClient
from ._helpers import _aggregate_topology_errors, _to_int
from ._normalize import parse_api
from ._specs import _CLOUDSYNC_VALS, _DATASET_VALS, _POOL_ENSURE_VALS, _POOL_VALS


class TrueNASState:
    """Normalized TrueNAS domain state, refreshed one endpoint at a time."""

    def __init__(self, client: TrueNASClient) -> None:
        self._client = client
        self._ds: dict[str, dict[str, dict[str, Any]]] = {
            "pool": {},
            "dataset": {},
            "cloudsync": {},
        }

    @property
    def ds(self) -> dict[str, dict[str, dict[str, Any]]]:
        """Normalized state, keyed by endpoint name then by object id/guid."""
        return self._ds

    async def get_dataset(self) -> dict[str, dict[str, Any]]:
        """Refresh and return normalized ZFS datasets (``pool.dataset.query``)."""
        self._ds["dataset"] = parse_api(
            data={},
            source=await self._client.call("pool.dataset.query"),
            key="id",
            vals=_DATASET_VALS,
        )
        return self._ds["dataset"]

    async def get_pool(self) -> dict[str, dict[str, Any]]:
        """Refresh and return normalized pools (``pool.query`` + boot-pool).

        Refreshes datasets first: a pool's usable capacity is derived from its
        root dataset's available/used figures (matching the TrueNAS WebUI),
        which requires up-to-date dataset data.
        """
        await self.get_dataset()

        raw_pools = await self._client.call("pool.query")
        self._ds["pool"] = parse_api(
            data=self._ds["pool"],
            source=raw_pools,
            key="guid",
            vals=_POOL_VALS,
            ensure_vals=_POOL_ENSURE_VALS,
        )
        self._apply_pool_errors(raw_pools)
        await self._add_boot_pool()

        # Build a lookup of datasets by their mountpoint so a pool's free/total
        # space can be derived from its root dataset. Matching the pool "path"
        # against the dataset "mountpoint" (e.g. "/mnt/tank") is the primary and
        # most reliable method; the dataset id (which equals the pool name for a
        # root dataset) is used only as a fallback.
        dataset_by_mountpoint: dict[str, dict[str, Any]] = {
            dataset["mountpoint"]: dataset
            for dataset in self._ds["dataset"].values()
            if isinstance(dataset.get("mountpoint"), str)
            and dataset["mountpoint"] not in ("", "unknown")
        }

        for uid, vals in self._ds["pool"].items():
            root_dataset = dataset_by_mountpoint.get(vals.get("path"))
            if root_dataset is None:
                root_dataset = self._ds["dataset"].get(vals.get("name"))

            self._apply_pool_capacity(uid, vals, root_dataset)

            # pool.query reports fragmentation as a percentage string (e.g. "48").
            self._ds["pool"][uid]["fragmentation"] = _to_int(vals.get("fragmentation"))

        return self._ds["pool"]

    async def _add_boot_pool(self) -> None:
        """Add the boot-pool to the pool data.

        ``pool.query`` does not include the boot-pool; ``boot.get_state``
        reports it with the same top-level shape (name/status/healthy/scan/
        size/allocated/free/fragmentation), so it is parsed with the same
        field mapping. It has no root dataset, so the capacity falls back to
        the pool's own free/size (handled in ``_apply_pool_capacity``).
        """
        raw_boot = await self._client.call("boot.get_state")
        if not isinstance(raw_boot, dict) or not raw_boot:
            return

        # boot.get_state carries no guid/id; use the pool name as a stable key.
        raw_boot.setdefault("guid", raw_boot.get("name", "boot-pool"))
        raw_boot.setdefault("id", raw_boot.get("name", "boot-pool"))
        self._ds["pool"] = parse_api(
            data=self._ds["pool"],
            source=raw_boot,
            key="guid",
            vals=_POOL_VALS,
            ensure_vals=_POOL_ENSURE_VALS,
            prune=False,
        )
        self._apply_pool_errors([raw_boot])

    def _apply_pool_capacity(
        self, uid: str, vals: dict[str, Any], root_dataset: dict[str, Any] | None
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
            self._ds["pool"][uid]["size"] = total
            self._ds["pool"][uid]["allocated"] = used
        else:
            available = vals.get("free") or 0
            total = vals.get("size") or (
                (vals.get("allocated") or 0) + (vals.get("free") or 0)
            )

        self._ds["pool"][uid]["available"] = available
        self._ds["pool"][uid]["total"] = total
        self._ds["pool"][uid]["usage"] = (
            round((total - available) / total * 100) if total > 0 else 0
        )

    def _apply_pool_errors(self, raw_pools: Any) -> None:
        """Aggregate read/write/checksum errors from each pool's topology."""
        if not isinstance(raw_pools, list):
            return

        for raw_pool in raw_pools:
            if not isinstance(raw_pool, dict):
                continue
            uid = raw_pool.get("guid")
            if uid not in self._ds["pool"]:
                continue

            read, write, checksum = _aggregate_topology_errors(raw_pool.get("topology"))
            pool = self._ds["pool"][uid]
            pool["read_errors"] = read
            pool["write_errors"] = write
            pool["checksum_errors"] = checksum
            pool["errors"] = read + write + checksum

    async def get_cloudsync(self) -> dict[str, dict[str, Any]]:
        """Refresh and return normalized cloud-sync tasks (``cloudsync.query``)."""
        self._ds["cloudsync"] = parse_api(
            data=self._ds["cloudsync"],
            source=await self._client.call("cloudsync.query"),
            key="id",
            vals=_CLOUDSYNC_VALS,
        )
        return self._ds["cloudsync"]
