"""Integration tests for TrueNASState against the fake WebSocket server."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest
from fake_server import FakeTrueNASServer

from aiotruenas import TrueNASClient, TrueNASState

API_KEY = "1-valid-key"


def make_client(server: FakeTrueNASServer, **kwargs) -> TrueNASClient:
    kwargs.setdefault("use_tls", False)
    kwargs.setdefault("query_timeout", 2.0)
    return TrueNASClient(server.host, API_KEY, port=server.port, **kwargs)


_ROOT_DATASET = {
    "id": "tank",
    "type": "FILESYSTEM",
    "name": "tank",
    "pool": "tank",
    "mountpoint": "/mnt/tank",
    "used": {"parsed": 400},
    "available": {"parsed": 600},
}

_POOL_TANK = {
    "guid": "111",
    "id": 1,
    "name": "tank",
    "path": "/mnt/tank",
    "status": "ONLINE",
    "healthy": True,
    "is_decrypted": True,
    "size": 999999,
    "allocated": 400,
    "free": 600,
    "fragmentation": "12",
    "autotrim": {"parsed": True},
    "scan": {
        "function": "SCRUB",
        "state": "FINISHED",
        "start_time": {"$date": 1700000000000},
        "end_time": {"$date": 1700003600000},
        "total_secs_left": 0,
    },
    "topology": {
        "data": [
            {"stats": {"read_errors": 1, "write_errors": 0, "checksum_errors": 0}}
        ],
    },
}

_BOOT_POOL = {
    "name": "boot-pool",
    "status": "ONLINE",
    "healthy": True,
    "is_decrypted": True,
    "size": 100,
    "allocated": 50,
    "free": 50,
    "fragmentation": "5",
}


async def test_get_dataset_normalizes_pool_dataset_query() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"pool.dataset.query": [_ROOT_DATASET]},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_dataset()

    assert result == {
        "tank": {
            "id": "tank",
            "type": "FILESYSTEM",
            "name": "tank",
            "pool": "tank",
            "mountpoint": "/mnt/tank",
            "comments": "",
            "deduplication": False,
            "atime": False,
            "casesensitivity": "unknown",
            "checksum": "unknown",
            "exec": False,
            "sync": "unknown",
            "compression": "unknown",
            "compressratio": "unknown",
            "quota": "unknown",
            "copies": 0,
            "readonly": False,
            "recordsize": 0,
            "encryption_algorithm": "unknown",
            "encryption_key_format": "unknown",
            "encrypted": False,
            "locked": False,
            "used": 400,
            "available": 600,
        }
    }
    assert state.ds["dataset"] == result


async def test_get_dataset_keeps_previous_snapshot_on_malformed_response() -> None:
    """A null/malformed pool.dataset.query response must preserve the
    previous dataset snapshot instead of collapsing it to empty."""
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"pool.dataset.query": [_ROOT_DATASET]},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            first = await state.get_dataset()

            server.responses["pool.dataset.query"] = None
            second = await state.get_dataset()

    assert second == first
    assert second != {}


async def test_get_pool_derives_capacity_from_root_dataset() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "pool.dataset.query": [_ROOT_DATASET],
            "pool.query": [_POOL_TANK],
            "boot.get_state": _BOOT_POOL,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_pool()

    tank = result["111"]
    assert tank["name"] == "tank"
    # Root dataset's available/used (600/400) win over pool.query's raw
    # size/allocated/free (999999/400/600) so figures match the WebUI.
    assert tank["available"] == 600
    assert tank["total"] == 1000
    assert tank["size"] == 1000
    assert tank["allocated"] == 400
    assert tank["usage"] == 40
    assert tank["fragmentation"] == 12
    assert tank["read_errors"] == 1
    assert tank["write_errors"] == 0
    assert tank["checksum_errors"] == 0
    assert tank["errors"] == 1
    assert tank["scrub_start"] == datetime.fromtimestamp(1700000000, tz=UTC)
    assert tank["scrub_end"] == datetime.fromtimestamp(1700003600, tz=UTC)


async def test_get_pool_merges_boot_pool_without_dropping_regular_pools() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "pool.dataset.query": [_ROOT_DATASET],
            "pool.query": [_POOL_TANK],
            "boot.get_state": _BOOT_POOL,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_pool()

    assert set(result) == {"111", "boot-pool"}
    boot = result["boot-pool"]
    assert boot["name"] == "boot-pool"
    # No matching root dataset for the boot-pool: falls back to its own
    # free/size instead of being derived from a dataset.
    assert boot["available"] == 50
    assert boot["total"] == 100
    assert boot["usage"] == 50
    assert boot["fragmentation"] == 5


async def test_get_pool_prunes_pools_absent_from_a_later_query() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "pool.dataset.query": [_ROOT_DATASET],
            "pool.query": [_POOL_TANK],
            "boot.get_state": _BOOT_POOL,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_pool()
            assert "111" in state.ds["pool"]

            server.responses["pool.query"] = []
            result = await state.get_pool()

    # The regular pool is gone from a subsequent, now-empty pool.query
    # response; the boot-pool (merged with prune=False) survives regardless.
    assert set(result) == {"boot-pool"}


async def test_get_pool_keeps_previous_snapshot_on_malformed_pool_query() -> None:
    """A null/malformed pool.query response must not be paired with a freshly
    refreshed dataset map -- both the dataset and pool caches stay on their
    previous, mutually-consistent snapshot instead."""
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "pool.dataset.query": [_ROOT_DATASET],
            "pool.query": [_POOL_TANK],
            "boot.get_state": _BOOT_POOL,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_pool()
            previous_dataset = state.ds["dataset"]
            previous_pool = state.ds["pool"]

            server.responses["pool.query"] = None
            result = await state.get_pool()

    assert result is previous_pool
    assert state.ds["dataset"] is previous_dataset
    assert state.ds["pool"] is previous_pool


@pytest.mark.parametrize("malformed_pool_query", [[None], [{}]])
async def test_get_pool_keeps_previous_snapshot_on_unusable_pool_entries(
    malformed_pool_query: list[Any],
) -> None:
    """A non-empty pool.query response containing no usable (guid-bearing)
    entries is just as untrustworthy as a null response: neither the dataset
    nor the pool cache may be updated from it. An empty list ([]) is exempt
    -- that legitimately means "no pools left", not "malformed"."""
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "pool.dataset.query": [_ROOT_DATASET],
            "pool.query": [_POOL_TANK],
            "boot.get_state": _BOOT_POOL,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_pool()
            previous_dataset = state.ds["dataset"]
            previous_pool = state.ds["pool"]

            server.responses["pool.query"] = malformed_pool_query
            result = await state.get_pool()

    assert result is previous_pool
    assert state.ds["dataset"] is previous_dataset
    assert state.ds["pool"] is previous_pool


async def test_get_pool_ignores_error_aggregation_for_unhashable_guid() -> None:
    """A malformed pool entry with an unhashable guid (e.g. a list) must be
    skipped during error aggregation instead of crashing the whole refresh
    with a TypeError from the `uid not in pools` membership check."""
    bad_pool = {**_POOL_TANK, "guid": ["not", "hashable"]}
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "pool.dataset.query": [_ROOT_DATASET],
            "pool.query": [_POOL_TANK, bad_pool],
            "boot.get_state": _BOOT_POOL,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_pool()

    assert result["111"]["errors"] == 1


async def test_get_pool_falls_back_to_own_free_size_for_unhashable_path_and_name() -> (
    None
):
    """A pool entry with a valid guid but a malformed (unhashable) path/name
    must not crash capacity derivation with a TypeError from dict.get()."""
    bad_capacity_pool = {
        **_POOL_TANK,
        "guid": "222",
        "path": ["not", "hashable"],
        "name": ["also", "not", "hashable"],
    }
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "pool.dataset.query": [_ROOT_DATASET],
            "pool.query": [bad_capacity_pool],
            "boot.get_state": {},
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_pool()

    pool = result["222"]
    # An unhashable path/name can't even be looked up against the dataset
    # maps: falls back to the pool's own free/size instead of crashing.
    assert pool["available"] == 600
    assert pool["total"] == 999999


async def test_get_pool_without_matching_dataset_falls_back_to_own_free_size() -> None:
    pool_no_match = {**_POOL_TANK, "path": "/mnt/other", "name": "other"}
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "pool.dataset.query": [_ROOT_DATASET],
            "pool.query": [pool_no_match],
            "boot.get_state": {},
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_pool()

    other = result["111"]
    # No matching root dataset (mountpoint/name mismatch): falls back to the
    # pool's own free/size instead of being derived from a dataset.
    assert other["available"] == 600
    assert other["total"] == 999999
    assert other["allocated"] == 400


async def test_get_pool_falls_back_to_zero_for_non_numeric_dataset_capacity() -> None:
    """A malformed (non-numeric) root-dataset available/used value must not
    crash capacity arithmetic with a TypeError, nor silently concatenate
    strings instead of adding numbers."""
    bad_dataset = {
        **_ROOT_DATASET,
        "used": {"parsed": "not-a-number"},
        "available": {"parsed": ["not", "a", "number"]},
    }
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "pool.dataset.query": [bad_dataset],
            "pool.query": [_POOL_TANK],
            "boot.get_state": {},
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_pool()

    tank = result["111"]
    assert tank["available"] == 0
    assert tank["allocated"] == 0
    assert tank["total"] == 0
    assert tank["usage"] == 0


async def test_get_pool_falls_back_to_zero_for_non_numeric_pool_capacity() -> None:
    """A malformed (non-numeric) pool free/size/allocated value must not
    crash the no-matching-dataset capacity fallback."""
    bad_pool = {
        **_POOL_TANK,
        "path": "/mnt/other",
        "name": "other",
        "free": "not-a-number",
        "size": None,
        "allocated": ["not", "a", "number"],
    }
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "pool.dataset.query": [_ROOT_DATASET],
            "pool.query": [bad_pool],
            "boot.get_state": {},
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_pool()

    other = result["111"]
    assert other["available"] == 0
    assert other["total"] == 0


async def test_get_cloudsync_normalizes_job_status_and_progress() -> None:
    raw_cloudsync = {
        "id": 1,
        "description": "Backup",
        "direction": "PUSH",
        "path": "/mnt/tank/backup",
        "enabled": True,
        "transfer_mode": "COPY",
        "snapshot": False,
        "job": {
            "state": "SUCCESS",
            "time_started": {"$date": 1700000000000},
            "time_finished": {"$date": 1700003600000},
            "progress": {"percent": 100, "description": "done"},
        },
    }
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"cloudsync.query": [raw_cloudsync]},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_cloudsync()

    task = result[1]
    assert task["description"] == "Backup"
    assert task["enabled"] is True
    assert task["state"] == "SUCCESS"
    assert task["job_percent"] == 100
    assert task["job_description"] == "done"
    assert task["time_started"] == datetime.fromtimestamp(1700000000, tz=UTC)
    assert state.ds["cloudsync"] == result


async def test_ds_property_starts_empty_for_all_endpoints() -> None:
    async with FakeTrueNASServer(valid_api_key=API_KEY) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)

    assert state.ds == {"pool": {}, "dataset": {}, "cloudsync": {}}
