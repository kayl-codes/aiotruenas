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

    assert state.ds == {
        "pool": {},
        "dataset": {},
        "cloudsync": {},
        "replication": {},
        "rsynctask": {},
        "snapshottask": {},
        "cronjob": {},
        "arc": {},
        "ups": {},
    }


async def test_get_replication_prefers_persistent_state_over_job_state() -> None:
    raw_replication = {
        "id": 1,
        "name": "tank-backup",
        "source_datasets": ["tank"],
        "target_dataset": "backup/tank",
        "recursive": True,
        "enabled": True,
        "direction": "PUSH",
        "transport": "SSH",
        "auto": True,
        "retention_policy": "SOURCE",
        "state": {"state": "FINISHED"},
        "job": {"state": "RUNNING"},
    }
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"replication.query": [raw_replication]},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_replication()

    task = result[1]
    assert task["name"] == "tank-backup"
    assert task["state"] == "FINISHED"
    assert "job_state" not in task


async def test_get_replication_falls_back_to_job_state_when_state_missing() -> None:
    raw_replication = {"id": 1, "name": "tank-backup", "job": {"state": "RUNNING"}}
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"replication.query": [raw_replication]},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_replication()

    assert result[1]["state"] == "RUNNING"


async def test_get_replication_falls_back_when_state_is_explicit_null() -> None:
    raw_replication = {
        "id": 1,
        "name": "tank-backup",
        "state": {"state": None},
        "job": {"state": "RUNNING"},
    }
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"replication.query": [raw_replication]},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_replication()

    assert result[1]["state"] == "RUNNING"


async def test_get_rsync_normalizes_job_status() -> None:
    raw_rsync = {
        "id": 1,
        "path": "/mnt/tank/share",
        "desc": "Nightly sync",
        "remotehost": "backup.example.com",
        "remotemodule": "share",
        "direction": "PUSH",
        "mode": "MODULE",
        "enabled": True,
        "job": {"state": "SUCCESS"},
    }
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"rsynctask.query": [raw_rsync]},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_rsync()

    task = result[1]
    assert task["desc"] == "Nightly sync"
    assert task["state"] == "SUCCESS"
    assert state.ds["rsynctask"] == result


async def test_get_snapshottask_normalizes_schedule_and_state() -> None:
    raw_snapshottask = {
        "id": 1,
        "dataset": "tank/data",
        "recursive": True,
        "lifetime_value": 2,
        "lifetime_unit": "WEEK",
        "enabled": True,
        "naming_schema": "auto-%Y%m%d",
        "state": {
            "state": "FINISHED",
            "datetime": {"$date": 1700000000000},
        },
    }
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"pool.snapshottask.query": [raw_snapshottask]},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_snapshottask()

    task = result[1]
    assert task["dataset"] == "tank/data"
    assert task["state"] == "FINISHED"
    assert task["datetime"] == datetime.fromtimestamp(1700000000, tz=UTC)


async def test_get_cronjob_derives_display_name_with_fallback_chain() -> None:
    raw_cronjobs = [
        {"id": 1, "description": "Nightly backup", "command": "backup.sh"},
        {"id": 2, "description": "", "command": "cleanup.sh"},
        {"id": 3, "description": ""},
    ]
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"cronjob.query": raw_cronjobs},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_cronjob()

    assert result[1]["display_name"] == "Nightly backup"
    assert result[2]["display_name"] == "cleanup.sh"
    assert result[3]["display_name"] == "Cronjob 3"


async def test_get_cronjob_display_name_survives_non_string_fields() -> None:
    raw_cronjobs = [{"id": 1, "description": ["not", "a", "string"], "command": 123}]
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"cronjob.query": raw_cronjobs},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_cronjob()

    assert result[1]["display_name"] == "Cronjob 1"


async def test_get_arc_computes_hit_percentages_from_netdata_graphs() -> None:
    means = {
        "demanddatahitpercentage": {"hits": 91.234},
        "demandmetadatahitpercentage": {"hits": 99.5},
        "l2architpercentage": {"hits": 10.0},
    }

    def netdata_graph(params: list) -> Any:
        return [{"aggregations": {"mean": means[params[0]]}}]

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"reporting.netdata_graph": netdata_graph},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_arc()

    assert result == {
        "data_hit_percent": 91.23,
        "metadata_hit_percent": 99.5,
        "l2_hit_percent": 10.0,
    }
    assert state.ds["arc"] == result


async def test_get_arc_sets_none_for_missing_graph_data() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"reporting.netdata_graph": None},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_arc()

    assert result == {
        "data_hit_percent": None,
        "metadata_hit_percent": None,
        "l2_hit_percent": None,
    }


async def test_get_ups_discovers_and_normalizes_available_graphs() -> None:
    means = {"upscharge": {"ups1": 80.0}, "upsload": {"ups1": 42.0}}

    def netdata_graph(params: list) -> Any:
        return [{"aggregations": {"mean": means[params[0]]}}]

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "reporting.netdata_graphs": [
                {"name": "upscharge"},
                {"name": "upsload"},
                {"name": "cpu"},
            ],
            "reporting.netdata_graph": netdata_graph,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_ups()

    assert result == {"battery_charge": 80.0, "load": 42.0}
    assert state.ds["ups"] == result


async def test_get_ups_returns_empty_when_no_ups_graphs_present() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "reporting.netdata_graphs": [{"name": "cpu"}, {"name": "load"}],
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_ups()

    assert result == {}
    assert state.ds["ups"] == {}


async def test_get_ups_keeps_previous_reading_on_failed_discovery() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "reporting.netdata_graphs": [{"name": "upscharge"}],
            "reporting.netdata_graph": lambda params: [
                {"aggregations": {"mean": {"ups1": 55.0}}}
            ],
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_ups()
            previous_ups = state.ds["ups"]

            server.responses["reporting.netdata_graphs"] = None
            result = await state.get_ups()

    assert result is previous_ups
    assert state.ds["ups"] is previous_ups
