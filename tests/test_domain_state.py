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
        "update": {
            "update_available": False,
            "update_state": "IDLE",
            "update_version": "up-to-date",
            "update_date": None,
            "update_profile": None,
            "update_train": None,
            "update_filename": None,
        },
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


async def test_get_service_derives_running_and_known_display_name() -> None:
    raw_services = [
        {
            "id": 1,
            "service": "cifs",
            "name": "unknown",
            "enable": True,
            "state": "RUNNING",
        },
        {
            "id": 2,
            "service": "ssh",
            "name": "Secure Shell",
            "enable": False,
            "state": "STOPPED",
        },
    ]
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"service.query": raw_services},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_service()

    assert result[1]["running"] is True
    assert result[1]["display_name"] == "SMB"
    assert result[2]["running"] is False
    assert result[2]["display_name"] == "Secure Shell"


async def test_get_service_falls_back_to_service_id_for_unknown_service() -> None:
    raw_services = [
        {
            "id": 1,
            "service": "some_new_service",
            "name": "",
            "enable": True,
            "state": "RUNNING",
        }
    ]
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"service.query": raw_services},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_service()

    assert result[1]["display_name"] == "some_new_service"


async def test_get_vm_converts_memory_and_derives_running() -> None:
    raw_vms = [
        {
            "id": 1,
            "name": "vm1",
            "type": "KVM",
            "vcpus": 2,
            "memory": 2097152,
            "autostart": True,
            "description": "Debian",
            "status": {"state": "RUNNING"},
        }
    ]
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"vm.query": raw_vms},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_vm()

    assert result[1]["cpu"] == 2
    assert result[1]["memory"] == round(2097152 / 1024)
    assert result[1]["running"] is True


async def test_get_vm_treats_null_memory_as_zero() -> None:
    raw_vms = [{"id": 1, "name": "vm1", "memory": None, "status": {"state": "STOPPED"}}]
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"vm.query": raw_vms},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_vm()

    assert result[1]["memory"] == 0
    assert result[1]["running"] is False


async def test_get_container_uses_virt_instance_query_below_truenas_26() -> None:
    raw_instances = [
        {
            "id": "ct1",
            "name": "container1",
            "type": "CONTAINER",
            "cpu": "2",
            "memory": 1048576,
            "autostart": True,
            "image": {"description": "Alpine"},
            "status": "RUNNING",
            "aliases": [{"type": "INET", "address": "10.0.0.5"}],
        },
        {"id": "vm1", "name": "not-a-container", "type": "VM"},
    ]
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": {"version": "TrueNAS-25.10.0"},
            "virt.instance.query": raw_instances,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_container()

    assert list(result) == ["ct1"]
    assert result["ct1"]["cpu"] == 2
    assert result["ct1"]["memory"] == 1
    assert result["ct1"]["running"] is True
    assert result["ct1"]["ip_address"] == "10.0.0.5"


async def test_get_container_v26_uses_container_query_and_caches_version() -> None:
    system_info_calls: list[list] = []

    def system_info(params: list) -> Any:
        system_info_calls.append(params)
        return {"version": "TrueNAS-26.0.0"}

    raw_containers = [
        {
            "id": "lxc1",
            "name": "container1",
            "cpuset": "0-1,4",
            "autostart": True,
            "description": "Debian",
            "status": {"state": "RUNNING"},
        }
    ]
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": system_info,
            "container.query": raw_containers,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_container()
            result = await state.get_container()

    assert len(system_info_calls) == 1
    assert result["lxc1"]["type"] == "CONTAINER"
    assert result["lxc1"]["cpu"] == 3
    assert result["lxc1"]["memory"] == 0
    assert result["lxc1"]["ip_address"] == "unknown"
    assert result["lxc1"]["running"] is True


async def test_get_container_defaults_to_legacy_api_when_version_undetectable() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": None,
            "virt.instance.query": [],
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_container()

    assert result == {}


async def test_get_app_derives_running_and_catalog_update_available() -> None:
    raw_apps = [
        {
            "id": "syncthing",
            "name": "syncthing",
            "version": "1.0.0",
            "custom_app": False,
            "upgrade_available": True,
            "image_updates_available": False,
            "state": "RUNNING",
        }
    ]
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"app.query": raw_apps},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_app()

    assert result["syncthing"]["running"] is True
    assert result["syncthing"]["update_available"] is True


async def test_get_app_ignores_image_updates_for_non_custom_apps() -> None:
    raw_apps = [
        {
            "id": "syncthing",
            "custom_app": False,
            "upgrade_available": False,
            "image_updates_available": True,
            "state": "STOPPED",
        }
    ]
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"app.query": raw_apps},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_app()

    assert result["syncthing"]["running"] is False
    assert result["syncthing"]["update_available"] is False


async def test_get_app_honors_image_updates_for_custom_apps() -> None:
    raw_apps = [
        {
            "id": "custom1",
            "custom_app": True,
            "upgrade_available": False,
            "image_updates_available": True,
            "state": "RUNNING",
        }
    ]
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"app.query": raw_apps},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_app()

    assert result["custom1"]["update_available"] is True


async def test_get_certificates_keys_by_name_and_derives_days_until_expiry() -> None:
    raw_certificates = [
        {
            "id": 1,
            "name": "truenas_default",
            "cert_type": "CERTIFICATE",
            "common": "truenas.local",
            "until": "Fri Mar 26 00:59:59 2100",
            "expired": False,
            "renew_days": 10,
        }
    ]
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"certificate.query": raw_certificates},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_certificates()

    assert set(result.keys()) == {"truenas_default"}
    cert = result["truenas_default"]
    assert cert["id"] == 1
    assert cert["expired"] is False
    assert isinstance(cert["days_until_expiry"], int)
    assert cert["days_until_expiry"] > 0


async def test_get_certificates_sets_days_until_expiry_none_for_unparsable_until() -> (
    None
):
    raw_certificates = [{"id": 1, "name": "broken", "until": "not-a-date"}]
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"certificate.query": raw_certificates},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_certificates()

    assert result["broken"]["days_until_expiry"] is None


async def test_get_certificates_keeps_previous_snapshot_on_malformed_query() -> None:
    raw_certificates = [{"id": 1, "name": "truenas_default", "expired": False}]
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"certificate.query": raw_certificates},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_certificates()
            previous = state.ds["certificate"]

            server.responses["certificate.query"] = None
            result = await state.get_certificates()

    assert result is previous
    assert "truenas_default" in result


async def test_get_directoryservices_returns_empty_when_not_configured() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "directoryservices.config": {"service_type": None, "enable": False},
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_directoryservices()

    assert result == {}


async def test_get_directoryservices_merges_config_and_status_and_derives_healthy() -> (
    None
):
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "directoryservices.config": {
                "id": 1,
                "service_type": "ACTIVEDIRECTORY",
                "enable": True,
                "enable_account_cache": True,
                "enable_dns_updates": True,
                "kerberos_realm": "EXAMPLE.COM",
                "configuration": {"domain": "example.com", "site": "Default-First"},
            },
            "directoryservices.status": {
                "status": "HEALTHY",
                "status_msg": None,
            },
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_directoryservices()

    entry = result[1]
    assert entry["type"] == "ACTIVEDIRECTORY"
    assert entry["domain"] == "example.com"
    assert entry["site"] == "Default-First"
    assert entry["status"] == "HEALTHY"
    assert entry["healthy"] is True


async def test_get_directoryservices_derives_unhealthy_from_faulted_status() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "directoryservices.config": {
                "id": 1,
                "service_type": "LDAP",
                "enable": True,
            },
            "directoryservices.status": {"status": "FAULTED"},
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_directoryservices()

    assert result[1]["healthy"] is False


async def test_get_directoryservices_id_key_ignores_config_id() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "directoryservices.config": {
                "id": 42,
                "service_type": "LDAP",
                "enable": True,
            },
            "directoryservices.status": {"status": "HEALTHY"},
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_directoryservices()

    assert 42 not in result
    assert result[1]["type"] == "LDAP"
    assert result[1]["healthy"] is True


async def test_get_directoryservices_keeps_previous_snapshot_on_malformed_config() -> (
    None
):
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "directoryservices.config": {
                "id": 1,
                "service_type": "LDAP",
                "enable": True,
            },
            "directoryservices.status": {"status": "HEALTHY"},
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_directoryservices()
            previous = state.ds["directoryservices"]

            server.responses["directoryservices.config"] = None
            result = await state.get_directoryservices()

    assert result is previous
    assert result[1]["status"] == "HEALTHY"


async def test_get_directoryservices_keeps_previous_status_on_malformed_response() -> (
    None
):
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "directoryservices.config": {
                "id": 1,
                "service_type": "LDAP",
                "enable": True,
            },
            "directoryservices.status": {"status": "HEALTHY"},
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_directoryservices()

            server.responses["directoryservices.status"] = None
            result = await state.get_directoryservices()

    assert result[1]["status"] == "HEALTHY"
    assert result[1]["healthy"] is True


async def test_get_directoryservices_keeps_previous_status_on_status_missing_key() -> (
    None
):
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "directoryservices.config": {
                "id": 1,
                "service_type": "LDAP",
                "enable": True,
            },
            "directoryservices.status": {"status": "HEALTHY"},
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_directoryservices()

            server.responses["directoryservices.status"] = {}
            result = await state.get_directoryservices()

    assert result[1]["status"] == "HEALTHY"
    assert result[1]["healthy"] is True


@pytest.mark.parametrize("invalid_status", [None, [], ""])
async def test_get_directoryservices_keeps_previous_status_on_invalid_status_value(
    invalid_status: Any,
) -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "directoryservices.config": {
                "id": 1,
                "service_type": "LDAP",
                "enable": True,
            },
            "directoryservices.status": {"status": "HEALTHY"},
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_directoryservices()

            server.responses["directoryservices.status"] = {"status": invalid_status}
            result = await state.get_directoryservices()

    assert result[1]["status"] == "HEALTHY"
    assert result[1]["healthy"] is True


async def test_get_alerts_excludes_dismissed_and_aggregates_by_level() -> None:
    raw_alerts = [
        {
            "uuid": "a1",
            "level": "CRITICAL",
            "klass": "PoolStatus",
            "title": "Pool degraded",
            "formatted": "Pool tank is degraded",
            "dismissed": False,
        },
        {
            "uuid": "a2",
            "level": "WARNING",
            "klass": "CertificateExpiry",
            "title": "Certificate expiring",
            "formatted": "Certificate expiring soon",
            "dismissed": False,
        },
        {
            "uuid": "a3",
            "level": "INFO",
            "klass": "Update",
            "title": "Update available",
            "formatted": "An update is available",
            "dismissed": True,
        },
    ]
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"alert.list": raw_alerts},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_alerts()

    assert result["count"] == 2
    assert result["critical"] == 1
    assert result["warning"] == 1
    assert result["info"] == 0
    assert result["uuids"] == ["a1", "a2"]
    assert result["messages"] == ["Pool tank is degraded", "Certificate expiring soon"]
    assert result["disk_issues"] is True


async def test_get_alerts_disk_issues_false_without_disk_pool_or_smart_match() -> None:
    raw_alerts = [
        {
            "uuid": "a1",
            "level": "INFO",
            "klass": "Update",
            "title": "Update available",
            "formatted": "An update is available",
            "dismissed": False,
        }
    ]
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"alert.list": raw_alerts},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_alerts()

    assert result["disk_issues"] is False


async def test_get_alerts_disk_issues_true_for_title_or_klass_cross_matches() -> None:
    raw_alerts = [
        {
            "uuid": "a1",
            "level": "WARNING",
            "klass": "SmartTest",
            "title": "Something failed",
            "formatted": "SMART self-test failed",
            "dismissed": False,
        },
        {
            "uuid": "a2",
            "level": "WARNING",
            "klass": "Hardware",
            "title": "Disk removed",
            "formatted": "Disk was removed",
            "dismissed": False,
        },
    ]
    for alert in raw_alerts:
        async with FakeTrueNASServer(
            valid_api_key=API_KEY,
            responses={"alert.list": [alert]},
        ) as server:
            async with make_client(server) as client:
                await client.connect()
                state = TrueNASState(client)
                result = await state.get_alerts()

        assert result["disk_issues"] is True


async def test_get_alerts_keeps_previous_state_on_malformed_response() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"alert.list": [{"uuid": "a1", "level": "CRITICAL"}]},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_alerts()
            previous_alerts = state.ds["alerts"]

            server.responses["alert.list"] = None
            result = await state.get_alerts()

    assert result is previous_alerts
    assert state.ds["alerts"] is previous_alerts


@pytest.mark.parametrize("malformed_alert_list", [[None], [{}]])
async def test_get_alerts_keeps_previous_state_on_unusable_entries(
    malformed_alert_list: list[Any],
) -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"alert.list": [{"uuid": "a1", "level": "CRITICAL"}]},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_alerts()
            previous_alerts = state.ds["alerts"]

            server.responses["alert.list"] = malformed_alert_list
            result = await state.get_alerts()

    assert result is previous_alerts
    assert state.ds["alerts"] is previous_alerts


async def test_get_ups_keeps_previous_reading_when_discovery_raises() -> None:
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

            server.responses["reporting.netdata_graphs"] = {
                "error": {
                    "code": -32603,
                    "message": "Internal error",
                    "data": {"error": 1, "errname": "EFAULT", "reason": None},
                }
            }
            result = await state.get_ups()

    assert result is previous_ups
    assert state.ds["ups"] is previous_ups


async def test_get_interface_normalizes_and_derives_link_up() -> None:
    raw_interfaces = [
        {
            "id": "eno1",
            "name": "eno1",
            "description": "",
            "mtu": 1500,
            "state": {
                "link_state": "LINK_STATE_UP",
                "active_media_type": "Ethernet",
                "active_media_subtype": "1000baseT Full-duplex",
                "link_address": "aa:bb:cc:dd:ee:ff",
            },
        },
        {
            "id": "eno2",
            "name": "eno2",
            "state": {"link_state": "LINK_STATE_DOWN"},
        },
    ]
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"interface.query": raw_interfaces},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_interface()

    assert result["eno1"] == {
        "id": "eno1",
        "name": "eno1",
        "description": "",
        "mtu": 1500,
        "link_state": "LINK_STATE_UP",
        "active_media_type": "Ethernet",
        "active_media_subtype": "1000baseT Full-duplex",
        "link_address": "aa:bb:cc:dd:ee:ff",
        "rx": 0,
        "tx": 0,
        "link_up": True,
    }
    assert result["eno2"]["link_up"] is False
    assert state.ds["interface"] == result


async def test_get_scrub_normalizes_pool_scrub_query() -> None:
    raw_scrubs = [{"id": 1, "pool_name": "tank", "enabled": True}]
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"pool.scrub.query": raw_scrubs},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_scrub()

    assert result == {1: {"id": 1, "pool_name": "tank", "enabled": True}}
    assert state.ds["scrub"] == result


async def test_get_smb_counts_sessions_from_list_response() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"smb.status": [{}, {}, {}]},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_smb()

    assert result == {"connections": 3}
    assert state.ds["smb"] == result


async def test_get_smb_counts_sessions_from_dict_response() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"smb.status": {"sessions": [{}, {}]}},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_smb()

    assert result == {"connections": 2}


async def test_get_smb_keeps_previous_count_on_malformed_response() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"smb.status": [{}, {}]},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_smb()

            server.responses["smb.status"] = None
            result = await state.get_smb()

    assert result == {"connections": 2}


async def test_get_smb_keeps_previous_count_when_query_raises() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"smb.status": [{}, {}]},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_smb()

            server.responses["smb.status"] = {
                "error": {
                    "code": -32603,
                    "message": "Internal error",
                    "data": {"error": 1, "errname": "EFAULT", "reason": None},
                }
            }
            result = await state.get_smb()

    assert result == {"connections": 2}


async def test_get_update_reports_available_update_with_manifest_fields() -> None:
    raw_status = {
        "status": {
            "state": "AVAILABLE",
            "new_version": {
                "version": "25.10.0",
                "manifest": {
                    "date": "2026-01-01",
                    "profile": "GENERAL",
                    "train": "TrueNAS-25.10-STABLE",
                    "filename": "x.update",
                },
            },
        }
    }
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"update.status": raw_status},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_update()

    assert result == {
        "update_available": True,
        "update_state": "AVAILABLE",
        "update_version": "25.10.0",
        "update_date": "2026-01-01",
        "update_profile": "GENERAL",
        "update_train": "TrueNAS-25.10-STABLE",
        "update_filename": "x.update",
    }
    assert state.ds["update"] == result


async def test_get_update_resets_to_no_update_pending_without_new_version() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"update.status": {"status": {"state": "IDLE"}}},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_update()

    assert result == {
        "update_available": False,
        "update_state": "IDLE",
        "update_version": "up-to-date",
        "update_date": None,
        "update_profile": None,
        "update_train": None,
        "update_filename": None,
    }


async def test_get_update_resets_to_no_update_pending_on_malformed_response() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"update.status": None},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_update()

    assert result["update_available"] is False
    assert result["update_state"] == "IDLE"


_DISK_SDA = {
    "name": "sda",
    "devname": "sda",
    "serial": "S1",
    "size": "1TB",
    "identifier": "{serial}S1",
}


async def test_get_disk_normalizes_and_applies_netdata_temperature() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "disk.query": [_DISK_SDA],
            "reporting.netdata_graphs": [
                {
                    "name": "disktemp",
                    "title": "Disk Temperature",
                    "vertical_label": "Celsius",
                }
            ],
            "reporting.netdata_graph": [
                {"identifier": "{serial}S1", "aggregations": {"mean": {"sda": 35.0}}}
            ],
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_disk()

    assert result["{serial}S1"]["temperature"] == 35.0
    assert state.ds["disk"] == result


async def test_get_disk_falls_back_to_disk_temperatures_when_no_netdata_graph() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "disk.query": [_DISK_SDA],
            "reporting.netdata_graphs": [],
            "disk.temperatures": {"sda": 42.5},
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_disk()

    assert result["{serial}S1"]["temperature"] == 42.5


async def test_get_disk_falls_back_when_netdata_query_fails_after_graph_found() -> None:
    """A netdata graph is discovered, but the actual reading query then fails.

    The ``disk.temperatures`` fallback must still run for every disk instead
    of being skipped because of the earlier netdata failure.
    """
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "disk.query": [_DISK_SDA],
            "reporting.netdata_graphs": [
                {
                    "name": "disktemp",
                    "title": "Disk Temperature",
                    "vertical_label": "Celsius",
                }
            ],
            "reporting.netdata_graph": {
                "error": {
                    "code": -32603,
                    "message": "Internal error",
                    "data": {"error": 1, "errname": "EFAULT", "reason": None},
                }
            },
            "disk.temperatures": {"sda": 42.5},
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_disk()

    assert result["{serial}S1"]["temperature"] == 42.5


async def test_get_disk_keeps_temperature_none_when_enrichment_fails() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "disk.query": [_DISK_SDA],
            "reporting.netdata_graphs": {
                "error": {
                    "code": -32603,
                    "message": "Internal error",
                    "data": {"error": 1, "errname": "EFAULT", "reason": None},
                }
            },
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_disk()

    assert result["{serial}S1"]["temperature"] is None


async def test_get_systeminfo_normalizes_and_derives_uptime_epoch() -> None:
    raw_system_info = {
        "version": "TrueNAS-25.10.0",
        "hostname": "truenas",
        "uptime_seconds": 3600,
        "system_serial": "SN123",
        "system_product": "TrueNAS Mini",
        "system_manufacturer": "iXsystems",
        "physmem": 16_000_000_000,
    }
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"system.info": raw_system_info},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_systeminfo()

    assert result["version"] == "TrueNAS-25.10.0"
    assert result["hostname"] == "truenas"
    assert result["system_serial"] == "SN123"
    assert result["memory-total_value"] == 16_000_000_000
    assert isinstance(result["uptimeEpoch"], int)
    assert result["uptimeEpoch"] > 0
    assert state.ds["system_info"] == result


async def test_get_systeminfo_keeps_previous_total_memory_on_bogus_physmem() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"system.info": {"physmem": 16_000_000_000}},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_systeminfo()
            assert state.ds["system_info"]["memory-total_value"] == 16_000_000_000

            server.responses["system.info"] = {"physmem": 0}
            result = await state.get_systeminfo()

    assert result["memory-total_value"] == 16_000_000_000


async def test_get_systeminfo_keeps_previous_total_memory_on_infinite_physmem() -> None:
    """A non-finite physmem must not be cached, since a later usage-percent
    calculation (100 * (total - available) / total) would produce nan for
    an infinite total, and round(nan) (no ndigits) raises ValueError.
    """
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"system.info": {"physmem": 16_000_000_000}},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_systeminfo()

            server.responses["system.info"] = {"physmem": float("inf")}
            result = await state.get_systeminfo()

    assert result["memory-total_value"] == 16_000_000_000


async def test_get_systeminfo_keeps_previous_total_memory_on_oversized_physmem() -> (
    None
):
    """An int too large to convert to float must not raise (OverflowError)."""
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"system.info": {"physmem": 16_000_000_000}},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_systeminfo()

            server.responses["system.info"] = {"physmem": 10**400}
            result = await state.get_systeminfo()

    assert result["memory-total_value"] == 16_000_000_000


async def test_get_systeminfo_keeps_uptime_epoch_stable_on_oversized_uptime() -> None:
    """An int too large to convert to float must not raise (OverflowError)."""
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"system.info": {"uptime_seconds": 1000}},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_systeminfo()
            first_epoch = state.ds["system_info"]["uptimeEpoch"]

            server.responses["system.info"] = {"uptime_seconds": 10**400}
            result = await state.get_systeminfo()

    assert result["uptimeEpoch"] == first_epoch


async def test_get_systeminfo_keeps_uptime_epoch_stable_within_tolerance() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"system.info": {"uptime_seconds": 1000}},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_systeminfo()
            first_epoch = state.ds["system_info"]["uptimeEpoch"]

            # A few seconds of poll jitter in the reported uptime must not
            # move the derived boot-time epoch.
            server.responses["system.info"] = {"uptime_seconds": 1003}
            await state.get_systeminfo()

    assert state.ds["system_info"]["uptimeEpoch"] == first_epoch


async def test_get_systeminfo_preserves_previous_values_on_malformed_response() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"system.info": {"hostname": "truenas", "uptime_seconds": 500}},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_systeminfo()
            previous = state.ds["system_info"]

            server.responses["system.info"] = None
            result = await state.get_systeminfo()

    assert result["hostname"] == "truenas"
    assert result is previous


async def test_get_systeminfo_preserves_previous_values_on_list_response() -> None:
    """A malformed list-shaped ``system.info`` response is rejected outright.

    Unlike a scalar/``None`` response (already caught by ``parse_api()``
    itself), a list of dicts would otherwise be accepted as ordinary
    multi-entry source data and applied to the singleton state entry by
    entry, letting unexpected extra entries silently overwrite cached
    fields.
    """
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"system.info": {"hostname": "truenas", "uptime_seconds": 500}},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_systeminfo()
            previous = state.ds["system_info"]

            server.responses["system.info"] = [
                {"hostname": "bogus1"},
                {"hostname": "bogus2"},
            ]
            result = await state.get_systeminfo()

    assert result["hostname"] == "truenas"
    assert result is previous


async def test_get_systeminfo_derives_uptime_epoch_from_zero_uptime() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"system.info": {"uptime_seconds": 0}},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_systeminfo()

    assert result["uptimeEpoch"] > 0


async def test_get_systemstats_updates_load_cpu_memory_arc_and_cputemp() -> None:
    def netdata_graph(params: list) -> Any:
        graph_name = params[0]
        if graph_name == "load":
            return [
                {
                    "legend": ["shortterm", "midterm", "longterm"],
                    "aggregations": {
                        "mean": {"shortterm": 0.5, "midterm": 0.75, "longterm": 1.0}
                    },
                }
            ]
        if graph_name == "cpu":
            return [{"legend": ["cpu"], "aggregations": {"mean": {"cpu": 12.345}}}]
        if graph_name == "cputemp":
            return [{"aggregations": {"mean": {"core0": 40.0, "core1": 45.5}}}]
        if graph_name == "memory":
            return [
                {
                    "legend": ["available"],
                    "aggregations": {"mean": {"available": 2000.0}},
                }
            ]
        if graph_name == "arcsize":
            return [{"legend": ["size"], "aggregations": {"mean": {"size": 1500.0}}}]
        raise AssertionError(f"unexpected graph {graph_name}")

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": {"physmem": 8000.0},
            "reporting.netdata_graph": netdata_graph,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_systeminfo()
            result = await state.get_systemstats()

    assert result["load_shortterm"] == 0.5
    assert result["load_midterm"] == 0.75
    assert result["load_longterm"] == 1.0
    assert result["cpu_usage"] == 12.35
    assert result["cpu_temperature"] == 45.5
    assert result["memory-free_value"] == 2000.0
    assert result["memory-total_value"] == 8000.0
    assert result["memory-usage_percent"] == 75
    assert result["cache_size-arc_value"] == 1500.0
    assert state.ds["system_info"] == result


async def test_get_systemstats_skips_cputemp_on_virtual_machine() -> None:
    called_graphs: list[str] = []

    def netdata_graph(params: list) -> Any:
        called_graphs.append(params[0])
        return None

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": {
                "system_manufacturer": "QEMU",
                "system_product": "Standard PC",
            },
            "reporting.netdata_graph": netdata_graph,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_systeminfo()
            await state.get_systemstats()

    assert "cputemp" not in called_graphs


async def test_get_systeminfo_handles_unhashable_manufacturer() -> None:
    """A malformed list value must not raise (set membership needs hashable)."""
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": {
                "system_manufacturer": ["QEMU"],
                "system_product": ["Standard PC"],
            }
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_systeminfo()

    assert result["system_manufacturer"] == ["QEMU"]


async def test_get_systemstats_skips_cputemp_on_vm_without_get_systeminfo() -> None:
    called_graphs: list[str] = []

    def netdata_graph(params: list) -> Any:
        called_graphs.append(params[0])
        return None

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": {
                "system_manufacturer": "QEMU",
                "system_product": "Standard PC",
            },
            "reporting.netdata_graph": netdata_graph,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_systemstats()

    assert "cputemp" not in called_graphs


async def test_get_systemstats_enriches_interface_throughput() -> None:
    raw_interfaces = [
        {"id": "eno1", "name": "eno1", "state": {"link_state": "LINK_STATE_UP"}}
    ]

    def netdata_graph(params: list) -> Any:
        if params[0] == "interface":
            return [
                {
                    "identifier": "eno1",
                    "legend": ["received", "sent"],
                    "aggregations": {"mean": {"received": 8192.0, "sent": 4096.0}},
                }
            ]
        return None

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": {},
            "interface.query": raw_interfaces,
            "reporting.netdata_graph": netdata_graph,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_interface()
            await state.get_systemstats()

    assert state.ds["interface"]["eno1"]["rx"] == 1000.0
    assert state.ds["interface"]["eno1"]["tx"] == 500.0


async def test_get_systemstats_keeps_previous_throughput_on_malformed_item() -> None:
    raw_interfaces = [
        {"id": "eno1", "name": "eno1", "state": {"link_state": "LINK_STATE_UP"}}
    ]
    call_count = 0

    def netdata_graph(params: list) -> Any:
        nonlocal call_count
        if params[0] != "interface":
            return None
        call_count += 1
        if call_count == 1:
            return [
                {
                    "identifier": "eno1",
                    "legend": ["received", "sent"],
                    "aggregations": {"mean": {"received": 8192.0, "sent": 4096.0}},
                }
            ]
        return [{"identifier": "eno1"}]

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": {},
            "interface.query": raw_interfaces,
            "reporting.netdata_graph": netdata_graph,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_interface()
            await state.get_systemstats()
            assert state.ds["interface"]["eno1"]["rx"] == 1000.0

            await state.get_systemstats()

    assert state.ds["interface"]["eno1"]["rx"] == 1000.0
    assert state.ds["interface"]["eno1"]["tx"] == 500.0


async def test_get_systemstats_skips_interface_query_without_prior_get_interface() -> (
    None
):
    called_graphs: list[str] = []

    def netdata_graph(params: list) -> Any:
        called_graphs.append(params[0])
        return None

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"system.info": {}, "reporting.netdata_graph": netdata_graph},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_systemstats()

    assert "interface" not in called_graphs


async def test_get_systemstats_keeps_previous_value_on_malformed_graph() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": {},
            "reporting.netdata_graph": lambda params: [
                {"legend": ["cpu"], "aggregations": {"mean": {"cpu": 20.0}}}
            ],
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_systemstats()
            assert state.ds["system_info"]["cpu_usage"] == 20.0

            server.responses["reporting.netdata_graph"] = None
            await state.get_systemstats()

    assert state.ds["system_info"]["cpu_usage"] == 20.0


async def test_get_systemstats_keeps_previous_value_when_graph_query_raises() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": {},
            "reporting.netdata_graph": lambda params: [
                {"legend": ["cpu"], "aggregations": {"mean": {"cpu": 20.0}}}
            ],
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_systemstats()
            assert state.ds["system_info"]["cpu_usage"] == 20.0

            server.responses["reporting.netdata_graph"] = {
                "error": {
                    "code": -32603,
                    "message": "Internal error",
                    "data": {"error": 1, "errname": "EFAULT", "reason": None},
                }
            }
            await state.get_systemstats()

    assert state.ds["system_info"]["cpu_usage"] == 20.0


async def test_get_systemstats_queries_graphs_when_virtual_detection_fails() -> None:
    """A failed lazy ``system.info`` (virtualization detection) call must not
    abort the rest of ``get_systemstats()`` -- each netdata graph is
    independent and best-effort, matching every other failure mode this
    endpoint already tolerates.
    """
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": {
                "error": {
                    "code": -32603,
                    "message": "Internal error",
                    "data": {"error": 1, "errname": "EFAULT", "reason": None},
                }
            },
            "reporting.netdata_graph": lambda params: [
                {"legend": ["cpu"], "aggregations": {"mean": {"cpu": 20.0}}}
            ],
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_systemstats()

    assert result["cpu_usage"] == 20.0


async def test_systemstats_stale_graphs_empty_before_first_call() -> None:
    async with FakeTrueNASServer(valid_api_key=API_KEY) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)

    assert state.systemstats_stale_graphs == frozenset()


def _well_formed_netdata_graph(params: list) -> Any:
    """Return a realistic, correctly-shaped response for every systemstats
    graph -- unlike a single shared fixture value, this lets a "full
    success" test actually succeed for every graph rather than only "cpu"
    (whose shape happens to also satisfy ``_netdata_max_mean``, used for
    "cputemp", but not the named-series lookups "load"/"memory"/"arcsize"
    need).
    """
    graph_name = params[0]
    if graph_name == "load":
        return [
            {
                "legend": ["shortterm", "midterm", "longterm"],
                "aggregations": {
                    "mean": {"shortterm": 0.5, "midterm": 0.75, "longterm": 1.0}
                },
            }
        ]
    if graph_name == "memory":
        return [
            {"legend": ["available"], "aggregations": {"mean": {"available": 2000.0}}}
        ]
    if graph_name == "arcsize":
        return [{"legend": ["size"], "aggregations": {"mean": {"size": 1500.0}}}]
    return [{"legend": ["cpu"], "aggregations": {"mean": {"cpu": 20.0}}}]


async def test_systemstats_stale_graphs_empty_on_full_success() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": {},
            "reporting.netdata_graph": _well_formed_netdata_graph,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_systemstats()

    assert state.systemstats_stale_graphs == frozenset()


async def test_systemstats_stale_graphs_reports_failed_graph() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": {},
            "reporting.netdata_graph": {
                "error": {
                    "code": -32603,
                    "message": "Internal error",
                    "data": {"error": 1, "errname": "EFAULT", "reason": None},
                }
            },
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_systemstats()

    assert state.systemstats_stale_graphs == frozenset(
        {"load", "cpu", "cputemp", "memory", "arcsize"}
    )


async def test_systemstats_stale_graphs_reports_failed_interface_graph() -> None:
    raw_interfaces = [
        {"id": "eno1", "name": "eno1", "state": {"link_state": "LINK_STATE_UP"}}
    ]

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": {},
            "interface.query": raw_interfaces,
            "reporting.netdata_graph": lambda params: (
                {
                    "error": {
                        "code": -32603,
                        "message": "Internal error",
                        "data": {"error": 1, "errname": "EFAULT", "reason": None},
                    }
                }
                if params[0] == "interface"
                else [{"legend": ["cpu"], "aggregations": {"mean": {"cpu": 20.0}}}]
            ),
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_interface()
            await state.get_systemstats()

    assert "interface" in state.systemstats_stale_graphs
    assert "cpu" not in state.systemstats_stale_graphs


async def test_systemstats_stale_graphs_reset_on_next_successful_call() -> None:
    call_count = 0

    def netdata_graph(params: list) -> Any:
        nonlocal call_count
        call_count += 1
        # 5 systemstats graphs (load, cpu, cputemp, memory, arcsize) are
        # queried per get_systemstats() call; fail all of them on the first
        # round, then succeed on the second.
        if call_count <= 5:
            return {
                "error": {
                    "code": -32603,
                    "message": "Internal error",
                    "data": {"error": 1, "errname": "EFAULT", "reason": None},
                }
            }
        return _well_formed_netdata_graph(params)

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"system.info": {}, "reporting.netdata_graph": netdata_graph},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_systemstats()
            assert state.systemstats_stale_graphs

            await state.get_systemstats()

    assert state.systemstats_stale_graphs == frozenset()


async def test_systemstats_stale_graphs_reports_malformed_graph_without_rpc_error() -> (
    None
):
    """A graph query that succeeds (no ``TrueNASError``) but returns a
    malformed/empty payload must be reported as stale too -- not just an
    outright RPC failure. Otherwise a caller sees ``systemstats_stale_graphs``
    empty even though e.g. ``cpu_usage`` was silently left at its previous
    value.
    """
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": {},
            "reporting.netdata_graph": _well_formed_netdata_graph,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_systemstats()
            assert state.systemstats_stale_graphs == frozenset()

            server.responses["reporting.netdata_graph"] = lambda params: None
            await state.get_systemstats()

    assert "cpu" in state.systemstats_stale_graphs


async def test_systemstats_stale_graphs_reports_malformed_interface_without_error() -> (
    None
):
    """Mirrors the graph-level case above for the interface throughput
    enrichment: an RPC that succeeds but returns no usable interface entries
    must mark ``"interface"`` stale, not just an outright RPC failure.
    """
    raw_interfaces = [
        {"id": "eno1", "name": "eno1", "state": {"link_state": "LINK_STATE_UP"}}
    ]

    def netdata_graph(params: list) -> Any:
        if params[0] == "interface":
            return []
        return [{"legend": ["cpu"], "aggregations": {"mean": {"cpu": 20.0}}}]

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": {},
            "interface.query": raw_interfaces,
            "reporting.netdata_graph": netdata_graph,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_interface()
            await state.get_systemstats()

    assert "interface" in state.systemstats_stale_graphs
    assert "cpu" not in state.systemstats_stale_graphs


async def test_systemstats_stale_graphs_reports_interface_with_no_matching_id() -> None:
    """A graph response with entries is still "no usable reading" if none of
    its identifiers match a known interface, or their throughput is empty --
    both must mark ``"interface"`` stale, not just an empty/failed response.
    """
    raw_interfaces = [
        {"id": "eno1", "name": "eno1", "state": {"link_state": "LINK_STATE_UP"}}
    ]

    def netdata_graph(params: list) -> Any:
        if params[0] == "interface":
            return [
                {
                    "identifier": "unknown0",
                    "legend": ["received", "sent"],
                    "aggregations": {"mean": {"received": 8192.0, "sent": 4096.0}},
                },
                {"identifier": "eno1"},
            ]
        return [{"legend": ["cpu"], "aggregations": {"mean": {"cpu": 20.0}}}]

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": {},
            "interface.query": raw_interfaces,
            "reporting.netdata_graph": netdata_graph,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_interface()
            await state.get_systemstats()

    assert "interface" in state.systemstats_stale_graphs
    assert "cpu" not in state.systemstats_stale_graphs
    assert state.ds["interface"]["eno1"]["rx"] == 0
