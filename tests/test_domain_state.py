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
