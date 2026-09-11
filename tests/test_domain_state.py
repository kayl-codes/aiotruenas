"""Integration tests for TrueNASState against the fake WebSocket server."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

import pytest
from fake_server import FakeTrueNASServer

from aiotruenas import TrueNASClient, TrueNASError, TrueNASState

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


async def test_get_pool_logs_warning_when_pool_query_is_malformed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A malformed ``pool.query`` response must surface a warning and clear
    it again once the endpoint recovers -- otherwise the pool snapshot can
    silently go stale with no trace anywhere.
    """
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
            with caplog.at_level(logging.DEBUG, logger="aiotruenas.domain.state"):
                await state.get_pool()

                server.responses["pool.query"] = None
                await state.get_pool()

                server.responses["pool.query"] = [_POOL_TANK]
                await state.get_pool()

    state_records = [r for r in caplog.records if r.name == "aiotruenas.domain.state"]
    warnings = [r for r in state_records if r.levelno == logging.WARNING]
    recoveries = [
        r
        for r in state_records
        if r.levelno == logging.DEBUG and "recovered" in r.getMessage()
    ]
    assert len(warnings) == 1
    assert "pool.query" in warnings[0].getMessage()
    assert len(recoveries) == 1


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
            stale = state.ups_stale_graphs

    assert result is previous_ups
    assert state.ds["ups"] is previous_ups
    # Discovery itself failed, so nothing was refreshed this poll -- every
    # field still in the (unrefreshed) snapshot is stale, not just left
    # unchanged from before.
    assert stale == frozenset({"upscharge"})


async def test_get_ups_reports_no_stale_graphs_on_first_ever_discovery_failure() -> (
    None
):
    """A failed discovery call on the very first ``get_ups()`` call ever must
    not report any graph as stale -- ``ds["ups"]`` is empty at that point, so
    there is nothing to be stale, unlike the case above where a previous
    snapshot already exists.
    """
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"reporting.netdata_graphs": None},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_ups()
            stale = state.ups_stale_graphs

    assert result == {}
    assert stale == frozenset()


async def test_get_service_derives_running_and_known_display_name() -> None:
    raw_services = [
        {
            "id": 1,
            "service": "cifs",
            "name": "unknown",
            "enable": True,
            "state": "RUNNING",
            "pids": [1234],
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
    assert result[1]["pids"] == [1234]
    assert result[2]["running"] is False
    assert result[2]["display_name"] == "Secure Shell"
    assert result[2]["pids"] == []


async def test_get_service_pids_default_is_not_shared_across_entries() -> None:
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

    assert result[1]["pids"] is not result[2]["pids"]
    result[1]["pids"].append(999)
    assert result[2]["pids"] == []


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


async def test_get_container_v26_matches_real_payload_shape() -> None:
    """Regression test for the full real container.query() shape.

    Verified against a real TrueNAS 26 instance on 2026-09-07 (see the
    comment above ``_CONTAINER_V26_VALS`` in ``_specs.py``): the API's own
    JSON schema and a live query both confirm the entry never carries
    memory, image or IP information anywhere, including nested under
    ``devices`` or ``status`` -- a NIC device carries only
    dtype/type/nic_attach/mac, never an address. This fixture freezes that
    full observed shape (minus identifying values), including a NIC device,
    and the key-set assertion below pins the current output shape against
    accidental changes to ``_CONTAINER_V26_VALS`` / ``_CONTAINER_V26_ENSURE_VALS``.

    Note this only guards ``image`` against a future payload that actually
    populates it (it is read from ``description`` and defaults to
    "unknown" only when that is falsy). ``memory``, ``aliases`` and
    ``ip_address`` are unconditionally overwritten with their constants in
    ``_compute_container_v26`` -- by design, since no such fields exist to
    read -- so this test cannot detect a future TrueNAS release that starts
    populating those; that would require changing ``_compute_container_v26``
    itself to read them.
    """
    raw_containers = [
        {
            "id": 1,
            "uuid": "11111111-2222-3333-4444-555555555555",
            "name": "test-linux",
            "description": "",
            "devices": [
                {
                    "dtype": "NIC",
                    "type": "NIC",
                    "nic_attach": "br0",
                    "mac": "00:00:00:00:00:00",
                },
            ],
            "cpuset": "1",
            "autostart": True,
            "time": "LOCAL",
            "shutdown_timeout": 90,
            "dataset": "tank/.truenas_containers/containers/test-linux",
            "init": "/sbin/init",
            "initdir": None,
            "initenv": {},
            "inituser": None,
            "initgroup": None,
            "idmap": {"type": "DEFAULT"},
            "capabilities_policy": "DEFAULT",
            "capabilities_state": {},
            "default_network": "br0",
            "status": {"state": "RUNNING", "pid": 7463, "domain_state": "RUNNING"},
        }
    ]
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": {"version": "TrueNAS-26.0.0"},
            "container.query": raw_containers,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_container()

    assert set(result[1]) == {
        "id",
        "name",
        "type",
        "cpu",
        "memory",
        "image",
        "status",
        "autostart",
        "aliases",
        "running",
        "ip_address",
    }
    assert result[1]["type"] == "CONTAINER"
    assert result[1]["cpu"] == 1
    assert result[1]["memory"] == 0
    assert result[1]["image"] == "unknown"
    assert result[1]["ip_address"] == "unknown"
    assert result[1]["running"] is True


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


async def test_detect_version_logs_warning_and_retries_on_non_dict_system_info(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The ``_detect_version()`` half of the malformed-``system.info`` warning
    (reached via ``get_container()``, mirroring ``get_systeminfo()``'s own
    non-dict handling) must also warn once and recover once a valid response
    follows -- this is the symmetric call site the local code-reviewer pass
    flagged as unverified by any log assertion.
    """
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": None,
            "virt.instance.query": [],
            "container.query": [],
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            with caplog.at_level(logging.DEBUG, logger="aiotruenas.domain.state"):
                await state.get_container()

                server.responses["system.info"] = {"version": "TrueNAS-26.0.0"}
                await state.get_container()

    state_records = [r for r in caplog.records if r.name == "aiotruenas.domain.state"]
    warnings = [r for r in state_records if r.levelno == logging.WARNING]
    recoveries = [
        r
        for r in state_records
        if r.levelno == logging.DEBUG and "recovered" in r.getMessage()
    ]
    assert len(warnings) == 1
    assert "malformed" in warnings[0].getMessage().lower()
    assert len(recoveries) == 1


async def test_detect_version_logs_warning_and_retries_on_unparsable_version(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An unparsable ``version`` field in ``system.info`` must warn once
    (not silently cache (0, 0)) and retry detection on the next call, logging
    recovery once the field becomes parsable. Regression test for a
    previously fully silent fallback to (0, 0) that left version-gated
    endpoints like ``get_container()`` on legacy behavior with no signal.
    """
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": {"version": "not-a-version"},
            "virt.instance.query": [],
            "container.query": [],
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            with caplog.at_level(logging.DEBUG, logger="aiotruenas.domain.state"):
                await state.get_container()

                server.responses["system.info"] = {"version": "TrueNAS-26.0.0"}
                await state.get_container()

    state_records = [r for r in caplog.records if r.name == "aiotruenas.domain.state"]
    warnings = [r for r in state_records if r.levelno == logging.WARNING]
    recoveries = [
        r
        for r in state_records
        if r.levelno == logging.DEBUG and "recovered" in r.getMessage()
    ]
    assert len(warnings) == 1
    assert "version" in warnings[0].getMessage().lower()
    assert "not-a-version" in warnings[0].getMessage()
    assert len(recoveries) == 1


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


async def test_get_directoryservices_logs_warning_when_config_is_malformed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A malformed ``directoryservices.config`` response must surface a
    warning and clear it again once the endpoint recovers.
    """
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
            with caplog.at_level(logging.DEBUG, logger="aiotruenas.domain.state"):
                await state.get_directoryservices()

                server.responses["directoryservices.config"] = None
                await state.get_directoryservices()

                server.responses["directoryservices.config"] = {
                    "id": 1,
                    "service_type": "LDAP",
                    "enable": True,
                }
                await state.get_directoryservices()

    state_records = [r for r in caplog.records if r.name == "aiotruenas.domain.state"]
    warnings = [r for r in state_records if r.levelno == logging.WARNING]
    recoveries = [
        r
        for r in state_records
        if r.levelno == logging.DEBUG and "recovered" in r.getMessage()
    ]
    assert len(warnings) == 1
    assert "directoryservices.config" in warnings[0].getMessage()
    assert len(recoveries) == 1


async def test_get_directoryservices_logs_warning_when_status_is_malformed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A malformed ``directoryservices.status`` response must surface a
    warning and clear it again once the endpoint recovers -- tracked
    separately from ``directoryservices.config``'s own failing flag.
    """
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
            with caplog.at_level(logging.DEBUG, logger="aiotruenas.domain.state"):
                await state.get_directoryservices()

                server.responses["directoryservices.status"] = None
                await state.get_directoryservices()

                server.responses["directoryservices.status"] = {"status": "HEALTHY"}
                await state.get_directoryservices()

    state_records = [r for r in caplog.records if r.name == "aiotruenas.domain.state"]
    warnings = [r for r in state_records if r.levelno == logging.WARNING]
    recoveries = [
        r
        for r in state_records
        if r.levelno == logging.DEBUG and "recovered" in r.getMessage()
    ]
    assert len(warnings) == 1
    assert "directoryservices.status" in warnings[0].getMessage()
    assert len(recoveries) == 1


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


async def test_get_alerts_logs_warning_across_both_failure_modes(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Both ``alert.list`` failure branches -- a non-list response and a
    list with no usable entries -- share one failing/recovered key, so
    switching between them must not double-warn, and a single recovery
    must clear the flag regardless of which failure mode was active last.
    """
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"alert.list": [{"uuid": "a1", "level": "CRITICAL"}]},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            with caplog.at_level(logging.DEBUG, logger="aiotruenas.domain.state"):
                await state.get_alerts()

                server.responses["alert.list"] = None
                await state.get_alerts()

                server.responses["alert.list"] = [{}]
                await state.get_alerts()

                server.responses["alert.list"] = [{"uuid": "a1", "level": "CRITICAL"}]
                await state.get_alerts()

    state_records = [r for r in caplog.records if r.name == "aiotruenas.domain.state"]
    warnings = [r for r in state_records if r.levelno == logging.WARNING]
    recoveries = [
        r
        for r in state_records
        if r.levelno == logging.DEBUG and "recovered" in r.getMessage()
    ]
    assert len(warnings) == 1
    assert "alert.list" in warnings[0].getMessage()
    assert len(recoveries) == 1


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
            stale = state.ups_stale_graphs

    assert result is previous_ups
    assert state.ds["ups"] is previous_ups
    # Discovery itself failed, so nothing was refreshed this poll -- every
    # field still in the (unrefreshed) snapshot is stale, not just left
    # unchanged from before.
    assert stale == frozenset({"upscharge"})


async def test_get_ups_logs_warning_when_graph_discovery_raises(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A raising ``reporting.netdata_graphs`` discovery call must surface a
    warning and clear it again once the endpoint recovers.
    """
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
            with caplog.at_level(logging.DEBUG, logger="aiotruenas.domain.state"):
                await state.get_ups()

                server.responses["reporting.netdata_graphs"] = {
                    "error": {
                        "code": -32603,
                        "message": "Internal error",
                        "data": {"error": 1, "errname": "EFAULT", "reason": None},
                    }
                }
                await state.get_ups()

                server.responses["reporting.netdata_graphs"] = [{"name": "upscharge"}]
                await state.get_ups()

    state_records = [r for r in caplog.records if r.name == "aiotruenas.domain.state"]
    warnings = [r for r in state_records if r.levelno == logging.WARNING]
    recoveries = [
        r
        for r in state_records
        if r.levelno == logging.DEBUG and "recovered" in r.getMessage()
    ]
    assert len(warnings) == 1
    assert "UPS" in warnings[0].getMessage()
    assert len(recoveries) == 1


async def test_get_ups_logs_warning_when_graph_discovery_returns_malformed_response(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A non-list (but non-raising) ``reporting.netdata_graphs`` response
    must surface a warning and clear it again once the endpoint recovers,
    just like a raised error -- regression test for the Pattern B bug where
    ``get_ups()`` used to declare "recovered" before validating the
    response's shape.
    """
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
            with caplog.at_level(logging.DEBUG, logger="aiotruenas.domain.state"):
                await state.get_ups()

                server.responses["reporting.netdata_graphs"] = {"not": "a list"}
                await state.get_ups()

                server.responses["reporting.netdata_graphs"] = [{"name": "upscharge"}]
                await state.get_ups()

    state_records = [r for r in caplog.records if r.name == "aiotruenas.domain.state"]
    warnings = [r for r in state_records if r.levelno == logging.WARNING]
    recoveries = [
        r
        for r in state_records
        if r.levelno == logging.DEBUG and "recovered" in r.getMessage()
    ]
    assert len(warnings) == 1
    assert "malformed" in warnings[0].getMessage().lower()
    assert len(recoveries) == 1


async def test_get_ups_keeps_other_readings_when_one_graph_query_raises() -> None:
    """A raising per-graph ``reporting.netdata_graph`` call must not abort the
    whole method -- the other, unaffected UPS graphs still resolve.

    Regression test: this call used to be unguarded, unlike the discovery
    call three lines above it and every other netdata-graph loop in this
    module -- a ``TrueNASError`` here used to propagate out of ``get_ups()``.
    """

    def netdata_graph(params: list) -> Any:
        if params[0] == "upscharge":
            return {
                "error": {
                    "code": -32603,
                    "message": "Internal error",
                    "data": {"error": 1, "errname": "EFAULT", "reason": None},
                }
            }
        return [{"aggregations": {"mean": {"ups1": 42.0}}}]

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "reporting.netdata_graphs": [
                {"name": "upscharge"},
                {"name": "upsload"},
            ],
            "reporting.netdata_graph": netdata_graph,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_ups()

    assert result == {"load": 42.0}
    assert "battery_charge" not in result
    # First-ever call: upscharge's field never had a prior value, so it must
    # not be reported as stale either -- it's absent, not outdated.
    assert state.ups_stale_graphs == frozenset()


async def test_get_ups_logs_warning_when_graph_query_raises(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A raising per-graph ``reporting.netdata_graph`` call must surface a
    warning and clear it again once that graph recovers.
    """

    def failing_netdata_graph(params: list) -> Any:
        if params[0] == "upscharge":
            return {
                "error": {
                    "code": -32603,
                    "message": "Internal error",
                    "data": {"error": 1, "errname": "EFAULT", "reason": None},
                }
            }
        return [{"aggregations": {"mean": {"ups1": 42.0}}}]

    def recovered_netdata_graph(params: list) -> Any:
        return [{"aggregations": {"mean": {"ups1": 55.0}}}]

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "reporting.netdata_graphs": [{"name": "upscharge"}],
            "reporting.netdata_graph": failing_netdata_graph,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            with caplog.at_level(logging.DEBUG, logger="aiotruenas.domain.state"):
                await state.get_ups()

                server.responses["reporting.netdata_graph"] = recovered_netdata_graph
                await state.get_ups()

    state_records = [r for r in caplog.records if r.name == "aiotruenas.domain.state"]
    warnings = [r for r in state_records if r.levelno == logging.WARNING]
    recoveries = [
        r
        for r in state_records
        if r.levelno == logging.DEBUG and "recovered" in r.getMessage()
    ]
    assert len(warnings) == 1
    assert "upscharge" in warnings[0].getMessage()
    assert len(recoveries) == 1


async def test_get_ups_treats_unusable_graph_as_failure_not_recovery() -> None:
    """A *successful* ``reporting.netdata_graph`` call whose payload carries no
    usable reading must be recorded as failing, not as recovered.

    Regression test: the fix for the raising case above originally called
    ``_note_fallback_outcome(..., failed=False, ...)`` unconditionally right
    after the RPC call succeeded, before checking whether ``_ups_value()``
    could actually extract a reading -- unlike ``_refresh_systemstat_graphs()``
    and ``_refresh_interface_throughput()``, which both gate the failed/
    recovered decision on the *parsed* result. A malformed-but-200-OK payload
    used to silently clear a stuck failing flag and silently drop the
    reading with no warning.
    """

    def netdata_graph(params: list) -> Any:
        if params[0] == "upscharge":
            return [{"aggregations": {}}]  # no "mean" -- unusable
        return [{"aggregations": {"mean": {"ups1": 42.0}}}]

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "reporting.netdata_graphs": [
                {"name": "upscharge"},
                {"name": "upsload"},
            ],
            "reporting.netdata_graph": netdata_graph,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_ups()

    assert result == {"load": 42.0}
    assert "battery_charge" not in result
    # The result dict alone can't distinguish "recorded as failing" from
    # "recorded as recovered" -- both omit the unusable graph from `result`
    # the same way. Assert the actual fallback-tracking state directly so
    # this test would have caught the original bug (unconditional
    # failed=False right after a successful RPC call, before the value was
    # validated).
    assert state._fallback_failing.get("ups_graph:upscharge") is True
    assert "ups_graph:upsload" not in state._fallback_failing
    # This is the very first get_ups() call ever, so upscharge's field never
    # had a prior value to be stale -- ups_stale_graphs must NOT name it, or
    # it would name a field that's entirely absent from `result`/`ds["ups"]`
    # rather than merely outdated (see the analogous raising-branch test
    # above for the same guarantee on the other failure path).
    assert state.ups_stale_graphs == frozenset()


async def test_get_ups_logs_debug_when_graph_structurally_empty(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A recognized-but-structurally-empty UPS netdata graph response (the
    shape reported in kayl-codes/homeassistant-truenas#142: a listed entry
    carrying both "name" and "identifier", with empty "data"/"aggregations")
    must not warn -- some UPS/NUT drivers simply never report a given
    metric for a particular device, and TrueNAS's own reporting UI shows no
    value for it either. Only a DEBUG trace is logged, and no failing flag
    is set for that graph.
    """
    graph_data = [
        {
            "name": "upscurrent",
            "identifier": "upscurrent",
            "data": [],
            "aggregations": {"min": {}, "mean": {}, "max": {}},
        }
    ]

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "reporting.netdata_graphs": [{"name": "upscurrent"}],
            "reporting.netdata_graph": graph_data,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            with caplog.at_level(logging.DEBUG, logger="aiotruenas.domain.state"):
                result = await state.get_ups()

    assert result == {}
    assert "ups_graph:upscurrent" not in state._fallback_failing
    # A field that never had a prior successful reading is not stale --
    # it's simply absent, same as an outright failure with no prior value.
    assert state.ups_stale_graphs == frozenset()

    state_records = [r for r in caplog.records if r.name == "aiotruenas.domain.state"]
    assert not [r for r in state_records if r.levelno == logging.WARNING]
    debug_traces = [
        r
        for r in state_records
        if r.levelno == logging.DEBUG and "no usable reading" in r.getMessage().lower()
    ]
    assert len(debug_traces) == 1


async def test_get_ups_clears_flag_after_failure_then_structurally_empty_reading() -> (
    None
):
    """A prior *real* netdata-graph-query failure (RPC error) must not stay
    stuck ``True`` once a later poll gets a recognized-but-empty reading --
    otherwise a second genuine failure after that would go unwarned forever,
    mirroring the analogous disk-temp regression test.
    """

    def always_failing_graph(params: list) -> Any:
        return {
            "error": {
                "code": -32603,
                "message": "Internal error",
                "data": {"error": 1, "errname": "EFAULT", "reason": None},
            }
        }

    empty_graph_data = [
        {
            "name": "upscurrent",
            "identifier": "upscurrent",
            "data": [],
            "aggregations": {"min": {}, "mean": {}, "max": {}},
        }
    ]

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "reporting.netdata_graphs": [{"name": "upscurrent"}],
            "reporting.netdata_graph": always_failing_graph,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_ups()
            assert state._fallback_failing.get("ups_graph:upscurrent") is True

            server.responses["reporting.netdata_graph"] = empty_graph_data
            await state.get_ups()

    assert "ups_graph:upscurrent" not in state._fallback_failing


async def test_get_ups_empty_top_level_list_logs_debug_not_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An empty top-level list from ``reporting.netdata_graph`` is the same
    "no samples this window" shape as a recognized-but-empty entry (see
    kayl-codes/homeassistant-truenas#142) -- DEBUG only, no failing flag."""

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "reporting.netdata_graphs": [{"name": "upscurrent"}],
            "reporting.netdata_graph": [],
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            with caplog.at_level(logging.DEBUG, logger="aiotruenas.domain.state"):
                result = await state.get_ups()

    assert result == {}
    assert "ups_graph:upscurrent" not in state._fallback_failing
    state_records = [r for r in caplog.records if r.name == "aiotruenas.domain.state"]
    assert not [r for r in state_records if r.levelno == logging.WARNING]


async def test_get_ups_non_list_graph_response_still_warns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A non-list (but non-raising) ``reporting.netdata_graph`` response is a
    genuinely malformed payload, not an empty sampling window -- it must
    still follow the real-failure path (WARNING once, failing flag set),
    unchanged by the #142 empty-window handling."""

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "reporting.netdata_graphs": [{"name": "upscurrent"}],
            "reporting.netdata_graph": None,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            with caplog.at_level(logging.DEBUG, logger="aiotruenas.domain.state"):
                await state.get_ups()

    assert state._fallback_failing.get("ups_graph:upscurrent") is True
    state_records = [r for r in caplog.records if r.name == "aiotruenas.domain.state"]
    warnings = [r for r in state_records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "upscurrent" in warnings[0].getMessage()


async def test_get_ups_garbage_first_entry_treated_as_malformed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """``_ups_value()`` only ever inspects ``graph_data[0]``; the
    recognizability check must be scoped the same way. A response whose
    first entry is unparseable is malformed *as far as value extraction is
    concerned*, even if a later entry happens to look well-formed -- it must
    warn, not be silently downgraded to DEBUG with the failing flag cleared.
    """

    def garbage_first(params: list) -> Any:
        return ["garbage", {"name": "upscurrent", "identifier": "upscurrent"}]

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "reporting.netdata_graphs": [{"name": "upscurrent"}],
            "reporting.netdata_graph": garbage_first,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            with caplog.at_level(logging.DEBUG, logger="aiotruenas.domain.state"):
                await state.get_ups()

    assert state._fallback_failing.get("ups_graph:upscurrent") is True
    state_records = [r for r in caplog.records if r.name == "aiotruenas.domain.state"]
    warnings = [r for r in state_records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "Malformed" in warnings[0].getMessage()


async def test_get_ups_keeps_previous_value_when_graph_turns_structurally_empty(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A UPS graph with a real prior reading that later starts returning a
    recognized-but-structurally-empty response (kayl-codes/homeassistant-
    truenas#142) must keep its previous value, surface it via
    ``ups_stale_graphs``, and *not* warn or set a failing flag -- the
    "previously worked, now silent" transition the #142 fix centers on.
    """

    def working_graph(params: list) -> Any:
        return [{"aggregations": {"mean": {"ups1": 1.4}}}]

    empty_graph = [
        {
            "name": "upscurrent",
            "identifier": "upscurrent",
            "data": [],
            "aggregations": {"min": {}, "mean": {}, "max": {}},
        }
    ]

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "reporting.netdata_graphs": [{"name": "upscurrent"}],
            "reporting.netdata_graph": working_graph,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            first = await state.get_ups()
            assert first == {"current": 1.4}

            server.responses["reporting.netdata_graph"] = empty_graph
            with caplog.at_level(logging.DEBUG, logger="aiotruenas.domain.state"):
                second = await state.get_ups()
            second_stale = state.ups_stale_graphs

    assert second == {"current": 1.4}
    assert second_stale == frozenset({"upscurrent"})
    assert "ups_graph:upscurrent" not in state._fallback_failing
    state_records = [r for r in caplog.records if r.name == "aiotruenas.domain.state"]
    assert not [r for r in state_records if r.levelno == logging.WARNING]


async def test_get_ups_keeps_previous_graph_reading_when_it_later_fails() -> None:
    """A UPS graph that succeeded on a prior poll but fails outright, or
    returns an unusable reading, on a later one must keep its previous
    value, not have it wiped from the result -- and both cases must be
    reported via ``ups_stale_graphs``.

    Regression test (Sourcery, PR #29): ``get_ups()`` used to build the
    ``ups`` dict from scratch each call and unconditionally overwrite
    ``self._ds["ups"]`` with it -- a graph omitted this poll (RPC failure or
    an unusable payload) silently erased its previously cached reading,
    unlike every sibling netdata-graph loop in this module, which leaves a
    failed field at its previous value.

    Also covers two related ``ups_stale_graphs`` guarantees documented on
    the property itself: a subsequent failed *discovery* call widens the
    stale set to every field still present in the snapshot -- not just the
    ones already flagged stale before that call -- and a fully successful
    poll afterwards resets the stale set back to empty.
    """

    def working_netdata_graph(params: list) -> Any:
        return [{"aggregations": {"mean": {"ups1": 42.0}}}]

    def upscharge_fails_upsload_unusable(params: list) -> Any:
        if params[0] == "upscharge":
            return {
                "error": {
                    "code": -32603,
                    "message": "Internal error",
                    "data": {"error": 1, "errname": "EFAULT", "reason": None},
                }
            }
        if params[0] == "upsload":
            return [{"aggregations": {}}]
        return [{"aggregations": {"mean": {"ups1": 55.0}}}]

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "reporting.netdata_graphs": [
                {"name": "upscharge"},
                {"name": "upsload"},
                {"name": "upsvoltage"},
            ],
            "reporting.netdata_graph": working_netdata_graph,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            first = await state.get_ups()
            assert first == {
                "battery_charge": 42.0,
                "load": 42.0,
                "voltage": 42.0,
            }
            assert state.ups_stale_graphs == frozenset()

            server.responses["reporting.netdata_graph"] = (
                upscharge_fails_upsload_unusable
            )
            second = await state.get_ups()
            second_stale = state.ups_stale_graphs
            assert second == {
                "battery_charge": 42.0,
                "load": 42.0,
                "voltage": 55.0,
            }
            assert second_stale == frozenset({"upscharge", "upsload"})

            # Discovery itself fails next -- every field still in the
            # (unrefreshed) snapshot must become stale, not just upscharge
            # and upsload, which were already flagged before this call.
            server.responses["reporting.netdata_graphs"] = None
            third = await state.get_ups()
            third_stale = state.ups_stale_graphs
            assert third == second
            assert third_stale == frozenset({"upscharge", "upsload", "upsvoltage"})

            # A fully successful poll afterwards must reset the stale set
            # back to empty, not leave any graph stuck as stale forever.
            server.responses["reporting.netdata_graphs"] = [
                {"name": "upscharge"},
                {"name": "upsload"},
                {"name": "upsvoltage"},
            ]
            server.responses["reporting.netdata_graph"] = working_netdata_graph
            fourth = await state.get_ups()
            fourth_stale = state.ups_stale_graphs

    assert fourth == {
        "battery_charge": 42.0,
        "load": 42.0,
        "voltage": 42.0,
    }
    assert fourth_stale == frozenset()


async def test_get_ups_drops_reading_when_graph_no_longer_discovered() -> None:
    """A UPS graph that succeeded on a prior poll but is no longer discovered
    at all on a later one must be dropped from the result, not kept forever.

    Regression test: the seeding step that restores previous values in
    ``get_ups()`` is restricted to graphs still present in this poll's
    ``available`` set specifically so a removed UPS (or a graph it stops
    exposing) doesn't leave a stale reading around indefinitely -- unlike a
    graph that merely fails while still discovered (see
    ``test_get_ups_keeps_previous_graph_reading_when_it_later_fails``), which
    does keep its previous value.
    """

    def working_netdata_graph(params: list) -> Any:
        return [{"aggregations": {"mean": {"ups1": 42.0}}}]

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "reporting.netdata_graphs": [
                {"name": "upscharge"},
                {"name": "upsload"},
            ],
            "reporting.netdata_graph": working_netdata_graph,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            first = await state.get_ups()
            assert first == {"battery_charge": 42.0, "load": 42.0}

            # upsload is no longer exposed at all this poll (UPS unplugged,
            # or netdata briefly stops reporting it) -- unlike a graph that
            # is still discovered but fails to query.
            server.responses["reporting.netdata_graphs"] = [{"name": "upscharge"}]
            second = await state.get_ups()
            second_stale = state.ups_stale_graphs

    assert second == {"battery_charge": 42.0}
    # upscharge is the only graph still discovered and it succeeded, so
    # nothing is stale -- upsload's disappearance drops it outright rather
    # than reporting it as a stale field that no longer exists.
    assert second_stale == frozenset()


async def test_get_ups_logs_warning_when_graph_response_malformed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A successful but malformed per-graph response (a non-empty list whose
    entry carries no recognizable "identifier"/"name" -- distinct from the
    recognized-but-empty #142 shape, which only logs DEBUG) must surface a
    warning and clear it again once that graph starts returning a usable
    reading.
    """

    def unusable_netdata_graph(params: list) -> Any:
        return [{"aggregations": {}}]

    def recovered_netdata_graph(params: list) -> Any:
        return [{"aggregations": {"mean": {"ups1": 55.0}}}]

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "reporting.netdata_graphs": [{"name": "upscharge"}],
            "reporting.netdata_graph": unusable_netdata_graph,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            with caplog.at_level(logging.DEBUG, logger="aiotruenas.domain.state"):
                await state.get_ups()

                server.responses["reporting.netdata_graph"] = recovered_netdata_graph
                await state.get_ups()

    state_records = [r for r in caplog.records if r.name == "aiotruenas.domain.state"]
    warnings = [r for r in state_records if r.levelno == logging.WARNING]
    recoveries = [
        r
        for r in state_records
        if r.levelno == logging.DEBUG and "recovered" in r.getMessage()
    ]
    assert len(warnings) == 1
    assert "upscharge" in warnings[0].getMessage()
    assert len(recoveries) == 1


async def test_get_ups_clears_stale_graph_flag_when_graph_disappears() -> None:
    """A UPS graph that stops being discovered must have its failing flag
    cleared, not left stuck -- it isn't queried at all once it's no longer
    discovered, so nothing will ever call ``_note_fallback_outcome`` for it
    again to clear it.

    Regression test: without clearing, a graph that fails, then disappears
    from discovery (UPS unplugged, or netdata briefly stops exposing it),
    then reappears and fails again, would silently skip the second warning --
    ``_note_fallback_outcome`` only warns on the failing *transition*, and
    the stale ``True`` from before it disappeared would make that look like
    an already-known failure.
    """

    def netdata_graph(params: list) -> Any:
        return {
            "error": {
                "code": -32603,
                "message": "Internal error",
                "data": {"error": 1, "errname": "EFAULT", "reason": None},
            }
        }

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "reporting.netdata_graphs": [{"name": "upscharge"}],
            "reporting.netdata_graph": netdata_graph,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_ups()
            assert state._fallback_failing.get("ups_graph:upscharge") is True

            # The UPS (or just this graph) is gone this poll -- discovery no
            # longer reports it at all.
            server.responses["reporting.netdata_graphs"] = []
            result = await state.get_ups()

    assert result == {}
    assert "ups_graph:upscharge" not in state._fallback_failing
    assert state.ups_stale_graphs == frozenset()


async def test_get_ups_fetches_graphs_concurrently() -> None:
    """Regression test mirroring
    ``test_get_systemstats_fetches_graphs_concurrently()``: all UPS netdata
    graphs must be requested concurrently (via ``asyncio.gather``), so one
    slow/unresponsive graph does not delay the others from even starting.
    """
    expected_graphs = {
        "upscharge",
        "upsruntime",
        "upsload",
        "upsvoltage",
        "upscurrent",
        "upsfrequency",
        "upstemperature",
    }
    started: set[str] = set()
    all_started = asyncio.Event()

    async def fake_call(method: str, params: Any = None, **kwargs: Any) -> Any:
        if method == "reporting.netdata_graphs":
            return [{"name": name} for name in expected_graphs]
        assert method == "reporting.netdata_graph"
        started.add(params[0])
        if started == expected_graphs:
            all_started.set()
        else:
            # If graphs were fetched one at a time, this call would already
            # hold the (implicit) turn while the others haven't started
            # yet, so waiting here for the rest to start would deadlock and
            # fail the test on timeout instead of hanging forever.
            await asyncio.wait_for(all_started.wait(), timeout=2)
        return [{"aggregations": {"mean": {params[0]: 1.0}}}]

    async with FakeTrueNASServer(valid_api_key=API_KEY) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            client.call = fake_call  # type: ignore[method-assign]
            await state.get_ups()

    assert started == expected_graphs


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


async def test_get_smb_logs_warning_when_status_query_raises(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A raising ``smb.status`` call must surface a warning and clear it
    again once the endpoint recovers.
    """
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"smb.status": [{}, {}]},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            with caplog.at_level(logging.DEBUG, logger="aiotruenas.domain.state"):
                await state.get_smb()

                server.responses["smb.status"] = {
                    "error": {
                        "code": -32603,
                        "message": "Internal error",
                        "data": {"error": 1, "errname": "EFAULT", "reason": None},
                    }
                }
                await state.get_smb()

                server.responses["smb.status"] = [{}, {}]
                await state.get_smb()

    state_records = [r for r in caplog.records if r.name == "aiotruenas.domain.state"]
    warnings = [r for r in state_records if r.levelno == logging.WARNING]
    recoveries = [
        r
        for r in state_records
        if r.levelno == logging.DEBUG and "recovered" in r.getMessage()
    ]
    assert len(warnings) == 1
    assert "SMB" in warnings[0].getMessage()
    assert len(recoveries) == 1


async def test_get_smb_logs_warning_when_status_query_returns_malformed_response(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A response matching neither accepted ``smb.status`` shape (a list, or
    a dict with a ``sessions`` list) must surface a warning and clear it
    again once the endpoint recovers, just like a raised error -- regression
    test for the Pattern B bug where ``get_smb()`` used to declare
    "recovered" before validating the response's shape.
    """
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"smb.status": [{}, {}]},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            with caplog.at_level(logging.DEBUG, logger="aiotruenas.domain.state"):
                await state.get_smb()

                server.responses["smb.status"] = {"unexpected": "shape"}
                await state.get_smb()

                server.responses["smb.status"] = [{}, {}]
                await state.get_smb()

    state_records = [r for r in caplog.records if r.name == "aiotruenas.domain.state"]
    warnings = [r for r in state_records if r.levelno == logging.WARNING]
    recoveries = [
        r
        for r in state_records
        if r.levelno == logging.DEBUG and "recovered" in r.getMessage()
    ]
    assert len(warnings) == 1
    assert "malformed" in warnings[0].getMessage().lower()
    assert len(recoveries) == 1


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

_DISK_SDB = {
    "name": "sdb",
    "devname": "sdb",
    "serial": "S2",
    "size": "1TB",
    "identifier": "{serial}S2",
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


async def test_get_disk_does_not_treat_empty_netdata_graph_reading_as_failure() -> None:
    """A discovered disk-temp graph whose query succeeds but yields no usable
    reading (e.g. an empty aggregation window) must *not* be recorded as
    failing, and the ``disk.temperatures`` fallback must still populate the
    temperature.

    TrueNAS's netdata backend legitimately has nothing to report for a disk
    right after a service restart, or for slower collectors (observed with
    NVMe SMART-temp probes) whose sampling cadence can miss this library's
    60-second query window -- warning on every such poll was pure log noise
    given the fallback already covers it (kayl-codes/homeassistant-truenas#139).
    This supersedes the opposite behavior this test previously asserted.
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
            "reporting.netdata_graph": [],
            "disk.temperatures": {"sda": 42.5},
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_disk()

    assert result["{serial}S1"]["temperature"] == 42.5
    assert "disk_temp_netdata" not in state._fallback_failing


async def test_get_disk_logs_debug_when_netdata_graph_returns_no_usable_reading(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An RPC-succeeded-but-empty netdata reading (empty samples/aggregations
    for every disk) must not warn -- it's expected right after a TrueNAS
    netdata service restart or for slower collectors (observed with NVMe
    SMART-temp probes), and the ``disk.temperatures`` fallback already
    covers it -- see kayl-codes/homeassistant-truenas#139. Only a DEBUG
    trace is logged, and any leftover failing flag is silently cleared (a
    no-op here since none was set) rather than warned as a recovery, since
    this isn't treated as a failing transition.

    Uses the exact payload shape from the linked issue (a listed entry per
    disk, with empty ``data``/``aggregations`` rather than an empty
    top-level list) so this stays a regression test for that report
    specifically, not just for the simpler no-entries case covered above.
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
            "reporting.netdata_graph": [
                {
                    "name": "disktemp",
                    "identifier": "{serial}S1",
                    "data": [],
                    "aggregations": {"min": {}, "mean": {}, "max": {}},
                    "legend": ["time", "temperature_value"],
                }
            ],
            "disk.temperatures": {"sda": 42.5},
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            with caplog.at_level(logging.DEBUG, logger="aiotruenas.domain.state"):
                result = await state.get_disk()

    assert result["{serial}S1"]["temperature"] == 42.5
    assert "disk_temp_netdata" not in state._fallback_failing

    state_records = [r for r in caplog.records if r.name == "aiotruenas.domain.state"]
    netdata_warnings = [r for r in state_records if r.levelno == logging.WARNING]
    netdata_debug_traces = [
        r
        for r in state_records
        if r.levelno == logging.DEBUG and "no usable reading" in r.getMessage().lower()
    ]
    assert not netdata_warnings
    assert len(netdata_debug_traces) == 1


async def test_get_disk_clears_netdata_flag_after_failure_then_empty_reading(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A prior *real* netdata-graph-query failure (RPC error) must not stay
    stuck ``True`` once a later poll gets an RPC-succeeded-but-empty
    reading -- otherwise a second genuine failure after that would go
    unwarned forever, since ``_note_fallback_outcome`` only warns on the
    False -> True transition. Regression test for the stuck-flag bug fixed
    alongside kayl-codes/homeassistant-truenas#139: the empty-reading branch
    now pops the flag (silently, since nothing was actually confirmed
    recovered) instead of leaving it untouched.
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
            first = await state.get_disk()
            assert first["{serial}S1"]["temperature"] == 42.5
            assert state._fallback_failing.get("disk_temp_netdata") is True

            caplog.clear()
            server.responses["reporting.netdata_graph"] = [
                {
                    "name": "disktemp",
                    "identifier": "{serial}S1",
                    "data": [],
                    "aggregations": {"min": {}, "mean": {}, "max": {}},
                    "legend": ["time", "temperature_value"],
                }
            ]
            second = await state.get_disk()

    assert second["{serial}S1"]["temperature"] == 42.5
    assert "disk_temp_netdata" not in state._fallback_failing

    state_records = [r for r in caplog.records if r.name == "aiotruenas.domain.state"]
    assert not [r for r in state_records if r.levelno == logging.WARNING]
    assert not [
        r
        for r in state_records
        if r.levelno == logging.DEBUG and "recovered" in r.getMessage().lower()
    ]


async def test_get_disk_warns_when_netdata_graph_response_has_no_disk_entries(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A *non-empty* netdata-graph response that contains no recognizable
    per-disk series at all is a malformed payload, not the expected
    "samples not accumulated yet" empty window -- it must follow the
    real-failure path (warn once, set the failing flag) rather than being
    silently demoted to a DEBUG trace. An empty top-level list stays the
    expected-window case (covered separately above).
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
            "reporting.netdata_graph": ["garbage", 42, {"no": "identifier"}],
            "disk.temperatures": {"sda": 42.5},
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            with caplog.at_level(logging.WARNING, logger="aiotruenas.domain.state"):
                result = await state.get_disk()

    assert result["{serial}S1"]["temperature"] == 42.5
    assert state._fallback_failing.get("disk_temp_netdata") is True

    state_records = [r for r in caplog.records if r.name == "aiotruenas.domain.state"]
    # "no disk entries" is unique to the new malformed-payload branch -- a bare
    # "malformed" substring also matches the two sibling warnings for a
    # non-list graphs/graph_data response, so the test could pass for the
    # wrong reason.
    malformed_warnings = [
        r
        for r in state_records
        if r.levelno == logging.WARNING and "no disk entries" in r.getMessage().lower()
    ]
    assert len(malformed_warnings) == 1


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
    """Both the netdata graph discovery and the ``disk.temperatures``
    fallback fail (the latter is simply unconfigured on the fake server, so
    it errors as method-not-found) -- the fallback must still have been
    *attempted*, and the failure correctly attributed to the netdata
    discovery path rather than a generic "unexpected error" bucket.
    """
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
    assert state._fallback_failing.get("disk_temp_netdata") is True
    assert "disk_temperature_update_unexpected" not in state._fallback_failing


async def test_get_disk_falls_back_when_netdata_discovery_fails() -> None:
    """Netdata graph *discovery* itself fails (not just the graph query).

    The ``disk.temperatures`` fallback must still run for every disk instead
    of the whole update being skipped because of the earlier discovery
    failure (regression: the discovery call briefly went unwrapped after
    moving the failing/recovered bookkeeping into
    ``_disk_temps_from_netdata()``).
    """
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
            "disk.temperatures": {"sda": 42.5},
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_disk()

    assert result["{serial}S1"]["temperature"] == 42.5
    assert state._fallback_failing.get("disk_temp_netdata") is True


async def test_get_disk_falls_back_when_netdata_graphs_response_is_malformed() -> None:
    """``reporting.netdata_graphs`` succeeds but returns a non-list payload.

    Must be treated as a real discovery failure (warned, ``disk.temperatures``
    fallback still runs, and retried on the next poll) rather than being
    silently cached as "no disk-temp graph configured" forever.
    """
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "disk.query": [_DISK_SDA],
            "reporting.netdata_graphs": {"unexpected": "shape"},
            "disk.temperatures": {"sda": 42.5},
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            result = await state.get_disk()

    assert result["{serial}S1"]["temperature"] == 42.5
    assert state._fallback_failing.get("disk_temp_netdata") is True
    assert state._disk_temp_graph is None


async def test_get_disk_clears_netdata_flag_once_discovery_finds_no_graph() -> None:
    """A prior discovery failure must not stay stuck once a later discovery
    call succeeds and legitimately finds no disk-temp graph configured.

    Regression test: ``_disk_temps_from_netdata()`` used to return early on
    "no matching graph" without ever calling ``_note_fallback_outcome``,
    so a flag set by an earlier discovery failure would stay ``True``
    forever -- with no corresponding warning ever logged again, since
    ``self._disk_temp_graph`` gets cached as ``""`` and discovery is never
    retried, so nothing else could clear it either.
    """
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
            "disk.temperatures": {"sda": 42.5},
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_disk()
            assert state._fallback_failing.get("disk_temp_netdata") is True

            # Discovery now succeeds, but nothing it reports looks like a
            # disk-temp graph -- a legitimate "no such graph" result.
            server.responses["reporting.netdata_graphs"] = [
                {
                    "name": "unrelated",
                    "title": "Something Else",
                    "vertical_label": "Watts",
                }
            ]
            result = await state.get_disk()

    assert result["{serial}S1"]["temperature"] == 42.5
    assert "disk_temp_netdata" not in state._fallback_failing
    assert state._disk_temp_graph == ""


async def test_get_disk_refreshes_temperature_once_netdata_stops_reporting_it() -> None:
    """A disk that already has a netdata-sourced temperature must still be
    refreshed via the ``disk.temperatures`` fallback once netdata stops
    reporting it on a later poll, instead of keeping the first-poll value
    frozen forever (regression covered by
    kayl-codes/homeassistant-truenas#131).
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
            "reporting.netdata_graph": [
                {"identifier": "{serial}S1", "aggregations": {"mean": {"sda": 35.0}}}
            ],
            "disk.temperatures": {"sda": 40.0},
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            first = await state.get_disk()
            assert first["{serial}S1"]["temperature"] == 35.0

            server.responses["reporting.netdata_graph"] = []
            second = await state.get_disk()

    assert second["{serial}S1"]["temperature"] == 40.0


async def test_get_disk_refreshes_disk_netdata_stops_covering_specifically() -> None:
    """netdata continuing to cover *some* disks must not stop the fallback
    from refreshing a disk it stops covering (regression covered by
    kayl-codes/homeassistant-truenas#131): gating the fallback on "netdata
    returned nothing at all" would miss this case, since ``netdata_temps``
    is still non-empty here -- only sdb's entry drops out of the graph.
    """
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "disk.query": [_DISK_SDA, _DISK_SDB],
            "reporting.netdata_graphs": [
                {
                    "name": "disktemp",
                    "title": "Disk Temperature",
                    "vertical_label": "Celsius",
                }
            ],
            "reporting.netdata_graph": [
                {"identifier": "{serial}S1", "aggregations": {"mean": {"sda": 35.0}}},
                {"identifier": "{serial}S2", "aggregations": {"mean": {"sdb": 30.0}}},
            ],
            "disk.temperatures": {"sda": 999.0, "sdb": 45.0},
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            first = await state.get_disk()
            assert first["{serial}S1"]["temperature"] == 35.0
            assert first["{serial}S2"]["temperature"] == 30.0

            server.responses["reporting.netdata_graph"] = [
                {"identifier": "{serial}S1", "aggregations": {"mean": {"sda": 36.0}}},
            ]
            second = await state.get_disk()

    assert second["{serial}S1"]["temperature"] == 36.0
    assert second["{serial}S2"]["temperature"] == 45.0


async def test_get_disk_logs_warning_when_fallback_response_is_malformed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Total temperature-enrichment failure (no netdata, malformed
    ``disk.temperatures`` response) must surface a warning -- otherwise a
    disk's temperature can freeze indefinitely with no trace anywhere
    (kayl-codes/homeassistant-truenas#131).
    """
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "disk.query": [_DISK_SDA],
            "reporting.netdata_graphs": [],
            "disk.temperatures": "not-a-dict",
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            with caplog.at_level(logging.DEBUG, logger="aiotruenas.domain.state"):
                result = await state.get_disk()

    assert result["{serial}S1"]["temperature"] is None
    warnings = [
        r
        for r in caplog.records
        if r.name == "aiotruenas.domain.state" and r.levelno == logging.WARNING
    ]
    assert len(warnings) == 1
    assert "disk.temperatures" in warnings[0].getMessage()


async def test_get_disk_logs_warning_when_fallback_rpc_raises(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A ``disk.temperatures`` RPC error (not just a malformed response) must
    also surface a warning and set the failing flag -- previously it
    propagated out of ``_fallback_disk_temperatures()`` and was swallowed
    silently by ``get_disk()``'s own ``TrueNASError`` handling
    (kayl-codes/homeassistant-truenas#131).
    """
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "disk.query": [_DISK_SDA],
            "reporting.netdata_graphs": [],
            "disk.temperatures": {
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
            with caplog.at_level(logging.DEBUG, logger="aiotruenas.domain.state"):
                result = await state.get_disk()

    assert result["{serial}S1"]["temperature"] is None
    warnings = [
        r
        for r in caplog.records
        if r.name == "aiotruenas.domain.state" and r.levelno == logging.WARNING
    ]
    assert len(warnings) == 1
    assert "disk.temperatures" in warnings[0].getMessage()


async def test_get_disk_treats_null_fallback_result_as_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A ``disk.temperatures`` RPC result of ``None`` is not a dict either,
    so it must warn and set the failing flag just like any other malformed
    response -- and, on a later poll, must not be mistaken for a recovery
    from a still-ongoing failure just because it equals the "no failure"
    sentinel value.
    """
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "disk.query": [_DISK_SDA],
            "reporting.netdata_graphs": [],
            "disk.temperatures": "not-a-dict",
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            with caplog.at_level(logging.DEBUG, logger="aiotruenas.domain.state"):
                first = await state.get_disk()

                server.responses["disk.temperatures"] = None
                second = await state.get_disk()

    assert first["{serial}S1"]["temperature"] is None
    assert second["{serial}S1"]["temperature"] is None
    state_records = [r for r in caplog.records if r.name == "aiotruenas.domain.state"]
    warnings = [r for r in state_records if r.levelno == logging.WARNING]
    recoveries = [
        r
        for r in state_records
        if r.levelno == logging.DEBUG and "recovered" in r.getMessage()
    ]
    assert len(warnings) == 1
    assert not recoveries


async def test_get_disk_only_warns_once_across_a_persistent_fallback_outage(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A persistent ``disk.temperatures`` failure must warn once on the
    failing transition, stay quiet on repeat polls, and log recovery at
    debug once it clears -- not re-warn every poll for the outage's
    duration.
    """
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "disk.query": [_DISK_SDA],
            "reporting.netdata_graphs": [],
            "disk.temperatures": "not-a-dict",
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            with caplog.at_level(logging.DEBUG, logger="aiotruenas.domain.state"):
                await state.get_disk()
                await state.get_disk()

                server.responses["disk.temperatures"] = {"sda": 41.0}
                recovered = await state.get_disk()

    state_records = [r for r in caplog.records if r.name == "aiotruenas.domain.state"]
    warnings = [r for r in state_records if r.levelno == logging.WARNING]
    recoveries = [
        r
        for r in state_records
        if r.levelno == logging.DEBUG and "recovered" in r.getMessage()
    ]
    assert len(warnings) == 1
    assert len(recoveries) == 1
    assert recovered["{serial}S1"]["temperature"] == 41.0


async def test_get_disk_warns_again_after_netdata_alone_clears_the_flag(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A poll where netdata alone covers every disk never calls the
    ``disk.temperatures`` fallback at all, so it must still clear a prior
    failing flag -- otherwise a *later*, independent fallback failure would
    stay permanently unwarned once the flag got stuck ``True``.

    A malformed (non-list) ``reporting.netdata_graph`` response is itself a
    netdata-side failure, distinct from the ``disk.temperatures`` fallback
    failure it triggers -- both warn independently on their own failing
    transition (kayl-codes/aiotruenas PR #29 review), so each failing poll
    below carries two warnings, not one.
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
            "reporting.netdata_graph": None,
            "disk.temperatures": "not-a-dict",
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            with caplog.at_level(logging.DEBUG, logger="aiotruenas.domain.state"):
                # netdata reading malformed -> netdata warns + fallback warns
                await state.get_disk()

                server.responses["reporting.netdata_graph"] = [
                    {
                        "identifier": "{serial}S1",
                        "aggregations": {"mean": {"sda": 35.0}},
                    }
                ]
                await state.get_disk()  # netdata covers sda -> both flags clear

                server.responses["reporting.netdata_graph"] = None
                # netdata fails again -> both must warn again
                await state.get_disk()

    state_records = [r for r in caplog.records if r.name == "aiotruenas.domain.state"]
    warnings = [r for r in state_records if r.levelno == logging.WARNING]
    recoveries = [
        r
        for r in state_records
        if r.levelno == logging.DEBUG and "recovered" in r.getMessage()
    ]
    assert len(warnings) == 4
    assert len(recoveries) == 1


async def test_get_disk_logs_warning_when_temperature_update_raises_unexpectedly(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unexpected exception from the disk temperature update step --
    outside the fallback's own handled ``TrueNASError`` paths -- must still
    surface a warning and clear it again once the step recovers, instead of
    disappearing silently.
    """
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"disk.query": [_DISK_SDA]},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)

            async def _raise_update() -> None:
                raise TrueNASError("boom")

            with caplog.at_level(logging.DEBUG, logger="aiotruenas.domain.state"):
                monkeypatch.setattr(state, "_update_disk_temperatures", _raise_update)
                await state.get_disk()

                async def _noop_update() -> None:
                    return None

                monkeypatch.setattr(state, "_update_disk_temperatures", _noop_update)
                await state.get_disk()

    state_records = [r for r in caplog.records if r.name == "aiotruenas.domain.state"]
    warnings = [r for r in state_records if r.levelno == logging.WARNING]
    recoveries = [
        r
        for r in state_records
        if r.levelno == logging.DEBUG and "recovered" in r.getMessage()
    ]
    assert len(warnings) == 1
    assert "disk temperature" in warnings[0].getMessage().lower()
    assert len(recoveries) == 1


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


async def test_get_systeminfo_logs_warning_on_unparsable_version(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """``get_systeminfo()``'s own, independent parse of the same ``version``
    field (shared cached state with ``_detect_version()``, but a separate
    code path) must warn once on an unparsable value and log recovery once a
    subsequent call succeeds -- mirroring ``_detect_version()``'s behavior
    for the same underlying fallback risk.
    """
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": {
                "version": "garbage",
                "system_manufacturer": "Supermicro",
                "system_product": "X11SPi-TF",
            }
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            with caplog.at_level(logging.DEBUG, logger="aiotruenas.domain.state"):
                await state.get_systeminfo()

                server.responses["system.info"] = {
                    "version": "TrueNAS-25.10.0",
                    "system_manufacturer": "Supermicro",
                    "system_product": "X11SPi-TF",
                }
                await state.get_systeminfo()

    state_records = [r for r in caplog.records if r.name == "aiotruenas.domain.state"]
    warnings = [r for r in state_records if r.levelno == logging.WARNING]
    recoveries = [
        r
        for r in state_records
        if r.levelno == logging.DEBUG and "recovered" in r.getMessage()
    ]
    assert len(warnings) == 1
    assert "version" in warnings[0].getMessage().lower()
    assert "garbage" in warnings[0].getMessage()
    assert len(recoveries) == 1


async def test_systeminfo_and_detect_version_agree_on_missing_version_reason(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """When the ``version`` key is absent outright (not just unparsable),
    ``get_systeminfo()`` and ``_detect_version()`` (via ``get_container()``)
    must log the same ``reason=`` text for their shared warning, even though
    ``get_systeminfo()`` reads through ``parse_api()``-normalized data (which
    defaults a missing key to the string ``"unknown"``) while
    ``_detect_version()`` reads the raw response directly (where a missing
    key is ``None``). Regression test for a gap the local silent-failure-
    hunter review found: before this fix, ``get_systeminfo()`` logged
    ``reason="unknown"`` and ``_detect_version()`` logged ``reason=None`` for
    the identical underlying failure under the identical shared warning key.
    """
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"system.info": {}, "virt.instance.query": []},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            with caplog.at_level(logging.DEBUG, logger="aiotruenas.domain.state"):
                await state.get_systeminfo()
            systeminfo_warning = next(
                r
                for r in caplog.records
                if r.name == "aiotruenas.domain.state" and r.levelno == logging.WARNING
            )
            caplog.clear()

            state = TrueNASState(client)
            with caplog.at_level(logging.DEBUG, logger="aiotruenas.domain.state"):
                await state.get_container()
            detect_version_warning = next(
                r
                for r in caplog.records
                if r.name == "aiotruenas.domain.state" and r.levelno == logging.WARNING
            )

    assert systeminfo_warning.getMessage() == detect_version_warning.getMessage()


async def test_get_systeminfo_does_not_warn_when_version_already_cached(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A later poll's unparsable ``version`` field must not warn -- or
    imply that ``get_container()`` will fall back to legacy behavior -- once
    a version has already been cached from a prior successful poll, since
    that cached value stays valid and continues to gate correctly.
    """
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": {
                "version": "TrueNAS-26.0.0",
                "system_manufacturer": "Supermicro",
                "system_product": "X11SPi-TF",
            }
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            with caplog.at_level(logging.DEBUG, logger="aiotruenas.domain.state"):
                await state.get_systeminfo()

                server.responses["system.info"] = {
                    "version": "garbage",
                    "system_manufacturer": "Supermicro",
                    "system_product": "X11SPi-TF",
                }
                await state.get_systeminfo()

    state_records = [r for r in caplog.records if r.name == "aiotruenas.domain.state"]
    warnings = [r for r in state_records if r.levelno == logging.WARNING]
    assert not warnings
    assert state._version == (26, 0)


async def test_get_systeminfo_logs_warning_on_non_dict_system_info(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A malformed (non-dict) ``system.info`` response must also warn once,
    not just an unparsable ``version`` field within an otherwise-valid dict --
    otherwise a caller that only ever polls via ``get_systeminfo()`` (never
    ``_detect_version()``/``get_container()``) gets no signal at all that
    ``system.info`` itself is stuck returning a malformed response.
    """
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"system.info": None},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            with caplog.at_level(logging.DEBUG, logger="aiotruenas.domain.state"):
                await state.get_systeminfo()

                server.responses["system.info"] = {
                    "version": "TrueNAS-25.10.0",
                    "system_manufacturer": "Supermicro",
                    "system_product": "X11SPi-TF",
                }
                await state.get_systeminfo()

    state_records = [r for r in caplog.records if r.name == "aiotruenas.domain.state"]
    warnings = [r for r in state_records if r.levelno == logging.WARNING]
    recoveries = [
        r
        for r in state_records
        if r.levelno == logging.DEBUG and "recovered" in r.getMessage()
    ]
    assert len(warnings) == 1
    assert "malformed" in warnings[0].getMessage().lower()
    assert len(recoveries) == 1


async def test_get_systeminfo_warns_on_non_dict_even_with_version_already_cached(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Unlike an unparsable ``version`` field, a malformed (non-dict)
    ``system.info`` response must warn on this failing transition even once
    a version is already cached -- since it also freezes every *other*
    ``system_info`` field (uptime, memory, hostname, ...) on stale data, not
    just version detection. Regression test for a gap the local
    silent-failure-hunter review found: the first fix gated this warning on
    ``self._version is None``, matching the version-field case but silencing
    the far more common in-lifetime failure (version detected once, then
    ``system.info`` starts returning garbage on a later poll).
    """
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": {
                "version": "TrueNAS-26.0.0",
                "system_manufacturer": "Supermicro",
                "system_product": "X11SPi-TF",
            }
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            with caplog.at_level(logging.DEBUG, logger="aiotruenas.domain.state"):
                await state.get_systeminfo()
                assert state._version == (26, 0)

                server.responses["system.info"] = None
                await state.get_systeminfo()

    state_records = [r for r in caplog.records if r.name == "aiotruenas.domain.state"]
    warnings = [r for r in state_records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "malformed" in warnings[0].getMessage().lower()
    assert state._version == (26, 0)


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


async def test_get_systeminfo_does_not_reset_is_virtual_once_cached() -> None:
    """A later poll's response missing ``system_manufacturer``/``system_product``
    must not overwrite an already-cached ``True`` with the "unknown" default's
    ``False`` -- hardware/hypervisor identity cannot change for the lifetime of
    a running system, so once detected it stays cached, mirroring
    ``_detect_virtual()``'s own "detect once" guard.
    """
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": {
                "system_manufacturer": "QEMU",
                "system_product": "Standard PC",
            }
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_systeminfo()
            assert state._is_virtual is True

            server.responses["system.info"] = {}
            await state.get_systeminfo()

    assert state._is_virtual is True


async def test_get_systeminfo_does_not_cache_is_virtual_when_fields_missing() -> None:
    """A first-ever poll whose response is missing both
    ``system_manufacturer`` and ``system_product`` must leave
    ``self._is_virtual`` at ``None`` (not cache the "unknown" default's
    ``False``), so a later poll with real VM identity can still detect it --
    and ``get_systemstats()`` must then correctly skip ``cputemp``. Before
    this fix, this exact case permanently and silently misclassified a VM as
    physical hardware from its very first poll.
    """
    called_graphs: list[str] = []

    def netdata_graph(params: list) -> Any:
        called_graphs.append(params[0])
        return None

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": {},
            "reporting.netdata_graph": netdata_graph,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_systeminfo()
            assert state._is_virtual is None

            server.responses["system.info"] = {
                "system_manufacturer": "QEMU",
                "system_product": "Standard PC",
            }
            await state.get_systeminfo()
            assert state._is_virtual is True

            await state.get_systemstats()

    assert "cputemp" not in called_graphs


async def test_get_systeminfo_does_not_cache_is_virtual_when_fields_are_null() -> None:
    """A first-ever poll where both fields are *present but null* (e.g. a
    dmidecode-less container) must leave ``self._is_virtual`` at ``None``,
    same as when the fields are missing entirely -- key presence alone is
    not usable detection evidence. A later poll with real VM identity must
    still be able to detect it.
    """
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": {
                "system_manufacturer": None,
                "system_product": None,
            }
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_systeminfo()
            assert state._is_virtual is None

            server.responses["system.info"] = {
                "system_manufacturer": "QEMU",
                "system_product": "Standard PC",
            }
            await state.get_systeminfo()
            assert state._is_virtual is True


async def test_get_systemstats_does_not_cache_is_virtual_when_fields_are_null() -> None:
    """Mirrors ``test_get_systeminfo_does_not_cache_is_virtual_when_fields_are_null``
    for ``_detect_virtual()``'s own cached-on-first-use path: a first poll
    with both fields present but null must not permanently mis-cache "not
    virtual" -- a later poll (here, via ``get_systeminfo()``) must still be
    able to detect it and have ``get_systemstats()`` skip ``cputemp``.
    """
    called_graphs: list[str] = []

    def netdata_graph(params: list) -> Any:
        called_graphs.append(params[0])
        return None

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": {
                "system_manufacturer": None,
                "system_product": None,
            },
            "reporting.netdata_graph": netdata_graph,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_systemstats()
            assert state._is_virtual is None

            server.responses["system.info"] = {
                "system_manufacturer": "QEMU",
                "system_product": "Standard PC",
            }
            await state.get_systeminfo()
            assert state._is_virtual is True

            called_graphs.clear()
            await state.get_systemstats()

    assert "cputemp" not in called_graphs


async def test_get_systeminfo_does_not_cache_is_virtual_when_fields_are_blank() -> None:
    """A first-ever poll where both fields are *present but blank* (``""``,
    a known dmidecode "not set" sentinel distinct from ``None``/missing) must
    also leave ``self._is_virtual`` at ``None`` -- being a string is not by
    itself usable detection evidence. A later poll with real VM identity must
    still be able to detect it.
    """
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": {
                "system_manufacturer": "",
                "system_product": "  ",
            }
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_systeminfo()
            assert state._is_virtual is None

            server.responses["system.info"] = {
                "system_manufacturer": "QEMU",
                "system_product": "Standard PC",
            }
            await state.get_systeminfo()
            assert state._is_virtual is True


async def test_get_systemstats_does_not_cache_is_virtual_fields_blank() -> None:
    """Mirrors ``test_get_systeminfo_does_not_cache_is_virtual_when_fields_are_blank``
    for ``_detect_virtual()``'s own cached-on-first-use path: a first poll
    with both fields present but blank must not permanently mis-cache "not
    virtual" -- a later poll (here, via ``get_systeminfo()``) must still be
    able to detect it and have ``get_systemstats()`` skip ``cputemp``.
    """
    called_graphs: list[str] = []

    def netdata_graph(params: list) -> Any:
        called_graphs.append(params[0])
        return None

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": {
                "system_manufacturer": "",
                "system_product": "",
            },
            "reporting.netdata_graph": netdata_graph,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_systemstats()
            assert state._is_virtual is None

            server.responses["system.info"] = {
                "system_manufacturer": "QEMU",
                "system_product": "Standard PC",
            }
            await state.get_systeminfo()
            assert state._is_virtual is True

            called_graphs.clear()
            await state.get_systemstats()

    assert "cputemp" not in called_graphs


async def test_get_systeminfo_detects_virtual_with_whitespace_padded_fields() -> None:
    """A known VM manufacturer/product padded with surrounding whitespace
    (e.g. ``"  QEMU  "``) must still be detected as virtual: the usability
    guard strips before checking for blankness, but the *stripped* value must
    also be what's passed to the exact-match detector -- passing the raw,
    unstripped value would fail the membership check and wrongly cache the
    host as physical.
    """
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": {
                "system_manufacturer": "  QEMU  ",
                "system_product": "",
            }
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_systeminfo()
            assert state._is_virtual is True


async def test_get_systeminfo_logs_warning_when_is_virtual_fields_missing(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Mirrors ``test_get_systeminfo_logs_warning_on_unparsable_version``:
    a response missing both virtualization-detection fields must warn once
    (while nothing is cached yet) and log recovery once a subsequent poll
    supplies them.
    """
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"system.info": {"version": "TrueNAS-25.10.0"}},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            with caplog.at_level(logging.DEBUG, logger="aiotruenas.domain.state"):
                await state.get_systeminfo()

                server.responses["system.info"] = {
                    "version": "TrueNAS-25.10.0",
                    "system_manufacturer": "QEMU",
                    "system_product": "Standard PC",
                }
                await state.get_systeminfo()

    state_records = [r for r in caplog.records if r.name == "aiotruenas.domain.state"]
    warnings = [r for r in state_records if r.levelno == logging.WARNING]
    recoveries = [
        r
        for r in state_records
        if r.levelno == logging.DEBUG and "recovered" in r.getMessage()
    ]
    assert len(warnings) == 1
    assert "virtual" in warnings[0].getMessage().lower()
    assert len(recoveries) == 1


async def test_detect_virtual_recovers_after_partial_response() -> None:
    """``get_systemstats()`` calling ``_detect_virtual()`` first against a
    response missing both fields must not permanently lock in "not virtual"
    either -- a later ``get_systeminfo()`` poll with real VM identity must
    still be able to detect it, and the *next* ``get_systemstats()`` call
    must then skip ``cputemp``. Covers the same regression as
    ``test_get_systeminfo_does_not_cache_is_virtual_when_fields_missing``,
    but entering through ``_detect_virtual()`` instead.
    """
    called_graphs: list[str] = []

    def netdata_graph(params: list) -> Any:
        called_graphs.append(params[0])
        return None

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": {},
            "reporting.netdata_graph": netdata_graph,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_systemstats()
            assert state._is_virtual is None
            assert "cputemp" in called_graphs

            called_graphs.clear()
            server.responses["system.info"] = {
                "system_manufacturer": "QEMU",
                "system_product": "Standard PC",
            }
            await state.get_systeminfo()
            assert state._is_virtual is True

            await state.get_systemstats()

    assert "cputemp" not in called_graphs


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


async def test_get_systemstats_logs_warning_when_virtual_detection_raises(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A raising ``system.info`` call during virtualization detection must
    surface a warning and clear it again once the endpoint recovers -- even
    though a failed detection is never cached (see ``_detect_virtual()``),
    so every poll independently re-attempts it.
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
            "reporting.netdata_graph": _well_formed_netdata_graph,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            with caplog.at_level(logging.DEBUG, logger="aiotruenas.domain.state"):
                await state.get_systemstats()

                server.responses["system.info"] = {
                    "system_manufacturer": "Dell Inc.",
                    "system_product": "PowerEdge R730",
                }
                await state.get_systemstats()

    state_records = [r for r in caplog.records if r.name == "aiotruenas.domain.state"]
    warnings = [r for r in state_records if r.levelno == logging.WARNING]
    recoveries = [
        r
        for r in state_records
        if r.levelno == logging.DEBUG and "recovered" in r.getMessage()
    ]
    assert len(warnings) == 1
    assert "virtualization" in warnings[0].getMessage().lower()
    assert len(recoveries) == 1


async def test_get_systemstats_logs_warning_when_virtual_detection_malformed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A non-dict (but non-raising) ``system.info`` response during
    virtualization detection must surface a warning and clear it again once
    the endpoint recovers, just like a raised error -- regression test for
    the Pattern B bug where ``_detect_virtual()`` used to declare "recovered"
    before validating the response's shape.
    """
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": ["not", "a", "dict"],
            "reporting.netdata_graph": _well_formed_netdata_graph,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            with caplog.at_level(logging.DEBUG, logger="aiotruenas.domain.state"):
                await state.get_systemstats()

                server.responses["system.info"] = {
                    "system_manufacturer": "Dell Inc.",
                    "system_product": "PowerEdge R730",
                }
                await state.get_systemstats()

    state_records = [r for r in caplog.records if r.name == "aiotruenas.domain.state"]
    warnings = [r for r in state_records if r.levelno == logging.WARNING]
    recoveries = [
        r
        for r in state_records
        if r.levelno == logging.DEBUG and "recovered" in r.getMessage()
    ]
    assert len(warnings) == 1
    assert "malformed" in warnings[0].getMessage().lower()
    assert len(recoveries) == 1


async def test_get_systemstats_fetches_graphs_concurrently() -> None:
    """Regression test for a sequential-fetch bug: all systemstats graphs
    must be requested concurrently (via ``asyncio.gather``), so one
    slow/unresponsive graph does not delay the others from even starting --
    see homeassistant-truenas PR #118, which hit exactly this with a
    sequential ``for`` loop.
    """
    expected_graphs = {"load", "cpu", "cputemp", "memory", "arcsize"}
    started: set[str] = set()
    all_started = asyncio.Event()

    async def fake_call(method: str, params: Any = None, **kwargs: Any) -> Any:
        if method == "system.info":
            return {}
        assert method == "reporting.netdata_graph"
        started.add(params[0])
        if started == expected_graphs:
            all_started.set()
        else:
            # If graphs were fetched one at a time, this call would already
            # hold the (implicit) turn while the others haven't started
            # yet, so waiting here for the rest to start would deadlock and
            # fail the test on timeout instead of hanging forever.
            await asyncio.wait_for(all_started.wait(), timeout=2)
        return [{"legend": ["cpu"], "aggregations": {"mean": {"cpu": 1.0}}}]

    async with FakeTrueNASServer(valid_api_key=API_KEY) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            client.call = fake_call  # type: ignore[method-assign]
            await state.get_systemstats()

    assert started == expected_graphs


async def test_systemstats_stale_graphs_empty_before_first_call() -> None:
    async with FakeTrueNASServer(valid_api_key=API_KEY) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)

    assert state.systemstats_stale_graphs == frozenset()


async def test_ups_stale_graphs_empty_before_first_call() -> None:
    async with FakeTrueNASServer(valid_api_key=API_KEY) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)

    assert state.ups_stale_graphs == frozenset()


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


def _netdata_graph_with_usable_interface_reading(params: list) -> Any:
    """Like ``_well_formed_netdata_graph``, but with a genuinely parseable
    "interface" reading for identifier "eno1" -- the shared fixture has no
    "interface" branch by design (see its own docstring), so tests that need
    a real interface-throughput recovery use this instead.
    """
    if params[0] == "interface":
        return [
            {
                "identifier": "eno1",
                "legend": ["received", "sent"],
                "aggregations": {"mean": {"received": 100.0, "sent": 50.0}},
            }
        ]
    return _well_formed_netdata_graph(params)


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


async def test_get_systemstats_logs_warning_for_single_stale_systemstat_graph(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Each systemstats graph tracks its own failing/recovered key (``f"systemstat:
    {graph_name}"``), so a single failing graph must warn without affecting the
    others, and its own recovery must not bleed into a different graph's key.
    """

    def netdata_graph(params: list) -> Any:
        graph_name = params[0]
        if graph_name == "cpu":
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
        responses={
            "system.info": {
                "system_manufacturer": "Supermicro",
                "system_product": "X11SPi-TF",
            },
            "reporting.netdata_graph": netdata_graph,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            with caplog.at_level(logging.DEBUG, logger="aiotruenas.domain.state"):
                await state.get_systemstats()

                server.responses["reporting.netdata_graph"] = _well_formed_netdata_graph
                await state.get_systemstats()

    state_records = [r for r in caplog.records if r.name == "aiotruenas.domain.state"]
    warnings = [r for r in state_records if r.levelno == logging.WARNING]
    recoveries = [
        r
        for r in state_records
        if r.levelno == logging.DEBUG and "recovered" in r.getMessage()
    ]
    assert len(warnings) == 1
    assert "cpu" in warnings[0].getMessage()
    assert len(recoveries) == 1
    assert "cpu" in recoveries[0].getMessage()


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


async def test_get_systemstats_logs_warning_when_interface_throughput_raises(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A raising interface-throughput netdata query must surface a warning
    and clear it again once the endpoint recovers, tracked independently of
    every other systemstats graph's own failing/recovered key.
    """
    raw_interfaces = [
        {"id": "eno1", "name": "eno1", "state": {"link_state": "LINK_STATE_UP"}}
    ]

    def netdata_graph(params: list) -> Any:
        if params[0] == "interface":
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
        responses={
            "system.info": {
                "system_manufacturer": "Supermicro",
                "system_product": "X11SPi-TF",
            },
            "interface.query": raw_interfaces,
            "reporting.netdata_graph": netdata_graph,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_interface()
            with caplog.at_level(logging.DEBUG, logger="aiotruenas.domain.state"):
                await state.get_systemstats()

                server.responses["reporting.netdata_graph"] = (
                    _netdata_graph_with_usable_interface_reading
                )
                await state.get_systemstats()

    state_records = [r for r in caplog.records if r.name == "aiotruenas.domain.state"]
    warnings = [r for r in state_records if r.levelno == logging.WARNING]
    recoveries = [
        r
        for r in state_records
        if r.levelno == logging.DEBUG and "recovered" in r.getMessage()
    ]
    assert len(warnings) == 1
    assert "interface throughput" in warnings[0].getMessage().lower()
    assert len(recoveries) == 1


async def test_get_systemstats_logs_warning_when_interface_throughput_unusable(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A non-raising ``interface`` netdata graph response that does not
    resolve to any usable rx/tx reading (e.g. an identifier not matching any
    known interface) must surface a warning, not a false "recovered" --
    regression test for the Pattern B bug where
    ``_refresh_interface_throughput()`` used to declare "recovered" before
    checking whether a reading was actually applied.
    """
    raw_interfaces = [
        {"id": "eno1", "name": "eno1", "state": {"link_state": "LINK_STATE_UP"}}
    ]

    def netdata_graph_with_unmatched_interface(params: list) -> Any:
        if params[0] == "interface":
            return [
                {
                    "identifier": "unknown0",
                    "legend": ["received", "sent"],
                    "aggregations": {"mean": {"received": 100.0, "sent": 50.0}},
                }
            ]
        return _well_formed_netdata_graph(params)

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": {
                "system_manufacturer": "Supermicro",
                "system_product": "X11SPi-TF",
            },
            "interface.query": raw_interfaces,
            "reporting.netdata_graph": netdata_graph_with_unmatched_interface,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_interface()
            with caplog.at_level(logging.DEBUG, logger="aiotruenas.domain.state"):
                await state.get_systemstats()

                server.responses["reporting.netdata_graph"] = (
                    _netdata_graph_with_usable_interface_reading
                )
                await state.get_systemstats()

    state_records = [r for r in caplog.records if r.name == "aiotruenas.domain.state"]
    warnings = [r for r in state_records if r.levelno == logging.WARNING]
    recoveries = [
        r
        for r in state_records
        if r.levelno == logging.DEBUG and "recovered" in r.getMessage()
    ]
    assert len(warnings) == 1
    assert "no usable reading" in warnings[0].getMessage().lower()
    assert len(recoveries) == 1


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


# --- stale_endpoints (endpoint-granular staleness API) -----------------------

_RPC_ERROR = {
    "error": {
        "code": -32603,
        "message": "Internal error",
        "data": {"error": 1, "errname": "EFAULT", "reason": None},
    }
}


async def test_stale_endpoints_empty_before_any_refresh() -> None:
    async with FakeTrueNASServer(valid_api_key=API_KEY) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)

    assert state.stale_endpoints == frozenset()


async def test_stale_endpoints_reports_smb_then_clears_on_recovery() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"smb.status": [{}, {}]},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_smb()
            assert state.stale_endpoints == frozenset()

            server.responses["smb.status"] = None
            await state.get_smb()
            assert "smb" in state.stale_endpoints

            server.responses["smb.status"] = [{}, {}]
            await state.get_smb()
            assert "smb" not in state.stale_endpoints


async def test_stale_endpoints_reports_pool_then_clears_on_recovery() -> None:
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
            assert "pool" not in state.stale_endpoints

            server.responses["pool.query"] = None
            await state.get_pool()
            assert "pool" in state.stale_endpoints

            server.responses["pool.query"] = [_POOL_TANK]
            await state.get_pool()
            assert "pool" not in state.stale_endpoints


async def test_stale_endpoints_reports_directoryservices_then_recovers() -> None:
    config = {"id": 1, "service_type": "LDAP", "enable": True}
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "directoryservices.config": config,
            "directoryservices.status": {"status": "HEALTHY"},
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_directoryservices()
            assert "directoryservices" not in state.stale_endpoints

            server.responses["directoryservices.config"] = None
            await state.get_directoryservices()
            assert "directoryservices" in state.stale_endpoints

            server.responses["directoryservices.config"] = config
            await state.get_directoryservices()
            assert "directoryservices" not in state.stale_endpoints


async def test_stale_endpoints_reports_alerts_then_recovers() -> None:
    alert = {"uuid": "a1", "level": "CRITICAL", "formatted": "boom"}
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"alert.list": [alert]},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_alerts()
            assert "alerts" not in state.stale_endpoints

            server.responses["alert.list"] = None
            await state.get_alerts()
            assert "alerts" in state.stale_endpoints

            server.responses["alert.list"] = [alert]
            await state.get_alerts()
            assert "alerts" not in state.stale_endpoints


async def test_stale_endpoints_reports_ups_on_discovery_failure_then_recovers() -> None:
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
            assert "ups" not in state.stale_endpoints

            server.responses["reporting.netdata_graphs"] = _RPC_ERROR
            await state.get_ups()
            assert "ups" in state.stale_endpoints

            server.responses["reporting.netdata_graphs"] = [{"name": "upscharge"}]
            await state.get_ups()
            assert "ups" not in state.stale_endpoints


async def test_stale_endpoints_reports_ups_via_per_graph_stale_reading() -> None:
    """A discovered UPS graph that stops returning a usable reading keeps its
    previous field value -- ``stale_endpoints`` must flag ``"ups"`` off the
    self-clearing ``ups_stale_graphs`` set, not only off a failed discovery
    call.
    """

    def usable(params: list) -> Any:
        return [{"aggregations": {"mean": {"ups1": 55.0}}}]

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "reporting.netdata_graphs": [{"name": "upscharge"}],
            "reporting.netdata_graph": usable,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_ups()
            assert "ups" not in state.stale_endpoints

            server.responses["reporting.netdata_graph"] = _RPC_ERROR
            await state.get_ups()
            assert state.ups_stale_graphs == frozenset({"upscharge"})
            assert "ups" in state.stale_endpoints

            server.responses["reporting.netdata_graph"] = usable
            await state.get_ups()
            assert "ups" not in state.stale_endpoints


async def test_stale_endpoints_ignores_permanently_failing_ups_graph() -> None:
    """A UPS graph that is discovered but whose ``reporting.netdata_graph``
    query fails on every poll from the very first one -- so it never produces
    a value and stays absent from ``ups_stale_graphs`` -- must NOT pin
    ``"ups"`` into ``stale_endpoints`` forever. The stuck ``ups_graph:<name>``
    fallback key is deliberately a non-endpoint key (a NUT driver that never
    reports "upscurrent" would otherwise make the whole UPS endpoint
    permanently unavailable).
    """
    import aiotruenas.domain.state as state_module

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "reporting.netdata_graphs": [{"name": "upscharge"}],
            "reporting.netdata_graph": _RPC_ERROR,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_ups()
            await state.get_ups()

    graph_key = f"{state_module._UPS_GRAPH_KEY_PREFIX}upscharge"
    assert state._fallback_failing.get(graph_key) is True
    assert state.ups_stale_graphs == frozenset()
    assert "ups" not in state.stale_endpoints
    assert state.stale_endpoints == frozenset()


async def test_stale_endpoints_ignores_disk_temperature_fallback_failure() -> None:
    """A failed disk-temperature enrichment must NOT flag ``"disk"``:
    ``disk.query`` (the primary result) is fresh, and a disk with no temp
    sensor would otherwise pin the whole endpoint stale forever.
    """
    import aiotruenas.domain.state as state_module

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "disk.query": [_DISK_SDA],
            "reporting.netdata_graphs": [],
            "disk.temperatures": _RPC_ERROR,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_disk()

    # The enrichment path did fail...
    assert state._fallback_failing.get(state_module._KEY_DISK_TEMP_FALLBACK) is True
    # ...but the endpoint is not reported stale.
    assert "disk" not in state.stale_endpoints
    assert state.stale_endpoints == frozenset()


async def test_stale_endpoints_ignores_systemstats_graph_failure() -> None:
    """Failed systemstats netdata graphs surface only in
    ``systemstats_stale_graphs`` -- they enrich an already-fresh
    ``ds["system_info"]`` and must not flag the endpoint.
    """
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": {"system_manufacturer": "Supermicro"},
            "reporting.netdata_graph": _RPC_ERROR,
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_systeminfo()
            await state.get_systemstats()

    assert state.systemstats_stale_graphs  # the fine-grained signal fired
    assert "system_info" not in state.stale_endpoints
    assert state.stale_endpoints == frozenset()


async def test_stale_endpoints_ignores_interface_throughput_failure() -> None:
    """A failed interface-throughput enrichment surfaces only in
    ``systemstats_stale_graphs`` (named ``"interface"``), never as a
    ``stale_endpoints`` entry -- ``interface.query`` is the primary result.
    """
    raw_interfaces = [
        {"id": "eno1", "name": "eno1", "state": {"link_state": "LINK_STATE_UP"}}
    ]

    def netdata_graph(params: list) -> Any:
        if params[0] == "interface":
            return _RPC_ERROR
        return [{"legend": ["cpu"], "aggregations": {"mean": {"cpu": 20.0}}}]

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": {"system_manufacturer": "Supermicro"},
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
    assert "interface" not in state.stale_endpoints


async def test_stale_endpoints_ignores_version_and_virtual_detection_failure() -> None:
    """A ``system.info`` response that is a valid dict but lacks a parseable
    version and manufacturer/product still freshly populates the core
    ``ds["system_info"]`` fields -- the failed capability detection must not
    flag the endpoint (a dmidecode-less container would never recover).
    """
    import aiotruenas.domain.state as state_module

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": {"version": "not-a-version", "hostname": "nas", "physmem": 8}
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_systeminfo()

    assert state._fallback_failing.get(state_module._KEY_DETECT_VERSION) is True
    assert state._fallback_failing.get(state_module._KEY_DETECT_VIRTUAL) is True
    assert state.ds["system_info"]["hostname"] == "nas"
    assert state.stale_endpoints == frozenset()


async def test_stale_endpoints_reports_system_info_on_malformed_response() -> None:
    good_info = {
        "version": "TrueNAS-25.04.0",
        "physmem": 8,
        "system_manufacturer": "Supermicro",
        "system_product": "X11SPi-TF",
    }
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"system.info": good_info},
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_systeminfo()
            assert "system_info" not in state.stale_endpoints

            server.responses["system.info"] = None
            await state.get_systeminfo()
            assert "system_info" in state.stale_endpoints

            server.responses["system.info"] = good_info
            await state.get_systeminfo()
            assert "system_info" not in state.stale_endpoints


async def test_stale_endpoints_ignores_detect_version_malformed_system_info() -> None:
    """A malformed ``system.info`` response reached via ``_detect_version()``
    (through ``get_container()``, before ``get_systeminfo()`` has ever run)
    must not flag ``"system_info"`` in ``stale_endpoints`` -- that method
    never refreshes ``ds["system_info"]``, so its own RPC failure says
    nothing about whether that endpoint's data is stale. Regression test for
    a Sourcery finding on PR #44: ``_detect_version()`` previously reused
    ``get_systeminfo()``'s own fallback key for its malformed-response check,
    so this exact sequence falsely marked ``"system_info"`` stale.
    """
    import aiotruenas.domain.state as state_module

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": None,
            "virt.instance.query": [],
            "container.query": [],
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)
            await state.get_container()

    assert state._fallback_failing.get(state_module._KEY_DETECT_VERSION) is True
    assert state_module._KEY_SYSTEM_INFO not in state._fallback_failing
    assert "system_info" not in state.stale_endpoints
    assert state.stale_endpoints == frozenset()


def test_stale_endpoints_fallback_keys_are_classified_exactly_once() -> None:
    """Every ``_KEY_*`` / key-prefix constant must be classified exactly once
    as either an endpoint key or a deliberate non-endpoint (enrichment /
    capability-detection) key -- so a renamed or newly added key can't
    silently fall through ``stale_endpoints`` unnoticed.
    """
    import aiotruenas.domain.state as m

    key_constants = {
        value
        for name, value in vars(m).items()
        if name.startswith("_KEY_") and isinstance(value, str)
    }
    # Runtime-built keys are matched by prefix; introspect every
    # ``*_KEY_PREFIX`` module constant so a newly added one is covered here
    # without editing this test.
    prefix_constants = {
        value
        for name, value in vars(m).items()
        if name.endswith("_KEY_PREFIX") and isinstance(value, str)
    }

    endpoint_keys = set(m._FALLBACK_KEY_ENDPOINTS)
    non_endpoint_keys = set(m._NON_ENDPOINT_FALLBACK_KEYS)
    assert endpoint_keys.isdisjoint(non_endpoint_keys)
    assert endpoint_keys | non_endpoint_keys == key_constants

    # Every runtime-built key prefix is currently a deliberate non-endpoint
    # one -- ``stale_endpoints`` never resolves an endpoint by prefix.
    assert m._NON_ENDPOINT_FALLBACK_KEY_PREFIXES == prefix_constants

    # The resolver returns the mapped endpoint for endpoint keys and None for
    # the deliberate non-endpoint ones (and for an unknown key).
    for key, endpoint in m._FALLBACK_KEY_ENDPOINTS.items():
        assert TrueNASState._fallback_endpoint(key) == endpoint
    for key in m._NON_ENDPOINT_FALLBACK_KEYS:
        assert TrueNASState._fallback_endpoint(key) is None
    for prefix in m._NON_ENDPOINT_FALLBACK_KEY_PREFIXES:
        assert TrueNASState._fallback_endpoint(f"{prefix}example") is None
    assert TrueNASState._fallback_endpoint("no_such_key") is None


async def test_stale_endpoints_mapping_values_are_real_ds_keys() -> None:
    import aiotruenas.domain.state as state_module

    async with FakeTrueNASServer(valid_api_key=API_KEY) as server:
        async with make_client(server) as client:
            await client.connect()
            state = TrueNASState(client)

    mapped = set(state_module._FALLBACK_KEY_ENDPOINTS.values())
    assert mapped <= set(state.ds)
