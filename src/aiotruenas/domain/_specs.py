"""Field-specification tables for ``TrueNASState``'s endpoint methods.

Ported from ``coordinator.py`` (Bronze-fork and Prod-HACS variants, identical
in this region) -- the declarative ``vals``/``ensure_vals`` lists passed to
``_normalize.parse_api()`` for each TrueNAS RPC endpoint. Kept separate from
``state.py`` so the endpoint methods themselves stay focused on orchestration
and derived-state logic rather than field mapping.
"""

from __future__ import annotations

from ._normalize import ApiValueSpec

# Field mapping shared by ``pool.query`` and ``boot.get_state`` (the boot-pool
# reports the same top-level shape, so both are parsed with these lists).
_POOL_VALS: list[ApiValueSpec] = [
    {"name": "guid", "default": 0},
    {"name": "id", "default": 0},
    {"name": "name", "default": "unknown"},
    {"name": "path", "default": "unknown"},
    {"name": "status", "default": "unknown"},
    {"name": "healthy", "type": "bool", "default": False},
    {"name": "is_decrypted", "type": "bool", "default": False},
    {"name": "size", "default": 0},
    {"name": "allocated", "default": 0},
    {"name": "free", "default": 0},
    {"name": "fragmentation", "default": 0},
    {
        "name": "autotrim",
        "source": "autotrim/parsed",
        "type": "bool",
        "default": False,
    },
    {
        "name": "scan_function",
        "source": "scan/function",
        "default": "unknown",
    },
    {"name": "scrub_state", "source": "scan/state", "default": "unknown"},
    {
        "name": "scrub_start",
        "source": "scan/start_time/$date",
        "default": 0,
        "convert": "utc_from_timestamp",
    },
    {
        "name": "scrub_end",
        "source": "scan/end_time/$date",
        "default": 0,
        "convert": "utc_from_timestamp",
    },
    {
        "name": "scrub_secs_left",
        "source": "scan/total_secs_left",
        "default": 0,
    },
]
_POOL_ENSURE_VALS: list[ApiValueSpec] = [
    {"name": "available", "default": 0.0},
    {"name": "total", "default": 0.0},
    {"name": "usage", "default": 0.0},
    {"name": "errors", "default": 0},
    {"name": "read_errors", "default": 0},
    {"name": "write_errors", "default": 0},
    {"name": "checksum_errors", "default": 0},
]

# pool.dataset.query.
_DATASET_VALS: list[ApiValueSpec] = [
    {"name": "id", "default": "unknown"},
    {"name": "type", "default": "unknown"},
    {"name": "name", "default": "unknown"},
    {"name": "pool", "default": "unknown"},
    {"name": "mountpoint", "default": "unknown"},
    {"name": "comments", "source": "comments/parsed", "default": ""},
    {
        "name": "deduplication",
        "source": "deduplication/parsed",
        "type": "bool",
        "default": False,
    },
    {
        "name": "atime",
        "source": "atime/parsed",
        "type": "bool",
        "default": False,
    },
    {
        "name": "casesensitivity",
        "source": "casesensitivity/parsed",
        "default": "unknown",
    },
    {"name": "checksum", "source": "checksum/parsed", "default": "unknown"},
    {
        "name": "exec",
        "source": "exec/parsed",
        "type": "bool",
        "default": False,
    },
    {"name": "sync", "source": "sync/parsed", "default": "unknown"},
    {
        "name": "compression",
        "source": "compression/parsed",
        "default": "unknown",
    },
    {
        "name": "compressratio",
        "source": "compressratio/parsed",
        "default": "unknown",
    },
    {"name": "quota", "source": "quota/parsed", "default": "unknown"},
    {"name": "copies", "source": "copies/parsed", "default": 0},
    {
        "name": "readonly",
        "source": "readonly/parsed",
        "type": "bool",
        "default": False,
    },
    {"name": "recordsize", "source": "recordsize/parsed", "default": 0},
    {
        "name": "encryption_algorithm",
        "source": "encryption_algorithm/parsed",
        "default": "unknown",
    },
    {
        "name": "encryption_key_format",
        "source": "key_format/parsed",
        "default": "unknown",
    },
    {"name": "encrypted", "type": "bool", "default": False},
    {"name": "locked", "type": "bool", "default": False},
    {"name": "used", "source": "used/parsed", "default": 0},
    {"name": "available", "source": "available/parsed", "default": 0},
]

# Job-progress fields shared by the cloudsync, replication and rsync queries.
_JOB_PROGRESS_VALS: list[ApiValueSpec] = [
    {
        "name": "time_started",
        "source": "job/time_started/$date",
        "default": 0,
        "convert": "utc_from_timestamp",
    },
    {
        "name": "time_finished",
        "source": "job/time_finished/$date",
        "default": 0,
        "convert": "utc_from_timestamp",
    },
    {"name": "job_percent", "source": "job/progress/percent", "default": 0},
    {
        "name": "job_description",
        "source": "job/progress/description",
        "default": "unknown",
    },
]

# Cloudsync and rsync report their status via the last job (job/state).
# Replication has its own persistent ``state`` object (``state/state``) -- the
# value shown in the TrueNAS WebUI -- and overrides this.
_JOB_STATUS_VALS: list[ApiValueSpec] = [
    {"name": "state", "source": "job/state", "default": "unknown"},
    *_JOB_PROGRESS_VALS,
]

# cloudsync.query.
_CLOUDSYNC_VALS: list[ApiValueSpec] = [
    {"name": "id", "default": "unknown"},
    {"name": "description", "default": "unknown"},
    {"name": "direction", "default": "unknown"},
    {"name": "path", "default": "unknown"},
    {"name": "enabled", "type": "bool", "default": False},
    {"name": "transfer_mode", "default": "unknown"},
    {"name": "snapshot", "type": "bool", "default": False},
    *_JOB_STATUS_VALS,
]

# replication.query. Unlike cloudsync/rsync, replication has its own
# persistent "state" object (state/state, what the WebUI shows) rather than
# relying solely on the last job; job_state is kept only as a fallback source
# and dropped again after TrueNASState.get_replication() resolves it.
_REPLICATION_VALS: list[ApiValueSpec] = [
    {"name": "id", "default": 0},
    {"name": "name", "default": "unknown"},
    {"name": "source_datasets", "default": "unknown"},
    {"name": "target_dataset", "default": "unknown"},
    {"name": "recursive", "type": "bool", "default": False},
    {"name": "enabled", "type": "bool", "default": False},
    {"name": "direction", "default": "unknown"},
    {"name": "transport", "default": "unknown"},
    {"name": "auto", "type": "bool", "default": False},
    {"name": "retention_policy", "default": "unknown"},
    {"name": "state", "source": "state/state", "default": "unknown"},
    {"name": "job_state", "source": "job/state", "default": "unknown"},
    *_JOB_PROGRESS_VALS,
]

# rsynctask.query.
_RSYNC_VALS: list[ApiValueSpec] = [
    {"name": "id", "default": 0},
    {"name": "path", "default": "unknown"},
    {"name": "desc", "default": "unknown"},
    {"name": "remotehost", "default": "unknown"},
    {"name": "remotemodule", "default": "unknown"},
    {"name": "direction", "default": "unknown"},
    {"name": "mode", "default": "unknown"},
    {"name": "enabled", "type": "bool", "default": False},
    *_JOB_STATUS_VALS,
]

# pool.snapshottask.query.
_SNAPSHOTTASK_VALS: list[ApiValueSpec] = [
    {"name": "id", "default": 0},
    {"name": "dataset", "default": "unknown"},
    {"name": "recursive", "type": "bool", "default": False},
    {"name": "lifetime_value", "default": 0},
    {"name": "lifetime_unit", "default": "unknown"},
    {"name": "enabled", "type": "bool", "default": False},
    {"name": "naming_schema", "default": "unknown"},
    {"name": "allow_empty", "type": "bool", "default": False},
    {"name": "vmware_sync", "type": "bool", "default": False},
    {"name": "schedule", "default": {}},
    {"name": "state", "source": "state/state", "default": "unknown"},
    {
        "name": "datetime",
        "source": "state/datetime/$date",
        "default": 0,
        "convert": "utc_from_timestamp",
    },
]

# cronjob.query.
_CRONJOB_VALS: list[ApiValueSpec] = [
    {"name": "id", "default": 0},
    {"name": "enabled", "type": "bool", "default": False},
    {"name": "command", "default": ""},
    {"name": "description", "default": ""},
    {"name": "user", "default": "unknown"},
    {"name": "schedule", "default": {}},
    {"name": "stdout", "type": "bool", "default": False},
    {"name": "stderr", "type": "bool", "default": False},
]
_CRONJOB_ENSURE_VALS: list[ApiValueSpec] = [
    {"name": "display_name", "default": ""},
]
