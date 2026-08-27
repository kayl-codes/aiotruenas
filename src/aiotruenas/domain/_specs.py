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

# service.query.
_SERVICE_VALS: list[ApiValueSpec] = [
    {"name": "id", "default": 0},
    {"name": "service", "default": "unknown"},
    {"name": "name", "default": ""},
    {"name": "enable", "type": "bool", "default": False},
    {"name": "state", "default": "unknown"},
]
_SERVICE_ENSURE_VALS: list[ApiValueSpec] = [
    {"name": "running", "type": "bool", "default": False},
    {"name": "display_name", "default": "unknown"},
]

# vm.query.
_VM_VALS: list[ApiValueSpec] = [
    {"name": "id", "default": 0},
    {"name": "name", "default": "unknown"},
    {"name": "type", "default": "unknown"},
    {"name": "cpu", "source": "vcpus", "default": 0},
    {"name": "memory", "default": 0},
    {"name": "autostart", "type": "bool", "default": False},
    {"name": "image", "source": "description", "default": "unknown"},
    {"name": "status", "source": "status/state", "default": "unknown"},
]
_VM_ENSURE_VALS: list[ApiValueSpec] = [
    {"name": "running", "type": "bool", "default": False},
]

# virt.instance.query, filtered to type == "CONTAINER" (legacy Incus
# containers, pre-TrueNAS-26.0; VM-type instances are covered by _VM_VALS).
_CONTAINER_VALS: list[ApiValueSpec] = [
    {"name": "id", "default": "unknown"},
    {"name": "name", "default": "unknown"},
    {"name": "type", "default": "unknown"},
    {"name": "cpu", "default": 0},
    {"name": "memory", "default": 0},
    {"name": "autostart", "type": "bool", "default": False},
    {"name": "image", "source": "image/description", "default": "unknown"},
    {"name": "status", "default": "unknown"},
    {"name": "aliases", "default": []},
]
_CONTAINER_ENSURE_VALS: list[ApiValueSpec] = [
    {"name": "running", "type": "bool", "default": False},
    {"name": "ip_address", "default": "unknown"},
]

# container.query (LXC containers, TrueNAS 26.0+, libvirt-based): the entry
# carries no memory, image or IP information and its status is nested
# (status/state); ensure_vals fills in the same keys as the legacy Incus
# path so the resulting record shape is unchanged either way.
_CONTAINER_V26_VALS: list[ApiValueSpec] = [
    {"name": "id", "default": "unknown"},
    {"name": "name", "default": "unknown"},
    {"name": "cpuset", "default": None},
    {"name": "autostart", "type": "bool", "default": False},
    {"name": "image", "source": "description", "default": "unknown"},
    {"name": "status", "source": "status/state", "default": "unknown"},
]
_CONTAINER_V26_ENSURE_VALS: list[ApiValueSpec] = [
    {"name": "type", "default": "CONTAINER"},
    {"name": "cpu", "default": 0},
    {"name": "memory", "default": 0},
    {"name": "aliases", "default": []},
    {"name": "running", "type": "bool", "default": False},
    {"name": "ip_address", "default": "unknown"},
]

# app.query.
_APP_VALS: list[ApiValueSpec] = [
    {"name": "id", "default": 0},
    {"name": "name", "default": "unknown"},
    {"name": "human_version", "default": "unknown"},
    {"name": "version", "default": "unknown"},
    {"name": "latest_version", "default": "unknown"},
    {"name": "custom_app", "type": "bool", "default": False},
    {
        "name": "update_available",
        "source": "upgrade_available",
        "type": "bool",
        "default": False,
    },
    {"name": "image_updates_available", "type": "bool", "default": False},
    {"name": "portal", "source": "portals/Web UI", "default": "unknown"},
    {"name": "state", "default": "unknown"},
]
# Update-job tracking fields (update_jobid/update_progress/update_state/
# update_description) are only defaulted here; polling the upgrade job itself
# stays with the consumer's own HA update-entity handling, not TrueNASState.
_APP_ENSURE_VALS: list[ApiValueSpec] = [
    {"name": "running", "type": "bool", "default": False},
    {"name": "update_jobid", "default": 0},
    {"name": "update_progress", "default": 0},
    {"name": "update_state", "default": "unknown"},
    {"name": "update_description", "default": ""},
]

# certificate.query. Keyed by "name" rather than "id" (see
# TrueNASState.get_certificates()): a manual renewal/reissue deletes the old
# database row and creates a new one with a fresh id but the same
# (database-unique) name.
_CERTIFICATE_VALS: list[ApiValueSpec] = [
    {"name": "id", "default": 0},
    {"name": "name", "default": "unknown"},
    {"name": "cert_type", "default": "unknown"},
    {"name": "common", "default": ""},
    {"name": "until", "default": None, "convert": "human_date_to_utc"},
    {"name": "expired", "type": "bool", "default": False},
    {"name": "renew_days", "default": 0},
]

# directoryservices.config + directoryservices.status, merged into a single
# source row before parse_api (config carries the service type/domain/
# options, status carries the live HEALTHY/FAULTED state). There is only
# ever one row -- "id" is a synthetic constant, not a real API field.
_DIRECTORYSERVICES_VALS: list[ApiValueSpec] = [
    {"name": "id", "default": 1},
    {"name": "type", "source": "service_type", "default": "unknown"},
    {"name": "enable", "type": "bool", "default": False},
    {
        "name": "account_cache",
        "source": "enable_account_cache",
        "type": "bool",
        "default": False,
    },
    {
        "name": "dns_updates",
        "source": "enable_dns_updates",
        "type": "bool",
        "default": False,
    },
    {"name": "kerberos_realm", "default": "unknown"},
    {"name": "domain", "source": "configuration/domain", "default": "unknown"},
    {"name": "site", "source": "configuration/site", "default": "unknown"},
    {"name": "status", "default": "unknown"},
    {"name": "status_msg", "default": None},
]
_DIRECTORYSERVICES_ENSURE_VALS: list[ApiValueSpec] = [
    {"name": "healthy", "type": "bool", "default": False},
]

# interface.query. Live rx/tx throughput -- sourced in the legacy coordinator
# from a separate, much larger multi-graph netdata "interface" stat batch --
# is out of scope; ensure_vals only defaults rx/tx to 0.
_INTERFACE_VALS: list[ApiValueSpec] = [
    {"name": "id", "default": "unknown"},
    {"name": "name", "default": "unknown"},
    {"name": "description", "default": "unknown"},
    {"name": "mtu", "default": "unknown"},
    {"name": "link_state", "source": "state/link_state", "default": "unknown"},
    {
        "name": "active_media_type",
        "source": "state/active_media_type",
        "default": "unknown",
    },
    {
        "name": "active_media_subtype",
        "source": "state/active_media_subtype",
        "default": "unknown",
    },
    {"name": "link_address", "source": "state/link_address", "default": "unknown"},
]
_INTERFACE_ENSURE_VALS: list[ApiValueSpec] = [
    {"name": "rx", "default": 0},
    {"name": "tx", "default": 0},
]

# disk.query. Keyed by "identifier" (stable across a devname/name change,
# e.g. on drive reseat) rather than "name".
_DISK_VALS: list[ApiValueSpec] = [
    {"name": "name", "default": "unknown"},
    {"name": "devname", "default": "unknown"},
    {"name": "serial", "default": "unknown"},
    {"name": "size", "default": "unknown"},
    {"name": "hddstandby", "default": "unknown"},
    {"name": "hddstandby_force", "type": "bool", "default": False},
    {"name": "advpowermgmt", "default": "unknown"},
    {"name": "acousticlevel", "default": "unknown"},
    {"name": "model", "default": "unknown"},
    {"name": "rotationrate", "default": "unknown"},
    {"name": "type", "default": "unknown"},
    {"name": "zfs_guid", "default": "unknown"},
    {"name": "identifier", "default": "unknown"},
]
_DISK_ENSURE_VALS: list[ApiValueSpec] = [
    {"name": "temperature", "default": None},
]

# pool.scrub.query.
_SCRUB_VALS: list[ApiValueSpec] = [
    {"name": "id", "default": None},
    {"name": "pool_name", "default": ""},
    {"name": "enabled", "type": "bool", "default": False},
]

# system.info. Keyless (flat singleton, like _ARC_GRAPHS/_UPS_GRAPHS in
# state.py) -- there is only ever one system. CPU/load/memory/ARC-size stats
# and interface throughput are not part of system.info itself; ensure_vals
# only guarantees their keys exist until TrueNASState.get_systemstats() (a
# separate, much larger netdata-graph query) fills them in.
_SYSTEMINFO_VALS: list[ApiValueSpec] = [
    {"name": "version", "default": "unknown"},
    {"name": "hostname", "default": "unknown"},
    {"name": "uptime_seconds", "default": 0},
    {"name": "system_serial", "default": "unknown"},
    {"name": "system_product", "default": "unknown"},
    {"name": "system_manufacturer", "default": "unknown"},
    {"name": "physmem", "default": 0},
]
_SYSTEMINFO_ENSURE_VALS: list[ApiValueSpec] = [
    {"name": "uptimeEpoch", "default": 0},
    {"name": "cpu_temperature", "default": None},
    {"name": "cpu_usage", "default": 0.0},
    {"name": "load_shortterm", "default": 0.0},
    {"name": "load_midterm", "default": 0.0},
    {"name": "load_longterm", "default": 0.0},
    {"name": "cache_size-arc_value", "default": 0.0},
    {"name": "memory-free_value", "default": 0.0},
    {"name": "memory-total_value", "default": 0.0},
    {"name": "memory-usage_percent", "default": 0},
]
