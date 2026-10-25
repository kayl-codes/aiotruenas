# Changelog

All notable changes to this project are documented here. Release notes with more detail are
published on the [GitHub releases page](https://github.com/kayl-codes/aiotruenas/releases).

## [Unreleased]

### Added

- `TrueNASSystem`: a stateless reader that returns immutable, fully typed dataclasses
  (`aiotruenas.models`) instead of dicts:
  - `get_system_id()` → `str` (`system.global.id`)
  - `get_system_info()` → `SystemInfo` (`system.info`), incl. `boot_time` as a UTC `datetime`
    and the 1/5/15-minute load averages
  - `get_network_interfaces()` → `dict[str, NetworkInterface]` (`interface.query`)
  - `get_system_stats()` → `SystemStats` (netdata graphs: CPU usage and temperature, available
    memory and usage percent, ARC size, per-interface rx/tx in kilobits/s)
- Fields TrueNAS does not deliver are `None`, never a placeholder; values keep TrueNAS's units.
  RPC failures and malformed responses are raised, not swallowed.
- `mypy` in the dev dependencies, with strict checking for the new modules.

### Unchanged

- `TrueNASState` and its dict-based API are untouched; existing consumers need no changes.
