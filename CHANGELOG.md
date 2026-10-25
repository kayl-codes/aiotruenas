# Changelog

All notable changes to this project are documented here. Release notes with more detail are
published on the [GitHub releases page](https://github.com/kayl-codes/aiotruenas/releases).

## [Unreleased]

## [1.7.0] - 2026-10-25

### Added

- `TrueNASSystem` (#53): a stateless reader that returns immutable, fully typed dataclasses
  (`aiotruenas.models`) instead of dicts:
  - `get_system_id()` → `str` (`system.global.id`)
  - `get_system_info()` → `SystemInfo` (`system.info`), incl. `boot_time` as a UTC `datetime`
    and the 1/5/15-minute load averages
  - `get_network_interfaces()` → `dict[str, NetworkInterface]` (`interface.query`)
  - `get_system_stats()` → `SystemStats` (netdata graphs: CPU usage and temperature, available
    memory and usage percent, ARC size, per-interface rx/tx in kilobits/s)
- Fields TrueNAS does not deliver are `None`, never a placeholder; values keep TrueNAS's units.
  RPC failures and malformed responses are raised, not swallowed. The only tolerated error is an
  unknown `cputemp` graph (errname `ENOENT`, live-verified on TrueNAS 25.10), which leaves just
  `cpu_temperature` as `None` (#56).
- `TrueNASState.get_pool()` parses a pool's scrub pause time as `scrub_pause` (UTC `datetime`
  from `scan.pause`, `None` when the scrub is not paused) and its progress as `scrub_percentage`
  (`scan.percentage`, raw percent as TrueNAS reports it, `0` without scan data). A paused scrub keeps
  `scrub_state` at `SCANNING`, so `scrub_pause` is what tells paused from running.
- `mypy` in the dev dependencies, with strict checking for the new modules.

### Fixed

- **Cancelled calls no longer leak:** a cancelled `call()`, for example inside a cancelled
  `asyncio.gather()`, used to leave its pending future registered until a reply arrived, possibly
  forever (#54). The same held for unserializable `params` and unexpected `send()` errors; those
  are now raised with nothing left behind, and the connection stays usable (#59).
- **Cancelled `close()` propagates:** cancelling `close()` no longer swallows the caller's
  cancellation. The connection is still torn down, so pending calls fail instead of hanging, and
  a cancel during the close handshake aborts the socket instead of leaving it open (#60).
- **Cancelled `connect()` closes its socket:** a cancel during login no longer leaves an
  untracked socket open (#61).
- **`connect()` during `close()`:** a `connect()` queued behind `close()`, or behind a call that
  timed out or failed to send, now opens a fresh connection. Before, it could report the closing
  one as connected, or a late teardown could destroy the new one. Pending calls fail with
  `TrueNASConnectionClosedError(phase="disconnect")`, and replies, notifications and teardowns
  of an old connection no longer touch a newer one (#62).
- **`subscribe()` / `unsubscribe()` stay on their own connection:** `subscribe()` raises
  `TrueNASConnectionClosedError` if its connection closes before it returns, instead of handing
  out a queue that is never fed. `unsubscribe()` only sends `core.unsubscribe` for a subscription
  of the current connection. A request the server rejects keeps the subscription tracked so it
  can be retried, and any other outcome ends it locally (#62).
- **First subscription event:** a notification the server sends right after its
  `core.subscribe` reply is now queued instead of dropped (#63).

### Unchanged

- `TrueNASState` keeps its dict-based API. Its only change is the two new `get_pool()` keys
  above, so existing consumers need no changes.
