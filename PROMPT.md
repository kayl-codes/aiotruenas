# Task: Build aiotruenas — an asyncio-native Python client for the TrueNAS JSON-RPC WebSocket API

## Context

This is a greenfield standalone library (repo: kayl-codes/aiotruenas, currently empty except
LICENSE/Apache-2.0). It will eventually replace the synchronous, thread-based API client used by
the Home Assistant custom integration "TrueNAS CE" (kayl-codes/homeassistant-truenas,
domain `truenas_ce`), but this repo has ZERO dependency on Home Assistant — it must be a
general-purpose, reusable Python package usable outside HA too.

## Non-goals for this step

- Do NOT import anything from `homeassistant.*`.
- Do NOT integrate this into the `truenas_ce` custom component yet — that is a later, separate step.
- Do NOT add typed per-domain convenience methods (e.g. `get_pools()`, `start_vm()`) — a generic
  RPC call surface is enough for v1. Convenience wrappers can be added later once real usage
  patterns are known.
- Do NOT copy source code from https://github.com/truenas/api_client verbatim — it is LGPL-3.0
  licensed and we want this repo to stay clean Apache-2.0. You may read its README/docs as prior
  art for protocol understanding (SCRAM-SHA-512 auth, channel binding, rate limits), but reimplement
  independently in your own words/code.

## Reference material to read FIRST (for behavioral parity / error taxonomy)

The following files are from the existing (synchronous) implementation and describe exactly which
error conditions must be distinguishable and which RPC methods must work. Do not port the code
as-is (it's thread/lock-based and will be replaced), but use it as the source of truth for behavior:

- Sync client to replace: the local working copy has it at
  `custom_components/truenas_ce/api.py` in the kayl-codes/homeassistant-truenas repo (class
  `TrueNASAPI`). Key things to preserve:
  - Connection setup with configurable `verify_ssl` (default True) and TLS context creation.
  - Distinct, stable error categories (see below) instead of one generic "connection failed".
  - A generic `query(method, params)` call surface (method name as string, params as list/dict).
  - A retry-once-after-5s behavior specifically for WebSocket handshake timeouts (TrueNAS may
    briefly hold a connection slot open after a clean disconnect, e.g. during integration reload).
- Error categories to preserve as a taxonomy (currently string constants in `const.py`, prefixed
  `ERR_*`): certificate verify failed, HTTP used on a wss:// port, TLS version not supported,
  WebSocket upgrade not supported, unknown hostname (DNS), connection refused, handshake timeout,
  invalid API key / login rejected, proxy/reverse-proxy intercepted the handshake (e.g. Cloudflare
  Access redirecting to an SSO login page — detected via 301/302/303/307/308 or 401/403 on the
  handshake), API method not found (404), request timeout, malformed/empty result, connection lost
  mid-login, connection lost mid-query, and an unknown/fallback category. Represent these as a
  small exception hierarchy (e.g. `TrueNASError` base, with subclasses like
  `TrueNASAuthenticationError`, `TrueNASConnectionError`, `TrueNASTimeoutError`,
  `TrueNASProxyInterceptedError`, etc.) rather than string constants — that's more idiomatic for a
  standalone library than the string-constant approach the HA integration uses internally.
- RPC methods that MUST work end-to-end (used by the coordinator today — treat this as your
  integration-test method list): `system.info`, `interface.query`, `update.status`, `service.query`,
  `pool.query`, `boot.get_state`, `pool.dataset.query`, `disk.query`, `vm.query`,
  `virt.instance.query`, `directoryservices.config`, `directoryservices.status`, `alert.list`,
  `certificate.query`, `smb.status`, `cloudsync.query`, `replication.query`, `rsynctask.query`,
  `pool.snapshottask.query`, `pool.scrub.query`, `app.query`, `cronjob.query`, `core.get_jobs`
  (used to poll long-running job status by id), plus various netdata-graph reporting calls.
  Also confirm the generic call surface supports write/action calls with the same shape (e.g.
  `pool.scrub.scrub(name, "START")`, `alert.dismiss(uuid)`, dataset lock/unlock with a passphrase,
  cronjob run, service start/stop/restart, VM/container/app start/stop/restart, system reboot/shutdown)
  — these are just `query(method, params)` calls with different method names/params, no special
  handling needed in the client itself.
- Job polling: several operations (scrub, replication, dataset lock/unlock, bulk operations) return
  a job id and must be polled via `core.get_jobs` until done. Consider an optional
  `call(method, params, job=True)` convenience that polls automatically (inspired by, but not copied
  from, the official truenas/api_client's `job=True` parameter) — nice-to-have, not mandatory for v1.

## Protocol version — decision point, please verify and default to the modern one

The current sync client talks to the LEGACY endpoint `ws(s)://<host>/websocket` using a DDP-style
envelope (`{"msg": "connect", "version": "1", "support": ["1"]}`, then
`{"msg": "method", "method": ..., "id": ..., "params": [...]}`), matched against responses by `id`.
This is NOT real JSON-RPC 2.0 — per TrueNAS's own docs
(https://api.truenas.com/v25.04/jsonrpc.html), the modern endpoint is
`ws(s)://<host>/api/current` speaking actual JSON-RPC 2.0
(`{"jsonrpc": "2.0", "method": ..., "id": ..., "params": [...]}`, no "msg" envelope, no separate
"connect" handshake step before login).

**Default to building this library against the modern `/api/current` JSON-RPC 2.0 endpoint**, since:
- It's simpler (no DDP envelope, likely no subprotocol-negotiation quirks to work around).
- It's required for SCRAM-SHA-512 API-key auth on TrueNAS 26+ (per api.truenas.com docs).
- Our HA integration already targets TrueNAS 25.04+ only, which supports it.

Before writing the connection code, fetch and read the current JSON-RPC docs at
https://api.truenas.com/v25.04/jsonrpc.html (and check whether a newer version page, e.g.
v25.10 or v26.0, describes any relevant differences) to confirm exact framing, the
`auth.login_with_api_key` call shape, and authentication flow on the modern endpoint — do not
guess the wire format from the legacy client alone.

## Required architecture

- Python >= 3.13, asyncio-native throughout. Use `websockets`'s asyncio client (the modern
  `websockets.asyncio.client.connect`, NOT `websockets.sync.client`), matching the
  `websockets>=15.0.1` version already used elsewhere in this project family.
- No threads, no `RLock` — use a single `asyncio.Lock` to serialize send/recv on the shared
  connection (asyncio is single-threaded, so the dual-lock scheme in the old sync client, needed
  there to order I/O-lock vs. fast-state-lock across OS threads, can collapse to one lock here).
- Public API shape (adjust names as you see fit, but keep this shape):
  ```python
  async with TrueNASClient(host, api_key, verify_ssl=True) as client:
      result = await client.call("system.info")
  ```
  - `async def connect(self) -> None` (raises on failure instead of returning a bool+error-string —
    that's a deliberate change from the HA-side sync client's "return None, check .error" style;
    a later integration step will adapt the HA coordinator to catch these exceptions).
  - `async def call(self, method: str, params: list | dict | None = None, *, timeout: float | None
    = None, job: bool = False) -> Any`
  - `async def close(self) -> None`
  - `connected: bool` property.
  - Async context manager support (`__aenter__`/`__aexit__`).
- SSL: lazily build the `ssl.SSLContext` (blocking call — `ssl.create_default_context()` loads
  system certs), but since everything here is already async and you control the event loop, either
  build it once in `connect()` via `asyncio.to_thread` or accept the minor blocking cost — do not
  reintroduce a full executor-thread architecture just for this one call.
- Reconnect: preserve the "retry once after a 5s delay on handshake timeout" behavior from the old
  client (TrueNAS may briefly hold a connection slot open after a clean disconnect).
- Timeouts: default query timeout should match the existing 30.0s (`QUERY_TIMEOUT` in the old
  `const.py`), configurable per-call.

## Testing & CI

- `pytest` + `pytest-asyncio`, with a mock WebSocket server (e.g. via `websockets.asyncio.server`)
  to test: successful connect+login, invalid API key, connection refused, TLS cert verification
  failure classification, timeout during a call, reconnect-after-disconnect, and at minimum one
  round-trip test per RPC method category listed above (can be table-driven against a fake server
  that echoes canned JSON-RPC responses).
- GitHub Actions CI mirroring the pattern used in kayl-codes/homeassistant-truenas's `ci.yml`:
  `ruff check .` (rules E, F, W, I, UP, ASYNC; line-length 88) + `ruff format --check .` + `pytest`.
- `pyproject.toml`: package name `aiotruenas` (import as `import aiotruenas`), Apache-2.0 license
  metadata matching the repo's LICENSE file, `requires-python = ">=3.13"`, single runtime dependency
  `websockets>=15.0.1`.

## Definition of done for this step

A pip-installable (from source, not yet published to PyPI) package that can connect to a real
TrueNAS 25.04+ instance, log in with an API key, execute the RPC methods listed above, correctly
classify and surface connection/auth errors as typed exceptions, and has a green CI (lint + tests).
Integrating it into the `truenas_ce` Home Assistant integration is explicitly a separate, later step.
