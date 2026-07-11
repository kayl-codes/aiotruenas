# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

`aiotruenas` is a standalone, asyncio-native Python client library for the TrueNAS **JSON-RPC 2.0**
WebSocket API (`ws(s)://<host>/api/current`, TrueNAS 25.04+). It has **zero dependency on Home
Assistant** — it is meant to be usable as a general-purpose package on its own, and will later be
consumed by the `truenas_ce` Home Assistant custom integration (`kayl-codes/homeassistant-truenas`)
as a replacement for that repo's synchronous, thread-based `TrueNASAPI` client. That integration step
is out of scope here; nothing in this repo may `import homeassistant.*`.

Full requirements, protocol decisions, and the behavioral parity checklist (error taxonomy, RPC
method list, etc.) for the initial implementation are specified in [PROMPT.md](PROMPT.md) — read it
before making architectural changes.

## Status

Implementation of the initial client (per PROMPT.md) is **in progress**. Until that lands, most of
the sections below are placeholders describing the *planned* shape, not existing code — check the
current tree before relying on any file path mentioned here.

## Commands

Planned to mirror `kayl-codes/homeassistant-truenas`'s CI pattern:

```bash
ruff check .            # lint (rules E, F, W, I, UP, ASYNC; py313, line-length 88) — TODO: not yet configured
ruff format --check .   # formatting check; drop --check to auto-format         — TODO: not yet configured
pytest                  # unit tests against a mock websockets server            — TODO: no tests/ yet
```

- Python target: **3.13** (`requires-python = ">=3.13"`).
- Single runtime dependency: `websockets>=15.0.1`.
- `pyproject.toml`, `ruff` config, `pytest`/`pytest-asyncio` setup, and CI (`.github/workflows/`) do
  not exist yet — this is tracked as part of the PROMPT.md implementation, not assumed to be present.

## Architecture (planned — see PROMPT.md for full detail)

- **`TrueNASClient`**: the sole public entry point. Asyncio-native, no threads, no `RLock` — a
  single `asyncio.Lock` serializes send/recv on the shared WebSocket connection. Public shape:
  ```python
  async with TrueNASClient(host, api_key, verify_ssl=True) as client:
      result = await client.call("system.info")
  ```
  `connect()` raises typed exceptions on failure (no `return None, check .error` pattern like the
  synchronous HA-side client). `call(method, params, *, timeout=None, job=False)` is the generic RPC
  surface — no typed per-domain convenience methods in v1 (e.g. no `get_pools()`).
- **Exception hierarchy**: `TrueNASError` base with subclasses for the error taxonomy carried over
  from the sync client's `ERR_*` constants (cert verification, wrong scheme/port, TLS/WS negotiation
  failures, DNS failure, connection refused, handshake timeout, invalid API key, proxy/SSO
  interception, method not found, timeout, malformed result, connection lost mid-login/mid-query,
  unknown fallback) — see PROMPT.md for the full list and rationale.
- **Reconnect**: retry once after a 5s delay specifically on WebSocket handshake timeout (TrueNAS may
  briefly hold a connection slot open after a clean disconnect).
- **Auth**: plain API-key login via `auth.login_with_api_key` (single `[api_key]` param, still
  supported through TrueNAS 26 even though deprecated server-side). Full SCRAM-SHA-512 / channel
  binding auth is explicitly **out of scope** for this step — a possible later addition, not required
  for TrueNAS 25.04+.

## Non-goals (see PROMPT.md for full rationale)

- No `homeassistant.*` imports.
- No typed per-domain convenience methods in v1 (generic `call(method, params)` only).
- No verbatim code from `truenas/api_client` (LGPL-3.0) — protocol understanding only, reimplemented
  independently to keep this repo Apache-2.0.
- No SCRAM-SHA-512 implementation in this step (see Architecture above).
