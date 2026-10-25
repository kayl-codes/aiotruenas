# Review checklist — aiotruenas

The local review agents check every diff against these rules before a push.

Asyncio-native client library for the TrueNAS JSON-RPC 2.0 WebSocket API (TrueNAS 25.04+), Python ≥ 3.13,
single runtime dependency `websockets`. `CLAUDE.md` and `PROMPT.md` in the repo root describe the architecture
and are the reference for these rules; the README's "Semantics" and stale-data paragraphs are the public contract.

## Code review focus

- Start each review comment with a one-sentence summary of the suggested fix.
- Focus on correctness: logic errors, unhandled edge cases, race conditions, resource leaks, silent failures
  (swallowed exceptions, fallbacks that hide stale or missing data).
- Do **not** comment on formatting, import order or line length — ruff (`ruff check` + `ruff format`, line length
  88, rule sets E, F, W, I, UP, ASYNC, FURB, PERF, PIE, PLE, PLW, RET, RSE) enforces those in CI.

## Project rules to check

- **No framework coupling:** nothing may import `homeassistant.*`. No verbatim code from `truenas/api_client` or the
  TrueNAS middleware (LGPL-3.0) — protocol understanding only.
- **Complexity:** cognitive complexity ≤ 15 per function (SonarQube S3776); no duplicated string literals — extract
  constants (S1192).
- **Error contract per layer:** `TrueNASClient` and `TrueNASSystem` raise typed `TrueNASError` subclasses and never
  fall back to previous values. `TrueNASState` may fall back to its cached snapshot, but every new fallback must be
  reflected in its stale signal (`stale_endpoints`, `systemstats_stale_graphs`, `ups_stale_graphs`) or the README
  must say why not. Flag a fallback that changes neither.
- **Tolerances match verified error shapes:** an error that is deliberately tolerated must be matched as narrowly as
  the evidence allows (e.g. `TrueNASCallError` with a specific `errname`, as for an unknown `cputemp` graph:
  `ENOENT`), never a whole exception class. The evidence (live run or TrueNAS source, with version) belongs in the
  PR. Flag a broad `except`/`isinstance` tolerance without it.
- **Cancellation:** `asyncio.CancelledError` (and any non-`TrueNASError` `BaseException` from
  `gather(..., return_exceptions=True)`) is re-raised, never swallowed or converted. Cleanup of state a call
  registers for itself (its `_pending_calls` entry) happens in `finally`, so a cancel in any phase releases it.
  Caller-owned state is not per-call: a cancelled read from a subscription must not remove that subscription —
  it stays until `unsubscribe()` or connection teardown.
- **Shared connection:** one `asyncio.Lock` serializes sends and guards the shared state (`_pending_calls`,
  `_subscriptions`, connection close); the background `_read_loop` receives without holding it. Flag a change
  that holds the lock across `recv()` or across waiting for a response — that blocks every other call. A failure
  or cancel while holding it must not leave the lock held or the connection unusable for the next call.
- **Docs follow behaviour:** a change to what a public method raises, tolerates or returns updates its docstring,
  the README contract text and `CHANGELOG.md` (`[Unreleased]`) in the same PR.

## Tests

- **A test must fail without the fix (mutation probe):** for every bug fix, state in the PR which line was reverted
  or replaced and which test failed. A tolerance is pinned from both sides: the tolerated case passes and a
  near-miss (other `errname`, missing `data`, a non-optional graph) still raises.
- **Cancel and timeout tests prove the phase:** a test for cancellation after the request was sent must wait for
  evidence that the request reached the fake server (e.g. `FakeTrueNASServer.request_seen`), not for a sleep; a
  test for cancellation during send must actually block in `send`. Flag sleep-based timing and tests that would
  also pass if the cancel landed in a different phase.
- **Fake responses match the real server:** fake-server payloads for a new or changed RPC follow the shape TrueNAS
  actually sends (live run or middleware source), including the "no data" shapes (e.g. a netdata graph item with
  `legend: ["time"]` and empty aggregations), not just the happy path.
- **One possibly-raising call per `pytest.raises` block** (SonarQube S5778).
