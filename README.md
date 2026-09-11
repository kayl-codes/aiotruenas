# aiotruenas

Asyncio-native Python client for the TrueNAS **JSON-RPC 2.0** WebSocket API
(`ws(s)://<host>/api/current`, TrueNAS 25.04+).

No dependency on Home Assistant or any other framework — usable as a standalone library.

## Installation

```bash
pip install .
```

## Usage

```python
import asyncio

from aiotruenas import TrueNASClient


async def main() -> None:
    async with TrueNASClient("truenas.local", "1-abcdef...") as client:
        info = await client.call("system.info")
        print(info)

        sub_id, _ = await client.subscribe("app.stats")
        try:
            events = await client.get_subscription_events(sub_id, event_timeout=5.0)
            print(events)
        finally:
            await client.unsubscribe(sub_id)


asyncio.run(main())
```

`call()` is a generic RPC surface — pass any TrueNAS JSON-RPC method name and its `params`
(list or dict). Long-running operations that return a job id (scrub, replication, dataset
lock/unlock, ...) can be polled automatically with `job=True`:

```python
await client.call("pool.scrub.scrub", ["tank", "START"], job=True)
```

Connection and protocol failures are raised as typed exceptions (see `aiotruenas.exceptions`)
rather than returned as an error code/string, so callers can `except TrueNASAuthenticationError`,
`except TrueNASConnectionError`, etc.

### Normalized domain state

`TrueNASState` wraps a `TrueNASClient` and exposes one `async def get_<endpoint>()` method per
supported TrueNAS RPC endpoint. Each method queries the endpoint, normalizes the raw payload into
a stable dict-keyed-by-id shape, and caches the result on `state.ds[<endpoint>]`:

```python
from aiotruenas import TrueNASClient, TrueNASState

async with TrueNASClient("truenas.local", "1-abcdef...") as client:
    state = TrueNASState(client)
    pools = await state.get_pool()
    print(pools)
    print(state.ds["pool"])  # same data, cached
```

Supported endpoints so far: `get_pool()`, `get_dataset()`, `get_cloudsync()`, `get_replication()`,
`get_rsync()`, `get_snapshottask()`, `get_cronjob()`, `get_arc()`, `get_ups()`, `get_service()`,
`get_vm()`, `get_container()`, `get_app()`, `get_certificates()`, `get_directoryservices()`,
`get_alerts()`, `get_interface()`, `get_disk()`, `get_scrub()`, `get_smb()`, `get_update()`,
`get_systeminfo()`, `get_systemstats()`. This covers the full endpoint set migrated from consumer
integrations' own normalization code — see [MIGRATION_PLAN.md](MIGRATION_PLAN.md) for details and
history.

`get_arc()`/`get_ups()`/`get_alerts()`/`get_smb()`/`get_update()`/`get_systeminfo()` are the
exception to the dict-keyed-by-id shape above: they have no natural object id and instead return a
flat dict of scalar/aggregate readings (e.g. `{"battery_charge": 80.0, ...}`,
`{"count": 2, "critical": 1, ...}` for alerts, or `{"connections": 3}` for `get_smb()`).
`get_systemstats()` enriches that same flat `ds["system_info"]` dict (CPU/load/memory/ARC-size)
and, as a side effect, `ds["interface"][id]["rx"/"tx"]` (interface throughput) rather than
returning its own endpoint. Its underlying netdata graph queries are best-effort: a failed graph
leaves its field(s) at the previous value instead of failing the whole call. Check
`state.systemstats_stale_graphs` after calling it to see which graphs (if any) failed on the most
recent refresh, so a caller can distinguish a partially stale result from a fully fresh one.
`get_ups()`'s per-graph netdata queries follow the same best-effort/partial-staleness contract —
check `state.ups_stale_graphs` after calling it. One difference: a UPS graph that has never once
produced a reading has no field in the result at all, so a failure on it is not reflected in
`ups_stale_graphs` either — there's no previous value to call stale. An empty
`ups_stale_graphs` therefore means "no field is outdated", not "nothing failed", unlike
`systemstats_stale_graphs`, whose fields are always present from the start.

Several other `get_*` methods return the previous cached snapshot (logging once) instead of raising
when their *primary* RPC result cannot be freshly fetched: `get_smb()` and `get_ups()` swallow an
actual RPC error this way, and `get_smb()`, `get_pool()`, `get_directoryservices()`, `get_alerts()`,
`get_systeminfo()`, `get_ups()`, `get_dataset()`, `get_interface()`, `get_scrub()`, `get_service()`,
`get_vm()` all swallow a malformed/unusable payload — `get_systeminfo()` treats a structurally-empty
`{}` `system.info` response as malformed too, not just a non-dict one (like `get_smb()` also rejects
`{}` on its own flat-dict shape, though as a side effect of requiring a `sessions` list rather than
an explicit emptiness check). A non-empty response missing individual fields is not malformed —
those fields are instead reset to their own normalized defaults (e.g. `"unknown"`/`0`), not carried
over from the previous snapshot the way a malformed response's fields are. `get_directoryservices()`
and `get_ups()` are narrower still: on a partial failure (the status call, or a single graph) they
still refresh every other field from the fresh response and only carry over the one piece that
failed, rather than leaving the whole cached snapshot untouched. `_add_boot_pool()` (part of
`get_pool()`) follows the same carry-over pattern for a malformed/empty `boot.get_state()` response,
when a previously cached boot-pool entry actually exists to carry over into `ds["pool"]` instead of
silently dropping it; a response with nothing to carry over (`boot.get_state` has never once
succeeded) is not reported as stale — there is no previous value to call stale, and permanently
pinning `"pool"` for a `boot.get_state` call that never once works would falsely mark every pool
entity unavailable even while `pool.query` itself stays fresh.
`state.stale_endpoints` is a `frozenset` of the `ds` endpoint names currently in that
fell-back-to-cache state; the reachable names are `"pool"`, `"dataset"`, `"ups"`, `"system_info"`,
`"directoryservices"`, `"alerts"`, `"smb"`, `"interface"`, `"scrub"`, `"service"`, `"vm"`. It is the
coarse counterpart to the per-graph `*_stale_graphs` sets, meant for a consumer that marks an
endpoint's entities unavailable when its data source stops being freshly reachable. It is a one-way
signal — a name in the set really is stale, but a primary `get_*` that fails by *raising* is not
listed (the caller sees that itself). `"dataset"` has one known exception to that one-way
guarantee: a malformed `pool.query` response always (re-)flags `"dataset"` too, even if a direct
`get_dataset()` call already republished a fully fresh dataset map earlier in the very same refresh
cycle — a consumer that calls both every cycle (e.g. a coordinator's poll loop) would see
`"dataset"` reported stale for the whole `pool.query` outage regardless, even though the data it
points at is in fact current. This is a deliberate over-report, not a fixed defect: distinguishing
the two cases would need its own cross-call tracking for a signal no current consumer reads yet.

`"pool"` also covers a field-level dependency, not just `pool.query` itself: pool capacity
(available/total/usage/size/allocated) is derived from the pool's root dataset, so when
`pool.dataset.query` falls back to cached data, `"pool"` is reported stale too for any pool that
actually has a root dataset to depend on — even if `pool.query` refreshed cleanly. A boot-pool-only
refresh (no root dataset at all) never triggers this.

Only a stale *primary* result (or, for `"pool"`, the dataset dependency above) counts. Best-effort
enrichment on top of a fresh primary result is deliberately excluded — disk temperatures (`"disk"`
never appears; `disk.query` raises on a real failure, which the caller already sees), interface
*throughput* specifically (`get_systemstats()`'s netdata enrichment of `rx`/`tx` onto an
already-fresh `ds["interface"]` — distinct from `interface.query` itself, which now *is* covered),
the CPU/load/memory/ARC-size netdata graphs (they only enrich an already-fresh `ds["system_info"]`),
a single failing `get_ups()` per-graph query while discovery still succeeds, and the internal
TrueNAS-version / virtualization detection. Mapping those to an endpoint would pin it — and every
entity derived from it — to a permanent stale state on hardware that simply never reports the
optional metric (a disk with no temperature sensor, a dmidecode-less container). Their staleness
stays visible through `systemstats_stale_graphs` / `ups_stale_graphs` only. `"ups"` is still
reported when the graph *discovery* call fails outright or when `ups_stale_graphs` is non-empty
(that set is self-clearing, so it can't stick). `"arc"` is likewise absent: `get_arc()` raises on a
failed graph query (and writes `None`, not a stale value, for a malformed-but-non-raising one), so
there is no silent fallback to report.

## Status

Early development. Generic `call()` RPC surface plus a growing set of normalized `TrueNASState`
endpoints (no typed per-domain convenience methods elsewhere yet). See [PROMPT.md](PROMPT.md) for
the full design brief and [CLAUDE.md](CLAUDE.md) for repo guidance.

## License

Apache-2.0, see [LICENSE](LICENSE).
