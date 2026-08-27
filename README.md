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
returning its own endpoint.

## Status

Early development. Generic `call()` RPC surface plus a growing set of normalized `TrueNASState`
endpoints (no typed per-domain convenience methods elsewhere yet). See [PROMPT.md](PROMPT.md) for
the full design brief and [CLAUDE.md](CLAUDE.md) for repo guidance.

## License

Apache-2.0, see [LICENSE](LICENSE).
