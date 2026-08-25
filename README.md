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
`get_rsync()`, `get_snapshottask()`, `get_cronjob()`, `get_arc()`, `get_ups()`. More endpoints are
being migrated incrementally from consumer integrations' own normalization code — see
[MIGRATION_PLAN.md](MIGRATION_PLAN.md) for the full list and status.

## Status

Early development. Generic `call()` RPC surface plus a growing set of normalized `TrueNASState`
endpoints (no typed per-domain convenience methods elsewhere yet). See [PROMPT.md](PROMPT.md) for
the full design brief and [CLAUDE.md](CLAUDE.md) for repo guidance.

## License

Apache-2.0, see [LICENSE](LICENSE).
