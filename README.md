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

## Status

Early development (v1: generic call surface only, no typed per-domain convenience methods yet).
See [PROMPT.md](PROMPT.md) for the full design brief and [CLAUDE.md](CLAUDE.md) for repo guidance.

## License

Apache-2.0, see [LICENSE](LICENSE).
