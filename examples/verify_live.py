"""One-off live verification: exercise TrueNASClient against a real TrueNAS instance.

Not part of the library's public surface and not run by CI. Reads connection
details from the environment so it can never accidentally run (or leak
secrets) in CI or another developer's shell:

    TRUENAS_HOST         required, bare hostname/IP (may include ":port")
    TRUENAS_API_KEY      required
    TRUENAS_VERIFY_SSL   optional, "true"/"false" (default: true)

Usage:
    TRUENAS_HOST=truenas.local TRUENAS_API_KEY=... python examples/verify_live.py
    python examples/verify_live.py --no-verify-ssl
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

from aiotruenas import TrueNASClient
from aiotruenas.exceptions import TrueNASError

#: Read-only methods from PROMPT.md's RPC method list, cheap enough to call
#: unconditionally against any TrueNAS instance.
_METHODS = ["system.info", "pool.query", "disk.query", "alert.list"]


def _env_flag(name: str, *, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    verify_group = parser.add_mutually_exclusive_group()
    verify_group.add_argument(
        "--verify-ssl", dest="verify_ssl", action="store_true", default=None
    )
    verify_group.add_argument(
        "--no-verify-ssl", dest="verify_ssl", action="store_false"
    )
    return parser.parse_args(argv)


def _summarize(result: object) -> str:
    if isinstance(result, list):
        return f"{len(result)} entries"
    if isinstance(result, dict):
        keys = list(result.keys())[:8]
        return f"dict with {len(result)} keys, e.g. {keys}"
    return repr(result)


async def _run(host: str, api_key: str, *, verify_ssl: bool) -> None:
    async with TrueNASClient(host, api_key, verify_ssl=verify_ssl) as client:
        print(f"connected and logged in to {host!r} (verify_ssl={verify_ssl})")
        for method in _METHODS:
            result = await client.call(method)
            print(f"  {method}: OK -> {_summarize(result)}")


def main(argv: list[str]) -> int:
    args = _parse_args(argv)

    host = os.environ.get("TRUENAS_HOST")
    api_key = os.environ.get("TRUENAS_API_KEY")
    if not host or not api_key:
        print(
            "TRUENAS_HOST and/or TRUENAS_API_KEY not set; skipping live "
            "verification (this is expected in CI and for developers without "
            "a real TrueNAS instance)."
        )
        return 0

    verify_ssl = (
        _env_flag("TRUENAS_VERIFY_SSL", default=True)
        if args.verify_ssl is None
        else args.verify_ssl
    )

    try:
        asyncio.run(_run(host, api_key, verify_ssl=verify_ssl))
    except TrueNASError as exc:
        print(f"FAILED with {type(exc).__name__}: {exc}", file=sys.stderr)
        raise

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
