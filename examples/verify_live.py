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

Calls every read-only method from PROMPT.md's "must work end-to-end" list. A
method failing (e.g. `directoryservices.*` with no AD/LDAP configured, or
`virt.instance.query` on a system still on the older `vm.query` API) is
reported by name but does not stop the remaining methods from running — only
a real connection/login failure aborts the script.

`job=True` (the `core.get_jobs` polling convenience) is deliberately NOT
exercised here: it only makes sense on a call whose result is a freshly
created job id (an action call like `pool.scrub.scrub(name, "START")`).
Triggering a write/action job just to test polling is out of scope for a
read-only spike, so this remains an untested gap against a real instance —
see `_check_job_polling` below.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

from aiotruenas import TrueNASClient
from aiotruenas.exceptions import TrueNASError

#: Read-only methods from PROMPT.md's "must work end-to-end" RPC list (the
#: coordinator's integration-test method list), in that order. Some are
#: expected to legitimately fail or return empty on a system without the
#: relevant feature configured (e.g. `directoryservices.*` with no AD/LDAP,
#: `virt.instance.query` if the older `vm.query` API is in use) — see
#: `_run()`, which surfaces per-method failures without aborting the rest.
_METHODS = [
    "system.info",
    "interface.query",
    "update.status",
    "service.query",
    "pool.query",
    "boot.get_state",
    "pool.dataset.query",
    "disk.query",
    "vm.query",
    "virt.instance.query",
    "directoryservices.config",
    "directoryservices.status",
    "alert.list",
    "certificate.query",
    "smb.status",
    "cloudsync.query",
    "replication.query",
    "rsynctask.query",
    "pool.snapshottask.query",
    "pool.scrub.query",
    "app.query",
    "cronjob.query",
]


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


async def _check_subscriptions(client: TrueNASClient) -> None:
    """Subscribe to ``app.stats`` and drain any queued events."""
    try:
        sub_id, queue = await client.subscribe("app.stats")
    except TrueNASError as exc:
        print(f"  subscribe(app.stats): FAILED ({type(exc).__name__}): {exc}")
        return
    try:
        events = await client.get_subscription_events(sub_id, event_timeout=2.0)
        print(f"  subscribe(app.stats): OK -> {len(events)} event(s) received")
    except TrueNASError as exc:
        print(f"  get_subscription_events: FAILED ({type(exc).__name__}): {exc}")
    finally:
        await client.unsubscribe(sub_id)


async def _check_job_polling(client: TrueNASClient) -> None:
    """Passively inspect recent jobs via `core.get_jobs`.

    Does not exercise `job=True` itself: that requires a call whose *result*
    is a fresh job id, which only real action/trigger calls produce. No
    already-completed job can be "replayed" through `job=True` without
    triggering a new one, so this is a documented gap, not a test.
    """
    try:
        jobs = await client.call(
            "core.get_jobs", [[], {"order_by": ["-id"], "limit": 5}]
        )
    except TrueNASError as exc:
        print(f"  core.get_jobs: FAILED ({type(exc).__name__}): {exc}")
        return
    print(f"  core.get_jobs: OK -> {_summarize(jobs)}")
    if jobs:
        latest = jobs[0]
        print(
            f"    most recent job: id={latest.get('id')} "
            f"method={latest.get('method')} state={latest.get('state')}"
        )
    print(
        "  job=True remains UNTESTED against a real instance: it needs a "
        "fresh job id from an action call (e.g. pool.scrub.scrub(..., "
        "'START')), and triggering one just to test polling is out of scope "
        "for this read-only spike."
    )


async def _run(host: str, api_key: str, *, verify_ssl: bool) -> None:
    async with TrueNASClient(host, api_key, verify_ssl=verify_ssl) as client:
        print(f"connected and logged in to {host!r} (verify_ssl={verify_ssl})")
        failed = []
        for method in _METHODS:
            try:
                result = await client.call(method)
            except TrueNASError as exc:
                failed.append(method)
                print(f"  {method}: FAILED ({type(exc).__name__}): {exc}")
            else:
                print(f"  {method}: OK -> {_summarize(result)}")

        await _check_job_polling(client)
        await _check_subscriptions(client)

        if failed:
            print(
                f"\n{len(failed)}/{len(_METHODS)} methods failed: {', '.join(failed)}"
            )
        else:
            print(f"\nall {len(_METHODS)} methods OK")


def main(argv: list[str]) -> None:
    args = _parse_args(argv)

    host = os.environ.get("TRUENAS_HOST")
    api_key = os.environ.get("TRUENAS_API_KEY")
    if not host or not api_key:
        print(
            "TRUENAS_HOST and/or TRUENAS_API_KEY not set; skipping live "
            "verification (this is expected in CI and for developers without "
            "a real TrueNAS instance)."
        )
        return

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


if __name__ == "__main__":
    main(sys.argv[1:])
