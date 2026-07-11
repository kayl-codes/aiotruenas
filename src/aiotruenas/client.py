"""Asyncio-native JSON-RPC 2.0 WebSocket client for TrueNAS."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import ssl
from typing import Any, Self

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, WebSocketException

from ._errors import build_call_error, classify_connect_exception
from .exceptions import (
    TrueNASAuthenticationError,
    TrueNASCallTimeoutError,
    TrueNASConnectionClosedError,
    TrueNASConnectionError,
    TrueNASMalformedResponseError,
)

_LOGGER = logging.getLogger(__name__)

#: Matches ``QUERY_TIMEOUT`` in the synchronous HA-side client.
DEFAULT_QUERY_TIMEOUT = 30.0

#: TrueNAS may briefly hold a connection slot open after a clean disconnect
#: (e.g. during an integration reload); one retry after this delay reliably
#: lets it finish internal cleanup before accepting a new connection.
_HANDSHAKE_RETRY_DELAY = 5.0

_OPEN_TIMEOUT = 10.0
_PING_INTERVAL = 20.0
_MAX_MESSAGE_SIZE = 16 * 1024 * 1024

_JOB_TERMINAL_STATES = {"SUCCESS", "FAILED", "ABORTED"}
_JOB_POLL_INTERVAL = 1.0
#: How many consecutive empty core.get_jobs lookups to tolerate before
#: concluding the job id will never appear (e.g. a stale/pruned id) and
#: raising instead of polling forever.
_JOB_MISSING_RETRY_LIMIT = 5

_JSONRPC_VERSION = "2.0"
_KEY_ERROR = "error"
_KEY_RESULT = "result"


def _normalize_params(params: list | dict | None) -> list | dict:
    """Match the sync client's ``query()`` normalization: bare values wrap in a list."""
    if params is None:
        return []
    if isinstance(params, list):
        return params
    return [params]


class TrueNASClient:
    """A connection to a single TrueNAS instance's JSON-RPC 2.0 WebSocket API."""

    def __init__(
        self,
        host: str,
        api_key: str,
        *,
        verify_ssl: bool = True,
        use_tls: bool = True,
        port: int | None = None,
        query_timeout: float = DEFAULT_QUERY_TIMEOUT,
    ) -> None:
        """Initialize the client (no I/O happens until :meth:`connect`).

        ``host`` must be a bare hostname or IP address, without scheme or
        path (e.g. ``"truenas.local"``), to avoid building a malformed
        WebSocket URL.
        """
        if "://" in host or "/" in host:
            raise ValueError(
                "Invalid host value. Expected a bare hostname or IP address "
                'without scheme or path (for example, "truenas.local" or "192.168.1.1")'
            )

        self._host = host
        self._api_key = api_key
        self._verify_ssl = verify_ssl
        self._use_tls = use_tls
        self._query_timeout = query_timeout

        scheme = "wss" if use_tls else "ws"
        netloc = host if port is None else f"{host}:{port}"
        self._url = f"{scheme}://{netloc}/api/current"

        self._ssl_context: ssl.SSLContext | None = None
        self._ws: ClientConnection | None = None
        self._lock = asyncio.Lock()
        self._next_id_value = 1

    @property
    def connected(self) -> bool:
        """Whether a WebSocket connection is currently established and logged in."""
        return self._ws is not None

    def _next_id(self) -> int:
        rpc_id = self._next_id_value
        self._next_id_value += 1
        return rpc_id

    def _build_ssl_context(self) -> ssl.SSLContext:
        context = ssl.create_default_context()  # noqa: S4423
        if not self._verify_ssl:
            # Insecure configuration, opt-in only: disables certificate and
            # hostname verification. Only safe on trusted (e.g. local) networks
            # -- vulnerable to MITM otherwise.
            _LOGGER.warning(
                "TrueNASClient configured with verify_ssl=False for '%s'. "
                "This disables TLS certificate verification and hostname "
                "checking and should only be used in trusted environments.",
                self._host,
            )
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        return context

    async def _get_ssl_context(self) -> ssl.SSLContext:
        if self._ssl_context is None:
            # ssl.create_default_context() loads system certs (blocking).
            self._ssl_context = await asyncio.to_thread(self._build_ssl_context)
        return self._ssl_context

    async def _open_websocket(self) -> ClientConnection:
        """Open the WebSocket, retrying once after a delay on handshake timeout."""
        kwargs: dict[str, Any] = {
            "max_size": _MAX_MESSAGE_SIZE,
            "ping_interval": _PING_INTERVAL,
            "open_timeout": _OPEN_TIMEOUT,
        }
        if self._use_tls:
            kwargs["ssl"] = await self._get_ssl_context()

        for attempt in range(2):
            try:
                return await connect(self._url, **kwargs)
            except TimeoutError as exc:
                if attempt == 0:
                    _LOGGER.debug(
                        "TrueNAS %s: handshake timed out on first attempt; "
                        "retrying in %.0fs",
                        self._host,
                        _HANDSHAKE_RETRY_DELAY,
                    )
                    await asyncio.sleep(_HANDSHAKE_RETRY_DELAY)
                    continue
                raise classify_connect_exception(exc) from exc
            except (OSError, WebSocketException) as exc:
                raise classify_connect_exception(exc) from exc

        raise AssertionError("unreachable")  # pragma: no cover

    async def _await_response(
        self, ws: ClientConnection, rpc_id: int
    ) -> dict[str, Any]:
        """Wait for the JSON-RPC response matching ``rpc_id``.

        Any message with a different (or missing) ``id`` is an unsolicited
        server notification/event and is silently skipped. The caller is
        expected to bound this with ``asyncio.timeout()``.
        """
        while True:
            message = await ws.recv()
            if not isinstance(message, str):
                continue
            try:
                candidate = json.loads(message)
            except json.JSONDecodeError:
                continue
            if isinstance(candidate, dict) and candidate.get("id") == rpc_id:
                return candidate

    async def _login(self, ws: ClientConnection) -> None:
        rpc_id = self._next_id()
        payload = {
            "jsonrpc": _JSONRPC_VERSION,
            "id": rpc_id,
            "method": "auth.login_with_api_key",
            "params": [self._api_key],
        }
        try:
            await ws.send(json.dumps(payload))
            async with asyncio.timeout(self._query_timeout):
                response = await self._await_response(ws, rpc_id)
        except TimeoutError as exc:
            raise TrueNASCallTimeoutError(
                "timed out while waiting for login response"
            ) from exc
        except (ConnectionClosed, OSError, WebSocketException) as exc:
            raise TrueNASConnectionClosedError(str(exc), phase="login") from exc

        if response.get(_KEY_ERROR):
            raise build_call_error(response[_KEY_ERROR])

        if response.get(_KEY_RESULT) is not True:
            raise TrueNASAuthenticationError("TrueNAS rejected the API key")

    async def connect(self) -> None:
        """Open the WebSocket connection and log in.

        Raises a subclass of :class:`~aiotruenas.exceptions.TrueNASError` on
        any failure. Idempotent: does nothing if already connected. Serialized
        against :meth:`call` and :meth:`close` via the same lock, so
        concurrent ``connect()`` calls cannot race and clobber ``self._ws``.
        """
        async with self._lock:
            if self._ws is not None:
                return

            ws = await self._open_websocket()
            try:
                await self._login(ws)
            except Exception:
                with contextlib.suppress(WebSocketException, OSError):
                    await ws.close()
                raise

            self._ws = ws

    async def close(self) -> None:
        """Close the WebSocket connection, if any. Safe to call repeatedly."""
        async with self._lock:
            await self._disconnect_locked()

    async def _disconnect_locked(self) -> None:
        """Close ``self._ws``. Caller must already hold ``self._lock``."""
        ws = self._ws
        self._ws = None
        if ws is not None:
            with contextlib.suppress(WebSocketException, OSError):
                await ws.close()

    async def call(
        self,
        method: str,
        params: list | dict | None = None,
        *,
        timeout: float | None = None,  # noqa: ASYNC109 -- deliberate public API (PROMPT.md)
        job: bool = False,
    ) -> Any:
        """Call a TrueNAS JSON-RPC method and return its ``result``.

        ``params`` may be a list (passed through as-is), a dict, or a bare
        value (wrapped in a single-element list) to match TrueNAS's
        positional-params convention.

        If ``job=True``, the result is treated as a job id and polled via
        ``core.get_jobs`` until it reaches a terminal state; the job's own
        result (or error) is returned/raised instead.
        """
        effective_timeout = self._query_timeout if timeout is None else timeout

        async with self._lock:
            if self._ws is None:
                raise TrueNASConnectionError("not connected; call connect() first")

            rpc_id = self._next_id()
            payload = {
                "jsonrpc": _JSONRPC_VERSION,
                "id": rpc_id,
                "method": method,
                "params": _normalize_params(params),
            }
            try:
                await self._ws.send(json.dumps(payload))
                async with asyncio.timeout(effective_timeout):
                    response = await self._await_response(self._ws, rpc_id)
            except TimeoutError as exc:
                await self._disconnect_locked()
                raise TrueNASCallTimeoutError(
                    f"timed out while waiting for response to {method!r}"
                ) from exc
            except (ConnectionClosed, OSError, WebSocketException) as exc:
                await self._disconnect_locked()
                raise TrueNASConnectionClosedError(str(exc), phase="call") from exc

        if response.get(_KEY_ERROR):
            raise build_call_error(response[_KEY_ERROR])
        if _KEY_RESULT not in response:
            raise TrueNASMalformedResponseError(
                f"response for {method!r} has no result"
            )

        result = response[_KEY_RESULT]
        if job:
            return await self._wait_for_job(result)
        return result

    async def _wait_for_job(self, job_id: Any) -> Any:
        if not isinstance(job_id, int) or isinstance(job_id, bool):
            raise TrueNASMalformedResponseError(
                f"job=True expected an integer job id, got {job_id!r}"
            )

        missing_count = 0
        while True:
            jobs = await self.call("core.get_jobs", [[["id", "=", job_id]]])
            if jobs:
                missing_count = 0
                state = jobs[0].get("state")
                if state in _JOB_TERMINAL_STATES:
                    return self._resolve_job_result(job_id, jobs[0], state)
            else:
                missing_count += 1
                if missing_count >= _JOB_MISSING_RETRY_LIMIT:
                    raise TrueNASMalformedResponseError(
                        f"job {job_id} did not appear in core.get_jobs after "
                        f"{missing_count} attempts"
                    )
            await asyncio.sleep(_JOB_POLL_INTERVAL)

    @staticmethod
    def _resolve_job_result(job_id: int, job: dict[str, Any], state: str) -> Any:
        if state == "SUCCESS":
            return job.get(_KEY_RESULT)
        message = job.get(_KEY_ERROR) or f"job {job_id} ended with state {state}"
        raise build_call_error({"message": message})

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()
