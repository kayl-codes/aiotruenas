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
from websockets.protocol import State

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
_CLOSED_BY_CLIENT = "connection closed by the client"


class _SubscriptionTerminator:
    """Sentinel used to signal teardown/unsubscribe on subscription queues."""

    __slots__ = ()


#: Internal singleton sentinel injected into subscription queues on teardown
#: or ``unsubscribe``. Tests and internals may reference this directly; it is
#: intentionally not exported as part of the public client API.
SUBSCRIPTION_TERMINATOR = _SubscriptionTerminator()

_SubscriptionMessage = dict[str, Any] | _SubscriptionTerminator


def _normalize_params(params: list | dict | None) -> list | dict:
    """Match the sync client's ``query()`` normalization: bare values wrap in a list."""
    if params is None:
        return []
    return params if isinstance(params, list) else [params]


class TrueNASClient:
    """A connection to a single TrueNAS instance's JSON-RPC 2.0 WebSocket API."""

    #: Backwards-compatible alias for :data:`SUBSCRIPTION_TERMINATOR`. External
    #: code should treat this as an internal constant; it may be removed in a
    #: future release.
    _QUEUE_TERMINATOR = SUBSCRIPTION_TERMINATOR

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
        self._pending_calls: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self._subscriptions: dict[str, asyncio.Queue[_SubscriptionMessage]] = {}
        self._subscription_events: dict[str, str] = {}
        self._reader_task: asyncio.Task[None] | None = None

    @property
    def connected(self) -> bool:
        """Whether a WebSocket connection is currently established and logged in."""
        return self._ws is not None

    def _next_id(self) -> int:
        rpc_id = self._next_id_value
        self._next_id_value += 1
        return rpc_id

    def _build_ssl_context(self, *, quiet: bool = False) -> ssl.SSLContext:
        context = ssl.create_default_context()  # noqa: S4423
        if not self._verify_ssl:
            # Insecure configuration, opt-in only: disables certificate and
            # hostname verification. Only safe on trusted (e.g. local) networks
            # -- vulnerable to MITM otherwise.
            log = _LOGGER.debug if quiet else _LOGGER.warning
            log(
                "TrueNASClient configured with verify_ssl=False for '%s'. "
                "This disables TLS certificate verification and hostname "
                "checking and should only be used in trusted environments.",
                self._host,
            )
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        return context

    async def _get_ssl_context(self, *, quiet: bool = False) -> ssl.SSLContext:
        if self._ssl_context is None:
            # ssl.create_default_context() loads system certs (blocking).
            self._ssl_context = await asyncio.to_thread(
                self._build_ssl_context, quiet=quiet
            )
        return self._ssl_context

    async def _open_websocket(self, *, quiet: bool = False) -> ClientConnection:
        """Open the WebSocket, retrying once after a delay on handshake timeout."""
        kwargs: dict[str, Any] = {
            "max_size": _MAX_MESSAGE_SIZE,
            "ping_interval": _PING_INTERVAL,
            "open_timeout": _OPEN_TIMEOUT,
        }
        if self._use_tls:
            kwargs["ssl"] = await self._get_ssl_context(quiet=quiet)

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
        raise AssertionError("unreachable: the last attempt returns or raises")

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

    async def _read_loop(self, ws: ClientConnection) -> None:
        """Background reader that routes the messages of ``ws``."""
        disconnect_cause: BaseException | None = None
        try:
            while True:
                message, cause = await self._receive_message(ws)
                if message is None:
                    disconnect_cause = cause
                    break

                candidate = self._parse_message(message)
                if candidate is None:
                    continue

                await self._route_message(ws, candidate)
        except (ConnectionClosed, OSError, WebSocketException):
            pass
        except Exception as exc:
            _LOGGER.exception("Unexpected error in websocket read loop")
            disconnect_cause = exc
        finally:
            await self._teardown_connection(ws, cause=disconnect_cause)

    @staticmethod
    async def _receive_message(
        ws: ClientConnection,
    ) -> tuple[str | None, BaseException | None]:
        """Receive one raw websocket message.

        Returns (message, None) on success or (None, cause) on any transport
        or unexpected error.
        """
        try:
            return await ws.recv(), None
        except (OSError, WebSocketException) as exc:
            reason = getattr(getattr(exc, "code", None), "name", None) or getattr(
                exc, "reason", None
            )
            _LOGGER.warning(
                "TrueNAS websocket connection closed (%s): %s",
                exc.__class__.__name__,
                reason or str(exc) or "no reason provided",
            )
            return None, exc
        except Exception as exc:  # noqa: BLE001
            _LOGGER.exception(
                "Unexpected error receiving message from TrueNAS websocket"
            )
            return None, exc

    @staticmethod
    def _parse_message(message: str) -> dict[str, Any] | None:
        """Decode a raw websocket message into a JSON dict, or None on failure."""
        try:
            candidate = json.loads(message)
        except json.JSONDecodeError:
            _LOGGER.exception(
                "Failed to decode JSON message from TrueNAS websocket: %r",
                message,
            )
            return None
        return candidate if isinstance(candidate, dict) else None

    async def _route_message(
        self, ws: ClientConnection, candidate: dict[str, Any]
    ) -> None:
        """Route a parsed JSON-RPC message of ``ws`` to its destination."""
        msg_id = candidate.get("id")
        params = candidate.get("params")

        if isinstance(msg_id, int):
            # RPC ids never repeat across connections: a late reply from an
            # old one finds no pending call.
            await self._handle_rpc_response(msg_id, candidate)
            return

        if isinstance(msg_id, str):
            await self._handle_subscription_notification(ws, msg_id, candidate)
            return

        await self._handle_collection_notification(ws, candidate, params)

    async def _handle_rpc_response(
        self, msg_id: int, candidate: dict[str, Any]
    ) -> None:
        """Complete a pending RPC call with the response payload."""
        async with self._lock:
            future = self._pending_calls.pop(msg_id, None)
        if future is not None and not future.done():
            future.set_result(candidate)

    async def _handle_subscription_notification(
        self, ws: ClientConnection, msg_id: str, candidate: dict[str, Any]
    ) -> None:
        """Push a subscription-specific notification into its queue."""
        async with self._lock:
            # Subscription ids come from the server and may repeat on a newer
            # connection, so only the current connection may deliver.
            if self._ws is not ws:
                return
            queue = self._subscriptions.get(msg_id)
        if queue is not None:
            await queue.put(candidate)

    async def _handle_collection_notification(
        self, ws: ClientConnection, candidate: dict[str, Any], params: Any
    ) -> None:
        """Route a collection-based notification to matching subscription queues."""
        notified_queues: set[asyncio.Queue[_SubscriptionMessage]] = set()

        for queue in await self._get_matching_queues(ws, candidate.get("collection")):
            notified_queues.add(queue)
            await queue.put(candidate)

        if candidate.get("method") == "collection_update" and isinstance(params, dict):
            for queue in await self._get_matching_queues(ws, params.get("collection")):
                if queue not in notified_queues:
                    notified_queues.add(queue)
                    await queue.put(candidate)

    async def _teardown_connection(
        self, ws: ClientConnection, cause: BaseException | None = None
    ) -> None:
        """Tear down the state of ``ws`` after the reader lost it.

        Does nothing if ``ws`` is no longer the current connection: close()
        already detached it, and a newer connection must stay untouched.
        """
        async with self._lock:
            if self._ws is not ws:
                return
            message = str(cause) if cause is not None else "connection lost"
            self._detach_connection_locked(message)

    def _detach_connection_locked(self, message: str) -> asyncio.Task[None] | None:
        """Clear the current connection's state and return its reader task.

        Fails every pending call with ``message`` and ends every subscription.
        Caller must already hold ``self._lock``.
        """
        self._ws = None
        reader_task = self._reader_task
        self._reader_task = None

        for future in self._pending_calls.values():
            if not future.done():
                future.set_exception(
                    TrueNASConnectionClosedError(message, phase="disconnect")
                )
        self._pending_calls.clear()

        for queue in self._subscriptions.values():
            queue.put_nowait(self._QUEUE_TERMINATOR)
        self._subscriptions.clear()
        self._subscription_events.clear()
        return reader_task

    @staticmethod
    def _matches_subscription(collection: str, event_name: str) -> bool:
        """Return True if a notification collection matches a subscription."""
        if collection == event_name:
            return True
        return ":" in collection and collection.split(":", 1)[0] == event_name

    async def _get_matching_queues(
        self, ws: ClientConnection, collection: str | None
    ) -> list[asyncio.Queue[_SubscriptionMessage]]:
        """Return the queues of ``ws``'s subscriptions matching ``collection``."""
        if not isinstance(collection, str):
            return []
        async with self._lock:
            if self._ws is not ws:
                return []
            return [
                self._subscriptions[sub_id]
                for sub_id, event_name in self._subscription_events.items()
                if self._matches_subscription(collection, event_name)
            ]

    async def connect(self, *, quiet: bool = False) -> None:
        """Open the WebSocket connection and log in.

        Raises a subclass of :class:`~aiotruenas.exceptions.TrueNASError` on
        any failure. Idempotent: does nothing if already connected. Serialized
        against :meth:`call` and :meth:`close` via the same lock, so
        concurrent ``connect()`` calls cannot race and clobber ``self._ws``.
        A ``connect()`` queued behind a client-side close (:meth:`close`, or a
        call that timed out or failed) opens a fresh connection; it never
        reports the closing one as connected.

        ``quiet=True`` downgrades the ``verify_ssl=False`` warning to DEBUG,
        for callers (e.g. discovery probing) that intentionally attempt many
        expected-to-fail insecure connections.
        """
        async with self._lock:
            if self._ws is not None:
                return

            ws = await self._open_websocket(quiet=quiet)
            try:
                await self._login(ws)
            except BaseException as exc:
                # Nothing tracks this socket yet: release it on any failure,
                # and drop it outright if the login or its graceful close is
                # interrupted (cancel or any other BaseException).
                try:
                    if isinstance(exc, Exception):
                        with contextlib.suppress(WebSocketException, OSError):
                            await ws.close()
                finally:
                    ws.transport.abort()
                raise

            self._ws = ws
            self._reader_task = asyncio.create_task(self._read_loop(ws))

    async def close(self) -> None:
        """Close the WebSocket connection, if any. Safe to call repeatedly.

        The lifecycle is intentionally split around the lock:
        1. under the lock, detach the connection (pending calls fail with
           :class:`~aiotruenas.exceptions.TrueNASConnectionClosedError`,
           subscriptions end) and close the websocket, which ends the reader
        2. await the reader task outside the lock, since it takes the lock
           itself while routing messages

        The client counts as disconnected from step 1 on, so a ``connect()``
        waiting for the lock opens a fresh connection that step 2 leaves alone.
        A cancelled ``close()`` re-raises. Cancelled before it holds the lock,
        it leaves the connection untouched; cancelled later, it still aborts
        the socket and stops the reader.
        """
        ws = None
        reader_task = None
        try:
            async with self._lock:
                ws = self._ws
                if ws is None:
                    return
                reader_task = self._detach_connection_locked(_CLOSED_BY_CLIENT)
                with contextlib.suppress(WebSocketException, OSError):
                    await ws.close()
            if reader_task is not None:
                await self._await_reader(reader_task)
        finally:
            if ws is not None:
                self._release_socket(ws, reader_task)

    @staticmethod
    async def _await_reader(reader_task: asyncio.Task[None]) -> None:
        """Wait for the reader to end, tolerating only its own cancellation."""
        current = asyncio.current_task()
        # Compare against the count on entry: a caller that caught an earlier
        # cancel (e.g. ``close()`` in an ``except CancelledError`` cleanup)
        # keeps a positive count without being cancelled again here.
        cancels_before = current.cancelling() if current is not None else 0
        try:
            await reader_task
        except asyncio.CancelledError:
            if current is not None and current.cancelling() > cancels_before:
                raise

    @staticmethod
    def _release_socket(
        ws: ClientConnection, reader_task: asyncio.Task[None] | None
    ) -> None:
        """Release the socket and reader of a close() that got past the lock."""
        if reader_task is not None and not reader_task.done():
            reader_task.cancel()
        if ws.state is not State.CLOSED:
            # The closing handshake was cut short (cancelled or failed):
            # drop the transport instead of leaving the socket open.
            ws.transport.abort()

    async def _close_websocket_locked(self, ws: ClientConnection, message: str) -> None:
        """Detach and close ``ws`` if it is still the current connection.

        Detached first, like in :meth:`close`, so a ``connect()`` queued on the
        lock opens a fresh connection; other pending calls fail with
        ``message``. A call that failed on an older connection must not close
        a newer one. Caller must already hold ``self._lock``.
        """
        if self._ws is ws:
            reader_task = self._detach_connection_locked(message)
            try:
                with contextlib.suppress(WebSocketException, OSError):
                    await ws.close()
            finally:
                # Nothing tracks the detached socket and reader any more.
                self._release_socket(ws, reader_task)

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

        ``params`` that cannot be serialized to JSON raise the error from
        :func:`json.dumps` (e.g. ``TypeError``, ``ValueError``) before anything
        is sent; the connection stays usable.
        """
        effective_timeout = self._query_timeout if timeout is None else timeout

        async with self._lock:
            ws = self._ws
            if ws is None:
                raise TrueNASConnectionError("not connected; call connect() first")

            rpc_id = self._next_id()
            # Serialized before the future is registered: params that are not
            # JSON-serializable raise TypeError/ValueError to the caller with
            # nothing left behind in _pending_calls and the connection intact.
            message = json.dumps(
                {
                    "jsonrpc": _JSONRPC_VERSION,
                    "id": rpc_id,
                    "method": method,
                    "params": _normalize_params(params),
                }
            )
            future: asyncio.Future[dict[str, Any]] = (
                asyncio.get_running_loop().create_future()
            )
            self._pending_calls[rpc_id] = future

            send_error: Exception | None = None
            try:
                await ws.send(message)
            except (ConnectionClosed, OSError, WebSocketException) as exc:
                send_error = exc
            except BaseException:
                # Cancellation or an unexpected error from send(): nobody
                # will wait for this future, so unregister it before raising.
                self._pending_calls.pop(rpc_id, None)
                raise

            if send_error is not None:
                self._pending_calls.pop(rpc_id, None)
                await self._close_websocket_locked(ws, str(send_error))
                raise TrueNASConnectionClosedError(
                    str(send_error), phase="call"
                ) from send_error

        try:
            async with asyncio.timeout(effective_timeout):
                response = await future
        except TimeoutError as e:
            async with self._lock:
                await self._close_websocket_locked(
                    ws, f"connection closed after {method!r} timed out"
                )
            raise TrueNASCallTimeoutError(
                f"timed out while waiting for response to {method!r}"
            ) from e
        except (
            ConnectionClosed,
            OSError,
            WebSocketException,
        ) as exc:
            async with self._lock:
                await self._close_websocket_locked(ws, str(exc))
            raise TrueNASConnectionClosedError(str(exc), phase="call") from exc
        finally:
            # Every exit unregisters the future -- including cancellation
            # (e.g. a cancelled gather()) and a cancel while waiting for the
            # lock above -- or it would linger until a reply that may never
            # come. On success the reader has already popped it (no-op).
            self._pending_calls.pop(rpc_id, None)

        if response.get(_KEY_ERROR):
            raise build_call_error(response[_KEY_ERROR])
        if _KEY_RESULT not in response:
            raise TrueNASMalformedResponseError(
                f"response for {method!r} has no result"
            )

        result = response[_KEY_RESULT]
        return await self._wait_for_job(result) if job else result

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

    async def subscribe(
        self, event: str
    ) -> tuple[str, asyncio.Queue[_SubscriptionMessage]]:
        """Subscribe to a TrueNAS event and return ``(subscription_id, queue)``.

        The queue receives raw notification payloads for the event. The
        caller reads from the queue and should call :meth:`unsubscribe`
        when done.

        Raises :class:`~aiotruenas.exceptions.TrueNASConnectionClosedError` if
        the connection closes (or is replaced by a reconnect) before the
        subscription is registered: its queue would never be fed or ended.
        """
        ws = self._ws
        result = await self.call("core.subscribe", [event])
        if not isinstance(result, str):
            raise TrueNASMalformedResponseError(
                f"core.subscribe returned non-string subscription id: {result!r}"
            )
        queue: asyncio.Queue[_SubscriptionMessage] = asyncio.Queue()
        async with self._lock:
            if self._ws is not ws:
                raise TrueNASConnectionClosedError(
                    f"connection closed while subscribing to {event!r}",
                    phase="call",
                )
            self._subscriptions[result] = queue
            self._subscription_events[result] = event
        return result, queue

    async def unsubscribe(self, subscription_id: str) -> None:
        """Unsubscribe from a TrueNAS event."""
        try:
            await self.call("core.unsubscribe", [subscription_id])
        finally:
            async with self._lock:
                queue = self._subscriptions.pop(subscription_id, None)
                self._subscription_events.pop(subscription_id, None)
            if queue is not None:
                queue.put_nowait(self._QUEUE_TERMINATOR)

    async def _get_subscription_queue(
        self, subscription_id: str
    ) -> asyncio.Queue[_SubscriptionMessage] | None:
        """Return the subscription queue, if it is currently tracked."""
        async with self._lock:
            return self._subscriptions.get(subscription_id)

    async def is_subscribed(self, subscription_id: str) -> bool:
        """Return True if the subscription_id is currently tracked."""
        async with self._lock:
            return subscription_id in self._subscriptions

    async def get_subscription_events(
        self, subscription_id: str, event_timeout: float | None = None
    ) -> list[dict[str, Any]]:
        """Read events from a subscription queue.

        Returns the raw notification payloads from queued WebSocket messages.

        Raises:
            KeyError: If the subscription_id is not currently tracked.
        """
        try:
            async with self._lock:
                queue = self._subscriptions[subscription_id]
        except KeyError as exc:
            raise KeyError(
                f"Unknown subscription_id {subscription_id!r}; "
                "subscription may have been torn down or never created."
            ) from exc

        events: list[dict[str, Any]] = []

        # Raceguard/pubsub note:
        # ``_QUEUE_TERMINATOR`` is a sentinel injected by teardown/unsubscribe.
        # If seen, stop draining because the queue is no longer valid.
        if event_timeout is None:
            self._drain_events(queue, events)
            return events

        try:
            async with asyncio.timeout(event_timeout):
                envelope = await queue.get()
        except TimeoutError:
            return events

        if self._is_queue_terminator(envelope):
            return events

        events.append(envelope)

        # Once the first event is received, drain any additional already-queued
        # events so callers get all available data instead of a single envelope.
        self._drain_events(queue, events)

        return events

    def _drain_events(
        self,
        queue: asyncio.Queue[_SubscriptionMessage],
        events: list[dict[str, Any]],
    ) -> None:
        while True:
            try:
                envelope = queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            if self._is_queue_terminator(envelope):
                break

            events.append(envelope)

    def _is_queue_terminator(self, envelope: object) -> bool:
        """Return True if ``envelope`` is the internal queue terminator sentinel."""
        return envelope is SUBSCRIPTION_TERMINATOR

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()
