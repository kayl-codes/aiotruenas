"""A minimal fake TrueNAS JSON-RPC 2.0 WebSocket server for tests.

Not a protocol-accurate TrueNAS reimplementation -- just enough surface
(``auth.login_with_api_key``, ``core.get_jobs``, and table-driven canned
responses for arbitrary methods) to drive :class:`aiotruenas.TrueNASClient`
through its success and failure paths without a real TrueNAS instance.
"""

from __future__ import annotations

import json
import ssl
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from websockets.asyncio.server import Server, ServerConnection, serve

#: Sentinel response value: send a JSON-RPC response with neither "result"
#: nor "error", to exercise the malformed-response path.
NO_RESULT = object()

_JSONRPC_VERSION = "2.0"
_KEY_ERROR = "error"
_KEY_RESULT = "result"


def _envelope(rpc_id: Any, **fields: Any) -> str:
    return json.dumps({"jsonrpc": _JSONRPC_VERSION, "id": rpc_id, **fields})


@dataclass
class RawEnvelope:
    """Send exactly these extra envelope fields, bypassing the "a dict
    containing an 'error' key means send an error" convention used for plain
    dict response values -- needed to test envelope-level edge cases like a
    falsy ``"error"`` field alongside a real ``"result"``.
    """

    fields: dict[str, Any]


@dataclass
class FakeTrueNASServer:
    valid_api_key: str = "1-valid-key"
    responses: dict[str, Any | Callable[[list], Any]] = field(default_factory=dict)
    job_states: dict[int, list[dict[str, Any]]] = field(default_factory=dict)
    drop_response_for: set[str] = field(default_factory=set)
    close_on_method: set[str] = field(default_factory=set)
    close_before_login_response: bool = False
    ssl_context: ssl.SSLContext | None = None

    host: str = "127.0.0.1"
    port: int = field(default=0, init=False)
    _server: Server | None = field(default=None, init=False, repr=False)

    async def __aenter__(self) -> FakeTrueNASServer:
        self._server = await serve(self._handle, self.host, 0, ssl=self.ssl_context)
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        assert self._server is not None
        self._server.close()
        await self._server.wait_closed()

    async def _handle(self, ws: ServerConnection) -> None:
        async for raw in ws:
            if not isinstance(raw, str):
                continue
            try:
                message = json.loads(raw)
            except json.JSONDecodeError:
                continue
            await self._dispatch(ws, message)

    async def _dispatch(self, ws: ServerConnection, message: dict[str, Any]) -> None:
        method = message.get("method")
        rpc_id = message.get("id")
        params = message.get("params") or []

        if method == "auth.login_with_api_key":
            await self._handle_login(ws, rpc_id, params)
            return
        if method in self.close_on_method:
            await ws.close()
            return
        if method in self.drop_response_for:
            return
        # Job-state simulation only kicks in when the test hasn't configured
        # an explicit canned response for core.get_jobs itself.
        if method == "core.get_jobs" and method not in self.responses:
            await self._handle_get_jobs(ws, rpc_id, params)
            return

        await self._handle_generic(ws, rpc_id, method, params)

    async def _handle_login(
        self, ws: ServerConnection, rpc_id: Any, params: list
    ) -> None:
        if self.close_before_login_response:
            await ws.close()
            return
        key = params[0] if params else None
        result = key == self.valid_api_key
        await ws.send(_envelope(rpc_id, **{_KEY_RESULT: result}))

    async def _handle_get_jobs(
        self, ws: ServerConnection, rpc_id: Any, params: list
    ) -> None:
        job_id = params[0][0][2]
        if job_id not in self.job_states:
            # No queue configured for this id at all: simulate "job not
            # found" (e.g. a stale/pruned id) rather than a synthetic SUCCESS.
            await ws.send(_envelope(rpc_id, **{_KEY_RESULT: []}))
            return
        queue = self.job_states[job_id]
        state = queue.pop(0) if queue else {"id": job_id, "state": "SUCCESS"}
        await ws.send(_envelope(rpc_id, **{_KEY_RESULT: [state]}))

    async def _handle_generic(
        self, ws: ServerConnection, rpc_id: Any, method: str, params: list
    ) -> None:
        if method not in self.responses:
            error = {
                "code": -32601,
                "message": "Method does not exist",
                "data": {"error": 601, "errname": "ENOMETHOD", "reason": None},
            }
            await ws.send(_envelope(rpc_id, **{_KEY_ERROR: error}))
            return

        entry = self.responses[method]
        value = entry(params) if callable(entry) else entry

        if isinstance(value, RawEnvelope):
            await ws.send(_envelope(rpc_id, **value.fields))
        elif value is NO_RESULT:
            await ws.send(_envelope(rpc_id))
        elif isinstance(value, dict) and _KEY_ERROR in value:
            await ws.send(_envelope(rpc_id, **{_KEY_ERROR: value[_KEY_ERROR]}))
        else:
            await ws.send(_envelope(rpc_id, **{_KEY_RESULT: value}))
