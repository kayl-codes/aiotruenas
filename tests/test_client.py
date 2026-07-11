"""Integration tests for TrueNASClient against the fake WebSocket server."""

from __future__ import annotations

import socket
import ssl

import pytest
import trustme
from fake_server import NO_RESULT, FakeTrueNASServer

from aiotruenas import (
    TrueNASAuthenticationError,
    TrueNASCallError,
    TrueNASCallTimeoutError,
    TrueNASCertificateVerificationError,
    TrueNASClient,
    TrueNASConnectionClosedError,
    TrueNASConnectionError,
    TrueNASConnectionRefusedError,
    TrueNASHandshakeTimeoutError,
    TrueNASMalformedResponseError,
)

API_KEY = "1-valid-key"


def make_client(server: FakeTrueNASServer, **kwargs) -> TrueNASClient:
    kwargs.setdefault("use_tls", False)
    kwargs.setdefault("query_timeout", 2.0)
    return TrueNASClient(server.host, API_KEY, port=server.port, **kwargs)


async def test_connect_and_login_success() -> None:
    async with FakeTrueNASServer(valid_api_key=API_KEY) as server:
        client = make_client(server)
        assert not client.connected
        await client.connect()
        try:
            assert client.connected
        finally:
            await client.close()
        assert not client.connected


async def test_connect_invalid_api_key() -> None:
    async with FakeTrueNASServer(valid_api_key="1-some-other-key") as server:
        client = make_client(server)
        with pytest.raises(TrueNASAuthenticationError):
            await client.connect()
        assert not client.connected


async def test_connection_refused() -> None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()  # nothing listens on this port anymore

    client = TrueNASClient("127.0.0.1", API_KEY, use_tls=False, port=port)
    with pytest.raises(TrueNASConnectionRefusedError):
        await client.connect()


async def test_tls_certificate_verification_failure() -> None:
    ca = trustme.CA()
    server_cert = ca.issue_cert("127.0.0.1")
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_cert.configure_cert(ssl_context)

    async with FakeTrueNASServer(
        valid_api_key=API_KEY, ssl_context=ssl_context
    ) as server:
        # Deliberately do NOT trust the test CA: the client uses the default
        # (system) trust store, so this self-signed cert must be rejected.
        client = TrueNASClient(server.host, API_KEY, use_tls=True, port=server.port)
        with pytest.raises(TrueNASCertificateVerificationError):
            await client.connect()


async def test_handshake_timeout_retries_once_then_raises(monkeypatch) -> None:
    calls = []

    async def fake_connect(url, **kwargs):
        calls.append(url)
        raise TimeoutError("simulated handshake timeout")

    sleeps = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr("aiotruenas.client.connect", fake_connect)
    monkeypatch.setattr("aiotruenas.client.asyncio.sleep", fake_sleep)

    client = TrueNASClient("127.0.0.1", API_KEY, use_tls=False, port=12345)
    with pytest.raises(TrueNASHandshakeTimeoutError):
        await client.connect()

    assert len(calls) == 2
    assert sleeps == [5.0]


async def test_call_timeout_disconnects_client() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY, drop_response_for={"system.info"}
    ) as server:
        client = make_client(server, query_timeout=0.2)
        await client.connect()
        with pytest.raises(TrueNASCallTimeoutError):
            await client.call("system.info")
        assert not client.connected


async def test_connection_lost_mid_query() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY, close_on_method={"system.info"}
    ) as server:
        client = make_client(server)
        await client.connect()
        with pytest.raises(TrueNASConnectionClosedError) as exc_info:
            await client.call("system.info")
        assert exc_info.value.phase == "call"
        assert not client.connected


async def test_connection_lost_mid_login() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY, close_before_login_response=True
    ) as server:
        client = make_client(server)
        with pytest.raises(TrueNASConnectionClosedError) as exc_info:
            await client.connect()
        assert exc_info.value.phase == "login"


async def test_reconnect_after_disconnect() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY, close_on_method={"system.info"}
    ) as server:
        client = make_client(server)
        await client.connect()
        with pytest.raises(TrueNASConnectionClosedError):
            await client.call("system.info")
        assert not client.connected

        await client.connect()
        assert client.connected
        await client.close()


async def test_malformed_response_raises() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY, responses={"system.info": NO_RESULT}
    ) as server:
        client = make_client(server)
        await client.connect()
        try:
            with pytest.raises(TrueNASMalformedResponseError):
                await client.call("system.info")
        finally:
            await client.close()


async def test_call_error_from_server() -> None:
    error = {
        "code": -32000,
        "message": "Call error",
        "data": {"error": 22, "errname": "EINVAL", "reason": "Invalid dataset name"},
    }
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"pool.dataset.query": {"error": error}},
    ) as server:
        client = make_client(server)
        await client.connect()
        try:
            with pytest.raises(TrueNASCallError) as exc_info:
                await client.call("pool.dataset.query")
            assert exc_info.value.reason == "Invalid dataset name"
        finally:
            await client.close()


async def test_call_not_connected_raises() -> None:
    client = TrueNASClient("127.0.0.1", API_KEY, use_tls=False, port=1)
    with pytest.raises(TrueNASConnectionError):
        await client.call("system.info")


async def test_call_params_normalization_dict_and_scalar() -> None:
    seen_params = []

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "pool.scrub.scrub": lambda params: seen_params.append(params) or True
        },
    ) as server:
        client = make_client(server)
        await client.connect()
        try:
            await client.call("pool.scrub.scrub", {"name": "tank", "action": "START"})
        finally:
            await client.close()

    assert seen_params == [[{"name": "tank", "action": "START"}]]


async def test_job_polling_waits_for_terminal_state(monkeypatch) -> None:
    async def fake_sleep(_delay):
        return

    monkeypatch.setattr("aiotruenas.client.asyncio.sleep", fake_sleep)

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"pool.scrub.scrub": 42},
        job_states={
            42: [
                {"id": 42, "state": "RUNNING"},
                {"id": 42, "state": "SUCCESS", "result": "scrub started"},
            ]
        },
    ) as server:
        client = make_client(server)
        await client.connect()
        try:
            result = await client.call("pool.scrub.scrub", ["tank", "START"], job=True)
        finally:
            await client.close()

    assert result == "scrub started"


async def test_job_polling_raises_on_failed_job(monkeypatch) -> None:
    async def fake_sleep(_delay):
        return

    monkeypatch.setattr("aiotruenas.client.asyncio.sleep", fake_sleep)

    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={"replication.run": 7},
        job_states={7: [{"id": 7, "state": "FAILED", "error": "replication failed"}]},
    ) as server:
        client = make_client(server)
        await client.connect()
        try:
            with pytest.raises(TrueNASCallError, match="replication failed"):
                await client.call("replication.run", [1], job=True)
        finally:
            await client.close()


# --- Table-driven round-trip coverage for the RPC method catalog ----------

_QUERY_METHODS: dict[str, tuple[list | dict | None, object]] = {
    "system.info": (None, {"version": "TrueNAS-25.04.0"}),
    "interface.query": (None, [{"name": "eth0"}]),
    "update.status": (None, {"status": "AVAILABLE"}),
    "service.query": (None, [{"service": "cifs", "state": "RUNNING"}]),
    "pool.query": (None, [{"name": "tank"}]),
    "boot.get_state": (None, {"name": "boot-pool"}),
    "pool.dataset.query": (None, [{"name": "tank/data"}]),
    "disk.query": (None, [{"name": "sda"}]),
    "vm.query": (None, [{"name": "vm1"}]),
    "virt.instance.query": (None, [{"name": "ct1"}]),
    "directoryservices.config": (None, {"enable": False}),
    "directoryservices.status": (None, {"type": None, "status": "DISABLED"}),
    "alert.list": (None, [{"uuid": "abc"}]),
    "certificate.query": (None, [{"name": "cert1"}]),
    "smb.status": (None, {"sessions": []}),
    "cloudsync.query": (None, [{"id": 1}]),
    "replication.query": (None, [{"id": 1}]),
    "rsynctask.query": (None, [{"id": 1}]),
    "pool.snapshottask.query": (None, [{"id": 1}]),
    "pool.scrub.query": (None, [{"id": 1}]),
    "app.query": (None, [{"name": "app1"}]),
    "cronjob.query": (None, [{"id": 1}]),
    "core.get_jobs": ([[["id", "=", 1]]], [{"id": 1, "state": "SUCCESS"}]),
    "pool.scrub.scrub": (["tank", "START"], True),
    "alert.dismiss": (["abc"], None),
    "pool.dataset.unlock": (["tank/data", {"datasets": []}], {"unlocked": []}),
    "cronjob.run": ([1], None),
    "service.start": (["cifs"], True),
    "service.stop": (["cifs"], True),
    "service.restart": (["cifs"], True),
    "vm.start": ([1], None),
    "vm.stop": ([1], None),
    "app.start": (["app1"], None),
    "app.stop": (["app1"], None),
    "system.reboot": (None, None),
    "system.shutdown": (None, None),
}


@pytest.mark.parametrize(("method", "params_and_result"), list(_QUERY_METHODS.items()))
async def test_rpc_method_round_trip(method, params_and_result) -> None:
    params, expected = params_and_result
    async with FakeTrueNASServer(
        valid_api_key=API_KEY, responses={method: expected}
    ) as server:
        client = make_client(server)
        await client.connect()
        try:
            result = await client.call(method, params)
        finally:
            await client.close()
    assert result == expected


async def test_async_context_manager() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY, responses={"system.info": {"version": "TrueNAS-25.04.0"}}
    ) as server:
        async with make_client(server) as client:
            assert client.connected
            result = await client.call("system.info")
            assert result == {"version": "TrueNAS-25.04.0"}
        assert not client.connected


async def test_unknown_method_raises_call_error() -> None:
    async with FakeTrueNASServer(valid_api_key=API_KEY) as server:
        client = make_client(server)
        await client.connect()
        try:
            with pytest.raises(TrueNASCallError):
                await client.call("does.not.exist")
        finally:
            await client.close()
