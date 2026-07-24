"""Integration tests for TrueNASClient against the fake WebSocket server."""

from __future__ import annotations

import asyncio
import socket
import ssl

import pytest
import trustme
from fake_server import NO_RESULT, FakeTrueNASServer, RawEnvelope

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
        async with make_client(server, query_timeout=0.2) as client:
            await client.connect()
            with pytest.raises(TrueNASCallTimeoutError):
                await client.call("system.info")
        assert not client.connected


async def test_connection_lost_mid_query() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY, close_on_method={"system.info"}
    ) as server:
        async with make_client(server) as client:
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
        async with make_client(server) as client:
            await client.connect()
            with pytest.raises(TrueNASConnectionClosedError):
                await client.call("system.info")
            assert not client.connected

            await client.connect()
            assert client.connected


async def test_malformed_response_raises() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY, responses={"system.info": NO_RESULT}
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            with pytest.raises(TrueNASMalformedResponseError):
                await client.call("system.info")


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
        async with make_client(server) as client:
            await client.connect()
            with pytest.raises(TrueNASCallError) as exc_info:
                await client.call("pool.dataset.query")
            assert exc_info.value.reason == "Invalid dataset name"


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
        async with make_client(server) as client:
            await client.connect()
            await client.call("pool.scrub.scrub", {"name": "tank", "action": "START"})

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
        async with make_client(server) as client:
            await client.connect()
            result = await client.call("pool.scrub.scrub", ["tank", "START"], job=True)

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
        async with make_client(server) as client:
            await client.connect()
            with pytest.raises(TrueNASCallError, match="replication failed"):
                await client.call("replication.run", [1], job=True)


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
        async with make_client(server) as client:
            await client.connect()
            result = await client.call(method, params)
            assert client.connected
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
        async with make_client(server) as client:
            await client.connect()
            with pytest.raises(TrueNASCallError):
                await client.call("does.not.exist")


# --- Regression tests for review findings ----------------------------------


async def test_falsy_empty_error_is_treated_as_success() -> None:
    """An empty ``"error": {}`` envelope field must not be treated as a real error."""
    async with FakeTrueNASServer(
        valid_api_key=API_KEY,
        responses={
            "system.info": RawEnvelope(
                {"error": {}, "result": {"version": "should-not-raise"}}
            )
        },
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            result = await client.call("system.info")
    assert result == {"version": "should-not-raise"}


async def test_explicit_small_call_timeout_is_not_replaced_by_default() -> None:
    """A short explicit ``timeout=`` must be honored, not `or`-ed away.

    Regression test: ``timeout or self._query_timeout`` would silently fall
    back to the (here: much larger) default for any falsy explicit value.
    Bounded by an outer 2s deadline so a regression fails fast instead of
    hanging for the full 30s default.
    """
    async with FakeTrueNASServer(
        valid_api_key=API_KEY, drop_response_for={"system.info"}
    ) as server:
        async with make_client(server, query_timeout=30.0) as client:
            await client.connect()
            with pytest.raises(TrueNASCallTimeoutError):
                async with asyncio.timeout(2.0):
                    await client.call("system.info", timeout=0.1)


async def test_concurrent_connect_calls_do_not_race() -> None:
    async with FakeTrueNASServer(valid_api_key=API_KEY) as server:
        async with make_client(server) as client:
            await asyncio.gather(client.connect(), client.connect())
            assert client.connected


async def test_close_while_call_in_flight_does_not_raise_attribute_error() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY, drop_response_for={"system.info"}
    ) as server:
        async with make_client(server, query_timeout=0.2) as client:
            await client.connect()

            call_task = asyncio.ensure_future(client.call("system.info"))
            close_task = asyncio.ensure_future(client.close())

            with pytest.raises(TrueNASConnectionClosedError):
                await call_task
            await close_task
            assert not client.connected


async def test_job_polling_rejects_bool_job_id() -> None:
    async with FakeTrueNASServer(
        valid_api_key=API_KEY, responses={"service.start": True}
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            with pytest.raises(TrueNASMalformedResponseError):
                await client.call("service.start", ["cifs"], job=True)


async def test_job_polling_gives_up_on_job_that_never_appears(monkeypatch) -> None:
    async def fake_sleep(_delay):
        return

    monkeypatch.setattr("aiotruenas.client.asyncio.sleep", fake_sleep)

    async with FakeTrueNASServer(
        valid_api_key=API_KEY, responses={"pool.scrub.scrub": 999}
    ) as server:
        async with make_client(server) as client:
            await client.connect()
            with pytest.raises(TrueNASMalformedResponseError):
                await client.call("pool.scrub.scrub", ["tank", "START"], job=True)


async def test_subscribe_returns_subscription_id_and_queue() -> None:
    async with FakeTrueNASServer(valid_api_key=API_KEY) as server:
        async with make_client(server) as client:
            await client.connect()
            sub_id, queue = await client.subscribe("app.stats")
            assert isinstance(sub_id, str)
            assert sub_id.startswith("sub-app.stats-")
            assert isinstance(queue, asyncio.Queue)


async def test_subscribe_raises_on_non_string_subscription_id(monkeypatch) -> None:
    async with FakeTrueNASServer(valid_api_key=API_KEY) as server:
        async with make_client(server) as client:
            await client.connect()

            async def fake_call(method, params=None, *, job: bool = False):
                return {"result": 123}

            monkeypatch.setattr(client, "call", fake_call)
            with pytest.raises(TrueNASMalformedResponseError) as excinfo:
                await client.subscribe("app.stats")
            assert "core.subscribe" in str(excinfo.value)


async def test_subscribe_delivers_notifications() -> None:
    async with FakeTrueNASServer(valid_api_key=API_KEY) as server:
        async with make_client(server) as client:
            await client.connect()
            sub_id, queue = await client.subscribe("app.stats")
            await server.send_subscription_event(
                sub_id, {"fields": [{"app_name": "test-app", "stats": {}}]}
            )
            payload = await asyncio.wait_for(queue.get(), timeout=2.0)
            assert payload.get("collection") == "app.stats"
            assert payload.get("fields") == [{"app_name": "test-app", "stats": {}}]


async def test_subscribe_routes_only_matching_subscription() -> None:
    async with FakeTrueNASServer(valid_api_key=API_KEY) as server:
        async with make_client(server) as client:
            await client.connect()
            sub_a, queue_a = await client.subscribe("app.stats")
            sub_b, queue_b = await client.subscribe("pool.query")

            await server.send_subscription_event(
                sub_a, {"fields": [{"app_name": "app-1"}]}
            )
            payload_a = await asyncio.wait_for(queue_a.get(), timeout=2.0)
            assert payload_a.get("collection") == "app.stats"

            await server.send_subscription_event(
                sub_b, {"fields": [{"pool": "pool-1"}]}
            )
            payload_b = await asyncio.wait_for(queue_b.get(), timeout=2.0)
            assert payload_b.get("collection") == "pool.query"


async def test_subscribe_ignores_similar_prefixed_event_names() -> None:
    async with FakeTrueNASServer(valid_api_key=API_KEY) as server:
        async with make_client(server) as client:
            await client.connect()
            sub_id, queue = await client.subscribe("app.stats")

            await server.send_subscription_event(
                sub_id,
                {"fields": [{"app_name": "app-1"}]},
                collection_override="app.stats_extra",
            )
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(queue.get(), timeout=0.2)


async def test_subscribe_routes_by_collection_prefix() -> None:
    async with FakeTrueNASServer(valid_api_key=API_KEY) as server:
        async with make_client(server) as client:
            await client.connect()
            sub_id, queue = await client.subscribe('app.stats:{"interval": 60}')

            await server.send_subscription_event(
                sub_id,
                {"fields": [{"app_name": "app-1"}]},
            )
            payload = await asyncio.wait_for(queue.get(), timeout=2.0)
            assert payload.get("collection") == 'app.stats:{"interval": 60}'


async def test_subscribe_routes_jsonrpc_collection_update_notification() -> None:
    async with FakeTrueNASServer(valid_api_key=API_KEY) as server:
        async with make_client(server) as client:
            await client.connect()
            sub_id, queue = await client.subscribe('app.stats:{"interval": 60}')

            await server.send_subscription_event(
                sub_id,
                {"fields": [{"app_name": "app-1"}]},
                jsonrpc_notification=True,
            )
            payload = await asyncio.wait_for(queue.get(), timeout=2.0)
            assert payload.get("method") == "collection_update"
            params = payload.get("params", {})
            assert params.get("collection") == 'app.stats:{"interval": 60}'
            assert params.get("fields") == [{"app_name": "app-1"}]


async def test_subscribe_routes_jsonrpc_notification_by_params_prefix() -> None:
    async with FakeTrueNASServer(valid_api_key=API_KEY) as server:
        async with make_client(server) as client:
            await client.connect()
            sub_id, queue = await client.subscribe("app.stats")

            await server.send_subscription_event(
                sub_id,
                {"fields": [{"app_name": "app-1"}]},
                jsonrpc_notification=True,
                collection_override="app.stats:other",
            )
            payload = await asyncio.wait_for(queue.get(), timeout=2.0)
            assert payload.get("method") == "collection_update"
            params = payload.get("params", {})
            assert params.get("collection") == "app.stats:other"


async def test_subscribe_ignores_different_parameterized_event() -> None:
    async with FakeTrueNASServer(valid_api_key=API_KEY) as server:
        async with make_client(server) as client:
            await client.connect()
            sub_id, queue = await client.subscribe('app.stats:{"interval": 60}')

            await server.send_subscription_event(
                sub_id,
                {"fields": [{"app_name": "app-1"}]},
                collection_override='app.stats:{"interval": 30}',
            )
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(queue.get(), timeout=0.2)


async def test_subscribe_removed_on_unsubscribe() -> None:
    async with FakeTrueNASServer(valid_api_key=API_KEY) as server:
        async with make_client(server) as client:
            await client.connect()
            sub_id, queue = await client.subscribe("app.stats")
            await client.unsubscribe(sub_id)

            await server.send_subscription_event(
                sub_id, {"fields": [{"app_name": "app-1"}]}
            )
            envelope = await asyncio.wait_for(queue.get(), timeout=0.5)
            assert envelope is TrueNASClient._QUEUE_TERMINATOR
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(queue.get(), timeout=0.5)


async def test_unsubscribe_calls_api() -> None:
    async with FakeTrueNASServer(valid_api_key=API_KEY) as server:
        async with make_client(server) as client:
            await client.connect()
            sub_id, queue = await client.subscribe("app.stats")
            await client.unsubscribe(sub_id)


async def test_is_subscribed_tracks_active_subscriptions() -> None:
    async with FakeTrueNASServer(valid_api_key=API_KEY) as server:
        async with make_client(server) as client:
            await client.connect()
            assert not await client.is_subscribed("sub-1")
            sub_id, _ = await client.subscribe("app.stats")
            assert await client.is_subscribed(sub_id)
            await client.unsubscribe(sub_id)
            assert not await client.is_subscribed(sub_id)


async def test_get_subscription_events_reads_from_queue() -> None:
    async with FakeTrueNASServer(valid_api_key=API_KEY) as server:
        async with make_client(server) as client:
            await client.connect()
            sub_id, queue = await client.subscribe("app.stats")
            await server.send_subscription_event(
                sub_id,
                {"fields": [{"app_name": "test"}]},
            )
            events = await client.get_subscription_events(sub_id, event_timeout=2.0)
            assert len(events) == 1
            assert events[0].get("fields") == [{"app_name": "test"}]


async def test_get_subscription_events_timeout() -> None:
    async with FakeTrueNASServer(valid_api_key=API_KEY) as server:
        async with make_client(server) as client:
            await client.connect()
            sub_id, queue = await client.subscribe("app.stats")
            events = await client.get_subscription_events(sub_id, event_timeout=0.1)
            assert events == []


async def test_get_subscription_events_unknown_subscription() -> None:
    async with FakeTrueNASServer(valid_api_key=API_KEY) as server:
        async with make_client(server) as client:
            await client.connect()
            with pytest.raises(KeyError):
                await client.get_subscription_events("nonexistent")


async def test_subscriptions_cleared_on_disconnect() -> None:
    async with FakeTrueNASServer(valid_api_key=API_KEY) as server:
        async with make_client(server) as client:
            sub_id, queue = await client.subscribe("app.stats")
            assert await client._get_subscription_queue(sub_id) is queue
            assert await client.is_subscribed(sub_id)
        assert not client.connected
        assert await client._get_subscription_queue(sub_id) is None
        assert not await client.is_subscribed(sub_id)


async def test_subscriptions_cleared_after_server_disconnect() -> None:
    """Unhelpful server-side close should leave no stale subscription state."""
    async with FakeTrueNASServer(valid_api_key=API_KEY) as server:
        client = make_client(server)
        await client.connect()
        sub_id, _ = await client.subscribe("app.stats")
        assert client.connected
        assert await client._get_subscription_queue(sub_id) is not None

        await server.close_connection()
        await asyncio.sleep(0.1)

        assert not client.connected
        assert client._ws is None
        assert await client._get_subscription_queue(sub_id) is None
        assert not await client.is_subscribed(sub_id)
