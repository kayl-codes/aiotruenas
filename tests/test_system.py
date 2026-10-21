"""Tests for the typed, stateless ``TrueNASSystem`` reader."""

from __future__ import annotations

import dataclasses
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any, cast

import pytest
from fake_server import FakeTrueNASServer

from aiotruenas import (
    InterfaceTraffic,
    NetworkInterface,
    SystemInfo,
    SystemStats,
    TrueNASCallError,
    TrueNASCallTimeoutError,
    TrueNASClient,
    TrueNASConnectionClosedError,
    TrueNASMalformedResponseError,
    TrueNASSystem,
)

API_KEY = "1-valid-key"
NETDATA = "reporting.netdata_graph"

_SYSTEM_INFO = {
    "hostname": "nas",
    "version": "25.04.2",
    "system_manufacturer": "Supermicro",
    "system_product": "X11SCH-F",
    "system_serial": "S123",
    "physmem": 34359738368,
    "boottime": {"$date": 1700000000000},
    "uptime_seconds": 12345.6,
    "loadavg": [0.5, 0.25, 0.125],
}

_CALL_ERROR = {
    "error": {
        "code": -32001,
        "message": "Method call error",
        "data": {"error": 1, "errname": "EFAULT", "reason": "boom"},
    }
}


def _graph(legend: list[str], mean: dict[str, Any], **extra: Any) -> dict[str, Any]:
    return {"legend": ["time", *legend], "aggregations": {"mean": mean}, **extra}


_GRAPHS: dict[str, Any] = {
    "cpu": [_graph(["cpu"], {"cpu": 12.5})],
    "cputemp": [_graph(["cpu0", "cpu1"], {"cpu0": 41.0, "cpu1": 47.25})],
    "memory": [_graph(["available"], {"available": 8589934592.4})],
    "arcsize": [_graph(["size"], {"size": 4294967296.0})],
    "interface": [
        _graph(
            ["received", "sent"],
            {"received": 800.0, "sent": 160.0},
            identifier="eno1",
        ),
        _graph(["rx", "tx"], {"rx": 1.5, "tx": 2.5}, identifier="eno2"),
    ],
}


def _netdata(graphs: dict[str, Any]) -> Callable[[list[Any]], Any]:
    def handler(params: list[Any]) -> Any:
        return graphs[params[0]]

    return handler


async def _run[T](
    responses: dict[str, Any], action: Callable[[TrueNASSystem], Awaitable[T]]
) -> T:
    async with FakeTrueNASServer(valid_api_key=API_KEY, responses=responses) as server:
        async with TrueNASClient(
            server.host, API_KEY, port=server.port, use_tls=False, query_timeout=2.0
        ) as client:
            await client.connect()
            return await action(TrueNASSystem(client))


def _info(**overrides: Any) -> SystemInfo:
    values: dict[str, Any] = {
        "hostname": "nas",
        "version": "25.04.2",
        "manufacturer": None,
        "product": None,
        "serial": None,
        "physical_memory": None,
        "boot_time": None,
        "uptime_seconds": None,
        "load_1m": None,
        "load_5m": None,
        "load_15m": None,
        "is_virtual": None,
    }
    return SystemInfo(**(values | overrides))


# --- get_system_id -----------------------------------------------------------


async def test_get_system_id_returns_uuid() -> None:
    result = await _run(
        {"system.global.id": "a1b2c3"}, lambda system: system.get_system_id()
    )
    assert result == "a1b2c3"


@pytest.mark.parametrize("payload", [None, "", "   ", 42, {"id": "x"}])
async def test_get_system_id_rejects_non_string(payload: Any) -> None:
    with pytest.raises(TrueNASMalformedResponseError, match="system.global.id"):
        await _run({"system.global.id": payload}, lambda system: system.get_system_id())


async def test_get_system_id_propagates_call_error() -> None:
    with pytest.raises(TrueNASCallError):
        await _run(
            {"system.global.id": _CALL_ERROR}, lambda system: system.get_system_id()
        )


# --- get_system_info ---------------------------------------------------------


async def test_get_system_info_parses_all_fields() -> None:
    result = await _run(
        {"system.info": _SYSTEM_INFO}, lambda system: system.get_system_info()
    )
    assert result == SystemInfo(
        hostname="nas",
        version="25.04.2",
        manufacturer="Supermicro",
        product="X11SCH-F",
        serial="S123",
        physical_memory=34359738368,
        boot_time=datetime(2023, 11, 14, 22, 13, 20, tzinfo=UTC),
        uptime_seconds=12345.6,
        load_1m=0.5,
        load_5m=0.25,
        load_15m=0.125,
        is_virtual=False,
    )
    assert result.boot_time is not None
    assert result.boot_time.tzinfo is UTC


async def test_get_system_info_missing_fields_are_none() -> None:
    result = await _run(
        {"system.info": {"hostname": "nas"}}, lambda system: system.get_system_info()
    )
    assert result == _info(version=None)


async def test_get_system_info_invalid_field_values_are_none() -> None:
    payload = {
        "hostname": "  ",
        "version": None,
        "system_manufacturer": "",
        "system_product": 7,
        "system_serial": [],
        "physmem": True,
        "boottime": {"$date": "soon"},
        "uptime_seconds": float("nan"),
        "loadavg": [1.0, "x", None],
    }
    result = await _run({"system.info": payload}, lambda s: s.get_system_info())
    assert result == _info(hostname=None, version=None, load_1m=1.0)


@pytest.mark.parametrize(
    "boottime", [1700000000000, "2023-11-14", {"$date": 10**30}, {}]
)
async def test_get_system_info_unparsable_boottime_is_none(boottime: Any) -> None:
    result = await _run(
        {"system.info": {"hostname": "nas", "boottime": boottime}},
        lambda system: system.get_system_info(),
    )
    assert result.boot_time is None


@pytest.mark.parametrize("loadavg", [[0.1, 0.2], [0.1, 0.2, 0.3, 0.4], "0.1", None])
async def test_get_system_info_wrong_loadavg_shape_is_none(loadavg: Any) -> None:
    result = await _run(
        {"system.info": {"hostname": "nas", "loadavg": loadavg}},
        lambda system: system.get_system_info(),
    )
    assert (result.load_1m, result.load_5m, result.load_15m) == (None, None, None)


@pytest.mark.parametrize(
    ("manufacturer", "product", "expected"),
    [
        ("QEMU", "Standard PC", True),
        ("Innotek", "VirtualBox", True),
        ("Supermicro", None, False),
        (None, "X11SCH-F", False),
        (None, None, None),
        ("", " ", None),
    ],
)
async def test_get_system_info_is_virtual(
    manufacturer: Any, product: Any, expected: bool | None
) -> None:
    payload = {
        "hostname": "nas",
        "system_manufacturer": manufacturer,
        "system_product": product,
    }
    result = await _run({"system.info": payload}, lambda s: s.get_system_info())
    assert result.is_virtual is expected


@pytest.mark.parametrize("payload", [None, {}, [], "nas"])
async def test_get_system_info_rejects_malformed_response(payload: Any) -> None:
    with pytest.raises(TrueNASMalformedResponseError, match="system.info"):
        await _run({"system.info": payload}, lambda s: s.get_system_info())


async def test_get_system_info_propagates_call_error() -> None:
    with pytest.raises(TrueNASCallError):
        await _run({"system.info": _CALL_ERROR}, lambda s: s.get_system_info())


async def test_system_info_is_frozen() -> None:
    info = _info()
    with pytest.raises(dataclasses.FrozenInstanceError):
        info.hostname = "other"  # type: ignore[misc]


# --- get_network_interfaces --------------------------------------------------


async def test_get_network_interfaces_parses_entries() -> None:
    payload = [
        {
            "id": "eno1",
            "name": "eno1",
            "description": "Uplink",
            "mtu": 1500,
            "state": {
                "link_state": "LINK_STATE_UP",
                "active_media_type": "Ethernet",
                "active_media_subtype": "1000Mb/s Twisted Pair",
                "link_address": "aa:bb:cc:dd:ee:ff",
            },
        },
        {"id": "eno2", "state": {"link_state": "LINK_STATE_DOWN"}},
    ]
    result = await _run(
        {"interface.query": payload}, lambda s: s.get_network_interfaces()
    )
    assert result == {
        "eno1": NetworkInterface(
            id="eno1",
            name="eno1",
            description="Uplink",
            mtu=1500,
            link_state="LINK_STATE_UP",
            link_up=True,
            active_media_type="Ethernet",
            active_media_subtype="1000Mb/s Twisted Pair",
            link_address="aa:bb:cc:dd:ee:ff",
        ),
        "eno2": NetworkInterface(
            id="eno2",
            name=None,
            description=None,
            mtu=None,
            link_state="LINK_STATE_DOWN",
            link_up=False,
            active_media_type=None,
            active_media_subtype=None,
            link_address=None,
        ),
    }


async def test_get_network_interfaces_unknown_state_is_none() -> None:
    payload = [{"id": "br0", "description": "", "mtu": "1500", "state": None}]
    result = await _run(
        {"interface.query": payload}, lambda s: s.get_network_interfaces()
    )
    interface = result["br0"]
    assert interface.link_state is None
    assert interface.link_up is None
    assert interface.description is None
    assert interface.mtu is None


async def test_get_network_interfaces_skips_entries_without_id(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING, logger="aiotruenas.system")
    payload = [{"id": "eno1"}, {"name": "noid"}, {"id": ""}, {"id": 5}, "junk"]
    result = await _run(
        {"interface.query": payload}, lambda s: s.get_network_interfaces()
    )
    assert list(result) == ["eno1"]
    skipped = [r for r in caplog.records if "without a usable id" in r.getMessage()]
    assert len(skipped) == 1
    assert skipped[0].levelno == logging.WARNING
    assert "Skipped 4" in skipped[0].getMessage()


async def test_get_network_interfaces_no_warning_when_nothing_skipped(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING, logger="aiotruenas.system")
    await _run(
        {"interface.query": [{"id": "eno1"}]}, lambda s: s.get_network_interfaces()
    )
    assert not [r for r in caplog.records if r.name == "aiotruenas.system"]


async def test_get_network_interfaces_empty_list() -> None:
    result = await _run({"interface.query": []}, lambda s: s.get_network_interfaces())
    assert result == {}


@pytest.mark.parametrize("payload", [None, {"id": "eno1"}, "eno1"])
async def test_get_network_interfaces_rejects_malformed_response(
    payload: Any,
) -> None:
    with pytest.raises(TrueNASMalformedResponseError, match="interface.query"):
        await _run({"interface.query": payload}, lambda s: s.get_network_interfaces())


# --- get_system_stats --------------------------------------------------------


async def test_get_system_stats_parses_all_graphs() -> None:
    result = await _run(
        {NETDATA: _netdata(_GRAPHS)},
        lambda s: s.get_system_stats(_info(physical_memory=34359738368)),
    )
    assert result == SystemStats(
        cpu_usage_percent=12.5,
        cpu_temperature=47.25,
        memory_available=8589934592,
        memory_usage_percent=75.0,
        arc_size=4294967296,
        interfaces={
            "eno1": InterfaceTraffic(rx_kbps=800.0, tx_kbps=160.0),
            "eno2": InterfaceTraffic(rx_kbps=1.5, tx_kbps=2.5),
        },
    )
    assert isinstance(result.memory_available, int)
    assert isinstance(result.arc_size, int)


async def test_get_system_stats_queries_aggregated_window() -> None:
    seen: list[list[Any]] = []

    def handler(params: list[Any]) -> Any:
        seen.append(params)
        return _GRAPHS[params[0]]

    await _run({NETDATA: handler}, lambda s: s.get_system_stats())
    assert sorted(p[0] for p in seen) == sorted(_GRAPHS)
    for _, query in seen:
        assert query["aggregate"] is True
        assert query["end"] - query["start"] == 60


async def test_get_system_stats_without_system_info() -> None:
    result = await _run({NETDATA: _netdata(_GRAPHS)}, lambda s: s.get_system_stats())
    assert result.memory_usage_percent is None
    assert result.cpu_temperature == 47.25


async def test_get_system_stats_skips_cputemp_on_vm() -> None:
    graphs = {k: v for k, v in _GRAPHS.items() if k != "cputemp"}
    result = await _run(
        {NETDATA: _netdata(graphs)},
        lambda s: s.get_system_stats(_info(is_virtual=True)),
    )
    assert result.cpu_temperature is None
    assert result.cpu_usage_percent == 12.5


@pytest.mark.parametrize("is_virtual", [False, None])
async def test_get_system_stats_queries_cputemp_unless_known_vm(
    is_virtual: bool | None,
) -> None:
    result = await _run(
        {NETDATA: _netdata(_GRAPHS)},
        lambda s: s.get_system_stats(_info(is_virtual=is_virtual)),
    )
    assert result.cpu_temperature == 47.25


async def test_get_system_stats_missing_series_are_none() -> None:
    empty = {
        "cpu": [_graph(["cpu"], {})],
        "cputemp": [_graph([], {})],
        "memory": [],
        "arcsize": [{"legend": ["time", "size"]}],
        "interface": [_graph(["received"], {"received": None}, identifier="eno1")],
    }
    result = await _run(
        {NETDATA: _netdata(empty)},
        lambda s: s.get_system_stats(_info(physical_memory=1024)),
    )
    assert result == SystemStats(
        cpu_usage_percent=None,
        cpu_temperature=None,
        memory_available=None,
        memory_usage_percent=None,
        arc_size=None,
        interfaces={"eno1": InterfaceTraffic(rx_kbps=None, tx_kbps=None)},
    )


@pytest.mark.parametrize(
    ("graph", "item", "match"),
    [
        ("interface", _graph(["received"], {"received": 1.0}), "without an identifier"),
        (
            "interface",
            _graph(["received"], {"received": 1.0}, identifier=""),
            "without an identifier",
        ),
        ("interface", "junk", "without a legend list"),
        (
            "interface",
            _graph(["received"], {"received": 1.0}, identifier="  "),
            "without an identifier",
        ),
        (
            "interface",
            _graph(["received"], {"received": 1.0}, identifier=5),
            "without an identifier",
        ),
        ("cputemp", {"legend": "cpu0"}, "without a legend list"),
        ("cputemp", 42, "without a legend list"),
        ("cpu", {"aggregations": {"mean": {"cpu": 1.0}}}, "without a legend list"),
        ("memory", {"legend": "available"}, "without a legend list"),
        ("arcsize", 42, "without a legend list"),
    ],
)
async def test_get_system_stats_rejects_malformed_graph_items(
    graph: str, item: Any, match: str
) -> None:
    graphs = dict(_GRAPHS)
    graphs[graph] = [*graphs[graph], item]
    with pytest.raises(TrueNASMalformedResponseError, match=match) as excinfo:
        await _run({NETDATA: _netdata(graphs)}, lambda s: s.get_system_stats())
    assert f"graph '{graph}'" in str(excinfo.value)


async def test_get_system_stats_empty_graph_is_not_malformed() -> None:
    graphs = dict(_GRAPHS)
    graphs["interface"] = []
    graphs["cpu"] = []
    result = await _run({NETDATA: _netdata(graphs)}, lambda s: s.get_system_stats())
    assert result.interfaces == {}
    assert result.cpu_usage_percent is None


async def test_get_system_stats_rate_alias_used_when_canonical_unusable() -> None:
    graphs = dict(_GRAPHS)
    graphs["interface"] = [
        _graph(
            ["received", "sent", "rx", "tx"],
            {"received": -1.0, "sent": None, "rx": 4.0, "tx": 5.0},
            identifier="a",
        )
    ]
    result = await _run({NETDATA: _netdata(graphs)}, lambda s: s.get_system_stats())
    assert result.interfaces == {"a": InterfaceTraffic(rx_kbps=4.0, tx_kbps=5.0)}


@pytest.mark.parametrize(
    ("physical_memory", "available", "expected"),
    [
        (1000, 250.0, 75.0),
        (1000, 0.0, 100.0),
        (1000, 1000.0, 0.0),
        (1000, 1500.0, None),
        (1000, -1.0, None),
        (0, 0.0, None),
        (None, 250.0, None),
    ],
)
async def test_get_system_stats_memory_usage_percent(
    physical_memory: int | None, available: float, expected: float | None
) -> None:
    graphs = dict(_GRAPHS)
    graphs["memory"] = [_graph(["available"], {"available": available})]
    result = await _run(
        {NETDATA: _netdata(graphs)},
        lambda s: s.get_system_stats(_info(physical_memory=physical_memory)),
    )
    assert result.memory_usage_percent == expected


@pytest.mark.parametrize("graph", ["cpu", "memory", "arcsize", "interface", "cputemp"])
async def test_get_system_stats_raises_on_malformed_graph(graph: str) -> None:
    graphs = dict(_GRAPHS)
    graphs[graph] = {"not": "a list"}
    with pytest.raises(TrueNASMalformedResponseError, match=f"graph '{graph}'"):
        await _run({NETDATA: _netdata(graphs)}, lambda s: s.get_system_stats())


@pytest.mark.parametrize("graph", ["cpu", "memory", "arcsize", "interface"])
async def test_get_system_stats_propagates_graph_call_error(graph: str) -> None:
    graphs = dict(_GRAPHS)
    graphs[graph] = _CALL_ERROR
    with pytest.raises(TrueNASCallError) as excinfo:
        await _run({NETDATA: _netdata(graphs)}, lambda s: s.get_system_stats())
    assert f"while fetching netdata graph '{graph}'" in excinfo.value.__notes__


async def test_get_system_stats_tolerates_cputemp_call_error() -> None:
    graphs = dict(_GRAPHS)
    graphs["cputemp"] = _CALL_ERROR
    result = await _run(
        {NETDATA: _netdata(graphs)},
        lambda s: s.get_system_stats(_info(physical_memory=34359738368)),
    )
    assert result.cpu_temperature is None
    assert result.cpu_usage_percent == 12.5
    assert result.memory_usage_percent == 75.0
    assert result.arc_size == 4294967296
    assert set(result.interfaces) == {"eno1", "eno2"}


def _raw_graph(legend: list[str], data: list[Any], **extra: Any) -> dict[str, Any]:
    """A netdata item with empty aggregations, as sent for an all-zero series."""
    return {"legend": legend, "aggregations": {}, "data": data, **extra}


@pytest.mark.parametrize("with_time", [True, False])
async def test_get_system_stats_falls_back_to_raw_data(with_time: bool) -> None:
    def legend(*names: str) -> list[str]:
        return ["time", *names] if with_time else list(names)

    graphs = {
        "cpu": [_raw_graph(legend("cpu"), [[1, 0], [2, 0]])],
        "cputemp": [_raw_graph(legend("cpu0", "cpu1"), [[1, 40, 50], [2, 42, 52]])],
        "memory": [_raw_graph(legend("available"), [[1, 100], [2, 300]])],
        "arcsize": [_raw_graph(legend("size"), [[1, 0], [2, 0]])],
        "interface": [
            _raw_graph(
                legend("received", "sent"),
                [[1, 0, 0], [2, 0, 0], "junk", [3]],
                identifier="eno1",
            )
        ],
    }
    result = await _run({NETDATA: _netdata(graphs)}, lambda s: s.get_system_stats())
    assert result.cpu_usage_percent == 0.0
    assert result.cpu_temperature == 51.0
    assert result.memory_available == 200
    assert result.arc_size == 0
    assert result.interfaces == {"eno1": InterfaceTraffic(rx_kbps=0.0, tx_kbps=0.0)}


async def test_get_system_stats_raw_fallback_only_for_missing_series() -> None:
    graphs = dict(_GRAPHS)
    graphs["interface"] = [
        {
            "legend": ["time", "received", "sent"],
            "aggregations": {"mean": {"received": 7.0}},
            "data": [[1, 100, 0], [2, 100, 0]],
            "identifier": "eno1",
        }
    ]
    result = await _run({NETDATA: _netdata(graphs)}, lambda s: s.get_system_stats())
    assert result.interfaces["eno1"] == InterfaceTraffic(rx_kbps=7.0, tx_kbps=0.0)


async def test_get_system_stats_interface_series_names() -> None:
    graphs = dict(_GRAPHS)
    graphs["interface"] = [
        _graph(["received", "sent"], {"received": 5.0, "sent": None}, identifier="a"),
        _graph(["received", "tx"], {"received": 6.0, "tx": 1.0}, identifier="b"),
        _graph(["received", "rx"], {"received": 9.0, "rx": 3.0}, identifier="c"),
    ]
    result = await _run({NETDATA: _netdata(graphs)}, lambda s: s.get_system_stats())
    assert result.interfaces == {
        "a": InterfaceTraffic(rx_kbps=5.0, tx_kbps=None),
        "b": InterfaceTraffic(rx_kbps=6.0, tx_kbps=1.0),
        "c": InterfaceTraffic(rx_kbps=9.0, tx_kbps=None),
    }


async def test_get_system_stats_interfaces_are_read_only() -> None:
    result = await _run({NETDATA: _netdata(_GRAPHS)}, lambda s: s.get_system_stats())
    with pytest.raises(TypeError):
        result.interfaces["eno9"] = InterfaceTraffic(  # type: ignore[index]
            rx_kbps=None, tx_kbps=None
        )


async def test_get_system_stats_cputemp_ignores_missing_cores() -> None:
    graphs = dict(_GRAPHS)
    graphs["cputemp"] = [_graph(["cpu0", "cpu1"], {"cpu0": None, "cpu1": 38.5})]
    result = await _run({NETDATA: _netdata(graphs)}, lambda s: s.get_system_stats())
    assert result.cpu_temperature == 38.5


@pytest.mark.parametrize(
    ("is_virtual", "queries_cputemp"), [(True, False), (False, True), (None, True)]
)
async def test_get_system_stats_graph_set(
    is_virtual: bool | None, queries_cputemp: bool
) -> None:
    seen: list[str] = []

    def handler(params: list[Any]) -> Any:
        seen.append(params[0])
        return _GRAPHS[params[0]]

    await _run(
        {NETDATA: handler}, lambda s: s.get_system_stats(_info(is_virtual=is_virtual))
    )
    expected = {"cpu", "memory", "arcsize", "interface"}
    if queries_cputemp:
        expected.add("cputemp")
    assert sorted(seen) == sorted(expected)


async def test_get_system_stats_negative_values_are_none() -> None:
    graphs = dict(_GRAPHS)
    graphs["memory"] = [_graph(["available"], {"available": -5.0})]
    graphs["arcsize"] = [_graph(["size"], {"size": -1.0})]
    result = await _run(
        {NETDATA: _netdata(graphs)},
        lambda s: s.get_system_stats(_info(physical_memory=1000)),
    )
    assert result.memory_available is None
    assert result.memory_usage_percent is None
    assert result.arc_size is None


async def test_get_system_info_negative_values_are_none() -> None:
    payload = {
        "hostname": "nas",
        "boottime": {"$date": 0},
        "uptime_seconds": -1,
        "loadavg": [-0.1, 0.2, 0.3],
    }
    result = await _run({"system.info": payload}, lambda s: s.get_system_info())
    assert result.boot_time is None
    assert result.uptime_seconds is None
    assert (result.load_1m, result.load_5m, result.load_15m) == (None, 0.2, 0.3)


async def test_get_system_info_huge_uptime_is_none() -> None:
    result = await _run(
        {"system.info": {"hostname": "nas", "uptime_seconds": 10**400}},
        lambda s: s.get_system_info(),
    )
    assert result.uptime_seconds is None


class _StubClient:
    """Answers ``reporting.netdata_graph`` per graph with a value or an error."""

    def __init__(self, outcomes: dict[str, Any]) -> None:
        self._outcomes = outcomes

    async def call(self, method: str, params: list[Any] | None = None) -> Any:
        assert method == NETDATA
        assert params is not None
        outcome = self._outcomes[params[0]]
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome


def _disconnected() -> TrueNASConnectionClosedError:
    return TrueNASConnectionClosedError("closed", phase="disconnect")


async def test_get_system_stats_prefers_root_cause_over_disconnects() -> None:
    timeout = TrueNASCallTimeoutError("timed out")
    outcomes = {
        "cpu": _disconnected(),
        "memory": _disconnected(),
        "arcsize": timeout,
        "interface": _disconnected(),
        "cputemp": _disconnected(),
    }
    system = TrueNASSystem(cast(TrueNASClient, _StubClient(outcomes)))
    with pytest.raises(TrueNASCallTimeoutError) as excinfo:
        await system.get_system_stats()
    assert excinfo.value is timeout
    assert "while fetching netdata graph 'arcsize'" in excinfo.value.__notes__


async def test_get_system_stats_raises_disconnect_if_only_cause() -> None:
    outcomes: dict[str, Any] = dict(_GRAPHS)
    outcomes["memory"] = _disconnected()
    system = TrueNASSystem(cast(TrueNASClient, _StubClient(outcomes)))
    with pytest.raises(TrueNASConnectionClosedError):
        await system.get_system_stats()


async def test_get_system_stats_cputemp_connection_error_propagates() -> None:
    outcomes: dict[str, Any] = dict(_GRAPHS)
    outcomes["cputemp"] = TrueNASCallTimeoutError("timed out")
    system = TrueNASSystem(cast(TrueNASClient, _StubClient(outcomes)))
    with pytest.raises(TrueNASCallTimeoutError):
        await system.get_system_stats()


@pytest.mark.parametrize("with_time", [True, False])
async def test_get_system_stats_raw_fallback_column_attribution(
    with_time: bool,
) -> None:
    legend = ["received", "sent"]
    graphs = dict(_GRAPHS)
    graphs["interface"] = [
        _raw_graph(
            ["time", *legend] if with_time else legend,
            # Rows of the wrong width (and non-rows) must not enter the mean.
            [[1, 10, 100], [9, 999], [9, 999, 999, 999], "junk", [2, 30, 300]],
            identifier="eno1",
        )
    ]
    result = await _run({NETDATA: _netdata(graphs)}, lambda s: s.get_system_stats())
    assert result.interfaces == {"eno1": InterfaceTraffic(rx_kbps=20.0, tx_kbps=200.0)}


async def test_get_system_stats_raw_fallback_drops_invalid_samples() -> None:
    graphs = dict(_GRAPHS)
    graphs["cpu"] = [
        {
            "legend": ["time", "cpu"],
            "aggregations": {"mean": {"cpu": float("nan")}},
            "data": [[1, True], [2, 10], [3, None], [4, "x"]],
        }
    ]
    result = await _run({NETDATA: _netdata(graphs)}, lambda s: s.get_system_stats())
    assert result.cpu_usage_percent == 10.0


async def test_get_system_stats_keeps_real_zero_means() -> None:
    graphs = dict(_GRAPHS)
    graphs["cpu"] = [
        {
            "legend": ["time", "cpu"],
            "aggregations": {"mean": {"cpu": 0.0}},
            "data": [[1, 50], [2, 50]],
        }
    ]
    graphs["interface"] = [
        _graph(
            ["received", "sent", "rx", "tx"],
            {"received": 0.0, "sent": 0.0, "rx": 5.0, "tx": 6.0},
            identifier="a",
        ),
        _graph(["rx", "tx"], {"rx": 5.0, "tx": 0.0}, identifier="b"),
    ]
    result = await _run({NETDATA: _netdata(graphs)}, lambda s: s.get_system_stats())
    assert result.cpu_usage_percent == 0.0
    assert result.interfaces == {
        "a": InterfaceTraffic(rx_kbps=0.0, tx_kbps=0.0),
        "b": InterfaceTraffic(rx_kbps=5.0, tx_kbps=0.0),
    }


async def test_get_system_stats_call_phase_close_is_root_cause() -> None:
    root = TrueNASConnectionClosedError("send failed", phase="call")
    outcomes: dict[str, Any] = dict(_GRAPHS)
    outcomes["cpu"] = _disconnected()
    outcomes["memory"] = root
    outcomes["arcsize"] = TrueNASCallTimeoutError("later")
    system = TrueNASSystem(cast(TrueNASClient, _StubClient(outcomes)))
    with pytest.raises(TrueNASConnectionClosedError) as excinfo:
        await system.get_system_stats()
    assert excinfo.value is root
    assert excinfo.value.__notes__ == ["while fetching netdata graph 'memory'"]


async def test_get_system_stats_reraises_non_truenas_errors() -> None:
    outcomes: dict[str, Any] = dict(_GRAPHS)
    outcomes["cpu"] = TrueNASCallTimeoutError("timed out")
    outcomes["memory"] = RuntimeError("bug")
    system = TrueNASSystem(cast(TrueNASClient, _StubClient(outcomes)))
    with pytest.raises(RuntimeError, match="bug"):
        await system.get_system_stats()


@pytest.mark.parametrize(
    ("link_state", "expected"),
    [
        ("LINK_STATE_UP", True),
        ("LINK_STATE_DOWN", False),
        ("LINK_STATE_UNKNOWN", None),
        (None, None),
    ],
)
async def test_get_network_interfaces_link_up(
    link_state: str | None, expected: bool | None
) -> None:
    payload = [{"id": "eno1", "state": {"link_state": link_state}}]
    result = await _run(
        {"interface.query": payload}, lambda s: s.get_network_interfaces()
    )
    assert result["eno1"].link_up is expected


async def test_get_network_interfaces_skip_warning_is_bounded(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING, logger="aiotruenas.system")
    payload = [{"name": "x" * 1000}]
    await _run({"interface.query": payload}, lambda s: s.get_network_interfaces())
    (record,) = [r for r in caplog.records if r.name == "aiotruenas.system"]
    assert len(record.getMessage()) < 300


async def test_get_system_info_negative_physmem_is_none() -> None:
    result = await _run(
        {"system.info": {"hostname": "nas", "physmem": -1}},
        lambda s: s.get_system_info(),
    )
    assert result.physical_memory is None


async def test_get_network_interfaces_negative_mtu_is_none() -> None:
    result = await _run(
        {"interface.query": [{"id": "eno1", "mtu": -1500}]},
        lambda s: s.get_network_interfaces(),
    )
    assert result["eno1"].mtu is None


async def test_get_system_stats_negative_rates_are_none() -> None:
    graphs = dict(_GRAPHS)
    graphs["cpu"] = [_graph(["cpu"], {"cpu": -0.5})]
    graphs["cputemp"] = [_graph(["cpu0"], {"cpu0": -5.0})]
    graphs["interface"] = [
        _graph(["received", "sent"], {"received": -1.0, "sent": 2.0}, identifier="a")
    ]
    result = await _run({NETDATA: _netdata(graphs)}, lambda s: s.get_system_stats())
    assert result.cpu_usage_percent is None
    # Sub-zero temperatures are physically possible and passed through.
    assert result.cpu_temperature == -5.0
    assert result.interfaces == {"a": InterfaceTraffic(rx_kbps=None, tx_kbps=2.0)}


async def test_get_system_stats_overflowing_raw_mean_is_none() -> None:
    graphs = dict(_GRAPHS)
    graphs["arcsize"] = [_raw_graph(["time", "size"], [[1, 1e308], [2, 1e308]])]
    result = await _run({NETDATA: _netdata(graphs)}, lambda s: s.get_system_stats())
    assert result.arc_size is None


def test_system_stats_copies_interfaces() -> None:
    source = {"eno1": InterfaceTraffic(rx_kbps=1.0, tx_kbps=2.0)}
    stats = SystemStats(
        cpu_usage_percent=None,
        cpu_temperature=None,
        memory_available=None,
        memory_usage_percent=None,
        arc_size=None,
        interfaces=source,
    )
    source["eno2"] = InterfaceTraffic(rx_kbps=None, tx_kbps=None)
    assert set(stats.interfaces) == {"eno1"}
    with pytest.raises(TypeError):
        stats.interfaces["eno3"] = source["eno2"]  # type: ignore[index]


@pytest.mark.parametrize(("rx_name", "tx_name"), [("received", "sent"), ("rx", "tx")])
async def test_get_system_stats_negative_tx_is_none(rx_name: str, tx_name: str) -> None:
    graphs = dict(_GRAPHS)
    graphs["interface"] = [
        _graph([rx_name, tx_name], {rx_name: 1.0, tx_name: -2.0}, identifier="a"),
        _graph([rx_name, tx_name], {rx_name: -1.0, tx_name: 0.0}, identifier="b"),
    ]
    result = await _run({NETDATA: _netdata(graphs)}, lambda s: s.get_system_stats())
    assert result.interfaces == {
        "a": InterfaceTraffic(rx_kbps=1.0, tx_kbps=None),
        "b": InterfaceTraffic(rx_kbps=None, tx_kbps=0.0),
    }


async def test_get_system_stats_overflow_only_drops_affected_series() -> None:
    graphs = dict(_GRAPHS)
    graphs["interface"] = [
        _raw_graph(
            ["time", "received", "sent"],
            [[1, 1e308, 5.0], [2, 1e308, 7.0]],
            identifier="a",
        )
    ]
    result = await _run({NETDATA: _netdata(graphs)}, lambda s: s.get_system_stats())
    assert result.interfaces == {"a": InterfaceTraffic(rx_kbps=None, tx_kbps=6.0)}


async def test_zero_counts_are_kept() -> None:
    info = await _run(
        {"system.info": {"hostname": "nas", "physmem": 0}},
        lambda s: s.get_system_info(),
    )
    interfaces = await _run(
        {"interface.query": [{"id": "lo", "mtu": 0}]},
        lambda s: s.get_network_interfaces(),
    )
    assert info.physical_memory == 0
    assert interfaces["lo"].mtu == 0
