"""Stateless, typed access to TrueNAS system, network and stats data.

``TrueNASSystem`` composes a :class:`~aiotruenas.client.TrueNASClient` and
returns the immutable dataclasses from :mod:`aiotruenas.models`. Unlike
:class:`~aiotruenas.domain.state.TrueNASState` it keeps no cache and never
falls back to previous values: an RPC failure propagates as the client's own
:class:`~aiotruenas.exceptions.TrueNASError` subclass, and a response whose
top-level shape is wrong raises
:class:`~aiotruenas.exceptions.TrueNASMalformedResponseError`. Only an
individual field missing from an otherwise valid response becomes ``None``.
"""

from __future__ import annotations

import asyncio
import math
from collections import defaultdict
from collections.abc import Mapping
from datetime import UTC, datetime
from logging import getLogger
from types import MappingProxyType
from typing import Any

from .client import TrueNASClient
from .domain._helpers import _is_virtual_machine
from .exceptions import (
    TrueNASCallError,
    TrueNASConnectionClosedError,
    TrueNASConnectionError,
    TrueNASError,
    TrueNASMalformedResponseError,
)
from .models import (
    InterfaceTraffic,
    NetworkInterface,
    SystemInfo,
    SystemStats,
)

_LOGGER = getLogger(__name__)

_SYSTEM_INFO_METHOD = "system.info"
_SYSTEM_ID_METHOD = "system.global.id"
_INTERFACE_QUERY_METHOD = "interface.query"
_NETDATA_GRAPH_METHOD = "reporting.netdata_graph"

_GRAPH_CPU = "cpu"
_GRAPH_CPUTEMP = "cputemp"
_GRAPH_MEMORY = "memory"
_GRAPH_ARCSIZE = "arcsize"
_GRAPH_INTERFACE = "interface"

# Graphs whose absence on the server ("... is not a valid graph plugin.",
# errname ENOENT) only blanks their own field instead of failing the whole
# get_system_stats() call. Every other error still propagates. Hardware
# without a temperature sensor is not an error at all: TrueNAS then returns
# the graph with no series.
_OPTIONAL_GRAPHS = frozenset({_GRAPH_CPUTEMP})
_ERRNAME_NO_SUCH_GRAPH = "ENOENT"
# Errors that concern one graph only: TrueNAS rejected that graph call, or its
# response has the wrong shape. get_system_stats(partial=True) reports these
# per graph; connection, timeout and authentication errors always propagate.
_PER_GRAPH_ERRORS = (TrueNASCallError, TrueNASMalformedResponseError)

# Netdata aggregation window, relative to now: the most recent samples are
# skipped since netdata may not have collected them yet.
_GRAPH_WINDOW_START_SECONDS = 90
_GRAPH_WINDOW_END_SECONDS = 30

_TIME_SERIES = "time"
# Only these two states are definite; anything else (e.g. LINK_STATE_UNKNOWN)
# leaves link_up as None.
_LINK_UP_BY_STATE = {"LINK_STATE_UP": True, "LINK_STATE_DOWN": False}
# Cap on how much of the skipped entries is echoed into a log line.
_LOG_REPR_MAX = 200


def _malformed(method: str, detail: str) -> TrueNASMalformedResponseError:
    return TrueNASMalformedResponseError(f"Malformed '{method}' response: {detail}")


def _optional_str(value: object) -> str | None:
    """Return ``value`` if it is a non-blank string, else ``None``."""
    return value if isinstance(value, str) and value.strip() else None


def _optional_count(value: object) -> int | None:
    """Return ``value`` as an int if it is a non-negative whole number.

    Accepts an int or an integral float (not a bool); an int beyond float
    range is rejected as corrupt rather than feeding a percentage.
    """
    number = _finite_float(value)
    if number is None or number < 0 or not number.is_integer():
        return None
    return value if isinstance(value, int) else int(number)


def _finite_float(value: object) -> float | None:
    """Return ``value`` as a float if it is a finite int/float (not a bool)."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        result = float(value)
    except OverflowError:
        return None
    return result if math.isfinite(result) else None


def _non_negative(value: float | None) -> float | None:
    return None if value is None or value < 0 else value


def _optional_bytes(value: float | None) -> int | None:
    value = _non_negative(value)
    return None if value is None else round(value)


def _parse_boot_time(value: object) -> datetime | None:
    """Parse TrueNAS's JSON datetime encoding, ``{"$date": <epoch millis>}``."""
    millis = _finite_float(value.get("$date")) if isinstance(value, Mapping) else None
    if millis is None or millis <= 0:
        return None
    try:
        return datetime.fromtimestamp(millis / 1000, tz=UTC)
    except (OverflowError, OSError, ValueError):
        return None


def _parse_loadavg(value: object) -> tuple[float | None, float | None, float | None]:
    if not isinstance(value, list) or len(value) != 3:
        return None, None, None
    load_1m, load_5m, load_15m = (_non_negative(_finite_float(v)) for v in value)
    return load_1m, load_5m, load_15m


def _parse_system_info(raw: Mapping[str, Any]) -> SystemInfo:
    manufacturer = _optional_str(raw.get("system_manufacturer"))
    product = _optional_str(raw.get("system_product"))
    load_1m, load_5m, load_15m = _parse_loadavg(raw.get("loadavg"))
    return SystemInfo(
        hostname=_optional_str(raw.get("hostname")),
        version=_optional_str(raw.get("version")),
        manufacturer=manufacturer,
        product=product,
        serial=_optional_str(raw.get("system_serial")),
        physical_memory=_optional_count(raw.get("physmem")),
        boot_time=_parse_boot_time(raw.get("boottime")),
        uptime_seconds=_non_negative(_finite_float(raw.get("uptime_seconds"))),
        load_1m=load_1m,
        load_5m=load_5m,
        load_15m=load_15m,
        is_virtual=(
            None
            if manufacturer is None and product is None
            else _is_virtual_machine(
                manufacturer and manufacturer.strip(), product and product.strip()
            )
        ),
    )


def _parse_interface(entry: Mapping[str, Any]) -> NetworkInterface | None:
    interface_id = _optional_str(entry.get("id"))
    if interface_id is None:
        return None
    state = entry.get("state")
    if not isinstance(state, Mapping):
        state = {}
    link_state = _optional_str(state.get("link_state"))
    return NetworkInterface(
        id=interface_id,
        name=_optional_str(entry.get("name")),
        description=_optional_str(entry.get("description")),
        mtu=_optional_count(entry.get("mtu")),
        link_state=link_state,
        link_up=_LINK_UP_BY_STATE.get(link_state) if link_state else None,
        active_media_type=_optional_str(state.get("active_media_type")),
        active_media_subtype=_optional_str(state.get("active_media_subtype")),
        link_address=_optional_str(state.get("link_address")),
    )


def _raw_series_means(data: object, legend: list[Any]) -> dict[str, float]:
    """Average each legend series over a netdata item's raw ``data`` points.

    If the legend lists the timestamp column (``"time"``, at any position),
    each point is positional to the legend; otherwise points are
    ``[timestamp, v1, ..., vN]``. A point whose width does not match is
    ignored rather than risking a value being attributed to the wrong series.
    """
    if not isinstance(data, list):
        return {}
    has_time = _TIME_SERIES in legend
    width = len(legend) if has_time else len(legend) + 1
    samples: defaultdict[str, list[float]] = defaultdict(list)
    for point in data:
        if not isinstance(point, list) or len(point) != width:
            continue
        for name, sample in zip(legend, point if has_time else point[1:], strict=True):
            value = _finite_float(sample)
            if isinstance(name, str) and name != _TIME_SERIES and value is not None:
                samples[name].append(value)
    means = {name: sum(values) / len(values) for name, values in samples.items()}
    # Individually finite samples can still overflow their sum.
    return {name: mean for name, mean in means.items() if math.isfinite(mean)}


def _series_means(item: Mapping[str, Any]) -> dict[str, float]:
    """Return one netdata graph item's mean per legend series.

    ``item`` has passed :func:`_graph_shape_error`. Prefers
    ``aggregations.mean``; falls back to the raw ``data`` points for a series
    it lacks -- TrueNAS's netdata backend returns empty aggregations for an
    all-zero series, which would otherwise turn a real 0 into ``None``.
    Without a legend the series are the ``aggregations.mean`` keys and there
    is no raw fallback (raw points cannot be attributed to series).
    """
    mean = _aggregated_means(item)
    if "legend" not in item:
        return {
            name: value
            for name, value in ((n, _finite_float(v)) for n, v in mean.items())
            if isinstance(name, str) and name != _TIME_SERIES and value is not None
        }
    legend: list[Any] = item["legend"]
    raw_means: dict[str, float] | None = None
    means: dict[str, float] = {}
    for name in legend:
        if not isinstance(name, str) or name == _TIME_SERIES:
            continue
        value = _finite_float(mean.get(name))
        if value is None:
            if raw_means is None:
                raw_means = _raw_series_means(item.get("data"), legend)
            value = raw_means.get(name)
        if value is not None:
            means[name] = value
    return means


def _first_item_means(graph_data: list[Any]) -> dict[str, float]:
    """Series means of a single-item graph (cpu, cputemp, memory, arcsize)."""
    return _series_means(graph_data[0]) if graph_data else {}


def _interface_traffic(graph_data: list[Any]) -> Mapping[str, InterfaceTraffic]:
    """Map each netdata "interface" graph item to its rx/tx (kilobits/s).

    Netdata names the series "received"/"sent"; "rx"/"tx" are accepted too.
    """
    traffic: dict[str, InterfaceTraffic] = {}
    for item in graph_data:
        means = _series_means(item)
        traffic[item["identifier"]] = InterfaceTraffic(
            rx_kbps=_first_rate(means, "received", "rx"),
            tx_kbps=_first_rate(means, "sent", "tx"),
        )
    return MappingProxyType(traffic)


def _first_rate(means: Mapping[str, float], *names: str) -> float | None:
    """Return the first usable (non-negative) reading among ``names``."""
    return next(
        (
            rate
            for name in names
            if (rate := _non_negative(means.get(name))) is not None
        ),
        None,
    )


def _aggregated_means(item: Mapping[str, Any]) -> Mapping[Any, Any]:
    aggregations = item.get("aggregations")
    mean = aggregations.get("mean") if isinstance(aggregations, Mapping) else None
    return mean if isinstance(mean, Mapping) else {}


def _item_shape_error(graph: str, item: object) -> str | None:
    if not isinstance(item, Mapping):
        return "is not an object"
    if "legend" in item:
        if not isinstance(item["legend"], list):
            return "has a non-list legend"
    elif not _aggregated_means(item):
        return "has neither a legend nor aggregations.mean"
    if graph == _GRAPH_INTERFACE and _optional_str(item.get("identifier")) is None:
        return "has no identifier"
    return None


def _graph_shape_error(graph: str, graph_data: object) -> str | None:
    """Describe why a netdata graph response is malformed, or ``None`` if not.

    The response must be a list of objects with a ``legend`` list or, failing
    that, a non-empty ``aggregations.mean``; ``interface`` items also need a
    string ``identifier``. An empty list (no data in the window) is fine and
    yields ``None`` readings.
    """
    if not isinstance(graph_data, list):
        return f"expected a list for graph '{graph}': {graph_data!r:.{_LOG_REPR_MAX}}"
    for item in graph_data:
        if (problem := _item_shape_error(graph, item)) is not None:
            return f"graph '{graph}' item {problem}: {item!r:.{_LOG_REPR_MAX}}"
    return None


def _memory_usage_percent(total: int | None, available: int | None) -> float | None:
    """Return used memory in percent; ``None`` if either figure is unusable."""
    if total is None or available is None or total <= 0:
        return None
    if not 0 <= available <= total:
        return None
    return 100 * (total - available) / total


def _is_disconnect_symptom(error: TrueNASError) -> bool:
    """Return True for errors caused by a sibling call closing the socket.

    That is a ``phase="disconnect"`` close for an in-flight call, or the
    plain ``TrueNASConnectionError`` ("not connected") that ``call()`` raises
    for a call that only reached the send step after the close.
    """
    if type(error) is TrueNASConnectionError:
        return True
    return isinstance(error, TrueNASConnectionClosedError) and (
        error.phase == "disconnect"
    )


def _is_absent_optional_graph(graph: str, result: object) -> bool:
    """Whether ``result`` is TrueNAS reporting an optional graph as unknown."""
    return (
        graph in _OPTIONAL_GRAPHS
        and isinstance(result, TrueNASCallError)
        and result.errname == _ERRNAME_NO_SUCH_GRAPH
    )


def _primary_error(errors: list[tuple[str, TrueNASError]]) -> TrueNASError:
    """Pick the error to raise from several failed graph calls.

    A timeout closes the shared WebSocket, failing every other in-flight
    call with a ``phase="disconnect"`` connection-closed error (or "not
    connected" for one that had not sent yet); those are symptoms, so the
    root cause is preferred. The graph name is attached as
    a note and the remaining errors are logged at DEBUG.
    """
    graph, error = next(
        ((name, err) for name, err in errors if not _is_disconnect_symptom(err)),
        errors[0],
    )
    for other_graph, other in errors:
        if other is not error:
            _LOGGER.debug("netdata graph '%s' also failed: %r", other_graph, other)
    error.add_note(f"while fetching netdata graph '{graph}'")
    return error


def _reportable_failures(
    errors: list[tuple[str, TrueNASError]], *, partial: bool
) -> dict[str, TrueNASError]:
    """Return the graph failures partial mode reports, or raise the primary error.

    Only per-graph errors are reported. In partial mode a connection-wide
    error among them is raised instead, since it -- not a per-graph error
    alongside it -- is what failed the call; strict mode raises any error.
    Either way the root cause is preferred (see ``_primary_error``). Per-graph
    errors are logged at DEBUG in partial mode, also when one is raised.
    """
    if not errors:
        return {}
    if not partial:
        raise _primary_error(errors)
    fatal: list[tuple[str, TrueNASError]] = []
    for graph, err in errors:
        if isinstance(err, _PER_GRAPH_ERRORS):
            _LOGGER.debug("netdata graph '%s' failed: %r", graph, err)
        else:
            fatal.append((graph, err))
    if fatal:
        raise _primary_error(fatal)
    return dict(errors)


class TrueNASSystem:
    """Typed, stateless reader for TrueNAS system, network and stats data.

    Every method issues fresh RPC call(s) and returns an immutable snapshot;
    errors are raised, never swallowed (see the module docstring).
    """

    def __init__(self, client: TrueNASClient) -> None:
        self._client = client

    async def get_system_id(self) -> str:
        """Return the stable, per-installation UUID (``system.global.id``).

        Unlike the hostname or IP address it never changes, which makes it
        suitable as a unique device identifier.
        """
        raw = await self._client.call(_SYSTEM_ID_METHOD)
        if not isinstance(raw, str) or not (system_id := raw.strip()):
            raise _malformed(_SYSTEM_ID_METHOD, f"expected a non-empty string: {raw!r}")
        # Stripped, so the same installation always maps to one identifier.
        return system_id

    async def get_system_info(self) -> SystemInfo:
        """Return hostname, version, hardware, boot time and load (``system.info``)."""
        raw = await self._client.call(_SYSTEM_INFO_METHOD)
        if not isinstance(raw, Mapping) or not raw:
            raise _malformed(
                _SYSTEM_INFO_METHOD, f"expected a non-empty object: {raw!r}"
            )
        return _parse_system_info(raw)

    async def get_network_interfaces(self) -> dict[str, NetworkInterface]:
        """Return all network interfaces (``interface.query``), keyed by id.

        Entries without a usable string ``id`` are skipped; one warning per
        call lists them.
        """
        raw = await self._client.call(_INTERFACE_QUERY_METHOD)
        if not isinstance(raw, list):
            raise _malformed(_INTERFACE_QUERY_METHOD, f"expected a list: {raw!r}")
        interfaces: dict[str, NetworkInterface] = {}
        skipped: list[object] = []
        for entry in raw:
            interface = _parse_interface(entry) if isinstance(entry, Mapping) else None
            if interface is None:
                skipped.append(entry)
            else:
                interfaces[interface.id] = interface
        if skipped:
            _LOGGER.warning(
                "Skipped %d '%s' entries without a usable id: %.*r",
                len(skipped),
                _INTERFACE_QUERY_METHOD,
                _LOG_REPR_MAX,
                skipped,
            )
        return interfaces

    async def get_system_stats(
        self, system_info: SystemInfo | None = None, *, partial: bool = False
    ) -> SystemStats:
        """Return current CPU, memory, ARC and per-interface throughput readings.

        Queries the ``cpu``, ``memory``, ``arcsize``, ``interface`` and
        ``cputemp`` netdata graphs concurrently, each averaged over a
        one-minute window. Pass the latest :meth:`get_system_info` result as
        ``system_info`` to

        * compute ``memory_usage_percent`` (needs the physical memory size), and
        * skip the ``cputemp`` graph on a virtual machine, which has no CPU
          temperature sensor (``cpu_temperature`` is then ``None``).

        Any failing graph fails the whole call with its error (the root cause
        is preferred over the connection-closed errors a timeout triggers in
        the other in-flight calls). The one exception is ``cputemp``: if
        TrueNAS does not know that graph at all (JSON-RPC error with errname
        ``ENOENT``), only ``cpu_temperature`` is ``None``. On hardware without
        a temperature sensor TrueNAS returns the graph without series, which
        also yields ``None``. A graph response of the wrong shape (not a list
        of items with a ``legend`` list, or an ``interface`` item without
        ``identifier``) raises
        :class:`~aiotruenas.exceptions.TrueNASMalformedResponseError`; a
        well-formed one that lacks a series yields ``None`` for that field.

        With ``partial=True`` a graph that fails on its own -- TrueNAS rejects
        the call (:class:`~aiotruenas.exceptions.TrueNASCallError`) or its
        response has the wrong shape -- no longer fails the call: its fields
        are ``None`` and the error is reported in ``failed_graphs``, keyed by
        graph name (``cpu`` -> ``cpu_usage_percent``, ``cputemp`` ->
        ``cpu_temperature``, ``memory`` -> ``memory_available`` and
        ``memory_usage_percent``, ``arcsize`` -> ``arc_size``, ``interface`` ->
        an empty ``interfaces``). Check ``failed_graphs`` to tell a failed
        graph from one that just had no data. A permission error on a graph
        call is a per-graph error too. Connection, timeout and authentication
        errors concern the whole connection, not one graph, and are raised in
        both modes.
        """
        graphs = [_GRAPH_CPU, _GRAPH_MEMORY, _GRAPH_ARCSIZE, _GRAPH_INTERFACE]
        if system_info is None or system_info.is_virtual is not True:
            graphs.append(_GRAPH_CPUTEMP)
        data, failed_graphs = await self._fetch_graphs(graphs, partial=partial)

        memory_available = _optional_bytes(
            _first_item_means(data.get(_GRAPH_MEMORY, [])).get("available")
        )
        core_temperatures = _first_item_means(data.get(_GRAPH_CPUTEMP, [])).values()
        return SystemStats(
            cpu_usage_percent=_non_negative(
                _first_item_means(data.get(_GRAPH_CPU, [])).get("cpu")
            ),
            cpu_temperature=max(core_temperatures, default=None),
            memory_available=memory_available,
            memory_usage_percent=_memory_usage_percent(
                None if system_info is None else system_info.physical_memory,
                memory_available,
            ),
            arc_size=_optional_bytes(
                _first_item_means(data.get(_GRAPH_ARCSIZE, [])).get("size")
            ),
            interfaces=_interface_traffic(data.get(_GRAPH_INTERFACE, [])),
            failed_graphs=failed_graphs,
        )

    async def _fetch_graphs(
        self, graphs: list[str], *, partial: bool
    ) -> tuple[dict[str, list[Any]], dict[str, TrueNASError]]:
        """Fetch netdata graphs concurrently; see get_system_stats() for errors.

        Returns the graph data and, in partial mode, the per-graph failures. A
        tolerated optional graph and a reported failure are absent from the data.
        """
        now = int(datetime.now(UTC).timestamp())
        query = {
            "start": now - _GRAPH_WINDOW_START_SECONDS,
            "end": now - _GRAPH_WINDOW_END_SECONDS,
            "aggregate": True,
        }
        results = await asyncio.gather(
            *(self._client.call(_NETDATA_GRAPH_METHOD, [g, query]) for g in graphs),
            return_exceptions=True,
        )
        data: dict[str, list[Any]] = {}
        errors: list[tuple[str, TrueNASError]] = []
        for graph, result in zip(graphs, results, strict=True):
            if _is_absent_optional_graph(graph, result):
                _LOGGER.debug(
                    "Optional netdata graph '%s' not available on server: %s",
                    graph,
                    result,
                )
            elif isinstance(result, TrueNASError):
                errors.append((graph, result))
            elif isinstance(result, BaseException):
                # Cancellation or a programming error: not a graph failure.
                raise result
            elif (shape_error := _graph_shape_error(graph, result)) is not None:
                errors.append((graph, _malformed(_NETDATA_GRAPH_METHOD, shape_error)))
            else:
                data[graph] = result
        return data, _reportable_failures(errors, partial=partial)
