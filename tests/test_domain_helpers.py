"""Unit tests for ``aiotruenas.domain._helpers``.

Ported from ``truenas_ce``'s ``tests/test_coordinator.py`` (the pure
computational helpers this module contains are a verbatim port of that
integration's ``coordinator.py``). Pure-function tests only.
"""

from __future__ import annotations

import pytest

from aiotruenas.domain._helpers import (
    _accumulate_vdev_errors,
    _aggregate_topology_errors,
    _arc_value,
    _as_int,
    _disk_temps_from_graph_data,
    _is_virtual_machine,
    _median,
    _netdata_interface_throughput,
    _netdata_max_mean,
    _netdata_mean_value,
    _netdata_named_means,
    _stable_uptime_epoch,
    _stat_name_similar,
    _to_int,
    _ups_value,
)


# ---------------------------
#   _stat_name_similar
# ---------------------------
@pytest.mark.parametrize(
    ("a", "b", "expected"),
    [
        ("cpu", "cpu", False),
        ("arc_size", "arcsize", True),
        ("cputemp", "cpu", True),
        ("cpu", "cputemp", True),
        ("memroy", "memory", True),
        ("load", "interface", False),
    ],
)
def test_stat_name_similar(a: str, b: str, expected: bool) -> None:
    assert _stat_name_similar(a, b) == expected


# ---------------------------
#   _median
# ---------------------------
def test_median_odd_count() -> None:
    assert _median([3.0, 1.0, 2.0]) == pytest.approx(2.0)


def test_median_even_count() -> None:
    assert _median([1.0, 2.0, 3.0, 4.0]) == pytest.approx(2.5)


def test_median_single_value() -> None:
    assert _median([42.0]) == pytest.approx(42.0)


def test_median_empty_list_raises_index_error() -> None:
    """Empty input is outside _median's contract (docstring: non-empty list);
    its only caller guards with a non-empty check. Lock in the current
    fail-loud behaviour instead of silently returning a value."""
    with pytest.raises(IndexError):
        _median([])


# ---------------------------
#   _as_int / _to_int
# ---------------------------
def test_as_int_returns_int_unchanged() -> None:
    assert _as_int(5) == 5


def test_as_int_returns_zero_for_non_int() -> None:
    assert _as_int("5") == 0
    assert _as_int(None) == 0
    assert _as_int(1.5) == 0


def test_as_int_rejects_bool() -> None:
    """bool is a subclass of int in Python; True/False are not valid counts."""
    assert _as_int(True) == 0
    assert _as_int(False) == 0


def test_to_int_parses_numeric_string() -> None:
    assert _to_int("48") == 48


def test_to_int_falls_back_to_default_on_invalid() -> None:
    assert _to_int("not-a-number", default=7) == 7
    assert _to_int(None, default=7) == 7


def test_to_int_rejects_bool() -> None:
    assert _to_int(True, default=7) == 7
    assert _to_int(False, default=7) == 7


def test_to_int_falls_back_to_default_on_overflow() -> None:
    """int(float("inf")) raises OverflowError, not ValueError/TypeError."""
    assert _to_int(float("inf"), default=7) == 7
    assert _to_int(float("-inf"), default=7) == 7


# ---------------------------
#   _accumulate_vdev_errors / _aggregate_topology_errors
# ---------------------------
def test_accumulate_vdev_errors_leaf_disk() -> None:
    totals = {"read": 0, "write": 0, "checksum": 0}
    vdev = {"stats": {"read_errors": 1, "write_errors": 2, "checksum_errors": 3}}
    _accumulate_vdev_errors(vdev, totals)
    assert totals == {"read": 1, "write": 2, "checksum": 3}


def test_accumulate_vdev_errors_recurses_into_children_only() -> None:
    """A mirror vdev's own stats must not be double-counted on top of its disks."""
    totals = {"read": 0, "write": 0, "checksum": 0}
    mirror = {
        "stats": {"read_errors": 99, "write_errors": 99, "checksum_errors": 99},
        "children": [
            {"stats": {"read_errors": 1, "write_errors": 0, "checksum_errors": 0}},
            {"stats": {"read_errors": 0, "write_errors": 1, "checksum_errors": 0}},
        ],
    }
    _accumulate_vdev_errors(mirror, totals)
    assert totals == {"read": 1, "write": 1, "checksum": 0}


def test_accumulate_vdev_errors_ignores_non_dict() -> None:
    totals = {"read": 0, "write": 0, "checksum": 0}
    _accumulate_vdev_errors("not-a-dict", totals)
    assert totals == {"read": 0, "write": 0, "checksum": 0}


def test_aggregate_topology_errors_sums_all_categories() -> None:
    topology = {
        "data": [
            {"stats": {"read_errors": 1, "write_errors": 0, "checksum_errors": 0}}
        ],
        "cache": [
            {"stats": {"read_errors": 0, "write_errors": 2, "checksum_errors": 0}}
        ],
    }
    assert _aggregate_topology_errors(topology) == (1, 2, 0)


def test_aggregate_topology_errors_non_dict_returns_zeros() -> None:
    assert _aggregate_topology_errors(None) == (0, 0, 0)


# ---------------------------
#   _netdata_mean_value / _arc_value / _ups_value
# ---------------------------
def test_netdata_mean_value_computes_mean() -> None:
    graph_data = [{"aggregations": {"mean": {"a": 1.0, "b": 3.0}}}]
    assert _netdata_mean_value(graph_data) == pytest.approx(2.0)


def test_netdata_mean_value_returns_none_for_empty_list() -> None:
    assert _netdata_mean_value([]) is None


def test_netdata_mean_value_returns_none_for_malformed_item() -> None:
    assert _netdata_mean_value(["not-a-dict"]) is None
    assert _netdata_mean_value([{"aggregations": {"mean": "not-a-dict"}}]) is None
    assert _netdata_mean_value([{"aggregations": {"mean": {}}}]) is None


def test_netdata_mean_value_returns_none_for_non_dict_aggregations() -> None:
    """A non-dict `aggregations` value (e.g. None) must not raise AttributeError."""
    assert _netdata_mean_value([{"aggregations": None}]) is None
    assert _netdata_mean_value([{}]) is None


def test_netdata_mean_value_excludes_bool_values() -> None:
    """bool is a subclass of int; True/False must not be averaged in as 1/0."""
    graph_data = [{"aggregations": {"mean": {"a": True, "b": 4.0}}}]
    assert _netdata_mean_value(graph_data) == pytest.approx(4.0)

    graph_data_only_bool = [{"aggregations": {"mean": {"a": True, "b": False}}}]
    assert _netdata_mean_value(graph_data_only_bool) is None


def test_arc_value_delegates_to_netdata_mean_value() -> None:
    graph_data = [{"aggregations": {"mean": {"a": 10.0}}}]
    assert _arc_value(graph_data) == pytest.approx(10.0)


def test_ups_value_delegates_to_netdata_mean_value() -> None:
    graph_data = [{"aggregations": {"mean": {"a": 5.0, "b": 15.0}}}]
    assert _ups_value(graph_data) == pytest.approx(10.0)


# ---------------------------
#   _disk_temps_from_graph_data
# ---------------------------
def test_disk_temps_from_graph_data_computes_median_per_disk() -> None:
    graph_data = [
        {"identifier": "disk1", "aggregations": {"mean": {"a": 30.0, "b": 40.0}}},
        {"identifier": "disk2", "aggregations": {"mean": {"a": 50.0}}},
    ]
    assert _disk_temps_from_graph_data(graph_data) == {"disk1": 35.0, "disk2": 50.0}


def test_disk_temps_from_graph_data_discards_out_of_range_values() -> None:
    graph_data = [{"identifier": "disk1", "aggregations": {"mean": {"a": 150.0}}}]
    assert _disk_temps_from_graph_data(graph_data) == {}


def test_disk_temps_from_graph_data_excludes_bool_values() -> None:
    """bool is a subclass of int; True must not be read as a 1 degC temperature."""
    graph_data = [{"identifier": "disk1", "aggregations": {"mean": {"a": True}}}]
    assert _disk_temps_from_graph_data(graph_data) == {}


def test_disk_temps_from_graph_data_ignores_malformed_entries() -> None:
    graph_data = [
        "not-a-dict",
        {"identifier": None, "aggregations": {"mean": {"a": 30.0}}},
        {"identifier": "disk1", "aggregations": "not-a-dict"},
        {"identifier": "disk2"},
    ]
    assert _disk_temps_from_graph_data(graph_data) == {}


# ---------------------------
#   _netdata_named_means
# ---------------------------
def test_netdata_named_means_extracts_only_requested_legend_entries() -> None:
    graph_data = [
        {
            "legend": ["shortterm", "midterm", "longterm"],
            "aggregations": {
                "mean": {"shortterm": 0.5, "midterm": 0.75, "longterm": 1.0}
            },
        }
    ]
    assert _netdata_named_means(graph_data, ("shortterm", "longterm")) == {
        "shortterm": 0.5,
        "longterm": 1.0,
    }


def test_netdata_named_means_omits_present_legend_entry_with_missing_value() -> None:
    """A legend entry with no matching mean value is omitted, not zeroed.

    Callers rely on the name being absent (rather than defaulted to 0.0) to
    tell a malformed reading apart from a legitimately-reported zero and
    leave their previous cached value untouched.
    """
    graph_data = [{"legend": ["cpu"], "aggregations": {"mean": {}}}]
    assert _netdata_named_means(graph_data, ("cpu",)) == {}


def test_netdata_named_means_omits_name_absent_from_legend() -> None:
    graph_data = [
        {"legend": ["shortterm"], "aggregations": {"mean": {"shortterm": 1.0}}}
    ]
    assert _netdata_named_means(graph_data, ("shortterm", "midterm")) == {
        "shortterm": 1.0
    }


def test_netdata_named_means_returns_empty_dict_for_malformed_response() -> None:
    assert _netdata_named_means(None, ("cpu",)) == {}
    assert _netdata_named_means([], ("cpu",)) == {}
    assert _netdata_named_means(["not-a-dict"], ("cpu",)) == {}
    assert _netdata_named_means([{"aggregations": {"mean": {}}}], ("cpu",)) == {}
    assert _netdata_named_means([{"legend": ["cpu"]}], ("cpu",)) == {}


def test_netdata_named_means_excludes_bool_values() -> None:
    graph_data = [{"legend": ["cpu"], "aggregations": {"mean": {"cpu": True}}}]
    assert _netdata_named_means(graph_data, ("cpu",)) == {}


# ---------------------------
#   _netdata_max_mean
# ---------------------------
def test_netdata_max_mean_returns_highest_series_value() -> None:
    graph_data = [{"aggregations": {"mean": {"core0": 40.0, "core1": 55.5}}}]
    assert _netdata_max_mean(graph_data) == pytest.approx(55.5)


def test_netdata_max_mean_returns_none_for_malformed_response() -> None:
    assert _netdata_max_mean(None) is None
    assert _netdata_max_mean([]) is None
    assert _netdata_max_mean(["not-a-dict"]) is None
    assert _netdata_max_mean([{"aggregations": {"mean": {}}}]) is None


def test_netdata_max_mean_excludes_bool_values() -> None:
    graph_data = [{"aggregations": {"mean": {"a": True, "b": 30.0}}}]
    assert _netdata_max_mean(graph_data) == pytest.approx(30.0)


# ---------------------------
#   _netdata_interface_throughput
# ---------------------------
def test_netdata_interface_throughput_converts_kilobits_to_kibibytes() -> None:
    graph_data = [
        {
            "identifier": "eno1",
            "legend": ["received", "sent"],
            "aggregations": {"mean": {"received": 8192.0, "sent": 4096.0}},
        }
    ]
    assert _netdata_interface_throughput(graph_data) == {
        "eno1": {"rx": 1000.0, "tx": 500.0}
    }


def test_netdata_interface_throughput_omits_missing_series() -> None:
    """A present-but-valueless series is omitted, not zeroed.

    ``get_systemstats()`` applies this via ``dict.update()``, so an omitted
    key leaves the interface's previously cached value for that key
    untouched instead of resetting it to zero.
    """
    graph_data = [
        {"identifier": "eno1", "legend": ["received"], "aggregations": {"mean": {}}}
    ]
    assert _netdata_interface_throughput(graph_data) == {"eno1": {}}


def test_netdata_interface_throughput_omits_malformed_item() -> None:
    graph_data = [{"identifier": "eno1"}]
    assert _netdata_interface_throughput(graph_data) == {"eno1": {}}


def test_netdata_interface_throughput_skips_entries_without_identifier() -> None:
    graph_data = ["not-a-dict", {"legend": [], "aggregations": {}}]
    assert _netdata_interface_throughput(graph_data) == {}


def test_netdata_interface_throughput_returns_empty_dict_for_malformed_response() -> (
    None
):
    assert _netdata_interface_throughput(None) == {}


# ---------------------------
#   _is_virtual_machine
# ---------------------------
@pytest.mark.parametrize(
    ("manufacturer", "product", "expected"),
    [
        ("QEMU", "Standard PC", True),
        ("iXsystems", "VirtualBox", True),
        ("iXsystems", "TrueNAS Mini", False),
        ("unknown", "unknown", False),
    ],
)
def test_is_virtual_machine(manufacturer: str, product: str, expected: bool) -> None:
    assert _is_virtual_machine(manufacturer, product) is expected


# ---------------------------
#   _stable_uptime_epoch
# ---------------------------
def test_stable_uptime_epoch_adopts_new_epoch_on_first_run() -> None:
    assert _stable_uptime_epoch(0, 100, 100_100) == 100_000


def test_stable_uptime_epoch_ignores_small_jitter() -> None:
    """A few seconds of poll timing drift must not move the stored epoch."""
    previous = 100_000
    # 3 seconds later, uptime_seconds also 3 higher -- same true boot time.
    assert _stable_uptime_epoch(previous, 103, 100_103) == previous


def test_stable_uptime_epoch_adopts_new_epoch_past_tolerance() -> None:
    previous = 100_000
    assert _stable_uptime_epoch(previous, 0, 100_500) == 100_500
