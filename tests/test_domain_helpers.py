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
    _has_disk_temp_entries,
    _has_netdata_series_entry,
    _is_finite_number,
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


def test_netdata_mean_value_falls_back_to_raw_data_when_aggregations_empty() -> None:
    """TrueNAS's netdata backend can return present-but-empty aggregations
    (e.g. all-``{}`` min/mean/max) for an all-zero-valued series.
    """
    graph_data = [
        {
            "aggregations": {"min": {}, "mean": {}, "max": {}},
            "data": [[1000, 0], [1002, 0], [1004, 0]],
        }
    ]
    assert _netdata_mean_value(graph_data) == pytest.approx(0.0)


def test_netdata_mean_value_falls_back_to_raw_data_when_aggregations_missing() -> None:
    graph_data = [{"data": [[1000, 2.0], [1002, 4.0]]}]
    assert _netdata_mean_value(graph_data) == pytest.approx(3.0)


def test_netdata_mean_value_falls_back_to_raw_data_when_aggregations_not_a_dict() -> (
    None
):
    graph_data = [{"aggregations": "not-a-dict", "data": [[1000, 2.0], [1002, 6.0]]}]
    assert _netdata_mean_value(graph_data) == pytest.approx(4.0)


def test_netdata_mean_value_raw_data_fallback_ignores_malformed_points() -> None:
    graph_data = [
        {
            "aggregations": {"mean": {}},
            "data": ["not-a-point", [1000], [1002, "nan"], [1004, True], [1006, 5.0]],
        }
    ]
    assert _netdata_mean_value(graph_data) == pytest.approx(5.0)


def test_netdata_mean_value_raw_data_fallback_ignores_non_finite_values() -> None:
    """A literal NaN/Infinity sample (valid JSON, real wire risk) must not
    silently become the reported reading -- it has to be filtered out like
    any other unusable value, not averaged in.
    """
    graph_data = [
        {
            "aggregations": {"mean": {}},
            "data": [[1000, float("nan")], [1002, float("inf")], [1004, 5.0]],
        }
    ]
    assert _netdata_mean_value(graph_data) == pytest.approx(5.0)


def test_netdata_mean_value_raw_data_fallback_returns_none_for_only_non_finite() -> (
    None
):
    graph_data = [{"aggregations": {"mean": {}}, "data": [[1000, float("nan")]]}]
    assert _netdata_mean_value(graph_data) is None


def test_netdata_mean_value_raw_data_fallback_averages_all_series_per_point() -> None:
    """Mirrors the primary ``aggregations.mean`` path, which averages every
    series in the mean dict together rather than picking just one.
    """
    graph_data = [{"aggregations": {"mean": {}}, "data": [[1000, 2.0, 4.0]]}]
    assert _netdata_mean_value(graph_data) == pytest.approx(3.0)


def test_netdata_mean_value_raw_data_fallback_weighs_series_equally() -> None:
    """A series with fewer valid samples than another must not be
    under-weighted -- each series is averaged independently first, then
    those per-series means are averaged together, matching the primary
    ``aggregations.mean`` path's equal per-series weighting. A flat average
    across all raw samples would instead skew toward whichever series has
    more valid samples (here series 1, which has one non-finite point).
    """
    graph_data = [
        {
            "aggregations": {"mean": {}},
            "data": [
                [1000, 2.0, 10.0],
                [1002, 4.0, float("nan")],
                [1004, 6.0, 30.0],
            ],
        }
    ]
    # series 0 mean = (2+4+6)/3 = 4.0; series 1 mean = (10+30)/2 = 20.0
    # -> (4.0 + 20.0) / 2 = 12.0, not the flat mean of 10.4.
    assert _netdata_mean_value(graph_data) == pytest.approx(12.0)


def test_netdata_mean_value_raw_data_fallback_drops_series_with_no_valid_sample() -> (
    None
):
    """A series with zero finite samples across every point must be
    omitted from the average entirely, not counted in as 0.0.
    """
    graph_data = [
        {
            "aggregations": {"mean": {}},
            "data": [[1000, float("nan"), 5.0], [1002, float("nan"), 15.0]],
        }
    ]
    assert _netdata_mean_value(graph_data) == pytest.approx(10.0)


def test_netdata_mean_value_returns_none_when_raw_data_also_empty() -> None:
    graph_data = [{"aggregations": {"mean": {}}, "data": []}]
    assert _netdata_mean_value(graph_data) is None


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


def test_disk_temps_from_graph_data_falls_back_to_raw_samples() -> None:
    """TrueNAS's netdata backend can return present-but-empty ``aggregations``
    (``{"min": {}, "mean": {}, "max": {}}``) alongside populated raw ``data``
    points -- mirrors the shape ``_netdata_mean_value()`` already falls back
    for (see f3533a7)."""
    graph_data = [
        {
            "identifier": "disk1",
            "aggregations": {"mean": {}},
            "data": [[100, 30.0], [101, 40.0]],
        }
    ]
    assert _disk_temps_from_graph_data(graph_data) == {"disk1": 35.0}


def test_disk_temps_from_graph_data_stays_empty_when_raw_data_also_empty() -> None:
    """The shape reported in kayl-codes/homeassistant-truenas#139: both the
    aggregations and the raw data are genuinely empty (no samples in this
    poll's window at all), so no fallback source can produce a reading."""
    graph_data = [
        {
            "identifier": "disk1",
            "aggregations": {"min": {}, "mean": {}, "max": {}},
            "data": [],
        }
    ]
    assert _disk_temps_from_graph_data(graph_data) == {}


# ---------------------------
#   _has_disk_temp_entries
# ---------------------------
def test_has_disk_temp_entries_true_for_series_without_readings() -> None:
    """The empty-sampling-window shape (kayl-codes/homeassistant-truenas#139)
    still counts as recognizable per-disk series."""
    graph_data = [
        {
            "identifier": "disk1",
            "aggregations": {"min": {}, "mean": {}, "max": {}},
            "data": [],
        }
    ]
    assert _has_disk_temp_entries(graph_data) is True


def test_has_disk_temp_entries_true_when_one_series_among_garbage() -> None:
    """A single recognizable per-disk series is enough -- ``any()``, not
    ``all()``: garbage entries alongside it must not push an otherwise
    expected empty window onto the real-failure path."""
    graph_data = [
        "garbage",
        42,
        {"identifier": "disk1", "aggregations": {"mean": {}}, "data": []},
    ]
    assert _disk_temps_from_graph_data(graph_data) == {}
    assert _has_disk_temp_entries(graph_data) is True


@pytest.mark.parametrize(
    "graph_data",
    [
        pytest.param([], id="empty-list"),
        pytest.param(["not-a-dict", 3, None], id="no-dict-entries"),
        pytest.param([{"aggregations": {"mean": {"a": 30.0}}}], id="dict-without-id"),
        pytest.param([{"identifier": None}, {"identifier": ""}], id="falsy-id"),
        pytest.param("not-a-list", id="not-a-list"),
    ],
)
def test_has_disk_temp_entries_false_for_unrecognizable_payloads(
    graph_data: object,
) -> None:
    assert _has_disk_temp_entries(graph_data) is False


# ---------------------------
#   _has_netdata_series_entry
# ---------------------------
def test_has_netdata_series_entry_true_for_name_and_identifier_without_readings() -> (
    None
):
    """The shape TrueNAS's UPS netdata graphs actually carry (both "name" and
    "identifier"), with no samples accumulated yet -- kayl-codes/
    homeassistant-truenas#142 -- still counts as a recognizable series."""
    graph_data = [
        {
            "name": "upscurrent",
            "identifier": "upscurrent",
            "data": [],
            "aggregations": {"min": {}, "mean": {}, "max": {}},
        }
    ]
    assert _has_netdata_series_entry(graph_data) is True


def test_has_netdata_series_entry_true_for_name_only() -> None:
    """A "name"-only entry (no "identifier") is also recognizable."""
    graph_data = [{"name": "upscurrent", "aggregations": {"mean": {}}}]
    assert _has_netdata_series_entry(graph_data) is True


def test_has_netdata_series_entry_true_when_one_series_among_garbage() -> None:
    graph_data = [
        "garbage",
        42,
        {"identifier": "ups1", "aggregations": {"mean": {}}, "data": []},
    ]
    assert _has_netdata_series_entry(graph_data) is True


@pytest.mark.parametrize(
    "graph_data",
    [
        pytest.param([], id="empty-list"),
        pytest.param(["not-a-dict", 3, None], id="no-dict-entries"),
        pytest.param([{"aggregations": {"mean": {"a": 30.0}}}], id="dict-without-id"),
        pytest.param(
            [{"identifier": None, "name": ""}, {"identifier": "", "name": None}],
            id="falsy-id-and-name",
        ),
        pytest.param("not-a-list", id="not-a-list"),
    ],
)
def test_has_netdata_series_entry_false_for_unrecognizable_payloads(
    graph_data: object,
) -> None:
    assert _has_netdata_series_entry(graph_data) is False


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


def test_is_finite_number_rejects_int_too_large_for_float() -> None:
    assert _is_finite_number(10**400) is False


def test_netdata_named_means_excludes_non_finite_values() -> None:
    graph_data = [
        {
            "legend": ["cpu", "load"],
            "aggregations": {"mean": {"cpu": float("nan"), "load": float("inf")}},
        }
    ]
    assert _netdata_named_means(graph_data, ("cpu", "load")) == {}


def test_netdata_named_means_excludes_int_too_large_for_float() -> None:
    """An oversized int must not raise (float() would OverflowError)."""
    graph_data = [{"legend": ["cpu"], "aggregations": {"mean": {"cpu": 10**400}}}]
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


def test_netdata_max_mean_excludes_non_finite_values() -> None:
    graph_data = [{"aggregations": {"mean": {"a": float("inf"), "b": 30.0}}}]
    assert _netdata_max_mean(graph_data) == pytest.approx(30.0)
    assert _netdata_max_mean([{"aggregations": {"mean": {"a": float("nan")}}}]) is None


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


def test_netdata_interface_throughput_omits_non_finite_values() -> None:
    graph_data = [
        {
            "identifier": "eno1",
            "legend": ["received", "sent"],
            "aggregations": {"mean": {"received": float("nan"), "sent": float("inf")}},
        }
    ]
    assert _netdata_interface_throughput(graph_data) == {"eno1": {}}


def test_netdata_interface_throughput_omits_int_too_large_for_float() -> None:
    """An oversized int must not raise (int * float would OverflowError)."""
    graph_data = [
        {
            "identifier": "eno1",
            "legend": ["received", "sent"],
            "aggregations": {"mean": {"received": 10**400, "sent": 10**400}},
        }
    ]
    assert _netdata_interface_throughput(graph_data) == {"eno1": {}}


def test_netdata_interface_throughput_falls_back_to_short_alias() -> None:
    """A non-numeric raw-name value doesn't shadow a valid short-alias value.

    Netdata graphs may report a series under either its long ("received"/
    "sent") or short ("rx"/"tx") legend name; both must be tried rather than
    stopping at whichever key happens to be present but invalid.
    """
    graph_data = [
        {
            "identifier": "eno1",
            "legend": ["received", "rx"],
            "aggregations": {"mean": {"received": None, "rx": 8192.0}},
        }
    ]
    assert _netdata_interface_throughput(graph_data) == {"eno1": {"rx": 1000.0}}


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


def test_is_virtual_machine_rejects_unhashable_metadata() -> None:
    """A malformed list value must not raise (set membership needs hashable)."""
    assert _is_virtual_machine(["QEMU"], ["Standard PC"]) is False


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


def test_stable_uptime_epoch_rejects_non_finite_uptime() -> None:
    """A non-finite uptime_seconds must not raise (int() rejects nan/inf)."""
    previous = 100_000
    assert _stable_uptime_epoch(previous, float("nan"), 200_000) == previous
    assert _stable_uptime_epoch(previous, float("inf"), 200_000) == previous


def test_stable_uptime_epoch_rejects_int_too_large_for_float() -> None:
    """An oversized int must not raise (math.isfinite() would OverflowError)."""
    previous = 100_000
    assert _stable_uptime_epoch(previous, 10**400, 200_000) == previous
