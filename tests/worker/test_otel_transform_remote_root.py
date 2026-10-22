"""A service's entry span with a remote parent is the root of what that service sent.

A service called by another traced service gets a W3C ``traceparent``, so its entry
span has a parent span id from the caller and nothing it exports has an empty parent.
The fixtures are real SDK output (see ``tests/fixtures/otlp/README.md``).
"""

import json
from pathlib import Path

import pytest

from rest.routers.public.traces import decode_otlp_protobuf
from tests.fixtures.otel_payloads import make_attr, make_otel_payload, make_span
from worker.otel_transform import transform_otel_to_clickhouse

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "otlp"


def _fixture(name: str) -> dict:
    return decode_otlp_protobuf((FIXTURES / name).read_bytes())


@pytest.mark.parametrize(
    "fixture", ["remote_parent_otel_python.pb", "remote_parent_traceroot_ts.pb"]
)
def test_entry_span_with_remote_parent_becomes_the_trace_root(fixture):
    traces, spans = transform_otel_to_clickhouse(_fixture(fixture), project_id="proj-1")

    (trace,) = traces
    assert trace["name"] == "handle_request"
    assert json.loads(trace["input"]) == "user question"
    assert json.loads(trace["output"]) == "final answer"
    assert trace["root_kind"] == "remote_parent"
    # The parent id is real data from the caller and stays on the span.
    (entry,) = [s for s in spans if s["name"] == "handle_request"]
    assert entry["parent_span_id"] == "cd" * 8


TRACE = "ab" * 16


def _span(span_id, name, parent=None, attrs=(), flags=None):
    span = make_span(TRACE, span_id, name=name, parent_span_id_hex=parent, attributes=list(attrs))
    if flags is not None:
        span["flags"] = flags
    return span


def test_local_parent_flag_is_not_a_root():
    """0x100 alone: the SDK knows the parent is local, so this is an ordinary child."""
    payload = make_otel_payload([_span("22" * 8, "child", parent="11" * 8, flags=0x100)])

    (trace,), _ = transform_otel_to_clickhouse(payload, project_id="proj-1")

    assert "root_kind" not in trace


def test_no_flags_and_no_ids_path_is_not_a_root():
    """Without either signal a span with a parent is treated as before: a child."""
    payload = make_otel_payload([_span("22" * 8, "child", parent="11" * 8)])

    (trace,), _ = transform_otel_to_clickhouse(payload, project_id="proj-1")

    assert "root_kind" not in trace


def test_child_of_a_local_root_with_ids_path_is_not_a_root():
    payload = make_otel_payload(
        [
            _span(
                "22" * 8,
                "child",
                parent="11" * 8,
                attrs=[
                    {
                        "key": "traceroot.span.ids_path",
                        "value": {"arrayValue": {"values": [{"stringValue": "11" * 8}]}},
                    }
                ],
            )
        ]
    )

    (trace,), _ = transform_otel_to_clickhouse(payload, project_id="proj-1")

    assert "root_kind" not in trace


def test_true_root_without_input_drops_the_remote_roots_input():
    """The trace's I/O is its root's: a downstream entry seen first must not keep it."""
    downstream = _span(
        "33" * 8,
        "downstream_entry",
        parent="22" * 8,
        attrs=[make_attr("traceroot.span.input", '"d"')],
        flags=0x300,
    )
    upstream = _span("11" * 8, "upstream_root")

    (trace,), _ = transform_otel_to_clickhouse(
        make_otel_payload([downstream, upstream]), project_id="proj-1"
    )

    assert trace["name"] == "upstream_root"
    assert "input" not in trace


@pytest.mark.parametrize("true_root_first", [True, False])
def test_true_root_wins_over_a_remote_parent_root(true_root_first):
    """Both services trace into one project: the upstream root names the trace."""
    upstream = _span("11" * 8, "upstream_root", attrs=[make_attr("traceroot.span.input", '"u"')])
    downstream = _span(
        "33" * 8,
        "downstream_entry",
        parent="22" * 8,
        attrs=[make_attr("traceroot.span.input", '"d"')],
        flags=0x300,
    )
    ordered = [upstream, downstream] if true_root_first else [downstream, upstream]

    (trace,), _ = transform_otel_to_clickhouse(make_otel_payload(ordered), project_id="proj-1")

    assert trace["name"] == "upstream_root"
    assert json.loads(trace["input"]) == "u"
    assert trace["root_kind"] == "root"


@pytest.mark.parametrize("earliest_first", [True, False])
def test_earliest_remote_root_wins_in_one_batch(earliest_first):
    """The same service called twice: the first call roots the trace, whole."""
    call_1 = _span(
        "33" * 8,
        "call_1",
        parent="22" * 8,
        attrs=[make_attr("traceroot.span.input", '"in1"')],
        flags=0x300,
    )
    call_2 = _span(
        "44" * 8,
        "call_2",
        parent="22" * 8,
        attrs=[make_attr("traceroot.span.output", '"out2"')],
        flags=0x300,
    )
    call_2["startTimeUnixNano"] = str(int(call_1["startTimeUnixNano"]) + 5 * 10**9)
    call_2["endTimeUnixNano"] = str(int(call_1["endTimeUnixNano"]) + 5 * 10**9)
    ordered = [call_1, call_2] if earliest_first else [call_2, call_1]

    (trace,), _ = transform_otel_to_clickhouse(make_otel_payload(ordered), project_id="proj-1")

    assert trace["name"] == "call_1"
    assert json.loads(trace["input"]) == "in1"
    assert "output" not in trace


def test_remote_root_user_wins_over_a_child_user():
    child = _span(
        "55" * 8,
        "child",
        parent="33" * 8,
        attrs=[make_attr("traceroot.trace.user_id", "child-user")],
    )
    entry = _span(
        "33" * 8,
        "entry",
        parent="22" * 8,
        attrs=[make_attr("traceroot.trace.user_id", "entry-user")],
        flags=0x300,
    )

    (trace,), _ = transform_otel_to_clickhouse(
        make_otel_payload([child, entry]), project_id="proj-1"
    )

    assert trace["user_id"] == "entry-user"


def test_true_root_user_wins_over_a_remote_root_user():
    entry = _span(
        "33" * 8,
        "entry",
        parent="11" * 8,
        attrs=[make_attr("traceroot.trace.user_id", "entry-user")],
        flags=0x300,
    )
    root = _span("11" * 8, "root", attrs=[make_attr("traceroot.trace.user_id", "root-user")])

    for ordered in ([entry, root], [root, entry]):
        (trace,), _ = transform_otel_to_clickhouse(make_otel_payload(ordered), project_id="proj-1")
        assert trace["user_id"] == "root-user"
