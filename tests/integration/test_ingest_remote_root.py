"""A service called by another traced service gets a root row.

Runs real SDK output (``tests/fixtures/otlp``) through the public decode and the real
ingest task into the throwaway server, then reads back through the trace reader.
"""

from __future__ import annotations

import base64
import time
from pathlib import Path

import pytest

import worker.ingest_tasks as ingest_tasks
from db.clickhouse.client import ClickHouseClient
from rest.routers.public.traces import decode_otlp_protobuf
from rest.services.trace_reader import TraceReaderService

from .conftest import Gateway

pytestmark = pytest.mark.integration

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "otlp"
PYTHON = ("remote_parent_otel_python.pb", "5a" * 16)
TRACEROOT_TS = ("remote_parent_traceroot_ts.pb", "5b" * 16)
UPSTREAM_SPAN = "cd" * 8  # the remote parent both fixtures point at


class _S3:
    def __init__(self, payload: dict):
        self._payload = payload

    def download_json(self, key: str) -> dict:
        return self._payload


def _ingest(gateway: Gateway, monkeypatch, project: str, payload: dict) -> list:
    enqueued: list = []
    client = ClickHouseClient(gateway.admin)
    monkeypatch.setattr("db.clickhouse.client.get_clickhouse_client", lambda: client)
    monkeypatch.setattr("rest.services.s3.get_s3_service", lambda: _S3(payload))
    monkeypatch.setattr(
        "worker.detector_tasks.enqueue_detector_runs",
        lambda project_id, trace_ids: enqueued.append(set(trace_ids)),
    )
    monkeypatch.setattr(ingest_tasks, "_publish_live_spans", lambda *a, **k: None)
    ingest_tasks.process_s3_traces("k.json", project)
    return enqueued


def _upstream_root(trace_id: str) -> dict:
    """The calling service's own root span: no parent, span id = the remote parent."""
    now = time.time_ns()
    return {
        "resourceSpans": [
            {
                "scopeSpans": [
                    {
                        "scope": {"name": "traceroot"},
                        "spans": [
                            {
                                "traceId": base64.b64encode(bytes.fromhex(trace_id)).decode(),
                                "spanId": base64.b64encode(bytes.fromhex(UPSTREAM_SPAN)).decode(),
                                "name": "upstream_root",
                                "startTimeUnixNano": str(now - 10**9),
                                "endTimeUnixNano": str(now + 10**10),
                                "attributes": [
                                    {
                                        "key": "traceroot.span.input",
                                        "value": {"stringValue": '"upstream in"'},
                                    }
                                ],
                            }
                        ],
                    }
                ]
            }
        ]
    }


def _trace(gateway: Gateway, monkeypatch, project: str, trace_id: str) -> dict | None:
    monkeypatch.setattr(
        "rest.services.trace_reader.get_clickhouse_client",
        lambda: ClickHouseClient(gateway.admin),
    )
    return TraceReaderService().get_trace(project, trace_id)


@pytest.mark.parametrize("fixture", [PYTHON, TRACEROOT_TS], ids=["python", "traceroot_ts"])
def test_remote_parent_entry_span_gets_the_root_row_but_not_the_detector_claim(
    gateway, monkeypatch, fixture
):
    name, trace_id = fixture
    project = f"it_remote_root_{name.split('_')[-1].split('.')[0]}"
    payload = decode_otlp_protobuf((FIXTURES / name).read_bytes())

    enqueued = _ingest(gateway, monkeypatch, project, payload)

    trace = _trace(gateway, monkeypatch, project, trace_id)
    assert trace is not None
    assert trace["name"] == "handle_request"
    assert trace["input"] == '"user question"'
    # The detector decision is made once and stays with a true root: a remote-root batch
    # is often the called service finishing before its caller's root arrives (#2641).
    assert enqueued == []


@pytest.mark.parametrize(
    "upstream_first", [True, False], ids=["upstream_first", "downstream_first"]
)
def test_upstream_root_keeps_the_trace_when_both_services_report(
    gateway, monkeypatch, upstream_first
):
    """Distributed tracing into one project: the downstream entry must not take over."""
    name, trace_id = PYTHON
    project = f"it_remote_root_both_{upstream_first}"
    downstream = decode_otlp_protobuf((FIXTURES / name).read_bytes())
    batches = [_upstream_root(trace_id), downstream]
    if not upstream_first:
        batches.reverse()

    enqueued = []
    for batch in batches:
        enqueued += _ingest(gateway, monkeypatch, project, batch)

    trace = _trace(gateway, monkeypatch, project, trace_id)
    assert trace is not None
    assert trace["name"] == "upstream_root"
    assert trace["input"] == '"upstream in"'
    # Only the caller's root takes the detector claim, in either arrival order.
    assert enqueued == [{trace_id}]


def _remote_entry(trace_id: str, span_hex: str, name: str, start_s: int) -> dict:
    return {
        "resourceSpans": [
            {
                "scopeSpans": [
                    {
                        "scope": {"name": "traceroot"},
                        "spans": [
                            {
                                "traceId": base64.b64encode(bytes.fromhex(trace_id)).decode(),
                                "spanId": base64.b64encode(bytes.fromhex(span_hex)).decode(),
                                "parentSpanId": base64.b64encode(
                                    bytes.fromhex(UPSTREAM_SPAN)
                                ).decode(),
                                "flags": 0x300,
                                "name": name,
                                "startTimeUnixNano": str(start_s * 10**9),
                                "endTimeUnixNano": str((start_s + 1) * 10**9),
                            }
                        ],
                    }
                ]
            }
        ]
    }


@pytest.mark.parametrize("earliest_first", [True, False], ids=["earliest_first", "earliest_last"])
def test_earliest_remote_root_holds_the_trace_across_batches(gateway, monkeypatch, earliest_first):
    """The same service called twice in one trace: the first call names it."""
    trace_id = "5c" * 16
    project = f"it_remote_root_two_calls_{earliest_first}"
    now_s = int(time.time())
    first = _remote_entry(trace_id, "01" * 8, "call_1", now_s)
    second = _remote_entry(trace_id, "02" * 8, "call_2", now_s + 5)
    batches = [first, second] if earliest_first else [second, first]

    for batch in batches:
        _ingest(gateway, monkeypatch, project, batch)

    trace = _trace(gateway, monkeypatch, project, trace_id)
    assert trace is not None
    assert trace["name"] == "call_1"
