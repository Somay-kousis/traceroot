"""A short trace from a service called by another traced service gets its root row.

The transform makes a remote-parent entry span the root of its batch. Ingest is
unchanged: a batch with no empty-parent span still never replaces a stored trace row,
and only a true root takes the detector claim. So a trace exported in one batch is
fixed, and a long trace whose children are exported first keeps the placeholder row,
exactly as before (#2710 tracks that part).

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


def _trace(gateway: Gateway, monkeypatch, project: str, trace_id: str) -> dict | None:
    monkeypatch.setattr(
        "rest.services.trace_reader.get_clickhouse_client",
        lambda: ClickHouseClient(gateway.admin),
    )
    return TraceReaderService().get_trace(project, trace_id)


def _only(payload: dict, name: str) -> dict:
    """The same export with only the span called ``name`` (an earlier or later batch)."""
    out = {"resourceSpans": []}
    for resource_spans in payload["resourceSpans"]:
        scopes = []
        for scope_spans in resource_spans["scopeSpans"]:
            spans = [s for s in scope_spans["spans"] if s["name"] == name]
            if spans:
                scopes.append({**scope_spans, "spans": spans})
        if scopes:
            out["resourceSpans"].append({**resource_spans, "scopeSpans": scopes})
    return out


def _upstream_root(trace_id: str) -> dict:
    """The calling service's own root span: no parent, span id = the remote parent."""
    now = time.time_ns()
    span = {
        "traceId": base64.b64encode(bytes.fromhex(trace_id)).decode(),
        "spanId": base64.b64encode(bytes.fromhex(UPSTREAM_SPAN)).decode(),
        "name": "upstream_root",
        "startTimeUnixNano": str(now - 10**9),
        "endTimeUnixNano": str(now + 10**10),
        "attributes": [{"key": "traceroot.span.input", "value": {"stringValue": '"upstream in"'}}],
    }
    return {"resourceSpans": [{"scopeSpans": [{"scope": {"name": "traceroot"}, "spans": [span]}]}]}


@pytest.mark.parametrize("fixture", [PYTHON, TRACEROOT_TS], ids=["python", "traceroot_ts"])
def test_short_trace_gets_the_entry_span_as_its_root(gateway, monkeypatch, fixture):
    name, trace_id = fixture
    project = f"it_remote_root_short_{name.split('_')[-1].split('.')[0]}"
    payload = decode_otlp_protobuf((FIXTURES / name).read_bytes())

    enqueued = _ingest(gateway, monkeypatch, project, payload)

    trace = _trace(gateway, monkeypatch, project, trace_id)
    assert trace is not None
    assert trace["name"] == "handle_request"
    assert trace["input"] == '"user question"'
    assert trace["output"] == '"final answer"'
    # Only a true root takes the one-time detector claim (#2641 for why not here).
    assert enqueued == []


def test_long_trace_keeps_the_placeholder_row(gateway, monkeypatch):
    """Child exported first, entry later: not covered here, the row stays as today."""
    name, trace_id = PYTHON
    project = "it_remote_root_long"
    payload = decode_otlp_protobuf((FIXTURES / name).read_bytes())

    enqueued = _ingest(gateway, monkeypatch, project, _only(payload, "llm-call"))
    enqueued += _ingest(gateway, monkeypatch, project, _only(payload, "handle_request"))

    trace = _trace(gateway, monkeypatch, project, trace_id)
    assert trace is not None
    assert trace["name"] == "llm-call"
    assert trace["input"] is None
    assert enqueued == []


@pytest.mark.parametrize(
    "upstream_first", [True, False], ids=["upstream_first", "downstream_first"]
)
def test_caller_root_keeps_the_trace_when_both_services_report(
    gateway, monkeypatch, upstream_first
):
    """Distributed tracing into one project: the downstream entry never takes over."""
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
    assert enqueued == [{trace_id}]
