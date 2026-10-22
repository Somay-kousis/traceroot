# OTLP fixtures

Real SDK output, so tests see the exact wire shape rather than a hand-built one.

Both files hold one trace from a service whose entry span `handle_request` has a
**remote parent** (a `traceparent` from an upstream service, span id `cdcd…`) and one
child span `llm-call`.

| File | Produced by | Root signal on `handle_request` |
|---|---|---|
| `remote_parent_otel_python.pb` | opentelemetry-sdk 1.45.1, `encode_spans` from opentelemetry-exporter-otlp-proto-common | `flags = 0x300` (parent is-remote known, and remote). The child has `0x100`. |
| `remote_parent_traceroot_ts.pb` | @traceroot-ai/traceroot 0.5.0 `TraceRootSpanProcessor` over @opentelemetry/sdk-trace-base 1.x, serialized with @opentelemetry/otlp-transformer 0.57.2 `ProtobufTraceSerializer` | no `flags` (OTel JS does not send them); `traceroot.span.ids_path = []`, which traceroot-ts writes when it treats a span with an unresolved parent as a root (`processor.ts`). |

To regenerate, start the entry span from a context carrying a remote
`SpanContext(trace_id, span_id="cdcdcdcdcdcdcdcd", is_remote=True)`, start the child
inside it, end both, and write the serialized `ExportTraceServiceRequest` bytes.
