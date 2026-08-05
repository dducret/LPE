---
type: Rust Module
title: observability
resource: LPE-CT/src/observability.rs#L1-L359
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/axum-extract-matchedpath-request-http-header-content-type-headervalue-statuscode-middleware-next-response-intoresponse-response
  - external/std-collections-btreemap-env-path-path-sync-mutex-oncelock-time-duration-instant
  - external/tracing-info
  - external/tracing-subscriber-envfilter
  - external/uuid-uuid
  member_of:
  - packages/LPE-CT
---

# Contains

- [CtMetrics](../../../classes/LPE-CT/src/observability/CtMetrics.md)
- [DurationAggregate](../../../classes/LPE-CT/src/observability/DurationAggregate.md)
- [metrics](../../../functions/LPE-CT/src/observability/metrics.md)
- [init_tracing](../../../functions/LPE-CT/src/observability/init_tracing.md)
- [trace_id_from_headers](../../../functions/LPE-CT/src/observability/trace_id_from_headers.md)
- [observe_http](../../../functions/LPE-CT/src/observability/observe_http.md)
- [metrics_endpoint](../../../functions/LPE-CT/src/observability/metrics_endpoint.md)
- [record_outbound_handoff](../../../functions/LPE-CT/src/observability/record_outbound_handoff.md)
- [record_inbound_delivery](../../../functions/LPE-CT/src/observability/record_inbound_delivery.md)
- [record_smtp_session](../../../functions/LPE-CT/src/observability/record_smtp_session.md)
- [record_outlook_test_message](../../../functions/LPE-CT/src/observability/record_outlook_test_message.md)
- [record_smtp_backpressure](../../../functions/LPE-CT/src/observability/record_smtp_backpressure.md)
- [set_active_smtp_sessions](../../../functions/LPE-CT/src/observability/set_active_smtp_sessions.md)
- [record_security_event](../../../functions/LPE-CT/src/observability/record_security_event.md)
- [record_http_request](../../../functions/LPE-CT/src/observability/record_http_request.md)
- [render_metrics](../../../functions/LPE-CT/src/observability/render_metrics.md)
- [count_queue](../../../functions/LPE-CT/src/observability/count_queue.md)
- [metrics_enabled](../../../functions/LPE-CT/src/observability/metrics_enabled.md)
- [escape_label](../../../functions/LPE-CT/src/observability/escape_label.md)

# Imports

- `axum::{
    extract::{MatchedPath, Request},
    http::{header::CONTENT_TYPE, HeaderValue, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
}`
- `std::{
    collections::BTreeMap,
    env,
    path::Path,
    sync::{Mutex, OnceLock},
    time::{Duration, Instant},
}`
- `tracing::info`
- `tracing_subscriber::EnvFilter`
- `uuid::Uuid`

# Member of

- [lpe-ct](../../../packages/LPE-CT.md)