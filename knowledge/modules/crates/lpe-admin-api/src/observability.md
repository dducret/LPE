---
type: Rust Module
title: observability
resource: crates/lpe-admin-api/src/observability.rs#L1-L570
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/axum-extract-matchedpath-request-http-header-content-type-headermap-headervalue-statuscode-middleware-next-response-intoresponse-response
  - external/std-collections-btreemap-env-sync-mutex-oncelock-time-duration-instant-systemtime-unix-epoch
  - external/tracing-info
  - external/tracing-subscriber-envfilter
  - external/uuid-uuid
  - external/super-should-log-outlook-http-route-gap
  member_of:
  - packages/crates/lpe-admin-api
---

# Contains

- [CoreMetrics](../../../../classes/crates/lpe-admin-api/src/observability/CoreMetrics.md)
- [DurationAggregate](../../../../classes/crates/lpe-admin-api/src/observability/DurationAggregate.md)
- [metrics](../../../../functions/crates/lpe-admin-api/src/observability/metrics.md)
- [init_tracing](../../../../functions/crates/lpe-admin-api/src/observability/init_tracing.md)
- [trace_id_from_headers](../../../../functions/crates/lpe-admin-api/src/observability/trace_id_from_headers.md)
- [safe_header](../../../../functions/crates/lpe-admin-api/src/observability/safe_header.md)
- [observe_http](../../../../functions/crates/lpe-admin-api/src/observability/observe_http.md)
- [should_log_outlook_http_route_gap](../../../../functions/crates/lpe-admin-api/src/observability/should_log_outlook_http_route_gap.md)
- [metrics_endpoint](../../../../functions/crates/lpe-admin-api/src/observability/metrics_endpoint.md)
- [record_mail_submission](../../../../functions/crates/lpe-admin-api/src/observability/record_mail_submission.md)
- [record_inbound_delivery](../../../../functions/crates/lpe-admin-api/src/observability/record_inbound_delivery.md)
- [record_outbound_dispatch](../../../../functions/crates/lpe-admin-api/src/observability/record_outbound_dispatch.md)
- [record_outbound_worker_poll](../../../../functions/crates/lpe-admin-api/src/observability/record_outbound_worker_poll.md)
- [record_outbound_worker_poll_failure](../../../../functions/crates/lpe-admin-api/src/observability/record_outbound_worker_poll_failure.md)
- [record_security_event](../../../../functions/crates/lpe-admin-api/src/observability/record_security_event.md)
- [record_http_request](../../../../functions/crates/lpe-admin-api/src/observability/record_http_request.md)
- [render_metrics](../../../../functions/crates/lpe-admin-api/src/observability/render_metrics.md)
- [metrics_enabled](../../../../functions/crates/lpe-admin-api/src/observability/metrics_enabled.md)
- [escape_label](../../../../functions/crates/lpe-admin-api/src/observability/escape_label.md)
- [unix_timestamp_seconds](../../../../functions/crates/lpe-admin-api/src/observability/unix_timestamp_seconds.md)
- [metrics_include_mapi_calendar_event_save_and_notification_counters](../../../../functions/crates/lpe-admin-api/src/observability/metrics_include_mapi_calendar_event_save_and_notification_counters.md)
- [outlook_http_route_gap_logs_unmatched_exchange_paths](../../../../functions/crates/lpe-admin-api/src/observability/outlook_http_route_gap_logs_unmatched_exchange_paths.md)
- [outlook_http_route_gap_does_not_duplicate_successful_protocol_routes](../../../../functions/crates/lpe-admin-api/src/observability/outlook_http_route_gap_does_not_duplicate_successful_protocol_routes.md)

# Imports

- `axum::{
    extract::{MatchedPath, Request},
    http::{header::CONTENT_TYPE, HeaderMap, HeaderValue, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
}`
- `std::{
    collections::BTreeMap,
    env,
    sync::{Mutex, OnceLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
}`
- `tracing::info`
- `tracing_subscriber::EnvFilter`
- `uuid::Uuid`
- `super::should_log_outlook_http_route_gap`

# Member of

- [lpe-admin-api](../../../../packages/crates/lpe-admin-api.md)