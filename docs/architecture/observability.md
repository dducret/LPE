# LPE Observability

This document defines the v1 observability model for `LPE` and `LPE-CT`.

## Goals

The observability layer must:

- expose scrape-friendly metrics for operations and alerting
- keep logs structured and correlation-friendly
- separate business, transport, and security signals
- preserve the architectural split between the core platform and the DMZ sorting center
- support the first `active/passive` failover step without hiding which node is traffic-ready

The implementation therefore exposes Prometheus-compatible text endpoints, structured `tracing` events, and stable correlation through `trace_id` and `message_id`.

## Endpoints

### Core `LPE`

- `GET /metrics`
  Exposes Prometheus-compatible metrics from the `lpe-admin-api` process, including the HTTP API and the outbound worker running in `lpe-cli`.

### Sorting center `LPE-CT`

- `GET /metrics`
  Exposes Prometheus-compatible metrics from the management plane and live spool gauges computed from the `LPE-CT` spool directories.

## Log model

`LPE` and `LPE-CT` now support two log formats:

- plain text for local development
- JSON for production pipelines and modern log shipping

Each HTTP request is tagged with `x-trace-id`. If the caller does not provide one, the server generates a new value and returns it in the response header.

For cross-service flows:

- the `LPE` outbound worker sends `x-trace-id` to `LPE-CT`
- `LPE-CT` forwards the same `x-trace-id` to the core inbound-delivery API
- transport and SMTP logs also include `message_id` and `internet_message_id` when available

This gives one correlation axis for the request chain and a second one for the canonical message object.

## Metric families

The v1 metric families are intentionally compact.

### Business metrics

On `LPE`:

- `lpe_mail_submissions_total{source=...}`
- `lpe_mail_inbound_deliveries_total{status=...}`

These represent user-visible mailbox behavior and final delivery into canonical `LPE` mailboxes.

### Transport metrics

On `LPE`:

- `lpe_outbound_worker_dispatch_total{status=...}`
- `lpe_outbound_worker_batch_size_last`
- `lpe_outbound_worker_last_poll_timestamp_seconds`

On `LPE-CT`:

- `lpe_ct_outbound_handoffs_total{status=...}`
- `lpe_ct_inbound_delivery_total{status=...}`
- `lpe_ct_smtp_sessions_total{result=...}`
- `lpe_ct_spool_messages{queue=...}`

These cover queue movement, relay handoff, final LAN delivery, and SMTP-edge activity.

### Security metrics

On `LPE`:

- `lpe_security_events_total{event=...}`

On `LPE-CT`:

- `lpe_ct_security_events_total{event=...}`

These count significant security decisions such as integration authentication failures, Magika quarantine/reject outcomes, and SMTP-perimeter quarantine/reject/defer decisions.

### HTTP metrics

Both services expose:

- `*_http_requests_total{method,route,status}`
- `*_http_request_duration_seconds_sum{method,route,status}`
- `*_http_request_duration_seconds_count{method,route,status}`

Routes use the matched path when available so labels stay operationally useful without exploding on message UUIDs.

## Configuration

### Core `LPE`

- `RUST_LOG`
  Standard `tracing` filter for the `LPE` process.
- `LPE_LOG_FORMAT`
  `plain` or `json`. Defaults to `plain`.
- `LPE_METRICS_ENABLED`
  Enables the `/metrics` endpoint. Defaults to enabled.
- `LPE_CT_API_BASE_URL`
  Base URL used by the outbound worker for `LPE -> LPE-CT` handoff.
- `LPE_OUTBOUND_WORKER_INTERVAL_MS`
  Poll interval for the outbound worker.
- `LPE_OUTBOUND_WORKER_BATCH_SIZE`
  Maximum batch size per worker cycle.
- `LPE_INTEGRATION_SHARED_SECRET`
  Shared secret used for internal `LPE <-> LPE-CT` authentication.

### Sorting center `LPE-CT`

- `RUST_LOG`
  Standard `tracing` filter for the `LPE-CT` process.
- `LPE_CT_LOG_FORMAT`
  `plain` or `json`. Defaults to `plain`.
- `LPE_CT_METRICS_ENABLED`
  Enables the `/metrics` endpoint. Defaults to enabled.
- `LPE_CT_BIND_ADDRESS`
  Management API bind address.
- `LPE_CT_SMTP_BIND_ADDRESS`
  Public SMTP listener bind address.
- `LPE_CT_STATE_FILE`
  Persistent management-state file.
- `LPE_CT_SPOOL_DIR`
  Root directory for spool queues used by live spool gauges.
- `LPE_CT_CORE_DELIVERY_BASE_URL`
  Base URL used for final inbound delivery toward `LPE`.
- `LPE_CT_RELAY_PRIMARY`
  Primary outbound relay target.
- `LPE_CT_RELAY_SECONDARY`
  Secondary outbound relay target.
- `LPE_INTEGRATION_SHARED_SECRET`
  Shared secret used for internal `LPE <-> LPE-CT` authentication.

## Operating notes

- Use JSON logs in production together with `RUST_LOG=info` or a stricter target-specific filter.
- Scrape both `/metrics` endpoints; `LPE` and `LPE-CT` expose complementary signals rather than duplicates.
- Probe `/health/ready` on both nodes when HA role gating is enabled; only the node marked `active` should be considered traffic-ready.
- Use `trace_id` for request-chain correlation and `message_id` for canonical message correlation.
- Alert primarily on sustained `deferred`, `failed`, `quarantined`, `rejected`, and spool-growth patterns rather than on single transient events.
