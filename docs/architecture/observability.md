# LPE Observability

This document defines the v1 observability model for `LPE` and `LPE-CT`.

## Goals

The observability layer must:

- expose scrape-friendly metrics for operations and alerting
- keep logs structured and correlation-friendly
- separate business, transport, and security signals
- preserve the architectural split between the core platform and the DMZ sorting center
- support the first `active/passive` failover step without hiding which node is traffic-ready

The implementation therefore exposes Prometheus-compatible text endpoints, structured `tracing` events, and stable correlation through:

- `trace_id`
- canonical `message_id`
- `internet_message_id` when present
- `tenant_id`
- protocol or `adapter`
- queue item identifier for queued transport work

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

This gives one correlation axis for the request chain and additional axes for tenant isolation, protocol-adapter diagnosis, and queue-custody investigation.

The minimum correlation fields for production diagnostics are:

- `trace_id`
- `message_id` when a canonical message exists
- `internet_message_id` when the message originated from SMTP or imported RFC 822 content
- `tenant_id`
- `adapter`, such as `jmap`, `imap`, `activesync`, `dav`, `webmail`, `smtp-ingress`, `smtp-submission`, `smtp-relay`, or `queue-worker`
- `queue_item_id` for `outbound_message_queue` and `LPE-CT` spool-owned transitions
- `account_id` or delegated principal when required for auth-failure investigation

These identifiers should be attached to logs and spans as fields, not exploded into high-cardinality Prometheus labels unless the metric is explicitly designed for bounded aggregation.

The `LPE-CT` management console also exposes a narrow host-log browser for
operator diagnostics. The supported host-log categories are:

- `Mail`: `/var/log/mail.log` and rotated `mail.log.*` files
- `Interface`: `CFMA.log`, reserved for the future console/interface logger
- `Messages`: `/var/log/dmesg` and rotated `dmesg.*` files

This browser is limited to listing, previewing, downloading, and deleting those
whitelisted host log files. It must not expose arbitrary filesystem paths and
must not become a source of canonical mailbox, collaboration, rights, or
user-visible state. When a current log file is not present yet, the console may
show an empty virtual row so the intended stream remains visible during early
deployment.

The `Reporting -> System Information` page in the `LPE-CT` management console
is the supported v1 host diagnostics surface for the sorting-center node. It
uses the runtime system snapshot from `/api/v1/dashboard` for processor,
memory, mail/log disk, uptime, and load-average values, and exposes only
whitelisted management diagnostics under `/api/v1/system-diagnostics/*`.

The supported diagnostics are service status and start/stop control for the
configured antivirus and `LPE-CT` systemd units, mail queue metrics from the
local `LPE-CT` spool, process list, network connections, routing table, support
connection command, health check, `ping`, `traceroute`, `dig`, spam-test upload,
and mail-queue flush. Host commands are fixed by diagnostic type or by explicit
environment-configured command paths such as `LPE_CT_SPAM_TEST_BIN`,
`LPE_CT_SUPPORT_CONNECT_BIN`, and `LPE_CT_FLUSH_MAIL_QUEUE_BIN`; the management
API must not accept arbitrary shell commands from the browser. These diagnostics
remain operational evidence only and must not become canonical mailbox,
collaboration, rights, or user-visible state.
 
## Metric families

The v1 metric families are intentionally compact.

### Business metrics

On `LPE`:

- `lpe_mail_submissions_total{source=...}`
- `lpe_mail_inbound_deliveries_total{status=...}`
- `lpe_protocol_request_duration_seconds_{sum,count}{adapter,route,status}`
- `lpe_jmap_push_delivery_total{result=...}`
- `lpe_imap_active_sessions`
- `lpe_activesync_long_poll_duration_seconds_{sum,count,status}`

These represent user-visible mailbox behavior and final delivery into canonical `LPE` mailboxes.

### Transport metrics

On `LPE`:

- `lpe_outbound_worker_dispatch_total{status=...}`
- `lpe_outbound_worker_batch_size_last`
- `lpe_outbound_worker_last_poll_timestamp_seconds`
- `lpe_outbound_queue_items{status=...}`
- `lpe_outbound_queue_oldest_age_seconds{status=...}`

On `LPE-CT`:

- `lpe_ct_outbound_handoffs_total{status=...}`
- `lpe_ct_inbound_delivery_total{status=...}`
- `lpe_ct_smtp_sessions_total{result=...}`
- `lpe_ct_smtp_delivery_outcomes_total{direction,result,stage}`
- `lpe_ct_smtp_backpressure_total`
- `lpe_ct_active_smtp_sessions`
- `lpe_ct_spool_messages{queue=...}`
- `lpe_ct_spool_oldest_age_seconds{queue=...}`
- `lpe_ct_submission_requests_total{result=...}`

These cover queue movement, relay handoff, final LAN delivery, and SMTP-edge activity.

### Security metrics

On `LPE`:

- `lpe_security_events_total{event=...}`

On `LPE-CT`:

- `lpe_ct_security_events_total{event=...}`
- `lpe_ct_auth_failures_total{surface=...}`
- `lpe_ct_quarantine_messages_total{reason=...}`

These count significant security decisions such as integration authentication failures, Magika quarantine/reject outcomes, and SMTP-perimeter quarantine/reject/defer decisions.

### HTTP metrics

Both services expose:

- `*_http_requests_total{method,route,status}`
- `*_http_request_duration_seconds_sum{method,route,status}`
- `*_http_request_duration_seconds_count{method,route,status}`

Routes use the matched path when available so labels stay operationally useful without exploding on message UUIDs.

## Diagnosability rules

Metrics are necessary but not sufficient.

The supported diagnosability model is:

- use metrics for fleet-level trend detection and alerting
- use structured logs for per-request and per-message investigation
- use shared correlation identifiers across `LPE`, `LPE-CT`, and protocol adapters
- preserve the distinction between canonical queue items in `LPE` and spool custody in `LPE-CT`

At minimum, operators must be able to answer:

- which tenant is affected
- which protocol adapter is affected
- which queue item or transport trace is stuck
- whether the failure is in client auth, canonical submission, queue dispatch, `LPE-CT` relay, or final inbound delivery

## Protocol dashboards

The baseline dashboard set is protocol-oriented rather than service-oriented only.

### `JMAP`

The `JMAP` dashboard must show:

- request latency by route and status
- request volume
- push or notification delivery outcomes
- reconnect spikes after failover

### `IMAP`

The `IMAP` dashboard must show:

- active session count
- login success and failure trends
- command latency for the most important mailbox operations
- disconnect spikes and abnormal session churn

### `ActiveSync`

The `ActiveSync` dashboard must show:

- request latency by command
- long-poll duration distribution for `Ping` or equivalent wait paths
- sync continuation success versus restart rate
- auth-failure and timeout trends

### `SMTP` and queueing

The transport dashboard must show:

- outbound delivery outcomes
- inbound final-delivery outcomes
- current spool depth by queue
- oldest item age by queue
- outbound queue growth and drain rate
- quarantine volume and surge behavior

## Alert thresholds

The initial alert set must prefer sustained symptoms over single-event noise.

### Queue growth and defer spikes

Alert when any of these conditions stay above the declared threshold window for the environment:

- outbound queue depth grows continuously while dispatch success falls
- oldest queued item age exceeds the operator target
- `deferred` outcomes spike above normal baseline
- `LPE-CT` spool `deferred` or `held` queues grow without corresponding drain

### Authentication failures

Alert on sustained or sharply elevated:

- mailbox submission authentication failures
- integration-auth signature failures between `LPE` and `LPE-CT`
- tenant or account login failures above the expected baseline

### Quarantine surges

Alert when:

- quarantine intake rate spikes materially above recent baseline
- quarantine backlog age exceeds the declared operational review window
- a single policy reason suddenly dominates quarantine outcomes

### Protocol health

Alert when:

- `JMAP` latency or error rate crosses the declared SLO
- `IMAP` active sessions collapse unexpectedly during expected business hours
- `ActiveSync` long-poll duration collapses from normal wait behavior to frequent early termination
- SMTP delivery outcomes shift sharply from `relayed` toward `deferred`, `bounced`, or `failed`

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
- `LPE-CT` readiness now also reports relay reachability, spool pressure, quarantine backlog, and core API reachability separately so operators can distinguish safe queueing from edge degradation.
- Use `trace_id` for request-chain correlation and combine it with `message_id`, `tenant_id`, `adapter`, and `queue_item_id` during incident review.
- Keep protocol dashboards separate for `JMAP`, `IMAP`, `ActiveSync`, and transport so failover symptoms do not disappear into generic HTTP aggregates.
- Alert primarily on sustained `deferred`, `failed`, `quarantined`, `rejected`, auth-failure, and spool-growth patterns rather than on single transient events.
