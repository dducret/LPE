# `LPE` / `LPE-CT` Integration Contract

### Goal

This document defines the explicit contract between the core `LPE` services and the `LPE-CT` sorting center.

The architectural split remains strict:

- `LPE` remains the system of record for mailboxes, user-visible views, and `Sent`
- `LPE-CT` remains responsible for exposed `SMTP`, outbound relay, quarantine, and the edge perimeter
- no Internet-facing `SMTP` is reintroduced into `LPE`

### Outbound flow `LPE -> LPE-CT`

1. a client submits a message through the canonical `LPE` model
2. `LPE` persists the authoritative copy in `Sent`
3. `LPE` inserts an `outbound_message_queue` row
4. the `LPE` outbound worker consumes items in `queued` or `deferred`
5. the worker calls `POST /api/v1/integration/outbound-messages` on `LPE-CT`
6. `LPE-CT` applies local policy and attempts `SMTP` relay
7. `LPE-CT` replies with an explicit status and a `trace_id`
8. `LPE` updates `outbound_message_queue.status`, `messages.delivery_status`, `remote_message_ref`, `attempts`, `last_error`, `next_attempt_at`, and the latest structured transport result

Minimum supported statuses:

- `queued`: message prepared in `LPE` before handoff
- `relayed`: `LPE-CT` accepted the handoff and relayed the message toward its `SMTP` target
- `deferred`: transient failure; `LPE` recomputes `next_attempt_at`
- `quarantined`: `LPE-CT` retained the message under policy
- `bounced`: permanent delivery failure with usable `DSN` feedback
- `failed`: permanent failure or relay-incompatible configuration

The outbound handoff result is now structured. In addition to the status and `trace_id`, `LPE-CT` may return:

- `remote_message_ref`: technical relay reference when available
- `retry`: retry guidance (`retry_after_seconds`, policy, reason)
- `dsn`: `DSN` action (`delayed`, `failed`, etc.), enhanced status, and diagnostic
- `technical`: phase (`connect`, `rcpt-to`, `data`, `final-response`, `throttle`), `SMTP` code, enhanced code, remote host, and detail
- `route`: applied routing rule and chosen relay
- `throttle`: throttling scope, key, window, limit, and suggested delay

`LPE` persists that detailed result on `outbound_message_queue` so queue state remains operationally useful without moving MTA logic into the core platform.

### Inbound flow `LPE-CT -> LPE`

1. `LPE-CT` receives the `SMTP` message from the Internet
2. `LPE-CT` applies drain, quarantine, and edge controls
3. if the message is accepted for final delivery, `LPE-CT` calls `POST /internal/lpe-ct/inbound-deliveries` on `LPE`
4. `LPE` verifies the integration key, resolves local mailboxes by recipient, creates the canonical `Inbox` copy, indexes the body, and records the action
5. `LPE` returns accepted and rejected recipients
6. `LPE-CT` updates its local spool into `sent`, `deferred`, or `held`

The raw `SMTP` body is carried into `LPE` to keep delivery context, but mailbox persistence remains controlled by the internal `LPE` model.

### Authenticated client submission flow `Client -> LPE-CT -> LPE`

1. the client connects to the external `LPE-CT` submission listener, preferably implicit `TLS` on `465`
2. `LPE-CT` authenticates the client mailbox credentials by calling `POST /internal/lpe-ct/submission-auth` on `LPE`
3. after `AUTH`, `LPE-CT` accepts `MAIL FROM`, `RCPT TO`, and `DATA`
4. `LPE-CT` forwards the authenticated principal, envelope, and raw RFC 822 body to `POST /internal/lpe-ct/submissions`
5. `LPE` validates attachments with `Magika`, derives visible and `Bcc` recipients, enforces sender ownership, and invokes the canonical submission workflow
6. `LPE` creates the authoritative `Sent` copy before inserting the outbound queue row
7. only after that canonical submission succeeds does `LPE-CT` return a successful SMTP final reply to the client
8. outbound relay still happens later through the existing `LPE -> LPE-CT` outbound handoff path

This keeps Internet-facing `SMTP` in `LPE-CT` while ensuring every client submission converges on the single `LPE` business workflow.

### Internal authentication

Both HTTP calls use the header:

- `x-lpe-integration-key`

The authenticated client-submission bridge uses the same header for:

- `POST /internal/lpe-ct/submission-auth`
- `POST /internal/lpe-ct/submissions`

The shared secret is provided through:

- `LPE_INTEGRATION_SHARED_SECRET`

That secret is mandatory at startup on both components, must fail closed when missing, and must not use a trivial or known placeholder value such as `change-me`. A minimum length of `32` characters is required. That secret must not be exposed in public interfaces or stored in the `LPE-CT` management JSON state.

### Main environment variables

On the `LPE` side:

- `LPE_CT_API_BASE_URL`
- `LPE_OUTBOUND_WORKER_INTERVAL_MS`
- `LPE_OUTBOUND_WORKER_BATCH_SIZE`
- `LPE_LOG_FORMAT`
- `LPE_METRICS_ENABLED`
- `LPE_INTEGRATION_SHARED_SECRET`

On the `LPE-CT` side:

- `LPE_CT_CORE_DELIVERY_BASE_URL`
- `LPE_CT_RELAY_PRIMARY`
- `LPE_CT_RELAY_SECONDARY`
- `LPE_CT_SUBMISSION_BIND_ADDRESS`
- `LPE_CT_SUBMISSION_TLS_CERT_PATH`
- `LPE_CT_SUBMISSION_TLS_KEY_PATH`
- `LPE_CT_SUBMISSION_MAX_MESSAGE_SIZE_MB`
- `LPE_CT_LOG_FORMAT`
- `LPE_CT_METRICS_ENABLED`
- `LPE_INTEGRATION_SHARED_SECRET`

Any dedicated local database used by `LPE-CT` remains a sorting-center-private technical store. It must not require direct access from the `DMZ` to the core `LPE` `PostgreSQL` database and must not hold canonical mailbox or collaboration state. The full boundary is documented in `docs/architecture/lpe-ct-local-data-stores.md`.

If `LPE-CT` adopts a dedicated local `PostgreSQL` service, the intended usage is technical only:

- Bayesian filtering
- reputation and greylisting
- quarantine metadata
- throttling and routing coordination
- `LPE-CT` cluster coordination

That service remains private on `5432` and does not alter the HTTP integration contract between `LPE` and `LPE-CT`.

### Observability

The two services now expose a Prometheus-compatible `GET /metrics` endpoint on their local HTTP API.

Correlation rules are:

- the initiating service sets or propagates `x-trace-id`
- the receiving service returns `x-trace-id` in the HTTP response
- transport logs include `message_id` and `internet_message_id` when available
- `LPE` records business metrics, worker transport metrics, and security events
- `LPE-CT` records spool gauges, SMTP-edge metrics, relay metrics, and security decisions

The detailed metric families and logging behavior are documented in `docs/architecture/observability.md`.

### v1 implementation notes

- the `LPE` worker performs synchronous handoff and updates state afterward
- the `LPE` worker now also sends `attempt_count` and `last_attempt_error`, allowing `LPE-CT` to apply informed retry and throttling policies
- authenticated client submission now terminates on `LPE-CT`, but the internal submission contract creates the canonical `Sent` copy in `LPE` before any relay
- `LPE-CT` keeps raw SMTP bytes intact on ingress and carries them through persistence and internal final delivery
- `LPE-CT` extracts inbound visible text from decoded MIME (`multipart/alternative`, `quoted-printable`, `base64`, HTML) instead of indexing the raw RFC 822 body blindly
- `LPE-CT` now persists a structured authentication summary (`SPF`, `DKIM`, `DMARC`, alignment, tempfail) and explicit `defer` / `quarantine` / `reject` reasons
- `LPE-CT` now records the full inbound edge pipeline in the decision trace, including protocol capture, `RBL` / DNS checks, active `bayespam`, the configured antivirus provider chain, and final score calculation
- when the dedicated local PostgreSQL store is enabled, `LPE-CT` upserts quarantined-message metadata into a private `quarantine_messages` table while keeping payload custody in the spool
- `LPE-CT` composes outbound relay as RFC 822 with either plain `text/plain` or `multipart/alternative` `text/plain` + `text/html` when `body_html_sanitized` is available, without reinjecting `Bcc` into visible headers
- `LPE-CT` applies outbound routing rules and throttling before the actual SMTP relay
- `LPE-CT` classifies outbound failures into `deferred`, `bounced`, or `failed` from SMTP replies and produces structured technical and `DSN` feedback with retry backoff derived from the upstream attempt count
- inbound final delivery creates per-mailbox `Inbox` copies in `LPE`
- standard search and visible projections do not reinject `Bcc`


