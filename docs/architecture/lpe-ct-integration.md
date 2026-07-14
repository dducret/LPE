# `LPE` / `LPE-CT` Integration Contract

## Current State/Functionality Overview

`LPE` and `LPE-CT` communicate through signed internal HTTP bridges. `LPE` remains the system of record; `LPE-CT` remains the SMTP edge, relay, quarantine, and perimeter component.

## Implementation/Usage

- Outbound flow `LPE -> LPE-CT`:
  - base URL: `${LPE_CT_API_BASE_URL}`
  - default `LPE-CT` port: `8380`
  - endpoint: `POST ${LPE_CT_API_BASE_URL}/api/v1/integration/outbound-messages`
  - `LPE` creates canonical `Sent` before handoff
  - `LPE` updates `outbound_message_queue`, `messages.delivery_status`, `remote_message_ref`, `attempts`, `last_error`, `next_attempt_at`, and latest transport result
  - repeated results for the same queue item and `trace_id` are idempotent
  - repeated `LPE -> LPE-CT` handoffs for the same queue item reuse the existing `LPE-CT` spool custody record across `outbound`, `deferred`, `held`, `quarantine`, `bounces`, and `sent` instead of creating a second relay attempt
  - duplicate handoff suppression is recorded in the transport audit stream
  - terminal states must not regress
- Inbound flow `LPE-CT -> LPE`:
  - base URL: `${LPE_CT_CORE_DELIVERY_BASE_URL}`
  - default `LPE` port: `8080`
  - final delivery endpoint: `POST ${LPE_CT_CORE_DELIVERY_BASE_URL}/internal/lpe-ct/inbound-deliveries`
  - recipient verification endpoint: `POST ${LPE_CT_CORE_DELIVERY_BASE_URL}/internal/lpe-ct/recipient-verification`
  - accepted Internet mail is delivered to the core final-delivery API
  - core `LPE` verifies the integration signature before mutation
  - core `LPE` derives the canonical visible body and attachments from the signed
    raw RFC 822/MIME message; edge-provided projections such as `body_text` are
    compatibility and diagnostic fields, not canonical mailbox state
  - core final delivery treats committed `LPE-CT` `trace_id` values as idempotency keys; bridge retries after a timeout return the committed delivery receipt instead of creating duplicate mailbox messages
  - same-trace inbound final delivery is serialized in PostgreSQL while the canonical commit is evaluated, so timeout races cannot produce parallel mailbox writes for one `LPE-CT` trace
  - bridge failure keeps custody in `LPE-CT`
  - `LPE-CT` keeps the accepted message durably in its local spool until core final delivery succeeds, quarantine/rejection applies, or an operator/policy terminal action deletes it
- Authenticated client submission:
  - public submission listener terminates on `LPE-CT`
  - preferred external submission port: implicit TLS `465`
  - auth endpoint: `POST ${LPE_CT_CORE_DELIVERY_BASE_URL}/internal/lpe-ct/submission-auth`
  - canonical submission endpoint: `POST ${LPE_CT_CORE_DELIVERY_BASE_URL}/internal/lpe-ct/submissions`
  - `LPE` enforces sender ownership and delegated `send-as` / `send-on-behalf`
  - `LPE` validates attachments with `Magika`
  - `LPE` creates authoritative `Sent` before relay
- Internal authentication headers:
  - `x-lpe-integration-key`
  - `x-lpe-integration-timestamp`
  - `x-lpe-integration-nonce`
  - `x-lpe-integration-signature`
- Signature rules:
  - method, path, serialized payload, timestamp, and nonce are signed
  - missing headers fail closed
  - invalid signatures fail closed
  - expired timestamps fail closed
  - replayed nonce/signature combinations fail closed
- Shared secret:
  - setting: `LPE_INTEGRATION_SHARED_SECRET`
  - mandatory on both components
  - minimum length: `32` characters
  - trivial placeholders such as `change-me` are invalid
  - must not be stored in `LPE-CT` management JSON state
- Observability:
  - both services expose `GET /metrics`
  - `x-trace-id` is propagated and returned
  - transport logs include `message_id` and `internet_message_id` when available
- Administration boundary:
  - quarantine lists, release/reject/delete actions, perimeter filtering rules,
    and quarantine retention policy are served by `LPE-CT`
  - core `LPE` administration surfaces may show immutable handoff and delivery
    result history received from `LPE-CT`, but must not read or persist
    quarantine custody or perimeter policy tables
  - core `LPE` API handlers reject antispam settings and filtering-rule writes;
    those administrative mutations must be sent to the `LPE-CT` API
  - core `LPE` built-in roles must not grant an `antispam` permission; LPE-CT
    owns any perimeter filtering operator roles

## Reference Table/List

| Endpoint | Direction | Purpose |
| --- | --- | --- |
| `POST ${LPE_CT_API_BASE_URL}/api/v1/integration/outbound-messages` | `LPE -> LPE-CT` | outbound handoff |
| `POST ${LPE_CT_CORE_DELIVERY_BASE_URL}/internal/lpe-ct/inbound-deliveries` | `LPE-CT -> LPE` | final inbound delivery |
| `POST ${LPE_CT_CORE_DELIVERY_BASE_URL}/internal/lpe-ct/recipient-verification` | `LPE-CT -> LPE` | recipient verification |
| `POST ${LPE_CT_CORE_DELIVERY_BASE_URL}/internal/lpe-ct/submission-auth` | `LPE-CT -> LPE` | mailbox credential verification |
| `POST ${LPE_CT_CORE_DELIVERY_BASE_URL}/internal/lpe-ct/submissions` | `LPE-CT -> LPE` | canonical client submission |
| `GET /metrics` | local | Prometheus metrics |

| Status | Meaning |
| --- | --- |
| `queued` | prepared in `LPE` before handoff |
| `relayed` | accepted and relayed by `LPE-CT` |
| `deferred` | transient failure; retry scheduled |
| `quarantined` | retained by `LPE-CT` policy |
| `bounced` | permanent failure with `DSN` feedback |
| `failed` | permanent failure or relay-incompatible configuration |

| Result field | Meaning |
| --- | --- |
| `remote_message_ref` | relay reference |
| `retry` | retry guidance with `retry_after_seconds`, policy, and reason |
| `dsn` | `DSN` action, enhanced status, diagnostic |
| `technical` | phase, SMTP code, enhanced code, remote host, detail |
| `route` | routing rule and chosen relay |
| `throttle` | throttling scope, key, window, limit, suggested delay |

| `LPE` setting | Default |
| --- | --- |
| `LPE_CT_API_BASE_URL` | required |
| `LPE_OUTBOUND_WORKER_INTERVAL_MS` | `1000` |
| `LPE_OUTBOUND_WORKER_BATCH_SIZE` | `50` |
| `LPE_LOG_FORMAT` | deployment-defined |
| `LPE_METRICS_ENABLED` | deployment-defined |
| `LPE_INTEGRATION_SHARED_SECRET` | required |

| `LPE-CT` setting | Default |
| --- | --- |
| `LPE_CT_CORE_DELIVERY_BASE_URL` | required |
| `LPE_CT_PUBLIC_TLS_CERT_PATH` | optional bootstrap |
| `LPE_CT_PUBLIC_TLS_KEY_PATH` | optional bootstrap |
| `LPE_CT_SUBMISSION_BIND_ADDRESS` | deployment-defined |
| `LPE_CT_SUBMISSION_TLS_CERT_PATH` | deployment-defined |
| `LPE_CT_SUBMISSION_TLS_KEY_PATH` | deployment-defined |
| `LPE_CT_SUBMISSION_MAX_MESSAGE_SIZE_MB` | deployment-defined |
| `LPE_CT_GREYLIST_DELAY_SECONDS` | `30` |
| `LPE_CT_LOG_FORMAT` | deployment-defined |
| `LPE_CT_METRICS_ENABLED` | deployment-defined |
| `LPE_INTEGRATION_SHARED_SECRET` | required |
