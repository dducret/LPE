# Observability

## Current State/Functionality Overview

`LPE` and `LPE-CT` expose structured logs, trace correlation, health checks, and Prometheus-compatible metrics. Observability must diagnose protocol behavior, queue state, transport decisions, and security events without leaking secrets or protected metadata.

## Implementation/Usage

- Endpoints:
  - `GET /metrics`
  - `/health/ready`
- Trace correlation:
  - inbound request trace header: `x-trace-id`
  - services return `x-trace-id`
  - logs include `message_id` and `internet_message_id` where available
- Core `LPE` logs:
  - authentication events
  - canonical submission
  - outbound queue worker results
  - protocol adapter errors
  - storage errors
- `LPE-CT` logs:
  - SMTP session events
  - edge policy decisions
  - spool custody
  - quarantine actions
  - relay attempts
  - bridge calls to `LPE`
- Metrics must cover:
  - business counts
  - outbound queue state
  - SMTP transport outcomes
  - quarantine and security decisions
  - HTTP request latency and status
  - protocol adapter health
- Do not log:
  - passwords
  - bearer tokens
  - integration secrets
  - `TOTP` secrets
  - protected `Bcc` values

## Reference Table/List

| Setting | Component |
| --- | --- |
| `LPE_LOG_FORMAT` | core `LPE` |
| `LPE_METRICS_ENABLED` | core `LPE` |
| `LPE_CT_LOG_FORMAT` | `LPE-CT` |
| `LPE_CT_METRICS_ENABLED` | `LPE-CT` |

| Metric family | Owner |
| --- | --- |
| mailbox and submission metrics | `LPE` |
| outbound worker metrics | `LPE` |
| JMAP/IMAP/ActiveSync protocol metrics | `LPE` |
| SMTP edge metrics | `LPE-CT` |
| relay metrics | `LPE-CT` |
| quarantine metrics | `LPE-CT` |
| security decision metrics | `LPE-CT` |

| Diagnostic path | Purpose |
| --- | --- |
| `/api/v1/dashboard` | management dashboard data |
| `/api/v1/system-diagnostics/` | system diagnostics |
| `/var/log/lpe-ct/mail.log` | `LPE-CT` mail log |
| `/var/log/dmesg` | host diagnostics |

`/api/v1/dashboard` is a management endpoint and requires a bearer token from
`/api/v1/auth/login`; health and readiness probes remain unauthenticated.
