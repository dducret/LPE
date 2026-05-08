# High Availability

## Current State/Functionality Overview

`LPE` high availability separates the core service from replaceable `LPE-CT` edge nodes. PostgreSQL remains the core state authority; `LPE-CT` local state is technical edge state only.

## Implementation/Usage

- Supported deployment modes:
  - single-node core with one `LPE-CT`
  - core with managed PostgreSQL and replaceable `LPE-CT`
  - multiple replaceable `LPE-CT` nodes with shared policy where configured
- Core critical components:
  - PostgreSQL
  - core `LPE` HTTP/API service
  - outbound queue worker
  - protocol adapters
  - blob storage backing canonical attachments
- `LPE-CT` critical components:
  - public `SMTP` listener
  - authenticated submission listener
  - HTTPS/WSS proxy surface
  - local spool under `/var/spool/lpe-ct`
  - private technical database where enabled
- Failover rules:
  - core mailbox state fails over with PostgreSQL and canonical blob storage
  - outbound queue processing must be idempotent
  - JMAP push listeners reconnect from canonical state
  - ActiveSync long-poll sessions may reconnect and resync from persisted sync keys
  - `LPE-CT` nodes can be replaced if accepted messages in local custody are preserved or replayed
- Health endpoints:
  - `/health`
  - `/health/live`
  - `/health/ready`
- Debian target:
  - Debian Trixie
  - systemd-managed services
  - explicit readiness checks before publication

## Reference Table/List

| Component | HA authority |
| --- | --- |
| mailbox state | core PostgreSQL |
| contacts/calendar/tasks | core PostgreSQL |
| canonical `Sent` | core PostgreSQL |
| outbound queue | core PostgreSQL |
| inbound SMTP custody | `LPE-CT` spool |
| quarantine metadata | `LPE-CT` technical store |
| JMAP push | canonical state plus reconnect |
| ActiveSync long poll | persisted sync keys plus reconnect |

| Path | Meaning |
| --- | --- |
| `/health/live` | process liveness |
| `/health/ready` | dependency readiness |
| `/var/spool/lpe-ct` | `LPE-CT` spool custody |
