# Operations and Disaster Recovery

## Current State/Functionality Overview

Operations protect canonical `LPE` PostgreSQL state, blob storage, and `LPE-CT` custody state separately. Restore and rollback must not create divergent canonical mailbox or collaboration state.

## Implementation/Usage

- Operating principles:
  - core `LPE` PostgreSQL is authoritative for user-visible state
  - `LPE-CT` local stores are technical and perimeter-owned
  - `LPE-CT` spool custody must be preserved for accepted but undelivered mail
  - readiness must pass before public routing
- Core backup boundary:
  - PostgreSQL database
  - canonical attachment/blob storage
  - configuration needed for protocol adapters and auth
  - secrets handled by the deployment secret manager
- `LPE-CT` backup boundary:
  - `/var/spool/lpe-ct`
  - quarantine payload custody
  - private technical database where enabled
  - TLS certificate/key profiles
  - routing, accepted-domain, DKIM, and policy configuration
- Restore rules:
  - restore core PostgreSQL before starting protocol adapters
  - restore blobs with database consistency
  - restore `LPE-CT` spool before accepting SMTP traffic
  - validate `/health/ready`
- Node replacement:
  - stop public routing
  - preserve or transfer `/var/spool/lpe-ct`
  - restore technical state where required
  - verify bridge connectivity to core `LPE`
  - re-enable public routing after readiness
- Upgrade safety:
  - back up before schema changes
  - verify schema compatibility
  - verify bridge signatures and integration secret presence
  - verify client autodiscovery gates after deployment
- Operations benchmark evidence:
  - run `tools/operations_benchmark.py` against deployed services, not mocked protocol handlers
  - capture cold-start readiness when a real service restart command is supplied
  - capture mailbox workspace and `JMAP` `Mailbox/query`, `Email/query`, `Email/queryChanges`, and WebSocket reconnect/push-enable latency
  - capture `IMAP` `SELECT`, `UID FETCH`, `UID SEARCH`, and text `SEARCH` latency against an operator-selected mailbox; set `LPE_OPS_BENCH_IMAP_MIN_EXISTS` when the run is intended to prove realistic mailbox-size behavior
  - capture `ActiveSync` `Sync` and `Ping` latency using real mailbox credentials and WBXML requests built by the benchmark tool
  - capture public `SMTP DATA` acceptance through the final LPE-CT reply, which is the point where accepted mail is either delivered to core, deferred, quarantined, or rejected by policy
  - capture outbound retry throughput by posting real LPE-CT trace retry actions for operator-supplied trace IDs
  - skipped benchmark sections are gaps, not release evidence

## Reference Table/List

| Item | Owner | Restore priority |
| --- | --- | --- |
| core PostgreSQL | `LPE` | highest |
| canonical blobs | `LPE` | highest |
| outbound queue | `LPE` PostgreSQL | highest |
| `LPE-CT` spool | `LPE-CT` | highest for accepted mail custody |
| quarantine metadata | `LPE-CT` | operational |
| greylisting/reputation | `LPE-CT` | operational |

| Path / command | Purpose |
| --- | --- |
| `/health/ready` | readiness check |
| `/var/spool/lpe-ct` | sorting-center spool |
| `/opt/lpe-ct/bin/lpe-ct-host-action` | host action helper |
| `tools/operations_benchmark.py` | live cold-start, protocol latency, SMTP acceptance, and retry-throughput benchmark |

| Benchmark variable | Purpose |
| --- | --- |
| `LPE_OPS_BENCH_BASE_URL` | core `LPE` HTTP base URL |
| `LPE_OPS_BENCH_EMAIL` / `LPE_OPS_BENCH_PASSWORD` | mailbox credentials for `JMAP`, `IMAP`, and `ActiveSync` |
| `LPE_OPS_BENCH_COLD_START_COMMAND` | explicit command for cold-start readiness measurement |
| `LPE_OPS_BENCH_IMAP_HOST` / `LPE_OPS_BENCH_IMAP_PORT` | `IMAP` benchmark target |
| `LPE_OPS_BENCH_IMAP_MIN_EXISTS` | minimum mailbox size required for realistic-size `IMAP` evidence |
| `LPE_OPS_BENCH_SMTP_HOST` / `LPE_OPS_BENCH_SMTP_RCPT_TO` | public `SMTP DATA` benchmark target |
| `LPE_OPS_BENCH_LPE_CT_BASE_URL` / `LPE_OPS_BENCH_RETRY_TRACE_IDS` | outbound retry throughput target |
