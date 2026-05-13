# Operations and Disaster Recovery

## Current State/Functionality Overview

Operations protect canonical `LPE` PostgreSQL state, blob storage, and `LPE-CT` custody state separately. Restore and rollback must not create divergent canonical mailbox or collaboration state.

## Implementation/Usage

- Operating principles:
  - core `LPE` PostgreSQL is authoritative for user-visible state
  - PostgreSQL remains the metadata authority for S3-compatible storage pools,
    blob placement metadata, checksums, lifecycle state, and policy references
  - S3-compatible object storage is provider-neutral object storage, not
    AWS-specific or Azure-specific support
  - protocol adapters use the `BlobStore` boundary and remain backend-agnostic
  - raw RFC 5322 message blobs remain database-backed initially
  - storage policy changes affect future writes only and do not implicitly
    migrate existing blobs
  - S3-compatible credentials use deployment secret references and must not be
    stored inline in normal storage-pool database rows
  - `LPE-CT` local stores are technical and perimeter-owned
  - `LPE-CT` spool custody must be preserved for accepted but undelivered mail
  - readiness must pass before public routing
  - core readiness includes a critical `storage-metadata` check: at least one
    active storage pool, an active platform default policy, valid policy
    references, active placements on active pools, and no blobs missing an
    active placement
  - storage health distinguishes required pools from optional pools; a required
    pool is one referenced by storage policy or active placements, while an
    optional pool can be configured without affecting readiness until it is used
  - S3-compatible pool health probes are provider-neutral and report operational
    states such as missing object, checksum mismatch, auth failure, timeout,
    unreachable endpoint, unavailable backend, invalid configuration, or missing
    deployment secret without exposing object keys or credentials
  - admin diagnostics expose storage-pool health, placement counts, explicit
    migration jobs, cleanup status, and cleanup blockers without exposing object
    keys, provider credentials, secrets, or provider-specific backend internals
- Core backup boundary:
  - PostgreSQL database
  - canonical attachment/blob storage, including S3-compatible object buckets
    for pools with active placements
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
  - restore database-backed blobs with PostgreSQL consistency
  - restore S3-compatible object storage from a backup taken at a time
    consistent with PostgreSQL `blob_placements`; object keys are derived from
    placement metadata and must not be reconstructed from tenant, domain,
    mailbox, message, or provider-specific identifiers
  - restore deployment secret references before enabling S3-compatible pools so
    health checks can authenticate without storing credentials in PostgreSQL
  - after restore, validate storage health before public routing; required
    S3-compatible pools must not report missing objects, checksum mismatch, auth
    failure, timeout, or unreachable endpoint
  - protocol adapters must surface storage backend failures as storage errors;
    missing or mismatched S3-compatible objects must not be treated as corrupted
    mailbox/message metadata
  - treat storage migration rollback as placement metadata recovery: after a
    switch, the old source placement remains `retiring` with a rollback window
    until Milestone 4 cleanup marks it `deleted` after rollback-window,
    live-reference, retention, and legal-hold guards pass
  - placement cleanup is not canonical message/blob deletion; restore and
    rollback procedures must preserve canonical `blobs`, `messages`,
    `mime_parts`, `attachments`, extraction jobs, and attachment text rows
  - restore `LPE-CT` spool before accepting SMTP traffic
  - validate `/health/ready`
  - Provider-Specific Cloud Backends, including AWS-specific and Azure-specific
    backends, remain future-release work
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
| database-backed canonical blobs | `LPE` | highest |
| S3-compatible object buckets for active placements | `LPE` | highest |
| outbound queue | `LPE` PostgreSQL | highest |
| `LPE-CT` spool | `LPE-CT` | highest for accepted mail custody |
| quarantine metadata | `LPE-CT` | operational |
| greylisting/reputation | `LPE-CT` | operational |

| Path / command | Purpose |
| --- | --- |
| `/health/ready` | readiness check |
| `/health/ready` `storage-metadata` | critical storage-pool metadata consistency check |
| Admin storage diagnostics | storage pool health, required/optional readiness role, provider-neutral backend state, placement state, migration jobs, and cleanup blockers |
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
