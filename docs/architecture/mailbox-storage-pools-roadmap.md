# Mailbox Storage Pools Roadmap

## Purpose

This document records the current status of the `LPE` mailbox storage-pool work
and the active Milestone 7 Codex prompt pack.

The target scale example remains 1000 mailboxes with quotas up to 100 GB each,
or up to 100 TB of logical mailbox capacity before replication, backup,
retention, legal hold, and migration safety windows.

## Current Status

Status date: 2026-05-13.

Completed:

- Milestone 0: architecture decision recorded. PostgreSQL remains the metadata
  authority, protocol adapters stay backend-agnostic, and storage movement uses
  copy, verify, metadata switch, rollback window, and cleanup.
- Milestone 1: internal `BlobStore` boundary implemented for durable attachment
  and MIME-part blobs while preserving database-backed byte storage. Raw RFC
  5322 message blobs remain database-backed initially.
- Milestone 2: storage-pool and blob-placement metadata implemented for
  database-backed durable attachment and MIME-part blobs. Blob bytes still live
  in PostgreSQL.
- Milestone 3: explicit online migration jobs and database-backed
  copy/verify/switch behavior implemented. Policy changes still do not create
  implicit migration jobs.
- Milestone 4: logical quota stability, retention/legal-hold guards, and safe
  old-placement cleanup implemented for database-backed placement rows.
- Milestone 5: admin policy and visibility implemented without exposing object
  keys, secrets, provider credentials, or provider-specific internals.
- Milestone 6: provider-neutral S3-compatible object storage implemented behind
  the existing storage-pool, placement, migration, quota, cleanup, and admin
  visibility model. Durable attachment and MIME-part blobs can be written,
  read, statted, verified, and explicitly migrated between database-backed and
  S3-compatible placements.

Active next step:

- Milestone 7: Provider-Specific Cloud Backends. This adds AWS S3 and Azure
  Blob provider-specific configuration, health checks, operational assumptions,
  and restore tests where generic S3-compatible behavior is not enough.

## Current Rules

- PostgreSQL is the authoritative store for canonical mailbox metadata,
  mailbox membership, sync state, quotas, retention, rights, blob metadata, and
  placement metadata.
- `LPE-CT` must not store canonical mailbox or collaboration state.
- User-facing protocols continue to read and write canonical `LPE` state.
- Protocol adapters must not know disk paths, buckets, object keys, or cloud
  vendor APIs.
- Storage policy is evaluated at write time only. Policy changes record future
  intent and do not implicitly move existing blobs.
- Platform, tenant, domain, and account storage policy are supported. Existing
  blobs move only through explicit migration jobs, not through policy changes.
- Mailbox-level policy remains deferred.
- Raw RFC 5322 message blobs remain database-backed initially.
- S3-compatible credentials use deployment secret references. Credentials are
  not stored inline in normal storage-pool database rows.
- Provider-specific cloud backends must continue to use PostgreSQL metadata,
  canonical placement rows, the `BlobStore` backend boundary, and the admin
  visibility redaction rules established by earlier milestones.
- Every new dependency or external implementation idea must be checked against
  `LICENSE.md`.

## Codex Prompt Pack: Milestone 7

Use these prompts one at a time. Milestone 7 adds provider-specific cloud
backend support after the provider-neutral S3-compatible backend is proven. It
must not move canonical mailbox state out of PostgreSQL, bypass `BlobStore`,
expose provider credentials, add mailbox-level policy, or make policy changes
implicitly migrate blobs.

### Prompt 1: Inspect Provider-Specific Gaps

```text
Read AGENTS.md, ARCHITECTURE.md, docs/architecture/initial-architecture.md,
LICENSE.md, docs/architecture/sql-schema-v2.md,
docs/architecture/data-lifecycle-and-compliance.md,
docs/architecture/operations-and-disaster-recovery.md, and
docs/architecture/mailbox-storage-pools-roadmap.md.

Goal: inspect the completed provider-neutral S3-compatible backend and identify
the smallest Milestone 7 gaps for AWS S3 and Azure Blob support.

Scope:
- lpe-storage BlobStore and backend-selection code
- storage pool configuration and admin redaction behavior
- health/readiness diagnostics
- operations and restore documentation
- current dependency set and LICENSE.md constraints

Return:
- which AWS S3 behavior can reuse the S3-compatible backend unchanged
- which AWS-specific settings need explicit configuration or validation
- which Azure Blob behavior cannot reuse the S3-compatible backend
- candidate SDK/protocol-client options and licenses for AWS and Azure
- provider-specific failure modes to expose in health/status
- files likely to change
- risks or unclear points before editing

Do not edit files.
```

### Prompt 2: Design Provider-Specific Backend Contracts

```text
Design the Milestone 7 provider-specific backend contracts for AWS S3 and Azure
Blob.

Constraints:
- PostgreSQL remains the metadata authority.
- Protocol adapters remain backend-agnostic.
- Reuse the existing BlobStore backend boundary.
- Reuse storage_pool, blob_placement, migration, quota, cleanup, and admin
  visibility models.
- Raw RFC 5322 message blobs remain database-backed initially.
- Storage policy is evaluated at write time only.
- Existing blobs move only through explicit migration jobs.
- Mailbox-level policy remains deferred.
- Credentials use the existing deployment secret model.
- Object keys, credentials, account names, connection strings, and provider
  internals must not leak through admin APIs or protocol responses.
- Do not add dependencies unless required and checked against LICENSE.md.

Return:
- provider kind names and configuration fields
- AWS S3 behavior that aliases the S3-compatible backend
- Azure Blob backend methods and error model
- provider-specific health, readiness, and diagnostics states
- object key/blob name derivation rules
- checksum, ETag, and size verification rules per provider
- retry, timeout, and rate-limit behavior
- tests required before implementation
- any simpler alternative and why it is or is not enough

Do not edit code yet.
```

### Prompt 3: Implement Provider Configuration And Redaction

```text
Implement the approved Milestone 7 provider-specific configuration and
redaction behavior.

Scope:
- lpe-storage storage-pool configuration validation
- lpe-admin-api validation and summaries only if provider configuration is
  exposed there
- no data transfer implementation yet
- no protocol adapter changes
- no mailbox-level policy
- no automatic migration on policy changes

Required behavior:
- AWS S3 pools can declare provider-specific options only when needed beyond
  the generic S3-compatible shape
- Azure Blob pools can declare account/container/endpoint or equivalent
  provider-specific settings through secret references
- credentials and sensitive provider internals are redacted in all admin
  summaries
- invalid provider configuration is rejected with actionable diagnostics
- existing database-backed and S3-compatible pool behavior remains unchanged

Verification:
- focused lpe-storage tests for provider configuration validation and redaction
- focused lpe-admin-api tests if API validation changed
- cargo test -p lpe-storage
- cargo test -p lpe-admin-api if admin API code changed

Report:
- changed files
- exact tests run
- dependency/license decisions
```

### Prompt 4: Implement AWS S3 Provider Behavior

```text
Implement Milestone 7 AWS S3 provider behavior.

Scope:
- lpe-storage backend routing and AWS S3-specific behavior
- prefer reusing the S3-compatible backend where behavior is equivalent
- no Azure implementation in this prompt
- no protocol adapter changes
- no mailbox-level policy
- no automatic migration on policy changes

Required behavior:
- AWS S3 put/read/stat/verify works through BlobStore and placement metadata
- AWS-specific endpoint, region, addressing, checksum, and error behavior is
  handled where it differs from generic S3-compatible storage
- AWS health/readiness checks expose provider-specific degraded states without
  leaking credentials or object keys
- migration to and from AWS S3 placements uses explicit migration jobs only
- database-backed and generic S3-compatible behavior remains unchanged

Verification:
- unit tests for AWS-specific configuration, error mapping, and redaction
- AWS integration tests gated by environment variables
- cargo test -p lpe-storage

Report:
- changed files
- exact tests run
- skipped integration tests and required environment variables, if not run
```

### Prompt 5: Implement Azure Blob Provider Behavior

```text
Implement Milestone 7 Azure Blob provider behavior.

Scope:
- lpe-storage backend routing and Azure Blob-specific behavior
- no AWS changes except shared abstractions required by Azure
- no protocol adapter changes
- no mailbox-level policy
- no automatic migration on policy changes

Required behavior:
- Azure Blob put/read/stat/verify works through BlobStore and placement metadata
- Azure container/blob naming, metadata, checksum, lease/concurrency, and error
  behavior are handled explicitly
- Azure health/readiness checks expose provider-specific degraded states without
  leaking credentials, account names where sensitive, connection strings, or
  blob names
- migration to and from Azure Blob placements uses explicit migration jobs only
- database-backed, generic S3-compatible, and AWS behavior remains unchanged

Verification:
- unit tests for Azure-specific configuration, blob naming, error mapping, and
  redaction
- Azure integration tests gated by environment variables
- cargo test -p lpe-storage

Report:
- changed files
- exact tests run
- skipped integration tests and required environment variables, if not run
```

### Prompt 6: Add Provider-Specific Operations And Restore Gates

```text
Add Milestone 7 operations, restore, and readiness gates for AWS S3 and Azure
Blob.

Scope:
- lpe-storage health/readiness diagnostics
- lpe-admin-api visibility only if existing storage health endpoints need new
  provider-specific states
- operations documentation where behavior changed
- no protocol adapter changes
- no mailbox-level policy

Required behavior:
- AWS and Azure degraded states are visible in summarized admin diagnostics
- restore procedures cover PostgreSQL plus provider-specific blob storage
- readiness behavior distinguishes required pools from optional/archive pools
- egress/cost and region/residency risks are documented
- user-facing protocols continue to fail as storage errors, not corrupted
  mailbox state

Verification:
- focused diagnostics tests for provider-specific degraded states
- AWS and Azure integration tests gated by environment variables for health and
  restore-critical operations
- cargo test -p lpe-storage
- cargo test -p lpe-admin-api if admin visibility changed

Report:
- changed files
- exact tests run
- skipped integration tests and required environment variables, if not run
```

### Prompt 7: Document Milestone 7 Completion

```text
Update only directly relevant architecture documentation for implemented
Milestone 7 provider-specific cloud backends.

Scope:
- docs/architecture/mailbox-storage-pools-roadmap.md
- docs/architecture/sql-schema-v2.md if storage-pool or placement schema
  behavior changed
- docs/architecture/data-lifecycle-and-compliance.md if lifecycle behavior
  changed
- docs/architecture/operations-and-disaster-recovery.md for restore, health,
  readiness, diagnostics, egress/cost, and residency behavior
- LICENSE.md only if a new accepted dependency exception is required and
  approved

Required documentation:
- AWS S3 and Azure Blob support remains behind the BlobStore boundary
- PostgreSQL remains metadata authority
- protocol adapters remain backend-agnostic
- raw RFC 5322 message blobs remain database-backed initially
- policy changes still do not implicitly migrate existing blobs
- credentials use the existing deployment secret model
- provider-specific risks and restore requirements are explicit

Verification:
- docs do not claim mailbox-level policy is implemented
- docs do not expose secret material, object keys, or provider internals
- run tests for any code touched
```

## Future Release: Operations Benchmarks And Restore Drills

After Milestone 7, the next release gate is proving the storage model at
realistic mailbox sizes and under provider failure modes.

Short roadmap:

1. Benchmark migration throughput across database-backed, S3-compatible, AWS S3,
   and Azure Blob pools.
2. Prove restore from PostgreSQL plus each configured blob backend.
3. Run degraded-pool drills for missing objects, checksum mismatch, auth
   failure, timeout, region outage, and provider throttling.
4. Capture JMAP, IMAP, ActiveSync, EWS, and MAPI attachment fetch behavior
   during degraded and recovered storage states.
5. Publish operations evidence before marking provider-specific backends as
   production-supported.

