# Mailbox Storage Pools Roadmap

## Purpose

This document records the current status of the `LPE` mailbox storage-pool work
and the completed Milestone 6 Codex prompt pack.

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
- Milestone 5: admin policy and visibility implemented. Global administrators
  can manage platform storage pools and platform default policy. Tenant
  administrators can inspect allowed policy summaries and manage authorized
  tenant, domain, and account policy controls. Admin APIs and the admin UI
  expose pool health, placement state, migration status, cleanup status, and
  degraded or blocked metadata without exposing object keys, secrets, provider
  credentials, or provider-specific internals.
- Milestone 6: S3-compatible object storage. This adds the first non-database
  blob backend through the existing storage-pool and placement model. Durable
  attachment and MIME-part blobs can be written, read, statted, verified, and
  explicitly migrated between database-backed and S3-compatible placements.
  Health summaries expose provider-neutral backend states and required/optional
  readiness roles. The implementation remains provider-neutral and does not add
  AWS- or Azure-specific behavior.

Deferred from the current release:

- Provider-Specific Cloud Backends will be addressed in a future release. This
  includes AWS S3, Azure Blob, and provider-specific durability, consistency,
  cost, restore, and operational behavior.

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
- Admin visibility surfaces status, health, explicit migration jobs, and cleanup
  blockers only through summarized pool and policy data. It must not expose
  backend object keys, database internals, secrets, provider credentials, or
  provider-specific backend configuration.
- Every new dependency or external implementation idea must be checked against
  `LICENSE.md`.

## Codex Prompt Pack: Milestone 6

Use these prompts one at a time. Milestone 6 adds a provider-neutral
S3-compatible object-storage backend behind the existing `BlobStore`,
storage-pool, placement, migration, quota, cleanup, and admin visibility model.
It must not add AWS-specific behavior, Azure-specific behavior, mailbox-level
policy, or automatic migration when a policy changes.

### Prompt 1: Inspect Backend Boundary And License Options

```text
Read AGENTS.md, ARCHITECTURE.md, docs/architecture/initial-architecture.md,
LICENSE.md, docs/architecture/sql-schema-v2.md,
docs/architecture/data-lifecycle-and-compliance.md,
docs/architecture/operations-and-disaster-recovery.md, and
docs/architecture/mailbox-storage-pools-roadmap.md.

Goal: inspect the completed database-backed storage-pool implementation and
identify the smallest S3-compatible backend design for Milestone 6.

Scope:
- lpe-storage BlobStore, storage pool, placement, migration, quota, and cleanup
  code
- lpe-admin-api storage pool and visibility code
- current dependency set and LICENSE.md constraints
- operations and restore documentation for blob storage

Return:
- current backend assumptions in BlobStore and placement code
- current storage_pool kind/config/status fields
- candidate S3-compatible client options and their licenses
- whether any new dependency is required
- the minimal provider-neutral configuration shape
- files likely to change
- risks or unclear points before editing

Do not edit files.
```

### Prompt 2: Design S3-Compatible Backend Contract

```text
Design the Milestone 6 provider-neutral S3-compatible backend contract.

Constraints:
- PostgreSQL remains the metadata authority.
- Protocol adapters must remain backend-agnostic.
- Use existing storage_pool and blob_placement metadata where possible.
- S3-compatible support is provider-neutral; no AWS-only or Azure-only behavior.
- Raw RFC 5322 message blobs remain database-backed initially.
- Storage policy is evaluated at write time only.
- Existing blobs move only through explicit migration jobs.
- Mailbox-level policy remains deferred.
- Secrets must use the existing deployment secret model.
- Object keys, credentials, and provider internals must not leak through admin
  APIs or protocol responses.
- Do not add new dependencies unless absolutely required and checked against
  LICENSE.md.

Return:
- backend methods and error model
- storage pool configuration fields
- object key derivation rules that avoid tenant/domain leakage
- checksum and size verification rules
- retry and timeout behavior
- readiness and health behavior
- tests required before implementation
- any simpler alternative and why it is or is not enough

Do not edit code yet.
```

### Prompt 3: Implement Configuration And Backend Selection

```text
Implement the approved Milestone 6 configuration and backend-selection plumbing.

Scope:
- lpe-storage backend selection and storage-pool configuration handling
- lpe-admin-api validation only if storage-pool configuration shape changes
- no S3 data transfer yet
- no AWS-specific behavior
- no Azure-specific behavior
- no mailbox-level policy
- no automatic migration on policy changes

Required behavior:
- database-backed pools continue to work unchanged
- S3-compatible pools can be configured with endpoint, bucket, region or
  region-like value, path-style/virtual-hosted addressing mode where needed,
  and secret references
- credentials are not stored inline in normal database rows unless the approved
  design explicitly says they are protected
- admin summaries redact secrets and object key internals

Verification:
- focused lpe-storage tests for configuration validation and backend selection
- focused lpe-admin-api tests if API validation changed
- cargo test -p lpe-storage
- cargo test -p lpe-admin-api if admin API code changed

Report:
- changed files
- exact tests run
- explicit non-goals preserved
```

### Prompt 4: Implement S3-Compatible Put Read Stat Verify

```text
Implement Milestone 6 S3-compatible object put, read, stat, and verify behavior
behind the BlobStore backend boundary.

Scope:
- lpe-storage only unless compile errors require narrow callers
- S3-compatible backend only
- no AWS-specific behavior
- no Azure-specific behavior
- no admin UI
- no mailbox-level policy
- no automatic migration on policy changes

Required behavior:
- writes store bytes in the S3-compatible bucket under deterministic,
  tenant/domain-safe object keys
- reads fetch bytes through placement metadata only
- stat returns size and checksum metadata without downloading bytes when the
  backend supports it
- verify checks size and checksum against PostgreSQL metadata
- errors are mapped to storage-layer errors, not missing mailbox/message state
- database-backed behavior remains unchanged

Verification:
- unit tests for object-key derivation and error mapping
- integration tests gated by environment variables for put/read/stat/verify
- cargo test -p lpe-storage

Report:
- changed files
- exact tests run
- skipped integration tests and required environment variables, if not run
```

### Prompt 5: Wire Migration To S3-Compatible Pools

```text
Extend explicit migration jobs so durable attachment and MIME-part blobs can
move between database-backed and S3-compatible placements.

Scope:
- lpe-storage migration worker and BlobStore backend routing
- explicit migration jobs only
- no policy-triggered automatic migration
- no raw RFC 5322 message blob migration
- no AWS-specific behavior
- no Azure-specific behavior
- no admin UI changes unless an existing API response must expose a new
  provider-neutral status

Required behavior:
- migration can copy database-backed placement to S3-compatible placement
- migration can copy S3-compatible placement to database-backed placement
- migration can copy between two S3-compatible pools if both are configured
- checksum and size verification happen before active placement switch
- source placement remains available through the rollback window
- repeated worker execution is idempotent

Verification:
- focused lpe-storage tests for migration state transitions
- S3 integration tests gated by environment variables for copy/verify/switch
- cargo test -p lpe-storage

Report:
- changed files
- exact tests run
- skipped integration tests and required environment variables, if not run
```

### Prompt 6: Add Health Readiness And Operations Coverage

```text
Add Milestone 6 health, readiness, operations, and restore coverage for
S3-compatible storage pools.

Scope:
- lpe-storage health/readiness diagnostics
- lpe-admin-api visibility only if existing storage health endpoints need new
  provider-neutral fields
- operations documentation where behavior changed
- no AWS-specific behavior
- no Azure-specific behavior
- no mailbox-level policy

Required behavior:
- degraded S3-compatible pools are visible in health/status summaries
- missing object, checksum mismatch, auth failure, timeout, and unreachable
  endpoint failures map to distinct operational states where feasible
- readiness behavior is documented for required and optional pools
- restore procedure covers PostgreSQL plus S3-compatible object storage
- user-facing protocols continue to fail as storage errors, not corrupted
  mailbox state

Verification:
- focused diagnostics tests for degraded pool states
- S3 integration tests gated by environment variables for health checks
- cargo test -p lpe-storage
- cargo test -p lpe-admin-api if admin visibility changed

Report:
- changed files
- exact tests run
- skipped integration tests and required environment variables, if not run
```

### Prompt 7: Document Milestone 6 Completion

```text
Update only directly relevant architecture documentation for implemented
Milestone 6 S3-compatible object storage.

Scope:
- docs/architecture/mailbox-storage-pools-roadmap.md
- docs/architecture/sql-schema-v2.md if storage-pool or placement schema
  behavior changed
- docs/architecture/data-lifecycle-and-compliance.md if lifecycle behavior
  changed
- docs/architecture/operations-and-disaster-recovery.md for restore, health,
  readiness, diagnostics, and backup behavior
- LICENSE.md only if a new accepted dependency exception is required and
  approved

Required documentation:
- S3-compatible storage is provider-neutral and not AWS/Azure-specific support
- PostgreSQL remains metadata authority
- protocol adapters remain backend-agnostic
- raw RFC 5322 message blobs remain database-backed initially
- policy changes still do not implicitly migrate existing blobs
- credentials use the existing deployment secret model
- Provider-Specific Cloud Backends remain future-release work

Verification:
- docs do not claim AWS-specific or Azure-specific support is implemented
- docs do not claim mailbox-level policy is implemented
- run tests for any code touched
```

## Future Release: Provider-Specific Cloud Backends

Provider-Specific Cloud Backends will be addressed in a future release after
Milestone 6 S3-compatible object storage is proven. This future work must still
preserve the rule that PostgreSQL is the metadata authority and protocol
adapters never talk directly to cloud providers.

Short roadmap:

1. Provider-neutral hardening: use the S3-compatible backend evidence to prove
   failure injection, restore drills, and degraded pool diagnostics.
2. Dependency and license review: select allowed SDKs or protocol clients under
   the `LICENSE.md` policy before implementation.
3. AWS S3 backend: implement provider-specific configuration, health checks,
   upload/read/verify/delete behavior, retry semantics, and restore tests.
4. Azure Blob backend: implement equivalent provider-specific behavior and
   document consistency, authentication, and operational differences from AWS.
5. Operations release gate: benchmark migration throughput, restore from
   PostgreSQL plus cloud blobs, egress/cost warnings, readiness behavior, and
   failure-mode diagnostics before exposing the backends as supported.
