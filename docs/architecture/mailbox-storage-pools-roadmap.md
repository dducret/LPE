# Mailbox Storage Pools Roadmap

## Purpose

This document defines the roadmap for managing large `LPE` mailbox estates where
logical mailbox capacity can exceed the capacity or performance profile of one
physical storage device.

The target example is 1000 mailboxes with quotas up to 100 GB each. That is up
to 100 TB of logical mailbox capacity before replication, backup, retention,
legal hold, and migration safety windows.

## Assumptions

- PostgreSQL remains the authoritative store for canonical mailbox metadata,
  mailbox membership, sync state, quotas, retention, and rights.
- User-facing protocols continue to read and write canonical `LPE` state.
- `LPE-CT` does not store canonical mailbox or collaboration state.
- Blob bytes may live outside PostgreSQL once placement metadata and non-database
  storage backends are implemented. The current `BlobStore` boundary preserves
  database-backed storage while keeping protocols independent from that detail.
- Storage movement must be transparent to users and protocol clients.
- Administrators should not need to know object keys or disk paths, but they
  must see policy, placement, health, migration progress, and risk.

## Resolved Decisions

| Decision | Outcome |
| --- | --- |
| RFC 5322 message blobs | Keep database-backed initially. Move attachment and MIME blob handling behind the storage boundary first. |
| Storage policy evaluation | Evaluate storage policy at write time only. Policy changes do not trigger implicit immediate migration. |
| Public-cloud encryption | Use the existing deployment secret model for the first public-cloud release. |
| Mailbox-level policy | Defer mailbox-level policy until tenant, domain, and account policy are proven. |

## Success Criteria

- A mailbox can keep the same account id, mailbox ids, IMAP UIDs, JMAP ids,
  ActiveSync sync keys, and MAPI sync semantics while its message and attachment
  bytes move between storage pools.
- Blob movement uses copy, checksum verification, metadata switch, rollback
  window, and garbage collection.
- Export can reconstruct every message with its original blobs after movement.
- Deduplicated blobs remain tenant/domain safe.
- Quota accounting remains independent from the physical storage backend.
- Restore procedures cover PostgreSQL and canonical blob storage consistently.

## Non-Goals

- Do not move canonical mailbox state to `LPE-CT`.
- Do not make protocol adapters aware of disks, buckets, or cloud vendors.
- Do not add arbitrary pluggable data stores for metadata.
- Do not add new attachment indexing formats as part of storage movement.
- Do not copy implementation details from Stalwart or any license-incompatible
  project.

## Target Model

`LPE` should treat physical storage as named storage pools behind a canonical
blob layer. The implemented local/current `BlobStore` boundary now records
database-backed storage pool and placement metadata for durable attachment and
MIME-part blobs, but blob bytes still live in PostgreSQL and mailbox movement is
not implemented.

```text
PostgreSQL
  tenants
  domains
  accounts
  mailboxes
  messages
  mailbox membership
  quotas
  blob metadata
  blob placement
  migration jobs

Blob storage manager
  local filesystem pool
  local HDD pool
  local SSD pool
  private object storage pool
  S3-compatible pool
  future AWS S3 pool
  future Azure Blob pool
```

Mailboxes do not point to paths or buckets. Messages and MIME parts reference
canonical blob ids. PostgreSQL remains the metadata authority. Placement
metadata points durable attachment and MIME-part blobs at the current
database-backed pool only. Cloud, object storage, migration workers, and
mailbox-level policy remain future milestones.

## Storage Policy Levels

Storage policy should be assignable in this order of specificity:

| Level | Purpose |
| --- | --- |
| Platform default | Installation-wide fallback |
| Tenant | Business or compliance default |
| Domain | Domain-specific storage and residency |
| Account | VIP, archive, or high-volume account rules |

Policy should describe intent, not vendor details:

- hot pool
- archive pool
- minimum verified replicas
- migration window
- retention/legal-hold behavior
- maximum tolerated degraded time

Policy is evaluated when new canonical blob bytes are written. Changing a
policy records new intent for future writes; explicit migration jobs are
required to move existing blobs.

Mailbox-level policy remains deferred. The first admin surface should stop at
platform, tenant, domain, and account policy.

## Phased Plan

### Milestone 0: Architecture Decision

Document the boundary before code changes.

Deliverables:

- define `BlobStore`, `BlobPlacement`, `StoragePool`, and `StoragePolicy`
  terminology
- update architecture docs that currently imply blobs are database-only
- define restore and rollback expectations
- define dependency and license constraints for future storage adapters

Verification:

- documentation states that PostgreSQL remains metadata authority
- documentation states that protocol adapters do not talk to storage backends
- documentation states that blob movement is copy-verify-switch-cleanup

### Milestone 1: Internal Blob Storage Boundary

Create one internal blob storage abstraction inside `lpe-storage`.

Status: implemented for the local/current storage behavior. `lpe-storage`
contains an internal `BlobStore` boundary for durable attachment blobs and the
schema-supported MIME-part blob kind. The boundary supports put, read, stat, and
checksum/size verification while continuing to store bytes in PostgreSQL.
Protocol adapters continue to call canonical `lpe-storage` APIs and do not know
about storage backends.

Deliverables:

- a minimal Rust service boundary for put, read, stat, and verify
- a local/current storage implementation that preserves PostgreSQL-backed byte
  storage
- raw RFC 5322 message blobs remain database-backed initially
- JMAP upload blobs remain expiring PostgreSQL staging objects, not durable
  storage placements
- tests proving current attachment, protocol fetch, and export flows still work
- no cloud dependency yet
- delete-after-retention remains deferred until retention, legal-hold, and
  placement garbage-collection rules exist

Verification:

- `cargo test -p lpe-storage`
- protocol tests that fetch attachments still pass
- export tests still reconstruct messages with blobs

Current non-goals for Milestone 1:

- no storage-pool table
- no blob-placement table
- no non-database backend
- no automatic movement of existing blobs when policy changes
- no mailbox-visible movement semantics yet

### Milestone 2: Blob Placement Metadata

Teach PostgreSQL where a blob is stored without moving mailbox state.

Status: implemented for database-backed durable attachment and MIME-part blobs.
`storage_pools` and `blob_placements` are metadata only in this milestone.
Blob bytes still live in `blobs.blob_bytes`, and active database-backed
placements are required for durable attachment and MIME-part `BlobStore`
read/stat/verify paths. Raw RFC 5322 message blobs remain database-backed
initially and do not require placement rows.

Deliverables:

- storage pool table
- blob placement table
- placement metadata for attachment and MIME blobs first; raw RFC 5322 message
  blob placement can remain database-backed until later evidence justifies
  moving it
- placement status values such as `active`, `copying`, `verified`,
  `retiring`, and `failed`
- checksum and size verification fields
- indexes for blob fetch and migration workers
- no cloud backend, migration worker, admin UI, mailbox-level policy, or
  automatic policy-triggered movement

Verification:

- schema constraints prevent cross-tenant/domain placement mistakes
- tests prove a message can fetch a blob through placement metadata
- tests prove a missing active placement fails as a storage error, not as
  missing mailbox state
- policy changes still record intent for future writes only and do not
  implicitly migrate existing blobs

### Milestone 3: Online Migration Worker

Move blob bytes between pools without changing mailbox-visible state.

Deliverables:

- migration job table
- copy worker
- checksum verification
- atomic active-placement switch
- old-placement retention window
- retry and failure states

Verification:

- migration can be interrupted and resumed
- reads continue during migration
- rollback can use the old placement before garbage collection
- duplicate migration jobs do not corrupt placement state

### Milestone 4: Quota, Retention, and Legal Hold Integration

Keep account and domain quota behavior independent from physical placement.

Deliverables:

- quota accounting based on canonical logical size
- policy checks before deleting old placements
- legal hold protection for blobs referenced by held messages
- garbage collection rules for unreferenced placements

Verification:

- deleting an old placement cannot delete a held blob
- deduplicated blobs are kept while any live reference remains
- mailbox quota reports remain stable before and after movement

### Milestone 5: Admin Policy and Visibility

Expose storage management as policy and health, not object paths.

Deliverables:

- admin API for storage pools and policies
- migration status endpoints
- health indicators for degraded pools and missing replicas
- policy controls for platform, tenant, domain, and account levels only
- admin UI list and right-side drawer following the default management pattern

Verification:

- tenant admins see only allowed tenant/domain/account policy controls
- global admins can manage platform pools
- mailbox-level policy is not exposed in the first admin release
- UI exposes progress and risk without leaking secrets or provider internals

### Milestone 6: S3-Compatible Object Storage

Add the first non-local backend only after the internal boundary is proven.

Deliverables:

- S3-compatible storage adapter
- configuration and secret handling
- encryption integrated with the existing deployment secret model
- multipart upload/download policy where needed
- integration tests gated by environment variables

Verification:

- local tests still run without S3
- S3 integration tests prove upload, read, verify, migration, and delete
- dependency licenses are reviewed before adoption

### Milestone 7: Provider-Specific Cloud Backends

Add AWS S3 or Azure Blob only when generic S3-compatible behavior is not enough.

Deliverables:

- provider-specific configuration
- encryption integrated with the existing deployment secret model unless a
  later architecture decision explicitly introduces a separate key hierarchy
- provider health checks
- documented durability and consistency assumptions
- cloud-cost and egress warnings in operational docs

Verification:

- each provider has explicit integration tests
- restore drills cover PostgreSQL plus cloud blob storage
- failure modes are visible through readiness and operations diagnostics

### Milestone 8: Operations Benchmarks and Restore Drills

Prove that the design works at realistic mailbox sizes.

Deliverables:

- benchmark profiles for large mailbox `IMAP`, `JMAP`, and `ActiveSync` fetches
- migration throughput measurements
- restore playbook for PostgreSQL plus blob pools
- degraded-pool runbook

Verification:

- benchmark evidence uses deployed services, not mocks
- restore drill reconstructs messages and attachments
- protocol clients remain stable during storage movement

## Codex Prompt Pack: Milestone 3

Use these prompts one at a time. Milestone 3 introduces online migration job
metadata and a database-backed migration worker over existing placement rows.
It must not add S3, AWS, Azure, private object storage, admin UI, mailbox-level
policy, retention/legal-hold deletion, or automatic migration when a policy
changes.

### Prompt 1: Inspect Placement Baseline

```text
Read AGENTS.md, ARCHITECTURE.md, docs/architecture/initial-architecture.md,
LICENSE.md, docs/architecture/sql-schema-v2.md, docs/architecture/attachments-v1.md,
and docs/architecture/mailbox-storage-pools-roadmap.md.

Goal: inspect the completed Milestone 2 placement metadata and identify the
smallest Milestone 3 online migration worker design.

Scope:
- lpe-storage only unless a test proves another crate must change
- inspect crates/lpe-storage/src/blob_store.rs
- inspect crates/lpe-storage/sql/schema.sql
- inspect schema_contract tests
- inspect durable attachment and MIME-part placement read/write paths

Return:
- current storage_pools and blob_placements schema
- current BlobStore placement behavior
- proposed migration job table fields
- proposed worker states and transitions
- where copy, verify, switch, rollback window, and retry should live
- risks or unclear points before editing

Do not edit files.
```

### Prompt 2: Design Migration Job Schema

```text
Design the minimal Milestone 3 migration job schema for durable attachment and
MIME-part blob placement movement.

Constraints:
- PostgreSQL remains the metadata authority.
- Existing blob bytes still live in PostgreSQL.
- Source and target placements are database-backed in this milestone.
- Raw RFC 5322 message blobs remain database-backed initially and are out of
  migration scope.
- Storage policy is evaluated at write time only; policy changes do not create
  implicit migration jobs.
- No cloud, S3, AWS, Azure, or private object storage implementation.
- No admin API or UI.
- No mailbox-level policy.
- No retention/legal-hold garbage collection.
- Do not add new dependencies.

Return:
- table name and columns
- CHECK constraints for job kind/status
- foreign keys proving tenant/domain/blob/source/target ownership
- idempotency and duplicate-job prevention rules
- indexes for pending work, retries, and blob lookup
- schema_contract tests to add
- any simpler alternative and why it is or is not enough

Do not edit code yet.
```

### Prompt 3: Implement Migration Job Schema

```text
Implement the approved Milestone 3 migration job schema.

Scope:
- update crates/lpe-storage/sql/schema.sql
- update schema_contract tests for table presence, constraints, foreign keys,
  indexes, and duplicate-job prevention
- update docs/architecture/sql-schema-v2.md only if table groups or migration
  terminology need to reflect the new schema
- no worker logic yet
- no runtime behavior changes unless schema compilation/tests require a small
  adjustment

Required behavior:
- migration jobs target durable attachment and MIME-part blobs only
- raw RFC 5322 message blobs stay out of migration scope
- jobs reference real source and target storage pools or placements
- jobs can represent pending, running, verified, switched, failed, and cancelled
  states without deleting any old placement

Verification:
- cargo test -p lpe-storage

Report:
- changed files
- exact tests run
- any runtime behavior intentionally left unchanged
```

### Prompt 4: Add Migration Job API Inside lpe-storage

```text
Add internal lpe-storage functions for creating and loading blob migration jobs.

Scope:
- lpe-storage only
- internal Rust API only; no admin API or UI
- no worker copy/switch execution yet
- no protocol adapter changes
- no cloud backend

Required behavior:
- create an explicit migration job for a durable attachment or MIME-part blob
- reject raw RFC 5322 message blobs
- reject jobs without an active source placement
- reject jobs where source and target pool are the same unless the design has a
  concrete repair use case
- make duplicate create calls idempotent or return the existing pending/running
  job
- expose a query for pending/retryable jobs in deterministic order

Verification:
- focused lpe-storage tests for create, duplicate create, invalid blob kind,
  missing source placement, and pending-job query order
- cargo test -p lpe-storage

Report:
- changed files
- exact tests run
- any explicit non-goals preserved
```

### Prompt 5: Implement Copy And Verify Worker Step

```text
Implement the Milestone 3 worker step that copies and verifies a durable blob
from the active source placement to a target database-backed placement.

Scope:
- lpe-storage only
- database-backed source and target placements only
- no external backend
- no active-placement switch yet
- no deletion of source placement
- no admin API or UI

Required behavior:
- claim one pending/retryable job safely
- copy bytes through the BlobStore boundary
- create or reuse a target placement in a non-active verifying/verified state
- verify checksum and size before marking the job verified
- record retryable failure state with attempt count and next_attempt_at
- repeated worker execution is idempotent

Verification:
- tests prove interrupted/repeated worker execution does not create duplicate
  target placements
- tests prove checksum or size mismatch fails the job without changing the
  active source placement
- tests prove reads continue from the original active placement during copy
- cargo test -p lpe-storage

Report:
- changed files
- exact tests run
- remaining work before active switch
```

### Prompt 6: Implement Atomic Placement Switch

```text
Implement the Milestone 3 atomic switch from source active placement to verified
target placement.

Scope:
- lpe-storage only
- database-backed placements only
- no source deletion
- no retention/legal-hold garbage collection
- no admin API or UI
- no protocol adapter storage-backend awareness

Required behavior:
- switch only verified migration jobs
- in one transaction, mark the target placement active and the old active
  placement retiring
- keep the old placement available for rollback until a later cleanup milestone
- ensure there is only one active placement per durable blob after the switch
- duplicate switch execution must be idempotent
- BlobStore read/stat/verify must use the new active placement after switch

Verification:
- tests prove there is one active placement after switch
- tests prove repeated switch execution is safe
- tests prove reads continue before, during, and after switch
- tests prove rollback-window metadata remains on the retiring old placement
- cargo test -p lpe-storage

Report:
- changed files
- exact tests run
- cleanup behavior intentionally left for later milestones
```

### Prompt 7: Document Milestone 3 Completion

```text
Update only directly relevant architecture documentation for the implemented
Milestone 3 online migration worker.

Scope:
- docs/architecture/mailbox-storage-pools-roadmap.md
- docs/architecture/sql-schema-v2.md
- docs/architecture/operations-and-disaster-recovery.md only if restore or
  rollback behavior changed

Required documentation:
- migration is explicit job-driven movement, not automatic policy-triggered
  movement
- Milestone 3 uses database-backed placements only
- raw RFC 5322 message blobs remain database-backed initially
- source placements are retained after switch for rollback
- cleanup, retention/legal-hold integration, cloud backends, admin UI, and
  mailbox-level policy remain future milestones

Verification:
- docs do not claim S3/AWS/Azure support is implemented
- docs do not claim old placements are garbage-collected
- docs do not claim mailbox-level policy is implemented
- cargo test -p lpe-storage if documentation touched schema_contract
  expectations
```

## Deferred Decisions

- When, if ever, raw RFC 5322 message blobs should move out of the database.
- Whether policy changes should gain an optional explicit "create migration
  plan" workflow after write-time policy behavior is proven.
- Whether public-cloud backends need a separate encryption key hierarchy after
  the existing deployment secret model is proven in restore drills.
- Whether mailbox-level policy is needed after tenant, domain, and account
  policy is proven.
