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
metadata points durable attachment and MIME-part blobs at database-backed pools
only. Milestone 3 adds explicit migration jobs and a database-backed
copy/verify/switch worker over those placement rows. Cloud, object storage,
cleanup, retention/legal-hold integration, admin UI, and mailbox-level policy
remain future milestones.

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

Status: implemented for explicit job-driven movement between database-backed
durable attachment and MIME-part placements. Policy changes still record intent
for future writes only and do not create implicit migration jobs. Raw RFC 5322
message blobs remain database-backed initially and outside migration scope.
Source placements are marked `retiring` and retained with rollback-window
metadata after the active-placement switch; cleanup and deletion remain future
work.

Deliverables:

- migration job table
- copy worker
- checksum verification
- atomic active-placement switch
- source-placement rollback window
- retry and failure states
- database-backed source and target placements only
- no cloud backend, admin UI, mailbox-level policy, or retention/legal-hold
  garbage collection

Verification:

- migration can be interrupted and resumed
- reads continue during migration
- rollback can use the old placement before garbage collection
- duplicate migration jobs do not corrupt placement state
- exactly one active placement remains after switch

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

## Codex Prompt Pack: Milestone 4

Use these prompts one at a time. Milestone 4 integrates logical quota,
retention, legal hold, and safe placement cleanup rules after explicit
database-backed migration. It must not add S3, AWS, Azure, private object
storage, admin UI, mailbox-level policy, or automatic migration when a policy
changes.

### Prompt 1: Inspect Lifecycle Baseline

```text
Read AGENTS.md, ARCHITECTURE.md, docs/architecture/initial-architecture.md,
LICENSE.md, docs/architecture/sql-schema-v2.md,
docs/architecture/data-lifecycle-and-compliance.md,
docs/architecture/attachments-v1.md, and
docs/architecture/mailbox-storage-pools-roadmap.md.

Goal: inspect the completed Milestone 3 migration flow and identify the
smallest Milestone 4 lifecycle integration for logical quota, retention, legal
hold, and placement garbage collection.

Scope:
- lpe-storage only unless a test proves another crate must change
- inspect blob placement, migration, and BlobStore code
- inspect current quota/account/domain size accounting
- inspect retention and legal-hold schema or documented gaps
- inspect attachment/blob reference paths from messages, MIME parts,
  attachments, extraction jobs, and attachment texts

Return:
- current logical size accounting behavior
- current retiring placement behavior after migration
- current retention/legal-hold model and any missing schema
- exact blockers to deleting old placements safely
- proposed minimal Milestone 4 implementation slices
- risks or unclear points before editing

Do not edit files.
```

### Prompt 2: Design Lifecycle And Cleanup Rules

```text
Design the minimal Milestone 4 lifecycle rules for quota, retention, legal hold,
and old-placement cleanup.

Constraints:
- PostgreSQL remains the metadata authority.
- Quota accounting is based on canonical logical blob/message size, not the
  number of physical placements.
- Raw RFC 5322 message blobs remain database-backed initially.
- Storage policy is evaluated at write time only; policy changes do not create
  implicit migration jobs.
- Cleanup only applies to non-active old placements that are safe to remove.
- Do not delete canonical blobs, messages, MIME parts, attachments, extraction
  jobs, or attachment text rows in this milestone unless the existing code
  already has a safe deletion path.
- No cloud, S3, AWS, Azure, or private object storage implementation.
- No admin API or UI.
- No mailbox-level policy.
- Do not add new dependencies.

Return:
- lifecycle state rules for active, retiring, and deleted placement rows
- the retention and legal-hold checks required before deleting placement bytes
- the reference checks required before deleting an old placement
- how quota reports remain stable before and after migration
- schema additions, if any, and why they are necessary
- tests required for each rule
- any simpler alternative and why it is or is not enough

Do not edit code yet.
```

### Prompt 3: Implement Logical Quota Stability

```text
Implement Milestone 4 quota stability so mailbox/account/domain quota behavior
uses canonical logical size and is not affected by placement count.

Scope:
- lpe-storage quota and storage-overview paths only
- no admin API or UI
- no external storage backend
- no placement deletion yet
- no unrelated quota refactor

Required behavior:
- active plus retiring placements for one blob do not double-count quota
- moving a blob between database-backed placements does not change mailbox,
  account, or domain logical size reports
- deduplicated blobs continue to count according to the existing canonical
  quota model, not physical placement count

Verification:
- focused lpe-storage tests for quota before migration, during retiring
  placement state, and after cleanup eligibility
- tests include deduplicated attachment blobs where feasible
- cargo test -p lpe-storage

Report:
- changed files
- exact tests run
- any existing quota behavior intentionally preserved
```

### Prompt 4: Add Retention And Legal-Hold Guards

```text
Add the minimal Milestone 4 guards that prevent old placement cleanup while a
blob is protected by retention, legal hold, or live canonical references.

Scope:
- lpe-storage only
- old placement cleanup eligibility only
- no message deletion feature expansion
- no admin API or UI
- no external storage backend

Required behavior:
- a retiring placement is not cleanup-eligible before its rollback window
  expires
- a retiring placement is not cleanup-eligible while the blob is referenced by
  live messages, MIME parts, attachments, extraction jobs, or attachment texts
  in a way that still needs the placement
- a retiring placement is not cleanup-eligible when existing or newly added
  retention/legal-hold metadata says the blob must be preserved
- guards return explicit reasons that tests can assert

Verification:
- tests for rollback-window protection
- tests for live reference protection
- tests for retention/legal-hold protection
- cargo test -p lpe-storage

Report:
- changed files
- exact tests run
- any retention/legal-hold limitation that remains documented
```

### Prompt 5: Implement Safe Placement Cleanup Worker

```text
Implement the Milestone 4 cleanup worker for old non-active placements that have
passed all lifecycle guards.

Scope:
- lpe-storage only
- database-backed placements only
- old placement cleanup only
- no canonical blob/message deletion
- no active placement deletion
- no external storage backend
- no admin API or UI

Required behavior:
- cleanup claims eligible retiring placements deterministically
- cleanup never deletes the only active placement for a blob
- cleanup marks placement cleanup/deletion state before removing or clearing
  physical placement bytes, according to the approved design
- repeated cleanup execution is idempotent
- failed cleanup records retryable state without making the blob unreadable

Verification:
- tests prove active placement reads still work after old placement cleanup
- tests prove cleanup refuses the last active placement
- tests prove repeated cleanup is safe
- tests prove failed cleanup can retry
- cargo test -p lpe-storage

Report:
- changed files
- exact tests run
- cleanup behavior intentionally limited to old placements
```

### Prompt 6: Prove Export And Protocol Fetch Stability

```text
Verify that Milestone 4 quota and cleanup behavior does not break canonical
message export or protocol attachment fetches.

Scope:
- lpe-storage tests first
- protocol crate changes only if an existing test proves a regression
- no new protocol features
- no external storage backend
- no admin API or UI

Required behavior:
- export reconstructs messages and attachments after migration and old-placement
  cleanup
- JMAP, ActiveSync, EWS/MAPI, and IMAP-facing storage paths still use canonical
  lpe-storage APIs
- missing cleaned-up old placements do not surface as missing mailbox/message
  state

Verification:
- add or update focused lpe-storage tests for export/fetch after cleanup
- run cargo test -p lpe-storage
- run narrow protocol tests only if lpe-storage API signatures or behavior
  changed in a way protocol crates compile against

Report:
- changed files
- exact tests run
- protocol paths checked
```

### Prompt 7: Document Milestone 4 Completion

```text
Update only directly relevant architecture documentation for the implemented
Milestone 4 lifecycle integration.

Scope:
- docs/architecture/mailbox-storage-pools-roadmap.md
- docs/architecture/sql-schema-v2.md
- docs/architecture/data-lifecycle-and-compliance.md
- docs/architecture/operations-and-disaster-recovery.md only if restore,
  rollback, or cleanup behavior changed

Required documentation:
- quota accounting is logical and independent of placement count
- old placement cleanup is guarded by rollback windows, live references,
  retention, and legal hold
- canonical blobs/messages are not deleted by placement cleanup
- raw RFC 5322 message blobs remain database-backed initially
- policy changes still do not implicitly migrate existing blobs
- cloud backends, admin UI, and mailbox-level policy remain future milestones

Verification:
- docs do not claim S3/AWS/Azure support is implemented
- docs do not claim mailbox-level policy is implemented
- docs clearly distinguish placement cleanup from canonical message/blob
  deletion
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
