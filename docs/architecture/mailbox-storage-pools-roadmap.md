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

## Codex Prompt Pack: Milestone 1

Use these prompts one at a time. Each prompt assumes the previous prompt has
been completed and verified.

### Prompt 1: Inspect Current Blob Flows

```text
Read AGENTS.md, ARCHITECTURE.md, docs/architecture/initial-architecture.md,
LICENSE.md, docs/architecture/sql-schema-v2.md, docs/architecture/attachments-v1.md,
and docs/architecture/mailbox-storage-pools-roadmap.md.

Goal: inspect current blob creation, lookup, attachment fetch, JMAP upload, and
message export flows in lpe-storage without changing code.

Return:
- the files and functions that currently write blobs
- the files and functions that currently read blobs
- where raw message blobs, MIME part blobs, attachment blobs, and JMAP upload
  blobs are handled
- the smallest implementation boundary for a BlobStore abstraction
- risks or unclear points that need confirmation before editing

Do not refactor or edit files.
```

### Prompt 2: Design The Minimal Boundary

```text
Using the inspection results, propose the smallest internal BlobStore boundary
for lpe-storage.

Constraints:
- PostgreSQL remains the metadata authority.
- Raw RFC 5322 message blobs remain database-backed initially.
- Storage policy is evaluated at write time only.
- Protocol adapters must not know storage backends.
- Keep lib.rs as module declarations, re-exports, and minimal wiring only.
- Do not add cloud support.
- Do not add new dependencies unless absolutely required and checked against
  LICENSE.md.

Return:
- trait or service methods with Rust signatures
- the module where the boundary should live
- the current code paths that should call it
- tests required before and after implementation
- any simpler alternative and why it is or is not enough

Do not edit code yet.
```

### Prompt 3: Implement Local BlobStore Only

```text
Implement the approved minimal BlobStore boundary in lpe-storage.

Scope:
- local/current storage behavior only
- keep raw RFC 5322 message blobs database-backed initially
- no S3, Azure, AWS, private cloud, or admin UI
- no unrelated cleanup
- no changes to protocol semantics

Verification:
- add or update focused lpe-storage tests for storing, reading, stat/checksum,
  and attachment fetch through the boundary
- run cargo test -p lpe-storage

Report:
- changed files
- exact tests run
- any behavior intentionally left unchanged
```

### Prompt 4: Reconnect Protocol Fetch Paths

```text
Audit JMAP, IMAP, ActiveSync, EWS/MAPI, and export paths that fetch message or
attachment bytes.

Goal: ensure they still fetch through canonical lpe-storage behavior and do not
gain direct storage-backend knowledge.

Scope:
- only fix call sites broken or bypassed by the BlobStore boundary
- no new storage backends
- no protocol feature expansion

Verification:
- cargo test -p lpe-storage
- run the narrow protocol crate tests affected by changed call sites

Report:
- changed files
- protocol paths checked
- tests run
```

### Prompt 5: Document The Implemented Boundary

```text
Update only the directly relevant architecture documentation for the implemented
BlobStore boundary.

Scope:
- docs/architecture/mailbox-storage-pools-roadmap.md
- docs/architecture/sql-schema-v2.md if schema terminology changed
- docs/architecture/operations-and-disaster-recovery.md if restore behavior
  changed

Do not document future S3/AWS/Azure implementation as complete.

Verification:
- docs consistently say PostgreSQL is metadata authority
- docs consistently say blob bytes may be stored through the BlobStore boundary
- docs consistently say raw RFC 5322 message blobs remain database-backed
  initially
- docs consistently say policy changes do not implicitly migrate existing blobs
- docs do not claim mailbox movement is implemented unless tests prove it
```

## Codex Prompt Pack: Milestone 2

Use these prompts one at a time. Milestone 2 is only about PostgreSQL placement
metadata. It must not add a non-database backend, migration worker, admin UI,
cloud provider, mailbox-level policy, or automatic policy-triggered movement.

### Prompt 1: Inspect Milestone 1 Baseline

```text
Read AGENTS.md, ARCHITECTURE.md, docs/architecture/initial-architecture.md,
LICENSE.md, docs/architecture/sql-schema-v2.md, docs/architecture/attachments-v1.md,
and docs/architecture/mailbox-storage-pools-roadmap.md.

Goal: inspect the completed Milestone 1 BlobStore boundary and identify the
smallest schema change needed for Milestone 2 blob placement metadata.

Scope:
- lpe-storage only unless a test proves another crate must change
- inspect crates/lpe-storage/src/blob_store.rs
- inspect crates/lpe-storage/sql/schema.sql
- inspect schema_contract tests
- inspect durable attachment fetch/write paths

Return:
- current BlobStore methods and callers
- current blobs table fields and constraints
- proposed storage_pools and blob_placements columns
- exact constraints needed to keep tenant/domain/blob references safe
- risks or unclear points before editing

Do not edit files.
```

### Prompt 2: Design Minimal Placement Schema

```text
Design the minimal Milestone 2 schema for storage pool and blob placement
metadata.

Constraints:
- PostgreSQL remains the metadata authority.
- Blob bytes remain in the current PostgreSQL blobs.blob_bytes column.
- Raw RFC 5322 message blobs remain database-backed initially.
- Durable attachment and MIME-part blob kinds get placement metadata first.
- Storage policy is evaluated at write time only.
- No migration worker.
- No admin API or UI.
- No cloud, S3, Azure, AWS, or private object storage implementation.
- No mailbox-level policy.
- Do not add new dependencies.

Return:
- table names and columns
- CHECK constraints for storage pool kind and placement status
- uniqueness rules for active placements
- foreign keys proving tenant/domain/blob ownership
- indexes for fetch and future migration workers
- tests to add to schema_contract.rs
- any simpler alternative and why it is or is not enough

Do not edit code yet.
```

### Prompt 3: Implement Placement Metadata Schema

```text
Implement the approved Milestone 2 storage-pool and blob-placement schema.

Scope:
- update crates/lpe-storage/sql/schema.sql
- update docs/architecture/sql-schema-v2.md only if terminology or table groups
  need to reflect the new schema
- update schema_contract tests for table presence, constraints, foreign keys,
  and active-placement uniqueness
- no runtime behavior changes unless schema compilation/tests require a small
  adjustment

Required behavior:
- include a database-backed storage pool representation for current blobs
- allow placement rows for durable attachment and MIME-part blobs
- do not require placement rows for raw RFC 5322 message blobs
- do not duplicate blob bytes into placement rows

Verification:
- cargo test -p lpe-storage

Report:
- changed files
- exact tests run
- any runtime behavior intentionally left unchanged
```

### Prompt 4: Write Placement Rows For New Durable Blobs

```text
Wire Milestone 2 placement metadata into new durable attachment and MIME-part
blob writes.

Scope:
- lpe-storage BlobStore write path only
- database-backed placement rows only
- no migration worker
- no external storage backend
- no admin API or UI
- no protocol adapter storage-backend awareness

Required behavior:
- when a new durable attachment or MIME-part blob is created, create an active
  database-backed placement row in the same transaction
- duplicate/deduplicated durable blob writes must not create duplicate active
  placement rows
- raw RFC 5322 message blob writes remain database-backed and do not need
  placement rows in this milestone

Verification:
- add or update focused lpe-storage tests proving new blobs get one active
  placement
- add or update tests proving duplicate writes reuse existing placement metadata
- cargo test -p lpe-storage

Report:
- changed files
- exact tests run
- whether any existing blobs require backfill outside this milestone
```

### Prompt 5: Read Through Placement Metadata

```text
Make durable attachment and MIME-part BlobStore reads consult placement metadata
without changing the physical byte source.

Scope:
- lpe-storage BlobStore read/stat/verify paths only
- current database-backed placement only
- no external backend
- no migration worker
- no protocol adapter changes unless a lpe-storage API signature changes

Required behavior:
- read/stat/verify succeeds through an active database-backed placement
- missing active placement returns a storage-layer error or explicit failure,
  not "mailbox/message missing"
- wrong-tenant, wrong-domain, wrong-kind, and wrong-blob placement rows cannot
  satisfy reads

Verification:
- tests prove reads fail when the durable blob lacks an active placement
- tests prove wrong-domain placement cannot satisfy a read
- tests prove verify still checks checksum and size
- cargo test -p lpe-storage

Report:
- changed files
- exact tests run
- residual compatibility risk for pre-placement blobs, if any
```

### Prompt 6: Document Milestone 2 Completion

```text
Update only directly relevant architecture documentation for the implemented
Milestone 2 placement metadata.

Scope:
- docs/architecture/mailbox-storage-pools-roadmap.md
- docs/architecture/sql-schema-v2.md
- docs/architecture/operations-and-disaster-recovery.md only if restore
  behavior changed

Required documentation:
- storage pools and placements are metadata only in Milestone 2
- blob bytes still live in PostgreSQL
- raw RFC 5322 message blobs remain database-backed initially
- policy changes still do not implicitly migrate existing blobs
- no cloud backend, migration worker, or mailbox-level policy is implemented

Verification:
- docs do not claim transparent mailbox movement is implemented yet
- docs do not claim S3/AWS/Azure support is implemented yet
- cargo test -p lpe-storage if documentation touched schema_contract expectations
```

## Deferred Decisions

- When, if ever, raw RFC 5322 message blobs should move out of the database.
- Whether policy changes should gain an optional explicit "create migration
  plan" workflow after write-time policy behavior is proven.
- Whether public-cloud backends need a separate encryption key hierarchy after
  the existing deployment secret model is proven in restore drills.
- Whether mailbox-level policy is needed after tenant, domain, and account
  policy is proven.
