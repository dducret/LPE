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
- Blob bytes may live outside PostgreSQL once a documented blob storage
  boundary exists.
- Storage movement must be transparent to users and protocol clients.
- Administrators should not need to know object keys or disk paths, but they
  must see policy, placement, health, migration progress, and risk.

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
blob layer.

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
canonical blob ids. Blob metadata points to one or more verified storage
placements.

## Storage Policy Levels

Storage policy should be assignable in this order of specificity:

| Level | Purpose |
| --- | --- |
| Platform default | Installation-wide fallback |
| Tenant | Business or compliance default |
| Domain | Domain-specific storage and residency |
| Account | VIP, archive, or high-volume account rules |
| Mailbox | Exceptional mailbox placement only |

Policy should describe intent, not vendor details:

- hot pool
- archive pool
- minimum verified replicas
- migration window
- retention/legal-hold behavior
- maximum tolerated degraded time

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

Deliverables:

- a minimal Rust trait or service boundary for put, read, stat, verify, and
  delete-after-retention
- a local storage implementation that preserves current behavior
- tests proving current attachment and message flows still work
- no cloud dependency yet

Verification:

- `cargo test -p lpe-storage`
- protocol tests that fetch attachments still pass
- export tests still reconstruct messages with blobs

### Milestone 2: Blob Placement Metadata

Teach PostgreSQL where a blob is stored without moving mailbox state.

Deliverables:

- storage pool table
- blob placement table
- placement status values such as `active`, `copying`, `verified`,
  `retiring`, and `failed`
- checksum and size verification fields
- indexes for blob fetch and migration workers

Verification:

- schema constraints prevent cross-tenant/domain placement mistakes
- tests prove a message can fetch a blob through placement metadata
- tests prove a missing active placement fails as a storage error, not as
  missing mailbox state

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
- admin UI list and right-side drawer following the default management pattern

Verification:

- tenant admins see only allowed tenant/domain/account policy controls
- global admins can manage platform pools
- UI exposes progress and risk without leaking secrets or provider internals

### Milestone 6: S3-Compatible Object Storage

Add the first non-local backend only after the internal boundary is proven.

Deliverables:

- S3-compatible storage adapter
- configuration and secret handling
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
- docs do not claim mailbox movement is implemented unless tests prove it
```

## Open Decisions

- Whether raw RFC 5322 message blobs should move to external blob storage in the
  first implementation or remain database-backed until attachment blobs are
  proven.
- Whether storage policy is evaluated at write time only or can trigger
  immediate migration when changed.
- Whether public-cloud backends require a separate encryption key hierarchy or
  can rely on the existing deployment secret model for the first release.
- Whether mailbox-level policy is needed in the first admin release or should
  wait until tenant/domain/account policy is proven.
