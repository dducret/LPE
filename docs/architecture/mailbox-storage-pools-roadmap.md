# Mailbox Storage Pools Roadmap

## Purpose

This document records the current status of the `LPE` mailbox storage-pool
work and the next active Codex prompt pack.

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
  Placement cleanup does not delete canonical blobs, messages, MIME parts,
  attachments, extraction jobs, or attachment text rows.

Active next step:

- Milestone 5: admin policy and visibility. This adds management APIs, health
  and migration status visibility, and the admin UI surface for storage pools
  and policy. It must not add cloud backends, mailbox-level policy, or automatic
  migration on policy change.

Deferred from the current release:

- Provider-Specific Cloud Backends will be addressed in a future release.
  This includes AWS S3, Azure Blob, and provider-specific durability,
  consistency, cost, restore, and operational behavior.

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
- Mailbox-level policy remains deferred until platform, tenant, domain, and
  account policy are proven.
- Raw RFC 5322 message blobs remain database-backed initially.
- Every new dependency or external implementation idea must be checked against
  `LICENSE.md`.

## Codex Prompt Pack: Milestone 5

Use these prompts one at a time. Milestone 5 exposes storage management as
policy, health, and migration visibility. It must not add S3, AWS, Azure,
private object storage, mailbox-level policy, or automatic migration when a
policy changes.

### Prompt 1: Inspect Admin And Storage Baseline

```text
Read AGENTS.md, ARCHITECTURE.md, docs/architecture/initial-architecture.md,
LICENSE.md, docs/architecture/sql-schema-v2.md,
docs/architecture/data-lifecycle-and-compliance.md,
docs/architecture/web-design.md, and
docs/architecture/mailbox-storage-pools-roadmap.md.

Goal: inspect completed storage-pool milestones and current admin API/UI
patterns before designing Milestone 5.

Scope:
- lpe-storage storage pool, placement, migration, quota, and cleanup code
- lpe-admin-api routes, authorization, and response patterns
- web admin list/drawer patterns
- existing health, dashboard, and diagnostics types

Return:
- current storage pool and policy data available in lpe-storage
- current migration and cleanup status data available in lpe-storage
- current admin roles and authorization checks relevant to platform, tenant,
  domain, and account policy
- files likely to change
- risks or unclear points before editing

Do not edit files.
```

### Prompt 2: Design Admin API Surface

```text
Design the minimal Milestone 5 admin API for storage pools, storage policy,
health, and migration visibility.

Constraints:
- PostgreSQL remains the metadata authority.
- No S3, AWS, Azure, or private object storage implementation.
- No mailbox-level policy.
- Policy changes do not implicitly create migration jobs.
- Tenant administrators can see and manage only allowed tenant/domain/account
  policy controls.
- Global administrators can manage platform storage pools and platform default
  policy.
- Do not expose backend object keys, database internals, secrets, or provider
  credentials.
- Do not add new dependencies unless absolutely required and checked against
  LICENSE.md.

Return:
- endpoints, methods, request bodies, and response bodies
- authorization matrix for global admin and tenant admin
- validation rules for platform, tenant, domain, and account policy
- health/status fields for degraded pools, missing active placements, retiring
  placements, migration jobs, and cleanup jobs
- tests required for authorization, validation, and visibility
- any simpler alternative and why it is or is not enough

Do not edit code yet.
```

### Prompt 3: Implement Storage Pool And Policy Admin APIs

```text
Implement the approved Milestone 5 admin APIs for storage pools and storage
policy.

Scope:
- lpe-storage storage/admin query and mutation functions
- lpe-admin-api request/response types and routes
- no admin UI yet
- no external storage backend
- no mailbox-level policy
- no automatic migration on policy changes

Required behavior:
- global admins can list and manage platform storage pools and platform default
  policy
- tenant admins can view allowed pool/policy summaries and manage tenant,
  domain, and account policy where authorized
- invalid policy references are rejected
- object keys, secrets, and provider-specific internals are never returned

Verification:
- focused lpe-storage tests for policy persistence and validation
- focused lpe-admin-api tests for routes and authorization
- cargo test -p lpe-storage
- cargo test -p lpe-admin-api

Report:
- changed files
- exact tests run
- explicit non-goals preserved
```

### Prompt 4: Implement Migration And Health Visibility APIs

```text
Implement Milestone 5 read-only visibility APIs for storage health, placement
state, migration jobs, and cleanup jobs.

Scope:
- lpe-storage query functions
- lpe-admin-api read-only endpoints
- no migration creation UI unless already approved by the API design
- no cloud backend
- no mailbox-level policy

Required behavior:
- expose pool health summary without leaking secrets
- expose counts for active, retiring, deleted, missing, or degraded placements
- expose migration job status, retry state, last error summary, and target pool
  summary
- expose cleanup status and blocked cleanup reasons
- tenant admins see only tenant-scoped data they are authorized to manage
- global admins see platform-wide status

Verification:
- tests for global and tenant visibility boundaries
- tests for degraded and blocked states
- cargo test -p lpe-storage
- cargo test -p lpe-admin-api

Report:
- changed files
- exact tests run
- status fields intentionally omitted for security or scope reasons
```

### Prompt 5: Build Admin UI List And Drawer

```text
Build the Milestone 5 admin UI for storage pool and policy visibility.

Read docs/architecture/web-design.md before editing UI files.

Scope:
- existing admin web UI only
- full-width storage pool/policy list
- primary action in the list header where creation or policy change is allowed
- right-side drawer for details, policy editing, health, and contextual actions
- no mailbox-level policy
- no cloud provider setup UI
- no placeholder runtime actions

Required behavior:
- global admins can inspect platform pools, default policy, health, migration
  status, and cleanup status
- tenant admins can inspect allowed tenant/domain/account policy and scoped
  status
- UI shows degraded, retiring, blocked, and failed states clearly
- UI never exposes object keys, secrets, or provider internals
- UI uses the shared Tailwind-based design system and default management
  pattern

Verification:
- run the relevant frontend build or tests available in the repository
- if a local dev server is required, open the admin UI and verify the storage
  management list/drawer in the browser
- verify text fits at desktop and mobile widths

Report:
- changed files
- exact verification run
- screenshots or browser checks if performed
```

### Prompt 6: Add Operations And Audit Coverage

```text
Add the minimal Milestone 5 operational and audit coverage for storage pool
policy and visibility.

Scope:
- admin/API audit events for policy changes where audit infrastructure exists
- health/readiness diagnostics for storage pool metadata consistency
- operations documentation only where behavior changed
- no cloud backend
- no mailbox-level policy

Required behavior:
- policy changes record who changed what, at which scope, and when
- degraded storage-pool metadata is visible through admin diagnostics
- readiness behavior is documented if storage-pool metadata can affect service
  readiness
- no user-facing protocol behavior changes

Verification:
- tests for audit/event records where supported
- tests or diagnostics checks for degraded metadata state
- cargo test -p lpe-storage
- cargo test -p lpe-admin-api

Report:
- changed files
- exact tests run
- operational gaps intentionally deferred
```

### Prompt 7: Document Milestone 5 Completion

```text
Update only directly relevant architecture documentation for implemented
Milestone 5 admin policy and visibility.

Scope:
- docs/architecture/mailbox-storage-pools-roadmap.md
- docs/architecture/sql-schema-v2.md if admin-facing schema behavior changed
- docs/architecture/data-lifecycle-and-compliance.md if policy or cleanup
  behavior changed
- docs/architecture/operations-and-disaster-recovery.md if health, readiness,
  restore, or diagnostics behavior changed
- docs/architecture/web-design.md only if a durable admin UI rule changed

Required documentation:
- platform, tenant, domain, and account policy are supported
- mailbox-level policy remains deferred
- policy changes still do not implicitly migrate existing blobs
- admin UI and APIs expose status, health, and migration visibility without
  leaking object keys, secrets, or provider internals
- Provider-Specific Cloud Backends remain future-release work

Verification:
- docs do not claim S3/AWS/Azure support is implemented
- docs do not claim mailbox-level policy is implemented
- docs clearly distinguish policy changes from explicit migration jobs
- run tests for any code touched
```

## Future Release: Provider-Specific Cloud Backends

Provider-Specific Cloud Backends will be addressed in a future release after
Milestone 5 admin policy and visibility is proven. This future work must still
preserve the rule that PostgreSQL is the metadata authority and protocol
adapters never talk directly to cloud providers.

Short roadmap:

1. Provider-neutral hardening: prove the storage backend contract with one
   non-local backend shape, failure injection, restore drills, and degraded
   pool diagnostics.
2. Dependency and license review: select allowed SDKs or protocol clients under
   the `LICENSE.md` policy before implementation.
3. AWS S3 backend: implement provider-specific configuration, health checks,
   upload/read/verify/delete behavior, retry semantics, and restore tests.
4. Azure Blob backend: implement equivalent provider-specific behavior and
   document consistency, authentication, and operational differences from AWS.
5. Operations release gate: benchmark migration throughput, restore from
   PostgreSQL plus cloud blobs, egress/cost warnings, readiness behavior, and
   failure-mode diagnostics before exposing the backends as supported.

