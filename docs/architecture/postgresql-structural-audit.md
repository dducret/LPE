# PostgreSQL Structural Audit

This audit targets the canonical `LPE` PostgreSQL model and the Rust code that depends on it.

## Scope

Reviewed sources:

- `README.md`
- `ARCHITECTURE.md`
- `docs/architecture/initial-architecture.md`
- `LICENSE.md`
- `docs/architecture/mail-security-and-traceability.md`
- `docs/architecture/lpe-ct-integration.md`
- `LPE-CT/docs/architecture/center-de-tri.md`
- `LPE-CT/docs/operations/mail-filtering.md`
- `crates/lpe-storage/migrations/*.sql`
- `crates/lpe-storage/src/lib.rs`

Requested file divergence:

- `docs/licensing/policy.md` does not exist in the repository at audit time.
- `LICENSE.md` contains the effective licensing policy and was used instead.

## Main findings

### 1. Multi-tenant referential integrity was incomplete

Many tables duplicated `tenant_id` but only referenced parent rows by `id`. That allowed cross-tenant drift between:

- `accounts` and `mailboxes`
- `accounts` and `messages`
- `messages` and recipients / attachments / outbound queue
- `accounts` and collaboration, `JMAP`, `ActiveSync`, `Sieve`, contacts, events, tasks

Correction implemented in the canonical creation script:

- composite uniqueness on parent tables with `UNIQUE (tenant_id, id)`
- composite foreign keys on child tables
- direct foreign keys from tenant-scoped credentials and sessions to their principals

Rust impact:

- no Rust change required
- existing write paths already bind `tenant_id` and parent ids coherently

### 2. Credential and session keys were inconsistent with the final multi-tenant schema

Migration `0019_multi_tenant_runtime.sql` changes:

- `admin_credentials` primary key to `(tenant_id, email)`
- `account_credentials` primary key to `(tenant_id, account_email)`

But Rust still used single-column conflict targets and incomplete joins.

Corrections implemented:

- `ON CONFLICT (tenant_id, account_email)` in [lib.rs](/C:/Development/LPE/crates/lpe-storage/src/lib.rs:1692)
- `ON CONFLICT (tenant_id, email)` in [lib.rs](/C:/Development/LPE/crates/lpe-storage/src/lib.rs:1934)
- `ON CONFLICT (tenant_id, email)` in [lib.rs](/C:/Development/LPE/crates/lpe-storage/src/lib.rs:1962)
- tenant-aware join to `admin_credentials` in [lib.rs](/C:/Development/LPE/crates/lpe-storage/src/lib.rs:2517)
- tenant-aware join to `account_credentials` in [lib.rs](/C:/Development/LPE/crates/lpe-storage/src/lib.rs:2611)

Risk before fix:

- runtime failure on upsert once the composite primary key exists
- possible cross-tenant credential/session ambiguity on email collisions

### 3. IMAP UID uniqueness was modeled globally instead of per mailbox

`IMAP` `UID`s are mailbox-scoped. The historical migration created a global unique index on `messages.imap_uid`, which is stricter than the protocol model and blocks future mailbox-local sequences.

Correction implemented in the canonical creation script:

- `UNIQUE (mailbox_id, imap_uid)` through `messages_mailbox_imap_uid_idx`
- supporting access index preserved on `(tenant_id, account_id, mailbox_id, imap_uid)`

Rust impact:

- no Rust change required
- existing reads already use mailbox-scoped access patterns

### 4. Several integrity rules were only enforced in Rust

The schema relied on application-side checks for:

- case-normalized emails
- non-empty identifiers and labels
- recipient kind visibility rules
- queue status vocabulary
- mailbox duplicate names
- non-negative counters and sizes

Corrections implemented in the canonical creation script:

- `CHECK` constraints on normalized emails, statuses, counters, and non-empty business fields
- generated `normalized_display_name` plus unique mailbox name constraint per account
- explicit `CHECK (kind IN ('to', 'cc'))` for visible recipients
- explicit `CHECK (metadata_scope = 'audit-compliance')` for protected `Bcc`

Rust impact:

- no Rust code change required
- current Rust normalization remains valid and now matches database guarantees

### 5. Historical migration chain remains fragile for fresh installs

The repository contains duplicate migration prefixes:

- `0006_admin_auth.sql`
- `0006_lpe_ct_integration.sql`
- `0007_outbound_transport_status_details.sql`
- `0007_pst_job_execution.sql`

The current Debian runner executes files lexicographically, so it works today, but the chain is not robust enough for a modern migration tool that expects unique versions.

Correction implemented:

- a canonical full schema creation script for fresh databases:
  [create_lpe_schema.sql](/C:/Development/LPE/crates/lpe-storage/sql/create_lpe_schema.sql)

Rust impact:

- no Rust change required

## Delivered artifacts

- Canonical fresh schema: [create_lpe_schema.sql](/C:/Development/LPE/crates/lpe-storage/sql/create_lpe_schema.sql)
- Rust multi-tenant credential/session fixes: [lib.rs](/C:/Development/LPE/crates/lpe-storage/src/lib.rs)

## Notes

- The audit deliberately keeps `LPE-CT` outside the core PostgreSQL business database, consistent with the documented split between `LPE` and the DMZ sorting center.
- The canonical schema strengthens invariants without reintroducing Internet-facing `SMTP` concerns into the core model.
