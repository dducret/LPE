# PostgreSQL Structural Rules

## Current State/Functionality Overview

PostgreSQL is the primary store for canonical `LPE` state. Schema structure must enforce tenant, account, mailbox, credential, session, and UID integrity in the database, not only in Rust.

## Implementation/Usage

- Keep canonical schema in `crates/lpe-storage/sql/schema.sql`.
- Enforce multi-tenant referential integrity with database constraints.
- Scope account data through tenant/domain/account ownership.
- Keep credential and session keys consistent with the multi-tenant schema.
- Model IMAP UIDs per mailbox, not globally.
- Enforce key integrity and uniqueness in PostgreSQL for:
  - domains
  - accounts
  - mailboxes
  - messages
  - credentials
  - sessions
  - IMAP UID mappings
  - outbound queue rows
  - attachment blobs
  - collaboration collections
- Keep `LPE-CT` technical stores separate from core `LPE` canonical PostgreSQL.
- Do not place canonical mailbox or collaboration state in `LPE-CT` local stores.
- Preserve fresh-install initialization from the canonical schema.

## Reference Table/List

| File | Purpose |
| --- | --- |
| `crates/lpe-storage/sql/schema.sql` | canonical database schema |
| `crates/lpe-storage/src/lib.rs` | storage adapter entry point |
| `docs/architecture/initial-architecture.md` | architecture source |
| `docs/architecture/lpe-ct-integration.md` | core/sorting-center bridge |
| `docs/architecture/mail-security-and-traceability.md` | mail security boundary |
