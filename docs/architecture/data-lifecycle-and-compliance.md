# Data Lifecycle and Compliance

## Current State/Functionality Overview

`LPE` keeps canonical mailbox and collaboration state in PostgreSQL and protects metadata that must not appear in user search or AI pipelines. Retention, deletion, and legal hold rules must preserve auditability and exportability.

## Implementation/Usage

- Mailbox size tiers are quota policy inputs.
- Quota accounting uses canonical logical message/blob size and is independent
  of durable blob placement count. Active plus retiring placements for the same
  blob must not double-count mailbox, account, or domain usage.
- `PST` import and export must preserve canonical messages and attachments.
- Attachment blobs are deduplicated per domain.
- Export must reconstruct every message with its blobs.
- PostgreSQL remains the metadata authority for blob identity, placement state,
  hashes, lifecycle fields, and policy references. Protocol adapters use the
  `BlobStore` boundary and remain backend-agnostic.
- Retention and legal hold must apply before destructive deletion.
- Recoverable-items retention is canonical mailbox lifecycle behavior in core
  `LPE`. Hard delete and expunge remove normal mailbox visibility but preserve
  a `recoverable_items` row until its retention deadline expires.
- Legal hold keeps recoverable items active regardless of retention expiry.
  User or protocol purge may mark only unheld, retention-expired recoverable
  items as purged; physical message/blob cleanup remains blocked while any
  active recoverable item, message retention flag, legal hold, live mailbox
  membership, or placement guard still references the content.
- Recoverable Items Root, Deletions, Versions, and Purges are compatibility
  projections over canonical recoverable state. They are not normal JMAP/IMAP
  mailboxes and must not become protocol-local Exchange storage.
- Versions and Purges are bounded LPE lifecycle projections. LPE does not
  implement full Exchange copy-on-write Versions storage or user-visible Purges
  recovery parity unless a future architecture update adds canonical state for
  those behaviors.
- `/api/mail/recoverable-items` is the canonical owner-facing recovery API for
  active recoverable items. Restore creates a fresh mailbox membership and
  purge is allowed only after recoverable retention expires and legal hold is
  inactive.
- Old placement cleanup is not canonical message/blob deletion. It applies only
  to non-active placement rows that have passed their rollback window and have
  passed live-reference, retention, and legal-hold guards.
- Placement cleanup must not delete canonical `blobs`, `messages`,
  `mime_parts`, `attachments`, `attachment_extraction_jobs`, or
  `attachment_texts` rows.
- Raw RFC 5322 message blobs remain database-backed initially and are outside
  old-placement cleanup scope.
- Durable attachment and MIME-part blobs can use database-backed or
  S3-compatible placements. S3-compatible storage is provider-neutral object
  storage, not AWS-specific or Azure-specific support.
- Storage policy changes affect future writes only; they do not implicitly
  create migration jobs for existing blobs.
- Storage policy can be assigned at platform, tenant, domain, and account
  scope. Mailbox-level storage policy remains deferred.
- S3-compatible credentials use deployment secret references and must not be
  stored inline in normal storage-pool database rows.
- Admin policy and visibility surfaces must expose only summarized pool,
  placement, migration, and cleanup state. They must not expose object keys,
  secrets, provider credentials, or provider-specific backend internals.
- `Bcc`:
  - is protected metadata
  - must not be indexed in user search
  - must not be exposed to user-facing AI pipelines
  - must not be reinjected into visible projections
- Deletion must preserve required audit events.
- Attachment text extraction runs asynchronously.
- Indexed attachment formats are limited to `PDF`, `DOCX`, and `ODT`.

## Reference Table/List

| Data | Rule |
| --- | --- |
| primary store | `PostgreSQL` |
| search default | `PostgreSQL` |
| attachment dedupe | per domain |
| durable attachment/MIME-part placements | database-backed or provider-neutral S3-compatible |
| quota accounting | canonical logical size, not placement count |
| old placement cleanup | rollback-window, live-reference, retention, and legal-hold guarded |
| canonical blob/message deletion | not performed by placement cleanup |
| recoverable item retention | core `recoverable_items`, not protocol-local dumpster folders |
| raw RFC 5322 message blobs | database-backed initially |
| protected recipient metadata | `Bcc` |
| attachment text index formats | `PDF`, `DOCX`, `ODT` |
