# SQL Schema v2

## Current State/Functionality Overview

`schema.sql` v1 stores several production concepts in the same row. Most
importantly, `messages` is both the canonical message and its mailbox
membership, IMAP `UID` and mod-sequence allocation are global, and core `LPE`
contains quarantine-style perimeter state that belongs to `LPE-CT`.

Schema v2 replaces that with a canonical PostgreSQL model. The database may be
recreated from scratch; no compatibility with the current SQL layout is
required.

## Design Goals

- Keep core `LPE` as the canonical owner for tenants, domains, accounts,
  mailboxes, contacts, calendars, tasks, ACLs, delegation, sender rights,
  message content, mailbox membership, submission state, search projections, and
  user-visible state.
- Keep `LPE-CT` state out of the core schema except for integration receipts,
  idempotency keys, delivery receipts, and outbound handoff result history.
- Store canonical message content separately from account visibility and mailbox
  membership.
- Allocate IMAP `UIDVALIDITY` and `UIDNEXT` per mailbox, not globally.
- Maintain per-account and per-mailbox mod-sequence counters plus durable
  object-level change and tombstone rows that can drive JMAP, IMAP QRESYNC,
  MAPI ICS, ActiveSync, DAV, and web push.
- Keep `Bcc` in protected metadata tables only. Search, snippets, AI document
  projections, MAPI manifests, and normal user projections must not include it.
- Treat JMAP upload blobs as expiring staging objects. Durable attachment blobs
  are created only after validation and attachment persistence.
- Treat ActiveSync and MAPI checkpoints as protocol-local cursors over
  canonical change state, never as canonical replicas.

## Ownership Model

Every durable tenant-scoped table includes `tenant_id`. User-visible or
account-owned state also includes the owning `account_id` or explicit
`owner_account_id`. Shared visibility is represented by grants, not by copying
canonical ownership.

Exceptions are intentionally limited:

- `schema_metadata` is a singleton.
- `tenants` is the root owner table.
- platform/global administration tables may be tenantless only when they manage
  the LPE installation itself; tenant administration remains tenant-scoped.

## Mail Model

The mail model has four layers.

| Layer | Tables | Purpose |
| --- | --- | --- |
| Canonical content | `message_raw_blobs`, `messages`, `message_headers`, `message_visible_recipients`, `message_protected_recipients`, `message_mime_parts`, `message_body_parts` | MIME fidelity, parsed projections, visible recipients, protected `Bcc` metadata |
| Account email object | `account_messages`, `account_message_keywords` | JMAP `Email` object state, thread linkage, per-account modseq |
| Mailbox membership | `mailboxes`, `mailbox_message_memberships` | IMAP folders, per-mailbox UID allocation, mailbox-specific membership state |
| Search and AI projection | `mail_search_documents`, `document_projections`, `document_chunks` | Bcc-safe searchable and AI-facing text |

`messages` is not a mailbox row. A single `messages.id` can be represented in
one or more accounts through `account_messages`, and each account-visible email
can appear in one or more mailboxes through `mailbox_message_memberships`.

`mailboxes` owns:

- `uidvalidity`
- `uidnext`
- `highest_modseq`
- special-use role
- hierarchy fields

`mailbox_message_memberships` owns:

- `imap_uid`
- `membership_modseq`
- membership timestamps
- soft-delete / expunge state while visible to sync logic

UID allocation must update the mailbox row and insert membership rows in the
same transaction. Expunge removes the live membership row only after writing a
tombstone row.

## Change and Tombstone Model

Schema v2 uses counters plus append-only logs.

| Table | Scope | Use |
| --- | --- | --- |
| `account_sync_state` | account/category | current modseq for mail, contacts, calendars, tasks, rights |
| `mailboxes.highest_modseq` | mailbox | IMAP `HIGHESTMODSEQ`, QRESYNC, mailbox-scoped refresh |
| `object_change_log` | object | JMAP changes, push replay, MAPI ICS manifests, DAV sync, ActiveSync deltas |
| `object_tombstones` | deleted object | JMAP destroyed ids, IMAP expunge, MAPI ICS deletes, ActiveSync deletes |

Change and tombstone rows may carry `retained_until` so cleanup jobs can prune
old replay data only after the configured protocol replay windows have passed.

Protocol adapters store only cursor rows:

- `jmap_query_states` stores query token metadata and last observed canonical
  change sequence.
- `activesync_sync_cursors` stores device, collection, and last change sequence.
- `mapi_sync_checkpoints` stores EMSMDB/ICS folder or hierarchy cursor state.

None of these tables stores canonical messages, folders, contacts, calendars,
tasks, attachments, `Sent`, drafts, or outbox state.

## MIME, Body, Attachment, and Blob Model

`message_raw_blobs` stores the canonical raw RFC 5322 bytes and content hash per
tenant/domain. `messages.raw_blob_id` points to that blob. Export and protocol
body fetches reconstruct from this canonical MIME plus parsed part metadata.

`message_mime_parts` records MIME tree structure, headers, content IDs, file
names, transfer encodings, byte offsets where available, and links to durable
attachment blobs when the part is an attachment or inline binary body part.
Part-to-blob references are domain-bound so a message in one tenant domain
cannot point at a deduplicated blob owned by another domain in the same tenant.

`message_body_parts` records Bcc-safe text and HTML body projections. Sanitized
HTML can be stored for client rendering; raw MIME remains the fidelity source.

`attachment_blobs` is deduplicated per tenant/domain/content hash. It stores
Magika validation results, validation status, and extraction lifecycle fields.
Only `PDF`, `DOCX`, and `ODT` can enter text extraction. Other validated formats
remain downloadable but not indexed.
Lifecycle rows include update timestamps and worker-oriented indexes for Magika
validation, async extraction, and retry scheduling.

`attachments` is message/account metadata for a MIME part or uploaded file:
file name, disposition, content ID, ordinal, size, and `attachment_blob_id`.
Attachment metadata must prove the account message, canonical message, MIME
part, and attachment blob belong to the same tenant and domain.

`attachment_extraction_jobs` records async extraction attempts and results.
`attachment_texts` stores extracted Bcc-safe text and search vectors after a
successful job.

## Bcc Protection

Visible recipients live in `message_visible_recipients` with `to`, `cc`, `from`,
`sender`, and `reply_to` roles. `Bcc` lives only in
`message_protected_recipients`.

The following tables must never include `Bcc` addresses or display names:

- `message_body_parts`
- `mail_search_documents`
- `document_projections`
- `document_chunks`
- `object_change_log.summary_json`
- protocol cursor/checkpoint payloads
- transport logs intended for normal user diagnostics

Audit/compliance access to `message_protected_recipients` must be explicit and
separate from user search, snippets, shared mailbox projections, and AI-facing
pipelines.

## Submission and Draft Model

Drafts are normal account-visible messages in the canonical `Drafts` mailbox.
They are not protocol-local rows.

Submission uses:

- `submission_requests` for canonical submission intent and sender
  authorization.
- `submission_recipients` for envelope recipients, including protected `Bcc`
  rows.
- `outbound_message_queue` for LPE-to-LPE-CT handoff state.
- `submission_result_history` for immutable relay, deferred, quarantine,
  bounce, failure, and duplicate-handoff results.

`LPE` creates the authoritative `Sent` mailbox membership before handoff to
`LPE-CT`. `LPE-CT` remains responsible for SMTP custody, retries, DKIM, SPF,
DMARC-related policy, queueing, quarantine, bounces, and DSN generation.

## Identity, Alias, and Sender Rights

Core identity tables are:

- `tenants`
- `domains`
- `accounts`
- `account_email_addresses`
- `aliases`
- `account_identities`
- `mailbox_delegation_grants`
- `sender_rights`

`account_identities` represents JMAP/EWS/MAPI send identities. Submission must
validate the authenticated actor against ownership, `send_as`, or
`send_on_behalf` rights before creating `submission_requests`.
Identity references are account-bound: a submission or sender-right grant for a
specific identity may only reference an identity owned by the submitting or
delegating account.

Aliases route inbound recipient resolution to accounts or groups but do not
become independent mailbox owners unless backed by an account/shared mailbox.

## Collaboration Model

Contacts, calendars, and tasks use canonical collections and items:

- `contact_books`, `contacts`
- `calendars`, `calendar_events`
- `task_lists`, `tasks`
- `contact_book_grants`, `calendar_grants`, `task_list_grants`

All collaboration objects are tenant-scoped and owner-account-scoped. Grants are
same-tenant only and use concrete tables with foreign keys to the owned
collection instead of a polymorphic `collection_id`. Changes write to
`object_change_log` and tombstones write to `object_tombstones`, allowing JMAP,
DAV, ActiveSync, EWS, and MAPI projections to synchronize from the same
canonical state.

## LPE and LPE-CT Boundary

Core `LPE` schema includes:

- recipient verification cache or receipts only when they are needed for core
  idempotency
- inbound delivery receipts keyed by `LPE-CT trace_id`
- outbound queue rows prepared by canonical submission
- immutable outbound result history received from `LPE-CT`

Core `LPE` schema excludes:

- Internet SMTP spool
- outbound relay spool
- quarantine indexes and quarantine message custody
- Bayesian filtering data
- greylisting state
- reputation state
- DKIM signing queues
- SPF/DMARC policy working state
- LPE-CT cluster coordination

Those belong to `LPE-CT` local stores and must never become canonical mailbox,
collaboration, rights, or user-visible state.

## Table Groups

### Tenancy and Accounts

- `schema_metadata`
- `tenants`
- `domains`
- `accounts`
- `account_email_addresses`
- `aliases`
- `account_identities`
- `account_credentials`
- `account_sessions`
- `admin_credentials`
- `admin_sessions`
- `server_administrators`

### Mail, MIME, Search, and Attachments

- `mailboxes`
- `message_raw_blobs`
- `messages`
- `message_headers`
- `message_visible_recipients`
- `message_protected_recipients`
- `message_mime_parts`
- `message_body_parts`
- `account_messages`
- `account_message_keywords`
- `mailbox_message_memberships`
- `attachment_blobs`
- `attachments`
- `attachment_extraction_jobs`
- `attachment_texts`
- `mail_search_documents`

### Sync, Changes, and Tombstones

- `account_sync_state`
- `object_change_log`
- `object_tombstones`
- `jmap_upload_blobs`
- `jmap_query_states`
- `activesync_sync_cursors`
- `mapi_sync_checkpoints`

### Submission and Transport Integration

- `submission_requests`
- `submission_recipients`
- `outbound_message_queue`
- `submission_result_history`
- `lpe_ct_inbound_delivery_receipts`

### Collaboration, ACLs, and Delegation

- `contact_books`
- `contacts`
- `calendars`
- `calendar_events`
- `task_lists`
- `tasks`
- `contact_book_grants`
- `calendar_grants`
- `task_list_grants`
- `mailbox_delegation_grants`
- `sender_rights`

### Search and AI Projections

- `document_projections`
- `document_chunks`
- `inference_runs`
- `inference_run_chunks`

Search and AI projection tables are derived, Bcc-safe, and local-execution
compatible. They do not replace canonical mail or collaboration tables.

## Implementation Notes

- `schema.sql` v2 should create a fresh `0.3.0-sql-v2` schema.
- Use composite foreign keys containing `tenant_id`, and include account or
  domain ownership columns where same-tenant is not precise enough.
- Prefer table-level `CHECK` constraints for bounded state values until the
  schema needs PostgreSQL enum migration semantics.
- Do not add an LPE-core `antispam_quarantine` table. Quarantine custody is
  represented only as LPE-CT result history against submission or inbound
  delivery receipts.
- Add indexes around the access paths protocol adapters need: account/mailbox
  UID scans, account/category change scans, tombstone replay, attachment
  validation/extraction queues, outbound queue workers, and visible collection
  grants.
- Prefer partial unique indexes for optional idempotency keys and nullable
  cursor scopes so the schema states the intended uniqueness directly.
- Include cleanup indexes for expiring JMAP staging uploads, query states,
  sessions, change logs, and tombstones.
