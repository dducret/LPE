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
Domain names are globally unique because Internet mail routing cannot assign the
same domain to two tenants at once.

Exceptions are intentionally limited:

- `schema_metadata` is a singleton.
- `tenants` is the root owner table.
- platform/global administration tables may be tenantless only when they manage
  the LPE installation itself; tenant administration remains tenant-scoped.

## Mail Model

The mail model has four layers.

| Layer | Tables | Purpose |
| --- | --- | --- |
| Canonical content | `blobs`, `messages`, `message_headers`, `message_recipients`, `protected_bcc_recipients`, `mime_parts`, `message_bodies` | MIME fidelity, parsed projections, visible recipients, protected `Bcc` metadata |
| Mailbox membership | `mailboxes`, `mailbox_messages` | IMAP folders, per-mailbox UID allocation, mailbox-specific membership state |
| Search and AI projection | `mail_search_documents`, `document_projections`, `document_chunks` | Bcc-safe searchable and AI-facing text |

`messages` is not a mailbox row. A single `messages.id` can be represented in
one or more mailbox memberships through `mailbox_messages`.

`mailboxes` owns:

- `uid_validity`
- `uid_next`
- `modseq`
- special-use role
- hierarchy fields

Mailbox cached counters must remain internally consistent; `unread_messages`
cannot exceed `total_messages`. Display names are unique within a parent
folder, and root names are unique per account. Parent-folder lookup is indexed
for hierarchy sync and folder-list projections.

`mailbox_messages` owns:

- `imap_uid`
- `modseq`
- flags and keywords
- visibility and deletion state
- membership timestamps
- soft-delete / expunge state while visible to sync logic

UID allocation must update the mailbox row and insert membership rows in the
same transaction. Expunge removes the live membership row only after writing a
tombstone row.

Protocol adapters and tests must treat `uid_validity` and `uid_next` as mailbox
state. They must not derive `UIDNEXT` from the maximum currently visible
`mailbox_messages.imap_uid`, because expunged and otherwise hidden historical
memberships still consume UIDs. The retired global `message_imap_uid_seq` is not
part of schema v2.

## Change and Tombstone Model

Schema v2 uses counters plus append-only logs.

| Table | Scope | Use |
| --- | --- | --- |
| `account_sync_state` | account/category | current modseq for mail, contacts, calendars, tasks, rights |
| `mailboxes.modseq` | mailbox | IMAP `HIGHESTMODSEQ`, QRESYNC, mailbox-scoped refresh |
| `mail_change_log` | object | JMAP changes, push replay, MAPI ICS manifests, DAV sync, ActiveSync deltas |
| `tombstones` | deleted object | JMAP destroyed ids, IMAP expunge, MAPI ICS deletes, ActiveSync deletes |

Change, tombstone, and submission-event rows are insert-only after creation.
They may carry `retained_until` so cleanup jobs can prune old replay data only
after the configured protocol replay windows have passed. Tombstones capture
deleted message and mailbox-membership identifiers as historical facts rather
than foreign-keying back to rows that deletion or expunge may remove.

JMAP `Mailbox/changes` and `Email/changes` use `mail_change_log` replay when
the client state token carries a retained change cursor and the intervening rows
map cleanly to the requested JMAP type. Rows that affect the projection but
cannot be mapped precisely, such as rights changes for mailbox projections,
force a current-state diff fallback. This fallback is compatibility behavior,
not a protocol-local canonical store.

Protocol adapters store only cursor rows:

- `jmap_query_states` stores query token metadata and last observed canonical
  change sequence plus the ordered object id snapshot needed to resume JMAP
  `queryChanges` without embedding full snapshots in client-visible tokens.
- `activesync_sync_cursors` stores device, collection, last change sequence,
  and compact protocol cursor state needed to validate the current sync key and
  finish paged responses. This cursor state may contain object identifiers,
  fingerprints, and pending change ids, but not canonical message, contact,
  calendar, task, attachment, `Sent`, draft, outbox, search, rights, or
  quarantine data.
- `mapi_sync_checkpoints` stores EMSMDB/ICS folder or hierarchy cursor state.

None of these tables stores canonical messages, folders, contacts, calendars,
tasks, attachments, `Sent`, drafts, or outbox state.

## MIME, Body, Attachment, and Blob Model

`blobs` stores durable raw RFC 5322 bytes, MIME part bytes, and attachment bytes
with content hashes per tenant/domain. `messages.blob_id` points to the raw
message blob. Export and protocol
body fetches reconstruct from this canonical MIME plus parsed part metadata.
The raw blob reference is domain-bound so a message cannot point at a raw MIME
blob deduplicated under another domain in the same tenant.

`mime_parts` records MIME tree structure, headers, content IDs, file
names, transfer encodings, byte offsets where available, and links to durable
attachment blobs when the part is an attachment or inline binary body part.
Part-to-blob references are domain-bound so a message in one tenant domain
cannot point at a deduplicated blob owned by another domain in the same tenant.

`message_bodies` records Bcc-safe text and HTML body projections. Sanitized
HTML can be stored for client rendering; raw MIME remains the fidelity source.

`blobs` is deduplicated per tenant/domain/blob kind/content hash, with an
explicit per-domain uniqueness constraint for attachment blobs. It stores
Magika validation results, validation status, and extraction lifecycle fields.
Only `PDF`, `DOCX`, and `ODT` can enter text extraction. Other validated formats
remain downloadable but not indexed.
Lifecycle rows include update timestamps and worker-oriented indexes for Magika
validation, async extraction, and retry scheduling.
The schema enforces lifecycle timestamp consistency: completed Magika validation
requires `validated_at`, queued extraction jobs have no start or completion
time, running extraction jobs have a start time but no completion time, and
terminal extraction jobs have a completion time.

`attachments` is message/account metadata for a MIME part or uploaded file:
file name, disposition, content ID, ordinal, size, and `blob_id`.
Attachment metadata must prove the mailbox membership, canonical message, MIME
part, and attachment blob belong to the same tenant and domain.

`attachment_extraction_jobs` records async extraction attempts and results.
`attachment_texts` stores extracted Bcc-safe text and search vectors after a
successful job.

## Bcc Protection

Visible recipients live in `message_recipients` with `to`, `cc`, `from`,
`sender`, and `reply_to` roles. `Bcc` lives only in
`protected_bcc_recipients`.

The following tables must never include `Bcc` addresses or display names:

- `message_bodies`
- `mail_search_documents`
- `document_projections`
- `document_chunks`
- `mail_change_log.summary_json`
- protocol cursor/checkpoint payloads
- transport logs intended for normal user diagnostics

Audit/compliance access to `protected_bcc_recipients` must be explicit and
separate from user search, snippets, shared mailbox projections, and AI-facing
pipelines.

## Submission and Draft Model

Drafts are normal account-visible messages in the canonical `Drafts` mailbox.
They are not protocol-local rows.

Submission uses:

- `submission_queue` for canonical submission intent, sender authorization, and
  LPE-to-LPE-CT handoff state.
- `submission_recipients` for envelope recipients, including protected `Bcc`
  rows.
- `submission_events` for immutable relay, deferred, quarantine,
  bounce, failure, and duplicate-handoff results.

`LPE` creates the authoritative `Sent` mailbox membership before handoff to
`LPE-CT`. `LPE-CT` remains responsible for SMTP custody, retries, DKIM, SPF,
DMARC-related policy, queueing, quarantine, bounces, and DSN generation.
Queue and event rows are constrained so a transport result cannot be recorded
against a different submission than the queue item it describes.
Recipient ordinal uniqueness is enforced per message/submission recipient role.

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
`send_on_behalf` rights before creating `submission_queue` rows.
Identity references are account-bound: a submission or sender-right grant for a
specific identity may only reference an identity owned by the submitting or
delegating account.
Default identities must be send-enabled, and a primary account email address
must use the primary address kind.

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
`mail_change_log` and tombstones write to `tombstones`, allowing JMAP,
DAV, ActiveSync, EWS, and MAPI projections to synchronize from the same
canonical state.
Non-custom collection roles are unique per owner for mailboxes, contact books,
calendars, and task lists.
Mailbox hierarchy rows cannot parent themselves. Contact books, calendars, and
task lists also reserve normalized display names per owner, matching mailbox
name behavior for client projections.
Contacts, calendar events, and tasks have stable per-collection `uid` values for
DAV/JMAP/EWS/MAPI import, export, and sync mappings. JSON payload columns used
for contact addresses, phone numbers, and event attendees are constrained to
arrays. Contacts store structured name parts, email/phone/address arrays,
organization/title fields, notes, raw vCard text, and source/import metadata so
JMAP, DAV, EWS, and MAPI can project from one canonical row. Calendar events
store `UID`, `SEQUENCE`, organizer, attendees, recurrence, recurrence
exceptions, timezone, location, and body fields without adapter-local event
tables. Tasks store start, due, completed, priority, and recurrence fields where
the documented adapters expose them. Completed tasks must carry `completed_at`,
while non-completed tasks must not.

Object-level change logs and tombstones cover mailbox and collaboration
objects. Custom mailbox deletes, collaboration grants, mailbox delegation
grants, sender rights, contacts, calendars, events, task lists, and tasks must
write canonical change rows and tombstones so JMAP, DAV, EWS, MAPI,
ActiveSync, and web push can remove visibility after revocation or deletion
without maintaining protocol-local rights tables.

## LPE and LPE-CT Boundary

Core `LPE` schema includes:

- recipient verification cache or receipts only when they are needed for core
  idempotency
- inbound delivery receipts keyed by `LPE-CT trace_id`
- outbound queue rows prepared by canonical submission
- immutable outbound result history received from `LPE-CT`

Inbound receipts always reference a real recipient account. Delivered and
duplicate receipts additionally reference the committed canonical account
message; rejected receipts must not reference a committed message.

Core `LPE` schema excludes:

- Internet SMTP spool
- outbound relay spool
- quarantine indexes and quarantine message custody
- core antispam rule, quarantine setting, and quarantine item tables
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
- `blobs`
- `messages`
- `message_headers`
- `message_recipients`
- `protected_bcc_recipients`
- `mime_parts`
- `message_bodies`
- `mailbox_messages`
- `attachments`
- `attachment_extraction_jobs`
- `attachment_texts`
- `mail_search_documents`

### Sync, Changes, and Tombstones

- `account_sync_state`
- `mail_change_log`
- `tombstones`
- `jmap_upload_blobs`
- `jmap_query_states`
- `activesync_sync_cursors`
- `mapi_sync_checkpoints`

### Submission and Transport Integration

- `submission_queue`
- `submission_recipients`
- `submission_events`
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
- Do not add LPE-core `antispam_settings` or `antispam_filter_rules` tables.
  Perimeter filtering policy, quarantine retention, release, reject, and delete
  workflows belong to LPE-CT local stores and LPE-CT administration APIs. Core
  LPE may expose immutable LPE-CT handoff or delivery result history, but not
  quarantine custody or perimeter policy state.
- Add indexes around the access paths protocol adapters need: account/mailbox
  UID scans, account/category change scans, tombstone replay, attachment
  validation/extraction queues, outbound queue workers, and visible collection
  grants.
- Include worker and replay indexes for search projection freshness, tenant-wide
  category replay, and LPE-CT trace lookup.
- Prefer partial unique indexes for optional idempotency keys and nullable
  cursor scopes so the schema states the intended uniqueness directly.
- Include cleanup indexes for expiring JMAP staging uploads, query states,
  sessions, change logs, and tombstones.
