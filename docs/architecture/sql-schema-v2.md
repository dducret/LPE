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

Tenant identity is a UUID identity in both SQL and Rust. The bootstrap platform
tenant row is:

- `00000000-0000-0000-0000-000000000001`
- slug `platform`
- display name `LPE Platform`

Runtime code refers to that row through `PLATFORM_TENANT_ID` as a `Uuid`.
Tenant-scoped tables must never receive string pseudo-tenants such as
`__platform__`. Platform/global operations that need to write into
tenant-scoped tables, including audit events and bootstrap administrator state,
use the real platform tenant UUID. Tables that model truly platform-wide scope,
such as storage policy assignment scope rows, may be tenantless only when their
schema explicitly allows that null tenant scope.

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
- mailbox-level retention policy in `retention_days`
- special-use role
- hierarchy fields

`mailbox_subscriptions` owns per-subscriber mailbox subscription state. JMAP
`Mailbox.isSubscribed` and IMAP `SUBSCRIBE`, `UNSUBSCRIBE`, and `LSUB` must use
that table; protocol adapters must not maintain separate subscription lists.

Mailbox cached counters must remain internally consistent; `unread_messages`
cannot exceed `total_messages`. Display names are unique within a parent
folder, and root names are unique per account. Parent-folder lookup is indexed
for hierarchy sync and folder-list projections.

`mailbox_messages` owns:

- `imap_uid`
- `modseq`
- flags and keywords, including Outlook category names projected through MAPI `PidNameKeywords`
- visibility and deletion state
- membership timestamps
- soft-delete / expunge state while visible to sync logic

`domains` owns domain defaults that affect mailbox runtime behavior, including
`default_sieve_script` for newly created or defaulted mailbox filtering and
`jmap_push_journal_retention_days` for tenant-domain JMAP push replay cleanup.
Those fields are schema fields, not admin-only view state.

`sieve_scripts` owns per-account Sieve documents and the single active script
selection used by inbound filtering and EWS OOF projection. OOF compatibility
must write the canonical active Sieve vacation script instead of maintaining an
EWS-local automatic-replies table. `sieve_vacation_responses` owns vacation
auto-reply suppression state keyed by account, sender, and vacation response
content.
Mailbox filtering rules are canonical `sieve_scripts` state. Protocol adapters
that expose rule-like behavior must project to or from Sieve-backed rule state
and must not add EWS-only, MAPI-only, or deferred-action-message rule stores.
Sieve script create, update, activation, rename, and delete paths write
canonical rule change rows; deletes write tombstones before removing the live
script.

UID allocation must update the mailbox row and insert membership rows in the
same transaction. Expunge removes the live membership row only after writing a
tombstone row.

Mailbox copy and move semantics are membership semantics, not message-row
rewrites. Copy creates another visible `mailbox_messages` row for the same
canonical `messages.id` and allocates a new UID from the target mailbox.
Move creates the target membership the same way, then marks the source
membership `visibility = 'expunged'`, preserves the original source
`imap_uid`, and writes a `tombstones` row with reason `move`. The target
membership receives a new UID from the target mailbox `uid_next`. Protocol
replay uses the move change row plus tombstone to report JMAP `Email/changes`,
mailbox count changes, IMAP QRESYNC-style expunge state, and MAPI checkpoint
replay without rewriting historical UIDs.

JMAP `Email/get` must expose all visible mailbox memberships for an email.
`mailboxIds` is derived from all visible `mailbox_messages` rows for the
requesting account. The storage projection also returns per-mailbox membership
state so compatibility adapters can distinguish mailbox-specific unread,
flagged, and draft values; the top-level unread/flagged/draft projection is a
rollup over visible memberships. `Email/query` deduplicates unscoped results by
canonical message id while mailbox-scoped queries still filter through the
selected membership.

Schema v2 keeps thread identity lightweight until the product needs first-class
thread lifecycle. `mailbox_messages.thread_id` and durable change-log
`threadId` summaries are enough for current JMAP thread projection and replay.
MAPI conversation actions use `conversation_actions.conversation_id` to persist
Outlook conversation-action FAI state against the existing lightweight thread
identifier. Same-store move actions also persist the resolved canonical target
mailbox id so delivery and non-MAPI protocol paths can apply the action without
parsing Outlook EntryIDs; cross-store move requests retain the raw Outlook
EntryIDs and leave the canonical target null. If broader thread
creation/deletion state, richer MAPI conversation
IDs, or retained JMAP `Thread/changes` need stable identity beyond message,
mailbox-message, and conversation-action summaries, add a real `threads` table
with tenant/account ownership and make messages or memberships reference it. Do
not stretch mailbox-message summary fields into an implicit thread store.

Protocol adapters and tests must treat `uid_validity` and `uid_next` as mailbox
state. They must not derive `UIDNEXT` from the maximum currently visible
`mailbox_messages.imap_uid`, because expunged and otherwise hidden historical
memberships still consume UIDs. The retired global `message_imap_uid_seq` is not
part of schema v2. Every new mailbox path uses the shared storage UIDVALIDITY
allocator rather than protocol-local constants or helper functions.

## Change and Tombstone Model

Schema v2 uses counters plus append-only logs.

| Table | Scope | Use |
| --- | --- | --- |
| `account_sync_state` | account/category | current modseq for mail, contacts, calendars, tasks, notes, journal, rights, search, rules, conversation actions |
| `mailboxes.modseq` | mailbox | IMAP `HIGHESTMODSEQ`, QRESYNC, mailbox-scoped refresh |
| `mail_change_log` | object | JMAP changes, push replay, MAPI ICS manifests, DAV sync, ActiveSync deltas |
| `tombstones` | deleted object | JMAP destroyed ids, IMAP expunge, MAPI ICS deletes, ActiveSync deletes |

Change, tombstone, and submission-event rows are insert-only after creation.
They may carry `retained_until` so cleanup jobs can prune old replay data only
after the configured protocol replay windows have passed. Tombstones capture
deleted message and mailbox-membership identifiers as historical facts rather
than foreign-keying back to rows that deletion or expunge may remove.

Replay rows are structurally constrained. `mail_change_log.summary_json` must
be a JSON object, affected-principal arrays must not contain nulls, and
mailbox-message rows must contain durable `messageId`, `threadId`, and
`imapUid` summary fields. Submission rows must carry `messageId` and `status`.
Tombstones reference the matching `(tenant_id, cursor, object_kind, object_id)`
change-log row so destroyed-object replay cannot point at an unrelated cursor.
Retained replay queries ignore rows whose `retained_until` has expired.

JMAP `Mailbox/changes`, `Email/changes`, `Thread/changes`,
`EmailSubmission/changes`, and collaboration object changes use
`mail_change_log` replay when the client state token carries a retained change
cursor and the intervening rows map cleanly to the requested JMAP type. Thread
replay requires mailbox-message log rows with durable `threadId` summary data;
older rows without that summary fall back to the current-state diff. Submission
creation and transport status changes write `submission` rows so
`EmailSubmission/changes` can replay from the same durable log. Rows that affect
the projection but cannot be mapped precisely force a current-state diff
fallback. Collection-level JMAP changes map contact-book, calendar, and
task-list grant rows through their durable `collectionId` summary. Item-level
visibility changes still fall back because one grant row can affect many child
objects. This fallback is compatibility behavior, not a protocol-local
canonical store.
Search-folder definitions, mailbox rule definitions, and Outlook conversation
actions also replay through the same durable object log. Built-in
Exchange-compatible search folders are stored as `search_folders` rows with
`object_kind = 'search_folder_definition'` change rows; Sieve-backed mailbox
rules use `object_kind = 'sieve_script'` change rows and tombstones;
Conversation Action Settings FAI messages are projections of
`conversation_actions` rows with `object_kind = 'conversation_action'` change
rows. These rows are canonical LPE state, not Exchange-only FAI message stores
or protocol-owned rule tables.

Protocol adapters store only cursor rows:

- `jmap_query_states` stores query token metadata and last observed canonical
  change sequence plus the ordered object id snapshot needed to resume JMAP
  `queryChanges` without embedding full snapshots in client-visible tokens.
- `activesync_sync_cursors` stores device, collection, last change sequence,
  and compact protocol cursor state needed to validate the current sync key and
  finish paged responses. This cursor state may contain object identifiers,
  fingerprints, hierarchy generation metadata, and pending change ids, but not
  canonical message, contact, calendar, task, attachment, `Sent`, draft,
  outbox, search, rights, or quarantine data. Expired ActiveSync cursor cleanup
  is scoped to device cursor rows and must not delete canonical mailbox or
  collaboration data.
- `activesync_devices` stores protocol-local device identity and provisioning
  state: account id, device id, device type, pending and active policy keys,
  provision status, last-seen time, and remote-wipe/account-only-wipe status.
  It is not a mailbox, contact, calendar, task, attachment, `Sent`, draft,
  outbox, search, rights, or quarantine store.
- `mapi_sync_checkpoints` stores EMSMDB/ICS folder or hierarchy cursor state.
  Hierarchy checkpoints are account-wide and must have `mailbox_id IS NULL`.
  Content and read-state checkpoints are folder/scope-scoped and must have a
  non-null `mailbox_id`: canonical folders use the real mailbox id, and virtual
  special folders use their stable projected folder UUID. MAPI checkpoints store
  positions over canonical change rows, not mailbox or message replicas.
- `mapi_mailbox_replicas` and `mapi_object_identities` store the durable MAPI
  identity projection for a canonical account: replica GUID, allocated
  48-bit global counters, FIDs, MIDs, source keys, change keys, and instance
  keys. These rows map canonical UUIDs to protocol identifiers; they do not
  store mailbox content, folder replicas, message bodies, attachments, `Sent`,
  drafts, outbox, or search state.
  The source, change, and instance key columns persist 22-byte REPLGUID-scoped
  XID/GID-compatible values. The 24-byte `LongTermID` form remains a protocol
  conversion value with the two-byte pad and is not stored in these columns.
- `mapi_named_properties` stores durable per-account named-property mappings
  for Outlook-cached property ids. `mapi_custom_property_values` stores opaque
  Outlook-specific property values by canonical object identity, property tag,
  and property type. These tables preserve object fidelity; they must not
  become protocol-local mailbox, `Sent`, outbox, search, rights, or AI/search
  projection state.

None of these tables stores canonical messages, folders, contacts, calendars,
tasks, attachments, `Sent`, drafts, or outbox state.

## MIME, Body, Attachment, and Blob Model

`blobs` stores durable raw RFC 5322 bytes, MIME part bytes, and attachment bytes
with content hashes per tenant/domain. PostgreSQL remains the metadata
authority for canonical blob identifiers, tenant/domain ownership, hashes,
lifecycle state, storage-pool configuration, placement metadata, and references
from mail objects. Durable attachment bytes and the schema-supported MIME-part
blob kind are accessed through the internal `lpe-storage` `BlobStore` boundary.
Raw RFC 5322 message blobs remain database-backed initially. `messages.blob_id`
points to the raw message blob.
Export and protocol body fetches reconstruct from this canonical MIME plus
parsed part metadata. The raw blob reference is domain-bound so a message cannot
point at a raw MIME blob deduplicated under another domain in the same tenant.
The schema enforces this with `blob_kind`: `messages` may reference only
`raw_message` blobs, and raw-message blobs must keep non-null
`blobs.blob_bytes`.

`mime_parts` records MIME tree structure, headers, content IDs, file
names, transfer encodings, byte offsets where available, and links to durable
attachment blobs when the part is an attachment or inline binary body part.
Part-to-blob references are domain-bound so a message in one tenant domain
cannot point at a deduplicated blob owned by another domain in the same tenant.
MIME part blob references may point only at durable `mime_part` or `attachment`
blobs, with the blob kind included in the foreign key.

`message_bodies` records Bcc-safe text and HTML body projections. Sanitized
HTML can be stored for client rendering; raw MIME remains the fidelity source.

`blobs` is deduplicated per tenant/domain/blob kind/content hash, with an
explicit per-domain uniqueness constraint for attachment blobs. It stores
Magika validation results, validation status, and extraction lifecycle fields.
Only `PDF`, `DOCX`, and `ODT` can enter text extraction. Other validated formats
remain downloadable but not indexed.
`storage_pools` and `blob_placements` record where durable attachment and
MIME-part blobs are stored. Database-backed pools read bytes from
`blobs.blob_bytes`. S3-compatible pools store bytes in provider-neutral object
storage using object keys derived from placement metadata rather than tenant,
domain, mailbox, message, or provider-specific identifiers, and externally
placed durable attachment/MIME-part blobs may have `blobs.blob_bytes = NULL`.
S3-compatible pool configuration records endpoint, bucket, signing-region or
region-like value, addressing style, optional object prefix, and deployment
secret reference; it must not store inline credentials. Raw RFC 5322 message
blobs remain database-backed initially, must keep non-null `blob_bytes`, and do
not require placement rows. Durable attachment and MIME-part `BlobStore`
read/stat/verify paths require an active placement row on an active supported
pool; a missing active placement is a storage-layer failure, not a missing
mailbox or message. Schema v2 still treats PostgreSQL as the authoritative
metadata store. Policy changes record intent for future writes only and do not
implicitly migrate existing blobs.
`storage_policy_assignments` stores admin-managed platform, tenant, domain, and
account policy assignments. Mailbox-level policy is not part of schema v2.
`blob_migration_jobs` records explicit online migration work for durable
attachment and MIME-part placements between database-backed and S3-compatible
pools, including retry, verification, switch, cancellation, and rollback-window
metadata. During the switch, the verified target placement becomes active and
the old source placement is retained as `retiring` with `rollback_until`
metadata. Old-placement cleanup transitions eligible non-active placement rows
through cleanup state to `deleted`. Cleanup is blocked while the rollback window
is active, while an active replacement is missing, while live canonical
message/MIME-part/attachment/extraction/text references still need the old
placement, or while blob/message retention or legal-hold metadata protects the
content. Placement cleanup does not delete canonical `blobs`, `messages`,
`mime_parts`, `attachments`, `attachment_extraction_jobs`, or
`attachment_texts` rows. Raw RFC 5322 message blobs remain database-backed
initially and outside migration and placement-cleanup scope. Admin APIs and UI
expose pool/policy summaries, health, migration jobs, and cleanup status
without exposing backend object keys, provider credentials, secrets, or
provider-specific backend internals. S3-compatible storage is provider-neutral
and is not AWS-specific or Azure-specific support. Provider-Specific Cloud
Backends remain future-release work.
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
`attachments`, `attachment_extraction_jobs`, and `attachment_texts` may
reference only `attachment` blobs. Composite foreign keys include `tenant_id`,
domain/message/membership identifiers where needed, `blob_id`, and `blob_kind`
so a row cannot silently cross domains or attach a raw-message blob.

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

Storage APIs follow the same boundary. Default mail fetches, IMAP fetch/search,
shared JMAP mailbox access, search documents, AI projections, and normal MIME
exports exclude protected `Bcc`. JMAP may include protected `Bcc` only through
the named protected fetch path and only for owned-account access requesting the
`bcc` property. Compliance or owner-only export paths must opt in through a
method name that makes protected metadata access explicit; accidental access
through generic fetch helpers is not allowed. Raw-message blob reads used by
default protocol paths strip `Bcc` headers before returning bytes.

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
Saving over an existing draft updates the existing draft mailbox membership and
writes a `mail_change_log` row for that mailbox-message projection; it must not
replace the draft through protocol-local state.

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
- `admin_credentials`
- `admin_sessions`
- `server_administrators`
- `server_settings`
- `security_settings`
- `admin_oidc_config`
- `account_oidc_config`
- `admin_oidc_identities`
- `account_oidc_identities`
- `admin_auth_factors`
- `account_auth_factors`
- `account_app_passwords`
- `local_ai_settings`

`account_identities` represents JMAP/EWS/MAPI send identities. Submission must
validate the authenticated actor against ownership, `send_as`, or
`send_on_behalf` rights before creating `submission_queue` rows.
Identity references are account-bound: a submission or sender-right grant for a
specific identity may only reference an identity owned by the submitting or
delegating account.
Default identities must be send-enabled, and a primary account email address
must use the primary address kind.
`accounts.directory_kind` uses the address-book projection values `person`,
`room`, and `equipment`; shared mailbox semantics belong to account/mailbox
kind and delegation fields, not to the directory-kind check.

Aliases route inbound recipient resolution to accounts or groups but do not
become independent mailbox owners unless backed by an account/shared mailbox.

Schema v2 supports internationalized mailbox addresses through centralized
normalization. Runtime code normalizes mailbox domains with IDNA/UTS 46 and
stores the ASCII domain form, while EAI local parts are retained as Unicode and
lowercased for LPE's mailbox identity comparisons. The schema exposes generated
normalized helper columns for domain names, account primary addresses,
additional account addresses, aliases, credentials, and sessions. Account,
alias, credential, inbound-recipient, submission, and login lookup paths use
those generated keys instead of duplicating ad hoc `lower(...)` comparisons.

## Collaboration Model

Contacts, calendars, and tasks use canonical collections and items:

- `contact_books`, `contacts`
- `calendars`, `calendar_events`, `calendar_event_attachments`
- `task_lists`, `tasks`
- `notes`
- `journal_entries`
- `contact_book_grants`, `calendar_grants`, `task_list_grants`

All collaboration objects are tenant-scoped and owner-account-scoped. Grants are
same-tenant only and use concrete tables with foreign keys to the owned
collection instead of a polymorphic `collection_id`. Changes write to
`mail_change_log` and tombstones write to `tombstones`, allowing JMAP,
DAV, ActiveSync, EWS, and MAPI projections to synchronize from the same
canonical state. Physical collaboration deletes write the tombstone before
removing the live row; shared task deletes include task-list grantees in the
affected principals so delegated projections can observe the removal.
Non-custom collection roles are unique per owner for mailboxes, contact books,
calendars, and task lists.
Mailbox roles include the standard mail roles plus canonical Outlook/Exchange
compatibility mail folders that are real mailboxes: `outbox`, `rss_feeds`,
`conversation_history`, `sync_issues`, `conflicts`, `local_failures`, and
`server_failures`.
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
exceptions, timezone, location, reminder metadata, and body fields without
adapter-local event tables. Calendar event attachments live in
`calendar_event_attachments`, point at durable attachment blobs, and participate
in calendar-category change logging; protocol adapters must project them from
that table instead of keeping Outlook or JMAP-local attachment state. Tasks
store start, due, completed, priority, recurrence, and reminder fields where the
documented adapters expose them.
Completed tasks must carry `completed_at`, while non-completed tasks must not.
Notes store sticky-note title/body, color, categories, and source metadata.
Journal entries store subject/body, entry type, MAPI message-class projection
metadata, start/end/occurred timestamps, company/contact metadata, and source
metadata.

Reminders do not have a canonical table. They are a computed API/search-folder
view over reminder-bearing canonical calendar events and tasks. Active reminder
queries exclude rows without `reminder_set`, rows with dismissed reminders,
completed tasks, and cancelled task/calendar state; diagnostic queries can
include inactive rows with explicit statuses.

Object-level change logs and tombstones cover mailbox and collaboration
objects. Custom mailbox deletes, collaboration grants, mailbox delegation
grants, sender rights, search-folder definitions, Sieve-backed rules, contacts,
calendars, events, task lists, and tasks must write canonical change rows and
tombstones so JMAP, DAV, EWS, MAPI, ActiveSync, and web push can remove
visibility after revocation or deletion without maintaining protocol-local
rights, search, or rule tables.
Grant and sender-right upsert/delete paths write object-level
`mail_change_log` rows before emitting rights journals. Collection grant rows
include `collectionId` summary data so `AddressBook/changes`,
`Calendar/changes`, and `TaskList/changes` can replay them durably; child item
changes fall back to a current-state diff when the row cannot map to one exact
JMAP object id.

Attachment metadata creates and deletes write `mail_change_log` rows with
`object_kind = 'attachment'` before message projection recomputation. They do
not create attachment tombstones in v2 because message-level export remains
anchored on canonical MIME and blob retention metadata.

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
- `server_settings`
- `security_settings`
- `admin_oidc_config`
- `account_oidc_config`
- `admin_oidc_identities`
- `account_oidc_identities`
- `admin_auth_factors`
- `account_auth_factors`
- `account_app_passwords`
- `local_ai_settings`
- `storage_policy_assignments`
- `audit_events`

### Mail, MIME, Search, and Attachments

- `mailboxes`
- `mailbox_subscriptions`
- `search_folders`
- `conversation_actions`
- `storage_pools`
- `blobs`
- `blob_placements`
- `blob_migration_jobs`
- `messages`
- `message_headers`
- `message_recipients`
- `protected_bcc_recipients`
- `mime_parts`
- `message_bodies`
- `mailbox_messages`
- `mailbox_pst_jobs`
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
- `activesync_devices`
- `activesync_sync_cursors`
- `mapi_sync_checkpoints`
- `mapi_mailbox_replicas`
- `mapi_object_identities`
- `mapi_named_properties`
- `mapi_custom_property_values`

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
- `calendar_event_attachments`
- `task_lists`
- `tasks`
- `notes`
- `journal_entries`
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
- Fresh schema initialization inserts the real platform tenant UUID row and the
  default PostgreSQL storage pool/policy rows. Runtime bootstrap must not
  synthesize pseudo-tenants.
- Use composite foreign keys containing `tenant_id`, and include account or
  domain ownership columns where same-tenant is not precise enough.
- Add a first-class `threads` table only when thread lifecycle, MAPI
  conversation IDs, or retained JMAP `Thread/changes` need stable durable
  thread identity beyond message-level `thread_id` summaries.
- Keep mailbox address comparison centralized through generated normalized
  email/domain/local-part helper columns. Runtime paths that accept mailbox
  domains must normalize with IDNA before persistence or lookup instead of
  reintroducing ad hoc lowercasing.
- Prefer ad hoc text state columns with table-level `CHECK` constraints for
  bounded state values while those state machines are still changing. Replace
  them with PostgreSQL enums only after state churn settles and the stricter
  migration semantics are worth the added rigidity.
- Do not add an LPE-core `antispam_quarantine` table. Quarantine custody is
  represented only as LPE-CT result history against submission or inbound
  delivery receipts.
- Do not add LPE-core `antispam_settings` or `antispam_filter_rules` tables.
  Perimeter filtering policy, quarantine retention, release, reject, and delete
  workflows belong to LPE-CT local stores and LPE-CT administration APIs. Core
  LPE may expose immutable LPE-CT handoff or delivery result history, but not
  quarantine custody or perimeter policy state. Core LPE admin API handlers
  must reject write attempts for these perimeter-owned settings and rules.
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
