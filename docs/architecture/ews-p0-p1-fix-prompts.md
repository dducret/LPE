# EWS P0/P1 Interoperability-Fix Prompts

This prompt set targets the **remaining interoperability gaps** of every
operation whose individual row is P0 or P1 in
`docs/architecture/ews-operation-contract.md`. It does not ask an agent to
reimplement handlers that already satisfy the documented bounded LPE contract.

The per-operation tables are authoritative. They contain **47 operations**:
23 P0 and 24 P1. The Priority Summary currently omits six P1 operations:
`FindConversation`, `GetConversationItems`, `AddDelegate`, `GetDelegate`,
`UpdateDelegate`, and `RemoveDelegate`. Those operations are intentionally
included below.

Run the prompts in order where a later prompt depends on canonical change-log,
Sieve, or synchronization behavior established by an earlier one. Each prompt
is deliberately bounded enough to be a reviewable implementation change.

## Shared Instructions

Prepend this block to every prompt below.

```text
Work in C:\Development\LPE. Implement the stated EWS priority fixes only.

Before editing, read AGENTS.md, ARCHITECTURE.md,
docs/architecture/initial-architecture.md, LICENSE.md,
docs/architecture/ews-operation-contract.md, and only the additional
architecture documents directly relevant to this prompt. Inspect the existing
handler, ExchangeStore method, PostgreSQL implementation, schema, and focused
tests before deciding that a change is needed. State concise assumptions, a
plan, and measurable success criteria first.

Treat the individual operation rows in ews-operation-contract.md as the source
of truth, not its incomplete Priority Summary. The goal is to eliminate proven
P0/P1 gaps within the documented bounded LPE scope, not to emulate unscoped
full Exchange behavior. Preserve every documented intentional bound unless
real-client evidence plus the architecture requires widening it.

EWS is an adapter over canonical LPE state. Do not create EWS-local mailbox,
folder, message, Sent, Outbox, rule, OOF, reminder, identity, subscription, or
configuration truth. Preserve tenant scoping, authenticated identity,
authorization, canonical modseq/change-log/tombstone behavior, and atomic
mutations. Unsupported or malformed shapes must fail with a parseable EWS
error before any mutation.

For Microsoft behavior, first inspect docs/microsoft/protocol-sources.toml and
then consult the applicable official Microsoft Learn Open Specifications. Add
the required source metadata before downloading a new reference. Cite protocol
ID and exact sections in code/tests/docs rather than copying specification text.

If a schema change is genuinely required, the supplied test database is
disposable: update crates/lpe-storage/sql/schema.sql as the canonical fresh
schema and update the matching schema-contract tests and relevant architecture
documentation directly. Do not create an upgrade or migration script. Never
put a database URL or credentials in source, documentation, test output, or a
commit. Do not add speculative columns or tables.

Add focused regression tests for successful behavior, authorization and
tenant isolation, malformed/unsupported input, no-partial-write behavior, and
cross-surface canonical state/change-log assertions. Use realistic SOAP
builders and fixed literals only for deterministic fixtures. Run the smallest
relevant cargo test filter first, then cargo test-lpe-exchange as the aggregate
gate; cargo test accepts only one test-name filter argument. If schema work is
done, also run relevant lpe-storage schema-contract tests and the applicable
TEST_DATABASE_URL-backed integration test. Do not build a deployment binary.

Finish with: changed files; exact test commands and results; the P0/P1 gaps
closed; intentional documented bounds that remain; and the Microsoft protocol
IDs/sections relied on. Do not mark a contract row Implemented unless evidence
shows no material gap remains within its documented LPE scope.
```

## 1. P0 Item Lifecycle And Canonical Submission

```text
Apply the shared instructions.

Close the documented P0 gaps in CreateItem, DeleteItem, FindItem, GetItem,
SendItem, and UpdateItem. Start with crates/lpe-exchange/src/service/ews/
items.rs, mail.rs, calendar.rs, contacts.rs, tasks.rs, public_folders.rs,
fields.rs, ids.rs, the ExchangeStore traits/implementations, and the related
EWS SOAP tests.

The supported scope is canonical mail, contacts, calendar events, tasks, and
public-folder posts. CreateItem must create/import only those bounded item
classes; SendItem must always use canonical submission and authoritative Sent
membership. DeleteItem must preserve the documented canonical deleted-event,
Trash, recoverable-item, hard-delete, and public-folder lifecycles. UpdateItem
must validate a whole single-item contact/calendar mutation against a current
ChangeKey before write; reject multi-item contact/calendar mutation until a
canonical atomic batch exists. FindItem/GetItem must project only the bounded
canonical properties and IDs that the contract permits.

Acceptance: SOAP tests prove create-draft then send produces exactly the
canonical Sent/submission state; supported item families round-trip through
Find/Get; update/delete produce consistent current ChangeKeys and change-log or
tombstone effects; invalid ID, stale ChangeKey, unauthorized target, and an
unsupported delete/update disposition leave no partial write; Bcc never enters
normal EWS projections, search, or AI paths. Do not add Exchange-local Outbox,
Sent, or item-property-bag storage.
```

## 2. P0 Folder Hierarchy

```text
Apply the shared instructions.

Close the P0 gaps in CreateFolder, DeleteFolder, FindFolder, and GetFolder.
Inspect crates/lpe-exchange/src/service/ews/folders.rs, mailboxes.rs,
public_folders.rs, sync_state.rs, ids.rs, fields.rs, the canonical mailbox and
public-folder storage APIs, and their SOAP tests.

Support only documented custom mailbox folders and permitted public-folder
children. Project canonical mailbox, collaboration collection, public-folder,
and supported distinguished-folder identities consistently. Reject protected,
system, cross-tenant, unauthorized, archive, and unsupported folder-class
shapes before mutation. A successful folder mutation must use the canonical
transaction/change-log path so hierarchy synchronization observes exactly the
committed state.

Acceptance: create/find/get/delete SOAP flows work for each supported folder
family; protected/system and inaccessible folders fail without mutation;
returned FolderId/ChangeKey and effective rights are stable across Find/Get;
subsequent SyncFolderHierarchy observes a create or delete exactly once. Do not
add Exchange archive, managed-folder, voice, or parallel folder state.
```

## 3. P0 File Attachments

```text
Apply the shared instructions.

Close the P0 gaps in CreateAttachment, GetAttachment, and DeleteAttachment.
Inspect crates/lpe-exchange/src/service/ews/attachments.rs, MIME/blob storage,
the Magika detector integration, attachment identifiers, and focused EWS and
storage tests.

Implement only file attachments on one supported canonical message parent.
Every client-provided attachment must pass Magika validation before normal
processing. Reuse canonical blobs, blob placements, MIME parts, attachment
rows, calendar-event attachment rows where applicable, deduplication, and
export reconstruction. Do not introduce ItemAttachment or an EWS-only blob
store.

Acceptance: validated file attachment create/get/delete round-trips content,
metadata, and LPE attachment IDs; unsafe or malformed input has no persistent
effect; parent item versions/change-log behavior remains correct; deletion does
not orphan or prematurely remove a deduplicated blob; authorization and
cross-tenant tests deny access. Bcc and unrelated MIME parts stay protected.
```

## 4. P0 Directory, Availability, And Time Zones

```text
Apply the shared instructions.

Close the P0 gaps in ResolveNames, GetUserAvailability, and
GetServerTimeZones. Inspect crates/lpe-exchange/src/service/ews/directory.rs,
availability.rs, calendar projections and grant checks, EWS XML/response
helpers, and relevant store APIs/tests.

ResolveNames must use only visible same-tenant accounts, directory entries,
accessible contact books, and contacts. GetUserAvailability must derive
free/busy only from canonical calendars/events and calendar grants for the
authenticated mailbox scope. GetServerTimeZones must provide a coherent,
versioned-or-explicitly-bounded catalog and request projection that matches the
calendar behavior LPE actually supports; do not pretend to offer a full
Exchange corpus.

Acceptance: tests cover tenant and visibility isolation, accessible versus
inaccessible contacts/calendars, ambiguous/invalid lookups, availability range
and grant enforcement, recurrence/time-zone boundary cases, and valid/invalid
time-zone requests. No operation may leak directory, event-detail, or
protected-recipient data, and no Exchange identity table may be introduced.
```

## 5. P0 Inbox Rules

```text
Apply the shared instructions.

Close the P0 gaps in GetInboxRules and UpdateInboxRules. Inspect
crates/lpe-exchange/src/service/ews/rules.rs, lpe-core Sieve ownership,
sieve_scripts and any rule-projection state, change/audit paths, and focused
EWS tests.

Expose and mutate only server-side rule shapes that map safely and reversibly to
canonical Sieve behavior. GetInboxRules must not present Exchange rule blobs or
client-only rules as canonical state. UpdateInboxRules must validate all
requested changes first and atomically apply the corresponding generated Sieve
mutation and required canonical change/audit rows.

Acceptance: create/update/delete supported rules round-trip through EWS and
the canonical Sieve projection; unsupported shapes and malformed rules are
rejected before mutation; a mixed valid/invalid request cannot partially
change Sieve; tenant/account isolation and concurrent-update behavior are
tested. Do not create an Exchange-only rules table or execute unvalidated
client-supplied script text.
```

## 6. P0/P1 Notifications

```text
Apply the shared instructions.

Close the P0 gaps in Subscribe, GetEvents, and Unsubscribe and the P1 gap in
GetStreamingEvents. Inspect crates/lpe-exchange/src/service/ews/notifications.rs,
mail_change_log storage/replay, EWS watermark helpers, and notification tests.

All event delivery must replay durable canonical mail_change_log changes using
validated deterministic cursors/watermarks and account/folder/event scopes.
Pull is the supported subscription model; streaming remains the documented
bounded immediate streaming-shaped replay, not Exchange push or long-held
affinity. Unsubscribe is compatibility cleanup because durable truth belongs to
the canonical replay cursor, not an EWS subscription row.

Acceptance: retained matching changes replay once in stable order; foreign,
tampered, expired, and scope-mismatched watermarks reject parseably; an empty
replay is valid; unsubscribe succeeds without altering mailbox truth; streaming
has the same filtering/expiry semantics as pull; restart and page-boundary
tests show no synthesized current-state events or event loss. Do not introduce
an EWS subscription table, push queue, or event truth outside canonical change
state.
```

## 7. P0 Folder Synchronization

```text
Apply the shared instructions.

Close the P0 gaps in SyncFolderHierarchy and SyncFolderItems. Inspect
crates/lpe-exchange/src/service/ews/sync_state.rs, folders.rs, items.rs,
public_folders.rs, canonical account_sync_state/mail_change_log/tombstone
storage, and synchronization SOAP tests.

Project only canonical mailbox folders, collaboration collections/grants,
public folders, and supported mail/collaboration/public-folder items. Make
opaque states durable enough for reliable incremental replay, pagination,
create/update/delete effects, and explicit expiry; do not merely regenerate a
current-state list under a reusable token. Keep the EWS scope bounded rather
than adding Exchange archive, voice, or search hierarchy semantics.

Acceptance: first sync, paged incremental sync, update, delete/tombstone,
reconnect/restart, cursor expiry, access revocation, and public/collaboration
scope tests pass; hierarchy and item changes agree with the canonical
change-log/version that committed them; invalid or foreign state has no effect.
No EWS-local item store or parallel durable synchronization truth is allowed.
```

## 8. P1 Mail And Folder Workflow Mutations

```text
Apply the shared instructions.

Close the P1 gaps in CopyItem, MoveItem, MarkAllItemsAsRead, EmptyFolder, and
UpdateFolder. Inspect crates/lpe-exchange/src/service/ews/items.rs,
folders.rs, public_folders.rs, recoverable-items and message/mailbox storage,
and the focused SOAP tests.

Copy/move only the documented canonical message and public-folder item
families. MarkAllItemsAsRead must use a bounded, safe canonical bulk mutation
with coherent change-log effects rather than per-item partial behavior.
EmptyFolder must obey canonical deletion/recoverable-item/retention lifecycles,
and UpdateFolder may change only documented DisplayName values. Protected or
system folders and unsupported public-folder moves must reject before write.

Acceptance: each successful operation has correct SOAP output and canonical
membership/read-state/deletion/change-log results; injected or modeled failure
does not leave a half-copied/moved/emptied batch; bounds such as the maximum
bulk size are explicit and parseably enforced; cross-tenant, inaccessible, and
protected targets fail unchanged. Never add EWS-only copy, move, or read-state
state.
```

## 9. P1 Reminders And Conversations

```text
Apply the shared instructions.

Close the P1 gaps in GetReminders, PerformReminderAction, FindConversation,
and GetConversationItems. Inspect crates/lpe-exchange/src/service/ews/
reminders.rs and conversations.rs, canonical calendar/task/follow-up reminder
state, reminder_occurrence_dismissals, and mailbox_messages.thread_id.

Reminders remain a computed canonical view. Dismiss and Snooze must atomically
affect the canonical event, task, message follow-up, or occurrence-dismissal
state and survive a later query. Conversations remain folder-scoped groupings
over lightweight message thread_id; do not introduce a first-class thread store
until the documented lifecycle threshold is met.

Acceptance: due/active reminders are returned while dismissed, cancelled, or
completed occurrences follow documented visibility; dismiss/snooze round-trip;
unsupported actions and mixed invalid item batches are atomic failures;
conversation find/get observes current permitted folder membership and handles
stale/inaccessible IDs safely. Test recurrence, paging, and authorization. Do
not add an Exchange reminder table or persistent future-message conversation
actions.
```

## 10. P1 ConvertId

```text
Apply the shared instructions.

Close the P1 gaps in ConvertId. Inspect crates/lpe-exchange/src/service/ews/
ids.rs and its tests before changing any identity code.

Keep the documented stateless LPEEWS1 opaque-ID codec authoritative. Support
only canonical message, folder, contact, event, task, attachment, public-folder,
and public-folder-item families and their documented alternate ID shapes. Do
not add an Exchange identity table, MAPI EntryId emulation, or a decoding path
that bypasses authorization at the operation that consumes an ID.

Acceptance: each supported EwsId/opaque/HexEntryId conversion round-trips
deterministically; malformed, unknown-family, wrong-shape, and cross-family
input returns a valid EWS error; public-folder folders/items use their required
distinct alternate elements; tests prove conversion itself does not disclose
object existence across tenants.
```

## 11. P1 Rooms, OOF, And Mail Tips

```text
Apply the shared instructions.

Close the P1 gaps in GetRoomLists, GetRooms, GetUserOofSettings,
SetUserOofSettings, and GetMailTips. Inspect crates/lpe-exchange/src/service/
ews/rooms.rs, oof.rs, mail_tips.rs, directory visibility APIs,
sieve_scripts/sieve_vacation_responses, and their focused tests.

Room lists are the documented computed same-tenant room/equipment projection
from directory_kind, not arbitrary Exchange room-list membership. OOF is a
projection and mutation of canonical Sieve vacation behavior: disabling clears
the active script; scheduled and external-audience behavior stays bounded.
MailTips returns only the documented invalid-recipient and OOF advice from the
canonical directory and vacation state.

Acceptance: room/equipment and hidden/foreign filtering work; explicit
nonmatching room-list filters reject; OOF enabled, disabled, scheduled, and
external-audience settings round-trip safely through generated Sieve; malformed
or unauthorized updates leave no script change; MailTips are tenant-scoped and
Bcc-safe. Do not add OOF, room-membership, or mail-tip protocol-local tables,
and do not expose quota, moderation, policy, or custom Exchange tips.
```

## 12. P1 Delegate Management

```text
Apply the shared instructions.

Close the P1 gaps in AddDelegate, GetDelegate, UpdateDelegate, and
RemoveDelegate. Inspect crates/lpe-exchange/src/service/ews/delegation.rs,
canonical delegation storage/APIs, mailbox_delegation_grants, calendar_grants,
sender_rights, delegate_preferences, audit/change rows, and focused tests.

Use the bounded canonical model only: same-tenant Inbox/Calendar grants,
send-on-behalf, and documented meeting-delivery copy/private preferences.
Delegate reads must reflect that same canonical tuple. Mutations must validate
the entire request and atomically update grants, sender rights, preferences,
audit/change data, and any required free/busy projection version. Reject
Exchange-only delegate-folder or unsupported permission shapes rather than
widening rights.

Acceptance: add/get/update/remove round-trip through the canonical data model;
cross-tenant, self/invalid delegate, unsupported permission, and unauthorized
cases fail before mutation; removing a delegate removes all documented canonical
rights/preferences and produces required tombstone/change behavior; concurrent
updates cannot split the delegate tuple. Do not create EWS-local delegate or
folder-permission state.
```

## 13. P1 User Configuration CRUD

```text
Apply the shared instructions.

Close the P1 gaps in CreateUserConfiguration, DeleteUserConfiguration,
GetUserConfiguration, and UpdateUserConfiguration. Inspect
crates/lpe-exchange/src/service/ews/user_configuration.rs, ExchangeStore and
PostgreSQL implementations, account_client_configurations, and focused tests.

Store only the documented bounded canonical dictionary, XML, and binary payloads
keyed by account, permitted mailbox/public-folder scope, configuration class,
and configuration name. Enforce payload and dictionary bounds before write.
Update must advance canonical modseq and the required audit/change projection;
missing delete/get behavior must return the documented EWS item-not-found error.

Acceptance: account-, mailbox-, and permitted public-folder-scoped create/get/
update/delete round-trip; selective-property retrieval works; foreign scope,
malformed payload, duplicate/conflicting key, over-limit payload, and missing
row behaviors are tested; writes are atomic and isolated by account/tenant. Do
not use FAI or an arbitrary Exchange user-configuration store.
```

## Coverage Checklist

| Prompt | Operations | Priority |
| --- | --- | --- |
| 1 | CreateItem, DeleteItem, FindItem, GetItem, SendItem, UpdateItem | P0 |
| 2 | CreateFolder, DeleteFolder, FindFolder, GetFolder | P0 |
| 3 | CreateAttachment, GetAttachment, DeleteAttachment | P0 |
| 4 | ResolveNames, GetUserAvailability, GetServerTimeZones | P0 |
| 5 | GetInboxRules, UpdateInboxRules | P0 |
| 6 | Subscribe, GetEvents, Unsubscribe; GetStreamingEvents | P0; P1 |
| 7 | SyncFolderHierarchy, SyncFolderItems | P0 |
| 8 | CopyItem, MarkAllItemsAsRead, MoveItem, EmptyFolder, UpdateFolder | P1 |
| 9 | GetReminders, PerformReminderAction, FindConversation, GetConversationItems | P1 |
| 10 | ConvertId | P1 |
| 11 | GetRoomLists, GetRooms, GetUserOofSettings, SetUserOofSettings, GetMailTips | P1 |
| 12 | AddDelegate, GetDelegate, UpdateDelegate, RemoveDelegate | P1 |
| 13 | CreateUserConfiguration, DeleteUserConfiguration, GetUserConfiguration, UpdateUserConfiguration | P1 |
