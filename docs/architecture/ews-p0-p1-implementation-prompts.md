# EWS P0/P1 Implementation Prompts

These prompts turn the P0 and P1 rows in
`docs/architecture/ews-operation-contract.md` into bounded implementation
slices. Run them in the listed order. They deliberately target contract gaps
and regressions in the existing adapter; they do not ask for a parallel EWS
implementation.

## Shared preamble

Prepend this to every prompt below:

```text
Work in C:\Development\LPE. Before editing, read AGENTS.md, ARCHITECTURE.md,
docs/architecture/initial-architecture.md, LICENSE.md, and
docs/architecture/ews-operation-contract.md, plus only the architecture and
implementation documents directly relevant to this slice. State assumptions,
the smallest plan, and concrete success criteria before changing code.

EWS must remain a bounded adapter over canonical LPE state. Do not add an
Exchange-specific store, a protocol-local Outbox or Sent, or client SMTP.
Protect Bcc from normal user-facing, search, AI, and sync projections. Keep
tenant, account, delegated, and public-folder ACL checks before any projection
or mutation. Validate whole requests and all batch members before writing;
rejections must be parseable EWS errors with no partial mutation.

For a Microsoft wire-semantic question not answered by project documentation,
first inspect docs/microsoft/protocol-sources.toml, then use only official
Microsoft Learn Open Specifications and follow AGENTS.md's cache/source-record
procedure. Do not copy external implementation code and do not introduce a
dependency without the LICENSE.md review.

This database is test-only. If a schema correction is genuinely required,
edit the canonical fresh schema directly; do not write an upgrade script. Use
the supplied TEST_DATABASE_URL through the environment rather than printing
credentials. Prefer existing canonical APIs/storage to ad-hoc EWS SQL. Do not
build or deploy a release binary.

Add focused SOAP regression tests in crates/lpe-exchange/src/tests/ews.rs (or
the existing nearest focused test module) before fixing each demonstrated gap.
Run focused cargo tests one filter at a time, then cargo test-lpe-exchange-ews;
run the matching schema/storage contract test if the schema changes. Update
architecture documentation only when behavior or schema facts actually change.
```

## Prompt 1 — P0 mailbox items

```text
Implement and harden the P0 EWS mailbox-item contract for CreateItem,
DeleteItem, FindItem, GetItem, SendItem, and UpdateItem. Apply the shared
preamble.

- Keep support bounded to mail, contacts, calendar events, tasks, and
  public-folder posts. Reject unsupported item classes and Exchange property
  bag shapes without broadening the model.
- CreateItem: validate the complete request first; allow exactly one supported
  item and, if present, exactly one well-formed supported SavedItemFolderId.
  Route mail through canonical messages, recipients, protected Bcc,
  MIME/blob, and canonical submission when sending. Route collaboration items
  through their canonical APIs. Accept sharing invitations only for same-tenant
  contact/calendar grants.
- DeleteItem: preflight targets and modes. Calendar MoveToDeletedItems uses
  the canonical deleted-event lifecycle and HardDelete is permanent; contacts
  allow HardDelete only. Keep canonical mail/task/public-folder delete paths.
- FindItem/GetItem: require valid accessible canonical folders/items before
  query or projection. Return bounded canonical properties, body/MIME, and
  attachment references only; never fall back to an unscoped result.
- SendItem: send only accessible canonical drafts through the canonical
  submission flow and authoritative Sent membership. A bad later ID must not
  submit an earlier draft.
- UpdateItem: mail is limited to canonical IsRead and FlagStatus. Contact and
  calendar writes require a current EWS ChangeKey, and multi-item
  contact/calendar updates remain rejected until canonical atomic batching
  exists.

Use canonical messages, mailbox_messages, recipients, protected Bcc, blobs,
MIME, contacts, calendars/events, tasks, public_folder_items, submission,
grants, change logs, and tombstones. Test successful bounded paths and every
shape/access/stale-key rejection, including atomic no-op behavior and canonical
Sent/submission effects.
```

## Prompt 2 — P0 folders and attachments

```text
Implement and harden the P0 EWS contract for CreateFolder, DeleteFolder,
FindFolder, GetFolder, CreateAttachment, GetAttachment, and DeleteAttachment.
Apply the shared preamble and also read docs/architecture/attachments-v1.md.

Folders:
- CreateFolder accepts exactly one ParentFolderId and one Folders collection
  with one IPF.Note custom mailbox folder below msgfolderroot/a custom folder,
  or one permitted public-folder child. Reject duplicate wrappers, blank names,
  protected/system/inaccessible parents, archive/managed/voice forms, and
  unsupported folder classes before writing.
- DeleteFolder accepts one canonical deletable custom mailbox or permitted
  public folder only. Reject roots, protected/system/inaccessible targets,
  duplicate/batch forms, and unsupported IDs before mutation.
- FindFolder returns only the bounded canonical hierarchy or immediate
  accessible children. GetFolder returns only exact supported mailbox or
  public-folder IDs, roots, documented distinguished roles, and accessible
  collaboration collections. Both return folder errors rather than an
  unscoped projection for malformed/inaccessible/unsupported inputs.

Attachments:
- Use only canonical blob/MIME/attachment storage. Every client-provided file
  must pass Google Magika validation; preserve per-domain blob deduplication
  and correct MIME/message reconstruction. Do not expand v1 text indexing
  beyond PDF, DOCX, and ODT.
- CreateAttachment accepts exactly one FileAttachment on one accessible
  canonical message parent. Reject ItemAttachment, reference attachments,
  batches, malformed wrappers, blocked Magika results, unknown parents, and
  stale parent ChangeKeys with no write.
- GetAttachment/DeleteAttachment accept one accessible canonical message or
  calendar attachment reference. Return bounded content/metadata or delete
  only the canonical relationship with required change evidence.

Use canonical mailboxes, public_folders, collaboration collections/grants,
blobs, blob_placements, mime_parts, attachments, and
calendar_event_attachments. Test ACL/scope, request shape, no-mutation failure,
change evidence, content reconstruction, and supported calendar attachment
read/delete paths.
```

## Prompt 3 — P0 identity, availability, rules, and time zones

```text
Implement and harden P0 ResolveNames, GetUserAvailability, GetInboxRules,
UpdateInboxRules, and GetServerTimeZones. Apply the shared preamble.

- ResolveNames searches only the authenticated account, same-tenant visible
  directory accounts, and readable contact books/contacts. Return exactly one
  bounded match. Invalid, empty, inaccessible, unsupported-scope, and
  ambiguous queries must return a parseable error with no candidates; do not
  add GAL templates or DL expansion.
- GetUserAvailability accepts one same-tenant mailbox with a readable
  canonical calendar. Return busy intervals only, expanding only supported
  daily/weekly recurrence within a maximum 42-day window and correctly across
  DST. Never disclose event subjects, bodies, attendees, or organization-wide
  availability data.
- GetInboxRules projects only the authenticated mailbox's exact active,
  generated lpe-ews-inbox-rules-v1 Sieve script. Never expose arbitrary Sieve,
  OOF, Exchange rule blobs, or client-only rules.
- UpdateInboxRules atomically creates, updates, and deletes ordered
  subject-contains rules with a custom-folder move or discard action in one
  generated active script. Fully validate target folders and every operation;
  under the transaction lock, recheck active-script identity/content. Never
  replace a non-generated or OOF script; return a parseable error unchanged.
- GetServerTimeZones is stateless lpe-ews-time-zones-v1: UTC and Europe/Berlin
  only, ReturnFullTimeZoneData=false only, and an optional unique exact-ID
  filter. Reject full data, absent/empty/multiple Ids, duplicates, unknown IDs,
  and case variants deterministically.

Test tenant/ACL/ambiguity redaction, availability window/DST limits, rules
ordering and complete rollback, OOF/non-generated script preservation, and
every compact time-zone input boundary.
```

## Prompt 4 — P0 sync and notifications plus P1 streaming

```text
Implement and harden P0 Subscribe, GetEvents, Unsubscribe, SyncFolderHierarchy,
and SyncFolderItems, plus P1 GetStreamingEvents. Apply the shared preamble.

Canonical source of truth is mail_change_log and current mailbox,
collaboration, and public-folder state. ews_sync_cursors is derived protocol
state only: opaque, account- and scope-bound, durable for 30 days, and
restart-safe. Do not create an Exchange subscription table, adapter-local
hierarchy store, push service, long-held streaming connection, or affinity
state.

- Subscribe accepts exactly one PullSubscriptionRequest, timeout 1..=1440,
  one visible unambiguous mailbox scope, and nonempty de-duplicated supported
  event types. Issue deterministic account/scope/cursor-bound opaque IDs and
  watermarks. Reject push, resume watermark, malformed/mixed/inaccessible
  scope, and duplicate fields.
- GetEvents validates account/scope-bound tokens and replays only retained
  matching canonical changes with deterministic advancement and MoreEvents.
  Never fabricate events or leak data through tampered/expired/mismatched
  tokens. Unsubscribe validates token shape/binding, then succeeds without a
  durable protocol-local mutation.
- GetStreamingEvents uses the same validation and canonical replay but returns
  an immediate bounded streaming-shaped response only.
- SyncFolderHierarchy projects current accessible canonical mailbox,
  collaboration, and public-folder hierarchy through paged durable cursors;
  omit inaccessible and unsupported archive/voice/search structures.
- SyncFolderItems supports canonical mail, collaboration, and public-folder
  items. The first mailbox page is bounded current state; continuation/resume
  replays retained canonical changes and tombstones with account/folder-bound
  cursors. Newly issued states are always durable opaque cursors. Only legacy
  empty reset markers remain accepted for collaboration/public folders;
  reject every nonempty legacy inventory.

Test restart-safe continuation, cursors/watermarks bound to account and scope,
tamper/expiry rejection, paging and max changes, create/delete/move tombstones,
access filtering, and Bcc exclusion. Serialize any shared change-log/cursor
schema work with other prompts.
```

## Prompt 5 — P1 mail and folder mutations

```text
Implement and harden P1 CopyItem, MoveItem, MarkAllItemsAsRead, EmptyFolder,
and UpdateFolder. Apply the shared preamble.

- CopyItem and MoveItem support at most 100 canonical mail or public-folder
  item IDs. Preflight all source/target IDs, access, and shapes first. Commit
  the supported batch atomically; copy retains source membership, while move
  records target membership plus source tombstone/change evidence and performs
  public-folder clone/delete canonically.
- MarkAllItemsAsRead supports one visible canonical mailbox folder and at most
  10,000 changed items. It uses canonical mailbox read-state and change-log
  mutation; public-folder per-user read state stays unsupported.
- EmptyFolder atomically empties at most 10,000 items in one supported custom
  mailbox or accessible public folder. Reject protected/system scopes, mixed
  targets, over-limit requests, undeletable descendants, and invalid shapes.
  Subfolder deletion is limited to canonical deletable subfolders.
- UpdateFolder changes exactly one folder:DisplayName through canonical
  mailbox/public-folder APIs only. Reject protected/system folders, unsupported
  properties, and batch shapes before mutation.

Test full preflight, late-invalid batch rollback, limits, ownership/ACL,
change/tombstone evidence, and no partial writes.
```

## Prompt 6 — P1 reminders and conversations

```text
Implement and harden P1 GetReminders, PerformReminderAction, FindConversation,
and GetConversationItems. Apply the shared preamble.

- GetReminders is a computed projection over existing calendar-event and task
  reminder fields, message follow-up metadata, and
  reminder_occurrence_dismissals. Do not create an Exchange reminder table.
- PerformReminderAction supports one bounded dismiss or one per-occurrence
  calendar/task snooze per request. Persist occurrence state canonically and
  use canonical message state for follow-up dismissal. Reject multi-action and
  multi-item requests before mutation.
- FindConversation groups current visible message memberships in one mailbox
  folder by existing mailbox_messages.thread_id, using every current canonical
  membership. Do not add a threads table, Exchange conversation index, or
  lifecycle identity.
- GetConversationItems returns current accessible canonical nodes for that
  lightweight thread. Folder-ignore behavior is limited to visible canonical
  memberships.

Test computed views, occurrence dismissal/snooze persistence, rejection
no-ops, folder visibility, and grouping from current memberships only.
```

## Prompt 7 — P1 IDs, rooms, OOF, and MailTips

```text
Implement and harden P1 ConvertId, GetRoomLists, GetRooms, GetUserOofSettings,
SetUserOofSettings, and GetMailTips. Apply the shared preamble.

- ConvertId is a stateless deterministic opaque LPE EWS codec for message,
  folder, contact, event, task, attachment, and public-folder families only.
  Reject unsupported formats and families; do not introduce an Exchange
  identity table or full MAPI EntryId parity.
- GetRoomLists computes one same-tenant room/resource list from room/equipment
  accounts' directory_kind. GetRooms returns visible same-tenant room/equipment
  accounts and rejects a room-list filter unless it matches that computed list.
  Do not model arbitrary Exchange room-list membership.
- GetUserOofSettings projects canonical Sieve vacation state. SetUserOofSettings
  uses locked compare-and-replace: recheck the active script name/content inside
  the transaction and update/disable only a marker-bearing EWS-owned vacation
  script. Preserve active Inbox-rule and every other canonical script, with a
  parseable error instead of replacement. Keep scheduling/external-audience
  support bounded.
- GetMailTips returns invalid-recipient and OOF tips only from visible canonical
  directory/contact/group and vacation-Sieve state. Do not add quota,
  moderation, policy, or custom Exchange MailTips data.

Test deterministic ID round-trips, exact room-list filtering, cross-tenant and
visibility redaction, OOF script race/preservation rules, and valid/invalid/OOF
MailTips paths.
```

## Prompt 8 — P1 canonical delegation

```text
Implement and harden P1 AddDelegate, GetDelegate, UpdateDelegate, and
RemoveDelegate as one canonical delegation capability. Apply the shared
preamble and read the directly relevant delegation and MAPI LocalFreebusy
architecture rules.

Use mailbox_delegation_grants, calendar_grants, sender_rights,
delegate_preferences, and canonical audit/change/tombstone rows as the single
source of truth. Support same-tenant Inbox/Calendar delegate grants,
send-on-behalf, and meeting delivery copy/private preferences only.

AddDelegate atomically establishes the supported tuple; GetDelegate returns
only that projection; UpdateDelegate atomically changes it; RemoveDelegate
atomically removes grants, sender rights, and preferences and emits canonical
change evidence. Reject Exchange-only folder-permission shapes and
delegate-folder semantics. Do not create protocol-local delegate state or
bypass the durable delegate_preferences/LocalFreebusy projection.

Test a same-tenant add/get/update/remove lifecycle, ACL/cross-tenant rejection,
compound-input rollback, exact projection, no residual rights after removal,
and audit/change evidence. Run directly affected MAPI free-busy/delegation
tests as well as EWS tests.
```

## Prompt 9 — P1 user configuration CRUD

```text
Implement and harden P1 CreateUserConfiguration, DeleteUserConfiguration,
GetUserConfiguration, and UpdateUserConfiguration. Apply the shared preamble.

Use one canonical account_client_configurations API. Key rows by authenticated
account, optional mailbox/public-folder scope, configuration class, and name.
Store only bounded dictionary, XML, and binary payloads; validate documented
size/shape limits before every mutation. Do not create a generic Exchange
arbitrary user-configuration store.

Create rejects duplicate keys through the established parseable conflict;
Get returns authorized bounded stored payloads; Update replaces payloads
atomically and advances canonical modseq plus required audit/change data;
Delete removes the row and reports a missing row as EWS item-not-found. Deny
cross-account and inaccessible mailbox/public-folder scope everywhere.

Test full CRUD for every payload type, duplicate create, missing get/delete,
bad/inaccessible scope, cross-account denial, oversized/invalid payload no-op,
and modseq advancement.
```

## Coverage check

| Prompt | Operations |
| --- | --- |
| 1 | P0 `CreateItem`, `DeleteItem`, `FindItem`, `GetItem`, `SendItem`, `UpdateItem` |
| 2 | P0 `CreateFolder`, `DeleteFolder`, `FindFolder`, `GetFolder`, `CreateAttachment`, `GetAttachment`, `DeleteAttachment` |
| 3 | P0 `ResolveNames`, `GetUserAvailability`, `GetInboxRules`, `UpdateInboxRules`, `GetServerTimeZones` |
| 4 | P0 `Subscribe`, `GetEvents`, `Unsubscribe`, `SyncFolderHierarchy`, `SyncFolderItems`; P1 `GetStreamingEvents` |
| 5 | P1 `CopyItem`, `MarkAllItemsAsRead`, `MoveItem`, `EmptyFolder`, `UpdateFolder` |
| 6 | P1 `GetReminders`, `PerformReminderAction`, `FindConversation`, `GetConversationItems` |
| 7 | P1 `ConvertId`, `GetRoomLists`, `GetRooms`, `GetUserOofSettings`, `SetUserOofSettings`, `GetMailTips` |
| 8 | P1 `AddDelegate`, `GetDelegate`, `UpdateDelegate`, `RemoveDelegate` |
| 9 | P1 `CreateUserConfiguration`, `DeleteUserConfiguration`, `GetUserConfiguration`, `UpdateUserConfiguration` |

The table covers all 23 P0 and all 24 P1 operation rows in the contract.
