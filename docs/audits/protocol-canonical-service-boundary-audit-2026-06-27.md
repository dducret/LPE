# Protocol Canonical Service Boundary Audit - 2026-06-27

## Scope

This audit reviewed protocol adapters that can mutate user-visible canonical
state:

- `lpe-jmap`
- `lpe-imap`
- `lpe-activesync`
- `lpe-exchange`
- `lpe-dav`
- `lpe-managesieve`

Read before auditing:

- `ARCHITECTURE.md`
- `docs/architecture/initial-architecture.md`
- `LICENSE.md`

Boundary used for this audit: protocol adapters may parse protocol payloads,
translate protocol identifiers, enforce protocol-specific response semantics,
and project protocol metadata. Creation or mutation of canonical mailbox,
submission, contacts, calendar, task, rights, rules, and recoverable-item state
should be owned by shared core services rather than separately implemented in
each adapter.

## Executive Findings

1. Most scoped protocol adapters mutate canonical state through protocol-local
   store traits that directly wrap `lpe-storage` methods. This is better than
   protocol-private tables, but it still leaves canonical behavior split across
   adapters instead of shared core services.
2. Submission, draft persistence, and Sent visibility have the highest drift
   risk. JMAP, IMAP, ActiveSync, EWS, and MAPI all build `SubmitMessageInput`
   or call submission/draft storage methods from adapter code.
3. Message moves, deletes, flag updates, copy/import, expunge, and recoverable
   item handling are also split by protocol. IMAP and MAPI include additional
   local restrictions or Outlook compatibility branches that should remain
   protocol-local only where protocol semantics require them.
4. Contacts, calendar, and tasks are parsed locally by JMAP, DAV, ActiveSync,
   EWS, and MAPI, then written directly to storage facades. A shared
   collaboration item service would reduce merge, permission, reminder, and
   deletion drift.
5. Rights and rule mutation are split across IMAP ACL, EWS delegate/sharing,
   MAPI permissions/rules, JMAP vacation, and ManageSieve. These need shared
   canonical rights and rule services, while Microsoft-specific ACL row and
   rule-table translations should remain in `lpe-exchange`.

## Prioritized Refactor List

### P0 - Canonical mail submission and draft service

Create one shared service for draft create/update/delete, direct submission,
draft submission, sender delegation enforcement, Sent copy visibility, Bcc
protection, attachment validation handoff, and outbound queue creation.

Current direct adapter entry points:

- `crates/lpe-jmap/src/mail.rs:615`, `crates/lpe-jmap/src/mail.rs:678`,
  `crates/lpe-jmap/src/mail.rs:1327`, `crates/lpe-jmap/src/mail.rs:1385`
  delete, submit, create, and update drafts through `JmapStore`.
- `crates/lpe-imap/src/append.rs:89` and
  `crates/lpe-imap/src/append.rs:166` save drafts and import appended mail
  directly from IMAP APPEND.
- `crates/lpe-activesync/src/service.rs:1741`,
  `crates/lpe-activesync/src/service.rs:1786`,
  `crates/lpe-activesync/src/service.rs:2078`, and
  `crates/lpe-activesync/src/service.rs:2876` save drafts and submit
  SendMail/SmartReply/SmartForward messages directly.
- `crates/lpe-exchange/src/service.rs:3771`,
  `crates/lpe-exchange/src/service.rs:3785`, and
  `crates/lpe-exchange/src/service.rs:5294` save, submit, and submit drafts
  for EWS.
- `crates/lpe-exchange/src/mapi/dispatch.rs:22066` submits MAPI messages
  directly from ROP dispatch.
- `crates/lpe-exchange/src/store.rs:7069`,
  `crates/lpe-exchange/src/store.rs:7077`, and
  `crates/lpe-exchange/src/store.rs:7097` expose protocol-facing wrappers for
  draft save, draft submit, and direct submit.

Adapter-local behavior to keep: protocol parsing, protocol status mapping,
MAPI ROP idempotency, Outlook duplicate execute request handling, and IMAP
APPEND response UID formatting.

### P0 - Canonical mailbox item mutation service

Create one shared service for message import/copy/move/delete, per-mailbox
membership changes, read/flag/deleted state, expunge, folder create/update/
delete, and change-log emission.

Current direct adapter entry points:

- `crates/lpe-jmap/src/mail.rs:434` and
  `crates/lpe-jmap/src/mail.rs:501` copy and import JMAP email directly.
- `crates/lpe-imap/src/messages.rs:89` and
  `crates/lpe-imap/src/messages.rs:154` update IMAP flags directly.
- `crates/lpe-imap/src/messages.rs:282`,
  `crates/lpe-imap/src/messages.rs:347`,
  `crates/lpe-imap/src/messages.rs:485`, and
  `crates/lpe-imap/src/messages.rs:498` implement IMAP copy/move restrictions
  locally.
- `crates/lpe-imap/src/mailboxes.rs:640` expunges deleted messages directly.
- `crates/lpe-activesync/src/service.rs:2295` and
  `crates/lpe-activesync/src/service.rs:2320` handle MoveItems locally and
  call storage through the ActiveSync facade.
- `crates/lpe-exchange/src/service.rs:2928`,
  `crates/lpe-exchange/src/service.rs:2944`,
  `crates/lpe-exchange/src/service.rs:3755`,
  `crates/lpe-exchange/src/service.rs:3870`,
  `crates/lpe-exchange/src/service.rs:4063`,
  `crates/lpe-exchange/src/service.rs:4088`,
  `crates/lpe-exchange/src/service.rs:4500`, and
  `crates/lpe-exchange/src/service.rs:5072` mutate messages, flags, imports,
  deletes, and folder contents from EWS code.
- `crates/lpe-exchange/src/mapi/dispatch.rs:1341`,
  `crates/lpe-exchange/src/mapi/dispatch.rs:1593`,
  `crates/lpe-exchange/src/mapi/dispatch.rs:6830`,
  `crates/lpe-exchange/src/mapi/dispatch.rs:17499`,
  `crates/lpe-exchange/src/mapi/dispatch.rs:18008`,
  `crates/lpe-exchange/src/mapi/dispatch.rs:20262`,
  `crates/lpe-exchange/src/mapi/dispatch.rs:20278`,
  `crates/lpe-exchange/src/mapi/dispatch.rs:20293`,
  `crates/lpe-exchange/src/mapi/dispatch.rs:22997`,
  `crates/lpe-exchange/src/mapi/dispatch.rs:25269`,
  `crates/lpe-exchange/src/mapi/dispatch.rs:25285`,
  `crates/lpe-exchange/src/mapi/dispatch.rs:25300`,
  `crates/lpe-exchange/src/mapi/dispatch.rs:25402`, and
  `crates/lpe-exchange/src/mapi/dispatch.rs:25478` directly map ROP actions to
  canonical message import, delete, move, and flag storage calls.

Adapter-local behavior to keep: IMAP restrictions around Sent/Drafts copy or
move where they preserve canonical submission state, ActiveSync status codes,
EWS SOAP response shape, and MAPI reserved/unsupported ROP behavior.

### P0 - Canonical rights and delegation service

Create one shared service for mailbox grants, calendar/contact/task sharing,
sender delegation, public-folder permissions, audit entries, and grant removal.

Current direct adapter entry points:

- `crates/lpe-imap/src/acl.rs:292`,
  `crates/lpe-imap/src/acl.rs:306`,
  `crates/lpe-imap/src/acl.rs:320`, and
  `crates/lpe-imap/src/acl.rs:506` mutate sender and mailbox delegation from
  IMAP ACL handling.
- `crates/lpe-exchange/src/service.rs:1896` writes EWS sharing grants.
- `crates/lpe-exchange/src/store.rs:3138`,
  `crates/lpe-exchange/src/store.rs:3304`,
  `crates/lpe-exchange/src/store.rs:3315`,
  `crates/lpe-exchange/src/store.rs:3325`,
  `crates/lpe-exchange/src/store.rs:3341`,
  `crates/lpe-exchange/src/store.rs:3352`,
  `crates/lpe-exchange/src/store.rs:3363`,
  `crates/lpe-exchange/src/store.rs:3415`,
  `crates/lpe-exchange/src/store.rs:3426`,
  `crates/lpe-exchange/src/store.rs:3442`,
  `crates/lpe-exchange/src/store.rs:4893`, and
  `crates/lpe-exchange/src/store.rs:4916` map EWS/MAPI permissions and sharing
  to storage grants.
- `crates/lpe-exchange/src/mapi/dispatch.rs:26414` handles
  `RopModifyPermissions`.

Adapter-local behavior to keep: IMAP ACL token parsing, MAPI permissions table
row encoding, MAPI member-id resolution, EWS SOAP envelopes, and
Microsoft-specific permission row compatibility.

### P1 - Canonical collaboration item service

Create one shared service for contact, calendar, and task create/update/delete
with common ownership checks, sharing checks, reminder updates, merge behavior,
and change emission. Protocol adapters should only parse vCard, iCalendar,
JMAP JSON, ActiveSync WBXML, EWS XML, or MAPI properties into service inputs.

Current direct adapter entry points:

- `crates/lpe-jmap/src/contacts.rs:342`,
  `crates/lpe-jmap/src/contacts.rs:370`, and
  `crates/lpe-jmap/src/contacts.rs:396` mutate contacts.
- `crates/lpe-jmap/src/calendar.rs:500`,
  `crates/lpe-jmap/src/calendar.rs:535`, and
  `crates/lpe-jmap/src/calendar.rs:573` mutate events.
- `crates/lpe-jmap/src/tasks.rs:104`,
  `crates/lpe-jmap/src/tasks.rs:125`,
  `crates/lpe-jmap/src/tasks.rs:149`, and
  `crates/lpe-jmap/src/tasks.rs:407` mutate task lists and tasks.
- `crates/lpe-dav/src/service.rs:337`,
  `crates/lpe-dav/src/service.rs:341`,
  `crates/lpe-dav/src/service.rs:354`,
  `crates/lpe-dav/src/service.rs:372`,
  `crates/lpe-dav/src/service.rs:376`,
  `crates/lpe-dav/src/service.rs:397`,
  `crates/lpe-dav/src/service.rs:405`, and
  `crates/lpe-dav/src/service.rs:413` mutate contacts, events, and tasks.
- `crates/lpe-activesync/src/service.rs:1724`,
  `crates/lpe-activesync/src/service.rs:1764`,
  `crates/lpe-activesync/src/service.rs:1804`,
  `crates/lpe-activesync/src/service.rs:1849`,
  `crates/lpe-activesync/src/service.rs:1876`,
  `crates/lpe-activesync/src/service.rs:1908`,
  `crates/lpe-activesync/src/service.rs:1944`,
  `crates/lpe-activesync/src/service.rs:1971`, and
  `crates/lpe-activesync/src/service.rs:2003` handle draft/contact/calendar
  sync command mutations directly through the ActiveSync store facade.
- `crates/lpe-exchange/src/service.rs:3683`,
  `crates/lpe-exchange/src/service.rs:3695`,
  `crates/lpe-exchange/src/service.rs:3706`,
  `crates/lpe-exchange/src/service.rs:3895`,
  `crates/lpe-exchange/src/service.rs:3913`,
  `crates/lpe-exchange/src/service.rs:3931`,
  `crates/lpe-exchange/src/service.rs:4023`,
  `crates/lpe-exchange/src/service.rs:4028`,
  `crates/lpe-exchange/src/service.rs:4033`,
  `crates/lpe-exchange/src/service.rs:5484`, and
  `crates/lpe-exchange/src/service.rs:5507` mutate EWS contacts, calendar
  items, tasks, and reminders.
- `crates/lpe-exchange/src/mapi/dispatch.rs:16214`,
  `crates/lpe-exchange/src/mapi/dispatch.rs:16341`,
  `crates/lpe-exchange/src/mapi/dispatch.rs:16433`,
  `crates/lpe-exchange/src/mapi/dispatch.rs:20121`,
  `crates/lpe-exchange/src/mapi/dispatch.rs:20132`, and
  `crates/lpe-exchange/src/mapi/dispatch.rs:20143` mutate contact, event, and
  task objects from MAPI.
- `crates/lpe-exchange/src/mapi/properties.rs:8021`,
  `crates/lpe-exchange/src/mapi/properties.rs:8072`,
  `crates/lpe-exchange/src/mapi/properties.rs:8078`,
  `crates/lpe-exchange/src/mapi/properties.rs:8089`,
  `crates/lpe-exchange/src/mapi/properties.rs:8122`, and
  `crates/lpe-exchange/src/mapi/properties.rs:8144` update canonical
  collaboration objects from MAPI property saves.

Adapter-local behavior to keep: protocol parsers, ETag/precondition handling,
ActiveSync recurrence translation, EWS XML parsing, and MAPI property-to-object
translation.

### P1 - Canonical rules and vacation service

Create one shared service for Sieve script lifecycle, active script selection,
JMAP vacation projection, EWS inbox rule translation targets, and MAPI bounded
rule mutation. It should own validation, naming, audit, and conflict behavior.

Current direct adapter entry points:

- `crates/lpe-managesieve/src/service.rs:107`,
  `crates/lpe-managesieve/src/service.rs:136`,
  `crates/lpe-managesieve/src/service.rs:156`, and
  `crates/lpe-managesieve/src/service.rs:176` put, activate, delete, and rename
  Sieve scripts directly.
- `crates/lpe-jmap/src/vacation.rs:155`,
  `crates/lpe-jmap/src/vacation.rs:224`, and
  `crates/lpe-jmap/src/vacation.rs:256` mutates active Sieve state and writes a
  generated vacation script.
- `crates/lpe-exchange/src/service.rs:3231`,
  `crates/lpe-exchange/src/service.rs:3264`,
  `crates/lpe-exchange/src/service.rs:5321`,
  `crates/lpe-exchange/src/service.rs:5362`,
  `crates/lpe-exchange/src/service.rs:5380`, and
  `crates/lpe-exchange/src/service.rs:6698` implement EWS rule/vacation to
  Sieve mappings locally.
- `crates/lpe-exchange/src/mapi/dispatch.rs:374`,
  `crates/lpe-exchange/src/mapi/dispatch.rs:26723`,
  `crates/lpe-exchange/src/mapi/dispatch.rs:26770`, and
  `crates/lpe-exchange/src/mapi/dispatch.rs:26795` implement bounded MAPI rule
  mutation and write/delete Sieve scripts.

Adapter-local behavior to keep: Microsoft-only rule table and rule organizer
compatibility, unsupported Exchange rule blob rejection, and protocol response
formatting.

### P1 - Canonical recoverable item service

Create one shared service for recoverable item listing, restore, purge,
retention checks, audit, and change notification. MAPI and EWS should translate
protocol folder IDs and response semantics but not own purge/restore policy.

Current direct adapter entry points:

- `crates/lpe-exchange/src/mapi/store_adapter.rs:452` loads recoverable items
  for MAPI snapshots.
- `crates/lpe-exchange/src/mapi/dispatch.rs:1651` and
  `crates/lpe-exchange/src/mapi/dispatch.rs:20103` purge recoverable items.
- `crates/lpe-exchange/src/mapi/dispatch.rs:22452` restores recoverable items.
- `crates/lpe-exchange/src/store.rs:6789`,
  `crates/lpe-exchange/src/store.rs:6800`, and
  `crates/lpe-exchange/src/store.rs:6813` expose list/restore/purge wrappers.

Adapter-local behavior to keep: MAPI recoverable folder IDs, root-folder
unsupported behavior, partial completion mapping, and Outlook table projection.

### P2 - Protocol store facade cleanup

After the P0/P1 services exist, narrow adapter store traits so they depend on
service-level operations rather than storage-level primitives. The current
facades are useful test seams but expose too much canonical mutation surface:

- `crates/lpe-jmap/src/store.rs`
- `crates/lpe-imap/src/store.rs`
- `crates/lpe-activesync/src/store.rs`
- `crates/lpe-dav/src/store.rs`
- `crates/lpe-exchange/src/store.rs`
- `crates/lpe-managesieve/src/store.rs`

## Deliberately Left Protocol-Local

The following should not be centralized as canonical services unless later
evidence proves the behavior is semantically shared:

- Protocol parsing and serialization: IMAP command grammar, ActiveSync WBXML,
  EWS SOAP XML, DAV vCard/iCalendar parsing, JMAP method dispatch, and MAPI ROP
  wire handling.
- Protocol-specific status/error mapping and response envelopes.
- Microsoft-specific MAPI compatibility metadata, including named properties,
  custom property values, profile settings, sync checkpoints, object identity
  projection, navigation shortcuts, associated config messages, and rule
  organizer placeholder suppression.
- Outlook-specific unsupported/reserved ROP behavior and MAPI table row output.
- IMAP APPEND UID responses and IMAP mailbox selection state.
- DAV ETag and HTTP precondition handling.

## Recommended Service Boundaries

1. `lpe-core::mail_submission`: owns draft lifecycle, message submission,
   sender validation, Sent visibility, Bcc handling, and queue handoff.
2. `lpe-core::mail_items`: owns message copy/import/move/delete, expunge,
   flags, per-mailbox membership, folder mutation, and canonical change log
   emission.
3. `lpe-core::rights`: owns mailbox, sender, collaboration, public folder, and
   delegate grants with a single audit model.
4. `lpe-core::collaboration_items`: owns contacts, calendar events, tasks,
   reminders, collection/task-list mutation, and shared-access enforcement.
5. `lpe-core::rules`: owns Sieve script lifecycle, active script selection,
   vacation projection, and bounded protocol rule mutations.
6. `lpe-core::recoverable_items`: owns restore/purge/list policy and retention
   outcomes.

These services can initially wrap existing `lpe-storage` methods without schema
changes. The first refactor should move behavior behind the service boundary,
then migrate protocol adapters one domain at a time with existing protocol tests
as regression coverage.
