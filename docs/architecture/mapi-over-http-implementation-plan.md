# MAPI over HTTP Implementation Plan

This document is the implementation contract for the guarded MAPI over HTTP
surface. It is documentation-only: changing it does not enable client
publication, widen runtime protocol support, or replace the canonical LPE data
model.

MAPI over HTTP is the first Outlook desktop Exchange-account path for Outlook
2016 and Outlook 2019 cached mode. Outlook Anywhere / RPC over HTTP remains a
later legacy compatibility shim for top-level `EXPR` autodiscover publication
and must stay aligned with real `/rpc/rpcproxy.dll` mailbox transport behavior
before it is advertised.

## Wire-Contract Requirements

### Publication and Protocol Scope

- MAPI over HTTP endpoints remain authenticated and opt-in. Autodiscover may
  publish the MAPI endpoint only when the MAPI gate is enabled and the documented
  interop gate has passed for the deployment.
- Top-level `EXPR` metadata is permitted only for the later Outlook Anywhere /
  RPC over HTTP path. It must not be used to imply that MAPI over HTTP or RPC
  proxy behavior is complete before the corresponding transport is implemented.
- The first supported lab target is a single LPE server with sticky MAPI session
  state. Cross-process session replay, session migration, and load-balanced
  failover are production-hardening work.

### Transport and Session Framing

- EMSMDB `Connect`, `Execute`, and `Disconnect` traffic must maintain a
  server-side MAPI session context with strict `MapiContext` and `MapiSequence`
  handling.
- `Execute` refreshes `MapiContext` and `MapiSequence` on every accepted request.
- Required MAPI HTTP request headers, request identifiers, client information,
  cookies, request-body framing, and response-code mapping are part of the wire
  contract. Missing, malformed, stale, duplicate, replayed, or overlapping
  same-session requests must receive deterministic protocol responses.
- `X-RequestId` and `X-ClientInfo` are echoed according to the accepted request.
  Duplicate replay of the same request id and body is idempotent for transport
  purposes; reuse of the same request id with a different body is rejected.
- `Content-Length` handling must be explicit. The server must not silently
  reinterpret malformed framing, and response framing must be compatible with
  Outlook's MAPI HTTP parser.
- Stale `Disconnect` cookies and missing or malformed session cookies must fail
  at the transport/session layer without mutating mailbox state.

### ROP Dispatch

- ROP dispatch is terminal within the current ROP request buffer when the first
  unsupported, reserved, or malformed ROP is encountered. The server returns one
  parseable unsupported/error response for that ROP and does not execute later
  ROP bytes in the same buffer.
- Typed protocol enum boundaries are strict. Unknown `RopId`, MAPI property
  type, restriction type, sync type, FastTransfer marker, or transfer marker must
  be logged with the raw numeric value and handled through the parseable
  unsupported/error path. Unknown values must not be coerced, must not panic, and
  must not produce partial side effects.
- Private mailbox logon is the supported mailbox logon mode. Public-folder
  logons return a parseable unsupported response and must not create
  protocol-local public-folder state.
- ROP folder and message identifiers use the MAPI wire layout at the protocol
  boundary: two-byte little-endian `REPLID` followed by a six-byte big-endian
  `GLOBCNT`. LPE's internal store id remains `GLOBCNT << 16 | REPLID`, and
  conversion between the two layouts must happen only when parsing or
  serializing ROP request and response fields, table identifier columns,
  FastTransfer/ICS identifier properties, and identifier-valued property rows
  such as `PidTagFolderId`, `PidTagParentFolderId`, and `PidTagMid`.
- `RopLongTermIdFromId` also accepts Outlook's observed conversion request
  layout with six-byte `GLOBCNT` followed by two-byte little-endian `REPLID`;
  responses still use the canonical LongTermID form with the store replica GUID.
- `RopLongTermIdFromId` also accepts stale `REPLID` values when the embedded
  `GLOBCNT` maps to an LPE-advertised MAPI special folder, including the
  observed stale short-ID forms that carry the six-byte counter before or after
  the `REPLID` and in either counter byte order; normal mailbox items still
  require the canonical store replica id.
- `RopLongTermIdFromId` also accepts Outlook's observed bare little-endian
  six-byte counter form for advertised IPM subtree virtual folders and dynamic
  content objects already emitted by FastTransfer/ICS, such as `Conflicts`,
  Common Views FAI/search objects, and message change counters; zero and
  private-logon-only reserved counters still fail with `ecNotFound`.
- `RopLongTermIdFromId` failures for unmapped or unusable `REPLID` values use
  the documented `ecNotFound` result, not a generic invalid-parameter result.

### EMSMDB, NSPI, and FastTransfer

- EMSMDB behavior must stay bounded to the Outlook cached-mode bootstrap,
  hierarchy, content synchronization, table, property, submission, and mutation
  surfaces explicitly covered by this plan.
- NSPI behavior is address-book resolution over canonical LPE account and
  contact visibility. NSPI mutation and link-table write behavior remain
  deferred.
- FastTransfer and ICS payloads use the MS-OXCFXICS wire grammar, including
  lexical value sizes such as two-byte `PtypBoolean` values.
- `RopSynchronizationConfigure` and `RopFastTransferSourceGetBuffer` require
  strict request and response framing. Any parser extension must be validated
  with deterministic golden vectors or local protocol builders.

### Table Projection Contract

Table projection must produce parseable Outlook-compatible rows from canonical
state. The supported projection surface is:

| Table surface | Required behavior |
| --- | --- |
| Hierarchy tables | Root/IPM subtree child folders, special folder identity, source keys, change keys, predecessor lists, display names, container class, content counts, unread counts, replica fields, and folder child counts. |
| Contents tables | Folder membership rows with stable message identifiers, source/change keys, predecessor lists, subject, dates, sender, recipients where supported, flags, message class, read state, size, and attachment indicators. |
| Attachment tables | Canonical attachment rows with stable attachment numbering and properties required by Outlook cached-mode reads. |
| Permission tables | Canonical permission projection plus bounded mutation through `mailbox_delegation_grants`; no MAPI-local ACL table is allowed. |
| Search and reminder folders | Persisted canonical built-in and user-saved search-folder definitions plus hierarchy/content projections; no Common Views search-definition FAI rows are published until documented search-folder BLOB parity exists. |

Categorized contents tables are bounded to the canonical rows already available
through the table projection. `RopSortTable` category counts create
session-local category metadata on the table handle, `RopQueryRows` emits
category header rows and expanded leaf rows from canonical contents rows,
`RopExpandRow` and `RopCollapseRow` update only that table handle, and
`RopGetCollapseState` / `RopSetCollapseState` serialize and restore a bounded
collapse-state blob for the active table. LPE does not persist categorized
collapse state as profile data unless a future architecture update explicitly
defines it as bounded profile state.

### Specification Basis

The wire contract is based on Microsoft MAPI over HTTP, EMSMDB, NSPI, ROP,
FastTransfer/ICS, store object, folder, property, special folder, search folder,
reminder, notes, journal, task, and free/busy protocol documentation reviewed
for this implementation plan. The plan treats those documents as protocol
requirements, not as permission to introduce Exchange-only stores or
non-canonical LPE state.

## Canonical LPE Mapping Decisions

- LPE remains the canonical store for mailboxes, contacts, calendars, tasks,
  search, rights, submission, and user-visible state. MAPI over HTTP is an
  authenticated compatibility surface over that state.
- Client-facing SMTP submission stays outside the core LPE server. MAPI must use
  canonical LPE submission and must not implement client SMTP submission.
- MAPI must not maintain protocol-local `Sent`, `Outbox`, draft, attachment,
  folder, search-folder, public-folder, reminder, or address-book truth.
- Any message sent from Outlook must be recorded by canonical LPE submission and
  visible in canonical `Sent`. Cross-protocol checks must agree through JMAP,
  IMAP where applicable, and the MAPI projection.
- MAPI submission cancellation is canonical queue cancellation, not message
  deletion and not a client-spooler side channel. `RopAbortSubmit` resolves the requested folder/message identifiers to the
  authenticated account's canonical `Sent` membership and then to the matching
  `submission_queue` row. It may transition only `queued`, `ready`, or
  `deferred` rows to terminal `cancelled`, set `terminal_at`, append a
  `submission_events.cancelled` row, write the canonical submission change-log
  event, and wake normal mailbox-change listeners. It must not remove the
  authoritative `Sent` copy, recreate the source draft, mutate recipients,
  cancel `handed_off` transport custody, or recall an already relayed message.
  Already terminal rows stay terminal; duplicate cancellation of an already
  `cancelled` row is idempotent, while `handed_off`, `relayed`, `bounced`, and
  `failed` rows return a parseable cannot-abort ROP error without side effects.
- Draft save, send, move, copy, delete, read/unread, flag, attachment, and
  protected-recipient behavior must map to canonical mailbox state.
- `Bcc` is protected metadata. It must not leak through MAPI search, AI-facing
  indexing, non-owner projections, or protocol shortcuts.
- NSPI resolves the authenticated mailbox and visible contacts from canonical
  account/contact visibility. `ModLinkAtt`, `ModProps`, and other NSPI mutation
  surfaces remain disabled until canonical write semantics are explicitly
  designed.
- NSPI `DNToMId` is authenticated but stateless so Outlook's late bootstrap
  name-resolution probes can complete after session rotation. NSPI `Unbind` is
  idempotent for already-removed session cookies and clears the session cookies;
  stateful NSPI table/property operations still require a live bound session.
- Outlook default-folder properties must be projected from canonical folder
  identities on both the Inbox and Root folder. Generated special-folder binary
  identifiers use the documented 46-byte folder EntryID form for the scalar
  special-folder properties, including IPM subtree, Outbox, Deleted Items, Sent
  Items, Views, Common Views, Finder/Search, Archive, Calendar, Contacts,
  Journal, Notes, Tasks, Reminders, and Drafts. Cached 24-byte LongTermIDs and
  46-byte folder EntryIDs written back by Outlook remain accepted, normalized,
  and retained on the live root-folder handle for cached-mode bootstrap, while
  canonical projection still wins after reconnect.
- `PidTagAdditionalRenEntryIds` is canonically an Inbox special-folder
  identification property under `[MS-OXOSFLD]` section 2.2.4. Outlook 2016/2019
  cached-mode startup can still write the same indexed values to the Root
  handle after hierarchy sync; LPE accepts that Root write as a transient cache
  write and strips it instead of persisting or advertising Root ownership.
- `RopIdFromLongTermId` advertises the canonical store replica GUID in
  `PidTagSerializedReplidGuidMap`, but it also accepts the authenticated
  mailbox account GUID byte layouts as legacy replica aliases so stale Outlook
  special-folder caches can resolve back to canonical LPE folder IDs. If a
  cached LongTermID carries another stale store GUID, LPE accepts it only when
  the global counter maps to an LPE-advertised MAPI special folder; normal
  mailbox items still require the canonical store replica GUID or authenticated
  mailbox GUID.
- Search folders are canonical persisted definitions and computed folder
  projections. Built-in definitions cover Outlook bootstrap surfaces such as
  To-Do, Tracked Mail Processing, Contacts Search, and Reminders, but LPE does
  not export them as Common Views FAI definition messages until it has a
  complete `[MS-OXOSRCH]` criteria serializer for canonical search JSON.
  Search-folder hierarchy and contents remain canonical projections; Common
  Views search-definition FAI rows must not be invented from LPE-private JSON.
  User-saved definitions project as MAPI `FOLDER_SEARCH` hierarchy rows with
  stable canonical identities and container classes derived from their canonical
  result object kind.
- MAPI projects Outlook's default Contacts and Calendar folders even when the
  account has no canonical contact or calendar collections yet. Those empty
  folder projections use reserved MAPI counters, stable source keys, and virtual
  folder checkpoint scopes; they must not create canonical collections until a
  user/API action creates real collaboration state. Outlook may write back or
  cache the Calendar default-folder EntryID during profile bootstrap, so the
  advertised folder must have durable MAPI backing even when the calendar has no
  events yet. A MAPI `IPM.Appointment` create/save against that advertised empty
  Calendar folder creates the event through canonical calendar storage using
  the default calendar collection, not through MAPI-local item state. Existing
  events in that implicit default collection can be read, updated, deleted, and
  opened with canonical attachments through the advertised Calendar folder. If
  collection discovery returns no explicit default Calendar row but canonical
  events already reference the default calendar collection, MAPI sync and
  selective object loads still project those events through the advertised
  Calendar folder. LPE does not
  synthesize Calendar configuration FAI rows during
  first sync. `[MS-OXOCFG]` defines how `IPM.Configuration.Calendar`,
  `IPM.Configuration.CategoryList`, and `IPM.Configuration.WorkHours` messages
  are stored when configuration data exists, but partially fabricated bootstrap
  configuration rows are not canonical calendar state and are unsafe for
  Outlook's initial Calendar FAI parser. Fresh-profile Calendar FAI content sync
  is therefore allowed to be state-only until Outlook creates real associated
  configuration messages that LPE can persist and replay.
- Content synchronization emits long-term `PidTagEntryId` values for message
  and FAI rows using the documented private mailbox Message EntryID shape:
  mailbox account GUID as provider UID, canonical store replica GUIDs, and the
  folder/message global counters used by `PidTagSourceKey`. Outlook relies on
  this identity material when deriving local item-friendly identifiers during
  cached-mode sync.
- Private-mailbox `RopLogon` responses expose the Microsoft fixed folder-id
  slots before `ResponseFlags` and `MailboxGuid`, including the `Shortcuts`
  slot required by current Outlook clients. The corresponding Root hierarchy
  table also exposes these documented Root children, including Schedule, Search,
  Common Views, Personal Views, and Shortcuts. The `Shortcuts` FID is a bounded
  openable Root child for Outlook startup compatibility; it does not create a
  canonical LPE shortcut store.
- Common Views, Schedule, Search, Personal Views, and Shortcuts are Root
  children outside the IPM subtree.
  Navigation shortcuts are projected as folder-associated information messages
  in Common Views, not as durable contents in the Shortcuts folder. This follows
  `[MS-OXOSFLD]` sections 2.2.2 and 3.1.1.1 for special-folder behavior and
  `[MS-OXOCFG]` navigation shortcut semantics: a shortcut is a Common Views FAI
  message with `WLink` properties.
- Outlook mail-folder default views point at the bounded synthetic Common Views
  named-view rows. Outlook-visible non-mail folders, including Calendar and
  Contacts, use folder-local synthetic `IPM.Microsoft.FolderDesign.NamedView`
  defaults so clients can open the advertised `PidTagDefaultViewEntryId` without
  exposing non-mail view definitions as Common Views FAI rows. Supported
  Outlook-visible folders also expose their folder-local default named view
  through associated-contents table discovery when clients restrict on
  `IPM.Microsoft.FolderDesign.NamedView`. Delete attempts against those
  synthetic folder-local default view rows are acknowledged as no-op success
  because the rows are compatibility projections, not canonical FAI messages.
- Navigation shortcut FAI rows persist in `mapi_navigation_shortcuts` for
  Outlook-created or imported Common Views shortcut messages. The bounded
  supported property surface is the visible shortcut subject, target folder
  EntryID, type, flags, section, ordinal, group header GUID, and group display
  name. LPE does not synthesize default `WunderBar` rows for fresh profiles:
  `[MS-OXOCFG]` defines navigation shortcuts as Common Views FAI messages that
  clients create, store, and later read, so fresh-profile Common Views content
  ICS can legitimately be empty. Outlook-created `WunderBar` group headers are
  persisted as Common Views FAI rows with `PidTagWlinkType = 4` and linked
  shortcuts retain the matching `PidTagWlinkGroupClsid`. This scope covers
  cached-mode profile creation and reopen; full Exchange navigation-pane
  presentation parity, shared-folder shortcut semantics, public-folder shortcut
  flags, and read-only group-type extensions remain deferred until real Outlook
  traces require them.
- Outlook-created folder-associated configuration FAI messages outside Common
  Views persist in `mapi_associated_config_messages`. This table is bounded
  MAPI compatibility state for view/form/client configuration sync: it stores
  the folder id, subject, message class, and typed MAPI property bag needed for
  later associated-contents table and FAI content-sync replay. These rows are
  not canonical mailbox messages and must not be exposed through normal message
  lists, JMAP mail, IMAP, search, AI pipelines, or mailbox export as user mail.
  `PidTagRoamingDictionary` values, including the `[MS-OXOCFG]` reserved
  `OLPrefsVersion` entry, are preserved as Outlook writes them. For
  LPE-synthesized minimal Inbox `IPM.Configuration.*` compatibility rows, LPE
  emits only the dictionary default `OLPrefsVersion = 0`, encoded as `9-0`, so
  Outlook can choose its local/default settings and rewrite the row without LPE
  fabricating unsupported Exchange preference data. Inbox associated-content
  sync does not emit broad synthetic or virtual-only rows such as aggregation,
  sharing, EAS, ELC, rule organizer, account preferences, or message-list
  settings unless Outlook has persisted a backed row with a valid payload.
- Reminder projection is a computed search-folder surface over canonical
  calendar/task/message data, not a protocol-local reminder store. LPE-owned
  search-folder definitions are not exported as `IPM.Microsoft.WunderBar.SFInfo`
  Common Views FAI rows until the MAPI adapter can persist and replay a
  documented `[MS-OXOSRCH]` `PidTagSearchFolderDefinition` blob. Publishing a
  locally invented SFInfo blob is an Outlook-visible protocol violation.
- `RopSetSearchCriteria` and `RopGetSearchCriteria` are bounded to canonical
  `search_folders` rows. The supported criteria subset is folder scope,
  unread/read predicates, follow-up flagged predicates, category keywords,
  attachment-presence predicates, sender display or address text, subject/body
  text, and received-date equality or inclusive bounds. Received-date criteria
  accept Outlook delivery-time restrictions and map them to canonical
  `receivedAt` JSON instead of storing an Exchange search-folder blob.
  Attachment-presence criteria accept either the bounded boolean property
  restriction or the Outlook-style existence restriction for
  `PidTagHasAttachments`; both serialize back from canonical JSON as the
  bounded property form. Category keywords serialize as
  `PidNameKeywords` `PtypMultipleString` values so accepted canonical category
  criteria remain round-trippable through `RopGetSearchCriteria`.
  `RopSetSearchCriteria` updates only existing
  user-saved search folders by translating that subset into canonical
  `scope_json` and `restriction_json` with `kind = "mapi_bounded"`.
  Built-in search folders remain read-only. Unsupported restriction operators,
  disjunctions, subobjects, comments, recipient/Bcc predicates, unknown
  folders, and any criteria that cannot round-trip through canonical JSON
  return parseable ROP-specific errors without creating a MAPI-local
  search-folder store.
- The current `[MS-OXOSRCH]` parity audit for bounded search criteria is:
  `RES_AND` over supported leaves is accepted and flattened into canonical JSON;
  `RES_CONTENT` is accepted only for subject, body, and sender text;
  `RES_PROPERTY` is accepted only for equality on read, flag status,
  attachment presence, category, sender, subject, and body plus received-date
  equality or inclusive bounds; `RES_BITMASK` is accepted only for the read bit
  in `PidTagMessageFlags`; and `RES_EXIST` is accepted only for
  `PidTagHasAttachments`. `RES_OR`, `RES_NOT`, `RES_SIZE`,
  `RES_COMPAREPROPS`, `RES_SUBRESTRICTION`, `RES_COMMENT`, `RES_COUNT`,
  recipient display predicates, Bcc-related predicates, Exchange template BLOBs,
  arbitrary Microsoft search-folder definition blobs, and malformed restriction
  blobs that leave trailing bytes after the parsed restriction remain rejected
  with parseable `RopSetSearchCriteria` / `RopGetSearchCriteria` errors until a
  canonical evaluator and serializer are explicitly documented.
- Delegate and free/busy objects are canonical projections over
  `calendar_grants`, `sender_rights`, and `calendar_events`. LPE does not create
  Exchange public-folder free/busy state or protocol-local delegate data-folder
  truth for this layer. The MAPI and EWS adapters consume the canonical
  delegate/free-busy API: same-tenant availability is exposed as computed
  free/busy blocks, calendar read grants preserve tentative/busy distinctions,
  and calendar write plus `send-on-behalf` is the supported canonical signal for
  receiving or processing meeting-related objects on behalf of a delegator.
  MAPI creates, updates, deletes, and attachment mutations against custom or
  shared calendar folders use the same canonical collection rights: read-only
  shared calendars remain visible but reject write/delete attempts without
  mutating `calendar_events` or `calendar_event_attachments`.
  Empty delegate/free-busy projection stays empty; LPE must not create
  placeholder `IPM.Microsoft.Delegate` or
  `IPM.Microsoft.ScheduleData.FreeBusy` messages just to satisfy Outlook folder
  contents.
- `PidTagSwappedToDoData` uses the documented version-1 validation. Malformed
  blobs fail validation instead of being accepted into canonical task state.
- Journal and Notes data are canonical account-owned items. MAPI coverage must
  project and mutate them only through canonical item tables, APIs, and change
  tracking.

### Transport Spooler Advisory ROPs

`RopSetSpooler`, `RopSpoolerLockMessage`, and `RopTransportNewMail` remain
parseable unsupported probes until LPE has a canonical advisory model that is
observable outside the MAPI session. The current canonical transport state is
`submission_queue`, `submission_recipients`, `submission_events`,
`mail_change_log`, and the LPE-to-LPE-CT handoff. None of those tables expresses
client-spooler ownership, per-message spooler locks, or client-announced new
mail delivery.

The supported design constraints are:

- `RopSetSpooler` must not persist a MAPI-local "spooler active" flag. If a
  later Outlook trace requires an acknowledgement, it can become a session-local
  no-op only after tests prove Outlook does not depend on durable behavior.
- `RopSpoolerLockMessage` must not lock canonical messages or queue rows because
  LPE-CT owns transport custody after handoff and canonical mailbox state uses
  normal transaction boundaries. A future implementation needs a documented
  queue lease or advisory lock model shared with the outbound worker before this
  ROP can mutate state.
- `RopTransportNewMail` must not create or announce inbound mail. Inbound
  delivery belongs to LPE-CT final delivery and canonical mailbox insertion; MAPI
  clients learn about new mail through contents sync, notifications, and
  `mail_change_log` replay.

Until those prerequisites exist, all three ROPs are parsed to their documented
request lengths and return ROP-specific protocol errors without modifying
mailbox, submission, notification, or LPE-CT state.

## Implemented Coverage

The implemented coverage described here is the guarded local surface and does
not by itself authorize broad client publication.

### Transport and Bootstrap

- Authenticated MAPI over HTTP endpoint routing exists for the bounded EMSMDB
  and NSPI surfaces.
- EMSMDB session context handling covers connection, execution, disconnect,
  request id handling, client info echoing, response-code mapping, cookies, and
  overlapping same-session sequence validation.
- Profile bootstrap projects private mailbox store/logon properties, default
  folder identities, hierarchy metadata, and basic contents sync data required
  by the local Outlook cached-mode gate.

### EMSMDB ROP Coverage

- Store and folder open paths cover the private mailbox root, IPM subtree,
  default folders, contents tables, hierarchy tables, attachment tables, and
  permission table projection.
- The current ROP surface includes bounded support for property reads/writes,
  table query, hierarchy sync, content sync, FastTransfer source buffering,
  message import/save, draft/send flows, read-state changes, deletes, whole-folder
  canonical mailbox content purges, moves, copies, and attachment reads/writes
  where backed by canonical state.
- Outlook's `PidTagAdditionalRenEntryIds` multi-binary special-folder cache is
  accepted as session-local Inbox metadata during cached-mode bootstrap. LPE
  keeps canonical values for the documented indexes and preserves client data at
  other indexes, matching the special-folder property contract without creating
  durable MAPI-only folder truth. Documented index aliases learned from this
  property, including index 4 for Junk E-mail, resolve to the canonical special
  folder for later `RopOpenFolder` calls in the same MAPI session. Unlearned
  client-local folder identifiers remain unmapped and fail through the normal
  `ecNotFound` folder-open path.
- Outlook scalar default-folder EntryID writebacks on Root or Inbox are validated
  against the canonical special-folder map and acknowledged for interoperability,
  but they do not override the canonical projection or create session-local
  folder identity state.
- Store-level `RopGetPropertiesAll` and `RopGetPropertiesList` enumerate the
  same computed default-folder identities as targeted store `GetProps` calls,
  including `PidTagIpmAppointmentEntryId`, so Outlook bootstrap paths that
  discover Calendar through broad store-property enumeration receive the
  canonical Calendar EntryID without relying on MAPI-local folder state.
- `RopGetReceiveFolder`, `RopGetReceiveFolderTable`, and bounded
  `RopSetReceiveFolder` use the same canonical receive-folder map. The only
  accepted `IPM.Appointment` or `IPM.Appointment.*` receive-folder write is the
  canonical Calendar folder; mismatched writes are rejected without creating
  protocol-local receive-folder state.
- Root and Inbox `RopGetPropertiesAll` / `RopGetPropertiesList` enumerate the
  same computed default-folder identity properties for Outlook's documented
  Inbox-first, Root-fallback special-folder discovery path; the values remain
  computed from canonical reserved MAPI folder identities.
- `PidTagValidFolderMask` is kept aligned with the special-folder EntryIDs LPE
  advertises for the documented store-level mask surface, including Finder /
  Search.
- Outlook store bootstrap metadata includes the private-store marker, store
  state, mailbox owner, user GUID, minimal valid server icon payloads, and max
  submit message size.
- Profile settings needed for cached-mode reuse are canonical account settings,
  not session-only state. Outlook's IPM subtree OST identity value
  (`0x7C04_0102` in the current bounded profile path) is persisted in
  `mapi_profile_settings.ipm_subtree_ost_id` when Outlook writes it to the IPM
  subtree and is reloaded when the folder is opened in a later session. The
  stored value remains a bounded profile setting, with a 2048-byte limit that
  covers observed Outlook cached-mode values without becoming a general OST
  profile store. If the persistence path is unavailable, the accepted write
  remains visible in the current session so Outlook bootstrap can continue, and
  installation checks must report the missing canonical schema state.
- `RopGetReceiveFolder` and `RopGetReceiveFolderTable` use the same primed
  receive-folder table: `IPM` and `IPM.Note` resolve to Inbox and
  `IPM.Appointment` resolves to the canonical Calendar folder. Empty or
  unmatched message-class probes return Inbox with an empty explicit class,
  matching the documented `RopGetReceiveFolder` longest-prefix fallback.
- Calendar RCA diagnostics log the `PR_IPM_APPOINTMENT_ENTRYID` folder EntryID,
  decoded Calendar FID, `IPF.Appointment` folder contract, default calendar
  collection presence, projected event count, and effective access state when
  Outlook opens the Calendar folder. The log must distinguish a truly wired
  canonical Calendar projection from an advertised special-folder shell.
- Unsupported or malformed ROPs use parseable error responses and terminate the
  current buffer as required by the wire contract.

### NSPI Coverage

- NSPI can resolve the authenticated mailbox and canonical visible contacts for
  Outlook address-book bootstrap.
- NSPI projects `PidTagAddressBookObjectGuid` as the Windows GUID byte layout
  expected by Outlook address book clients.
- NSPI mutation and advanced link-table operations are intentionally deferred.

### ICS and FastTransfer Coverage

- Hierarchy synchronization emits canonical folder identities, source keys,
  change keys, predecessor lists, special-folder fields, content counts, unread
  counts, `PidTagLocalCommitTimeMax`, `PidTagDeletedCountTotal`, and final state.
  Folder-change rows include `PidTagFolderId` when the client requests the `Eid`
  synchronization extra flag; Outlook's cached-mode hierarchy request does so,
  which lets it bind default folders such as Calendar.
  `MetaTagIdsetGiven` is sent as property tag `0x40170003` while its payload is
  serialized as binary, matching the Microsoft ICS state compatibility rule.
- Contents synchronization emits canonical message-change rows, folder-associated
  information rows for the bounded bootstrap surface, conversation action FAI
  rows, destroyed conversation actions as `IncrSyncDel`, tombstones, read-state
  changes, and final state.
- Content and hierarchy manifests are selected from canonical folder membership
  and canonical change tracking rather than from primary mailbox fields alone.
- FastTransfer source buffering emits parseable transfer chunks and validates
  strict ICS/FastTransfer value encoding.

### Canonical Projection Coverage

- Search folder and reminder bootstrap projection is backed by canonical
  persisted definitions and bounded evaluators. User-saved Search Folders are
  synchronized as canonical folder definitions through hierarchy tables and
  hierarchy sync; full arbitrary Exchange search-result materialization remains
  deferred until the canonical restriction evaluator is widened.
- Conversation action FAI rows and destroyed conversation actions are projected
  for the supported cached-mode sync path.
- Notes and Journal item projection uses canonical item state and must remain
  aligned with canonical API behavior.
- Session-scoped notification support can mark content and hierarchy changes as
  pending, include bounded TableModified-style payloads with the changed folder
  ID, changed message/object ID, canonical change cursor, modseq, folder counts,
  object/change kind, display names, and message subject when those values are
  available from canonical `mail_change_log` replay. Registrations and pending
  event delivery remain session-local; after process restart or movement to a
  different worker, the session must re-register and resume from canonical
  sync/checkpoint behavior rather than relying on cross-process notification
  delivery. Full notification registration, all table row values, and Exchange
  delivery parity remain deferred.

## Deferred Surfaces

| Surface | Status |
| --- | --- |
| Public folders | Public-folder logon, hierarchy/content projections, post create/update/delete/copy/move, ACL read/write, read-state ROPs, bounded LPE-owned per-user information stream round-trip, canonical replica topology projection through `RopGetOwningServers`, and `RopPublicFolderIsGhosted` ghost-state derivation are implemented over the canonical public-folder layer documented in `docs/architecture/public-folders-mapi-mvp.md`. Exchange-compatible cross-server public-folder replication, recipient-bearing item conversion, and arbitrary Exchange-compatible per-user binary blobs remain deferred and must return parseable errors without creating protocol-local public-folder state. |
| Outlook Anywhere / RPC over HTTP | Deferred legacy compatibility shim. `EXPR` publication requires a real `/rpc/rpcproxy.dll` path and separate evidence. |
| Cross-process MAPI session replay and load-balanced failover | Deferred production hardening. First lab gate may use single-node sticky sessions. |
| Client SMTP in core LPE | Forbidden. Submission must use canonical LPE submission, not a client SMTP endpoint in the core server. |
| Protocol-local Sent/Outbox | Forbidden. Sent and submission state must be canonical. |
| NSPI mutation | Deferred. Address-book writes and link-table mutation remain disabled. |
| Raw FastTransfer destination upload streams | Partially implemented. Destination configure plus PutBuffer / PutBufferExtended accepts bounded FastTransfer property streams on pending canonical objects and routes them through the existing save/import paths. Exchange marker/subobject stream shapes remain unsupported and return parseable ROP errors without creating protocol-local state. |
| Non-mailbox recursive purge | Deferred until canonical folder lifecycle semantics and interoperability evidence are complete. `RopEmptyFolder` is bounded to hard-deleting visible memberships in the target canonical mailbox folder through the canonical tombstone/change-log path. `RopHardDeleteMessagesAndSubfolders` recurses only through canonical mailbox descendants and does not delete non-mailbox objects. Public-folder whole-folder purge returns a parseable not-supported ROP error; public-folder item delete/move/copy remains item-scoped through canonical public-folder APIs. |
| Recoverable Items / dumpster ROP exposure | Bounded MAPI Recoverable Items Root, Deletions, Versions, and Purges virtual folders project canonical `recoverable_items` lifecycle state for browse, restore, and purge only. `RopMoveCopyMessages` move from a concrete recoverable subfolder uses canonical recoverable restore, `RopMoveCopyMessages` copy returns a parseable not-supported error, `RopDeleteMessages` on recoverable folders returns partial completion without purging because LPE does not implement Exchange's Deletions-to-Purges soft-delete progression, purge and empty-folder on Deletions, Versions, or Purges use canonical recoverable purge, Recoverable Items Root message mutation and purge calls return parseable not-supported errors, retention/legal-hold failures return partial completion, and recovery state stays out of normal mailbox hierarchy/content sync. `RopGetContentsTable` with `SoftDeletes` (`0x20`) returns a parseable not-supported ROP error because canonical LPE hard delete/Trash purge removes normal folder membership and writes `recoverable_items` rows instead of keeping folder-local soft-deleted rows. `OpenSoftDeleted`, complete Exchange dumpster folder parity, and any MAPI-local dumpster store remain unsupported. Versions and Purges are bounded virtual projections over canonical lifecycle rows; LPE does not claim Exchange copy-on-write Versions behavior or full Purges post-recovery parity. |
| Sync move import | `RopSynchronizationImportMessageMove` uses the documented length-prefixed source folder/source message request shape. LPE derives the destination mailbox from the synchronization collector folder and ignores client-supplied destination message-id/change-number values as durable identifiers, so imported moves converge through canonical mailbox membership move state instead of creating MAPI-local identity truth. |
| Sync hierarchy import | `RopSynchronizationImportHierarchyChange` creates canonical custom mailbox folders only. Imported system-folder rows are acknowledged as no-op reconciliation so Outlook hierarchy sync does not surface `MAPI_E_NO_SUPPORT` for Inbox, Deleted Items, Sent Items, Drafts, Sync Issues, and other built-in folders. Imported parent source keys are resolved against existing canonical mailbox MAPI identities so Outlook-created child folders keep their canonical parent; if the parent source key is absent or not canonical, the synchronization collector folder is used when it maps to a canonical mailbox, otherwise the folder is created at the account root. |
| Full search-folder parity | Partially implemented. Bounded `RopSetSearchCriteria` / `RopGetSearchCriteria` support exists only for canonical `mapi_bounded` JSON over folder scope, unread, flagged, attachment presence including `PidTagHasAttachments` existence probes, `PidNameKeywords` category property equality, sender, subject/body text, and received-date bounds. Full Microsoft template BLOB parity, arbitrary restriction trees, recipient/Bcc predicates, and secondary sender/recipient reminder promotion remain deferred. |
| Rules and deferred actions | Partially implemented. `RopGetRulesTable` projects canonical Sieve-backed mailbox rules for Outlook profile visibility. Bounded `RopModifyRules` support writes only generated canonical Sieve rules for cleanly mapped move/delete/mark-read/forward/redirect/stop-processing mutations. Exchange rule blobs, client-only rules, provider-specific predicates, delegate rule templates, deferred-action provider data, and `RopUpdateDeferredActionMessages` remain unsupported; no MAPI-local rule store is allowed and rejected deferred actions do not activate Sieve. |
| Folder permission mutation | Partially implemented. `RopModifyPermissions` maps bounded same-tenant account ACL rows to canonical `mailbox_delegation_grants` for mail folders and canonical `calendar_grants` for default, owned custom, and share-right delegated calendar folders, with audit and change-log writes; Exchange-only ACL subjects and MAPI-local ACL storage remain unsupported. |
| Full notification registration and delivery | Partially implemented through session-local pending events with bounded folder/message/table payloads and canonical change-cursor replay. Cross-process notification replay remains deferred; clients must re-register after reconnect or worker movement and use normal sync to converge. |
| Outlook tolerance beyond the documented lab matrix | Unknown until captured through the release gates below. |

## Outlook Server-Side Profile Data Matrix

| Profile data | Canonical storage | API | JMAP | MAPI over HTTP | Tests and gaps |
| --- | --- | --- | --- | --- | --- |
| Messages | `messages`, `mailbox_messages`, `recoverable_items`, MIME/body/blob tables, submission rows | `/api/mail/messages/submit`, draft and flag APIs; `/api/mail/recoverable-items` browse/restore/purge | `Email/*`, `Mailbox/*`, `Thread/*`, `EmailSubmission/*`; normal views exclude recoverable items | Contents tables, ICS, FastTransfer, import/save/send ROPs; bounded Recoverable Items virtual folders for browse/restore/purge over `recoverable_items` | Covered by existing mail/JMAP/MAPI tests plus canonical recoverable-state tests; no PST/OST content handling. |
| Contacts | collaboration contact collections and contact rows | `/api/mail/contacts` and sharing APIs | `AddressBook/*`, `ContactCard/*` | NSPI and MAPI contact projections | Covered by collaboration/JMAP/MAPI tests; NSPI mutation remains deferred. |
| Calendars | calendar collections, events, grants, free/busy projections | `/api/mail/calendar/events`, delegation/free-busy APIs | `Calendar/*`, `CalendarEvent/*` | Calendar folder, appointment EntryIDs, free/busy/delegate projections | Covered by calendar/JMAP/MAPI tests; full Exchange delegate data folders remain unsupported. |
| Tasks | task lists, task rows, grants, reminder metadata | `/api/mail/tasks`, `/api/mail/task-lists`, reminders API | `TaskList/*`, `Task/*`, `Reminder/*` | Task folder and reminder/search-folder projections | Covered by task/reminder/JMAP/MAPI tests. |
| Notes | canonical client note rows | `/api/mail/notes` | private `Note/*` | Notes folder item projection and custom properties | Covered by notes API/JMAP/MAPI tests. |
| Journals | canonical journal rows | `/api/mail/journal` | private `JournalEntry/*` | Journal folder item projection and custom properties | Covered by journal API/JMAP/MAPI tests. |
| Search Folders | `search_folders` definitions plus hierarchy/content projections | `/api/mail/search-folders` | private `SearchFolder/*` | `FOLDER_SEARCH` hierarchy rows and bounded evaluators; no Common Views SFInfo rows until `[MS-OXOSRCH]` BLOB parity exists | CRUD and projection tests cover canonical wiring; full Microsoft search template BLOB parity remains deferred. |
| Rules | `sieve_scripts` | `/api/mail/rules` read projection; Sieve API mutates | private read-only `Rule/*` | `RopGetRulesTable` projection plus bounded generated-Sieve `RopModifyRules` mutations | Persistence/retrieval/profile visibility tests cover canonical wiring; Exchange rule blobs, client-only rules, provider-specific predicates, delegate templates, and deferred actions remain unsupported. |
| Settings | `server_settings`, mailbox state, `mapi_profile_settings`, computed store/folder defaults | `/api/mail/outlook-profile` read summary and server setting APIs | private read-only `OutlookProfile/*` | Store/logon properties, default-folder properties, IPM subtree OST identity reload | Tests cover profile-state summary and OST identity reuse; full Exchange profile blobs and client registry state are unsupported. |
| Identities | `account_identities`, authenticated account state, sender rights | workspace/session APIs and delegation APIs | `Identity/*` | mailbox owner/user GUID/store identity properties | Covered by identity/delegation tests. |
| Storage/profile state | `mapi_named_properties`, `mapi_custom_property_values`, `mapi_navigation_shortcuts`, `mapi_associated_config_messages`, `mapi_sync_checkpoints`, `mapi_object_identities` | `/api/mail/outlook-profile` read summary | private read-only `OutlookProfile/*` plus object-specific projections | named property mapping, shortcut and associated configuration FAI rows, ICS checkpoints, object IDs/source keys/change keys | Covered by schema/runtime/MAPI profile tests; client-local PST/OST files are intentionally out of scope. |

## Outlook Profile Settings Matrix

| Setting area | Canonical storage today | Profile behavior |
| --- | --- | --- |
| Server/bootstrap defaults | `server_settings`, request host/proxy headers, and computed MAPI logon/store properties | Used for URLs, store display metadata, private-store marker, mailbox owner, max submit size, and minimal valid icon payloads. No per-profile copy is stored. |
| Send identities | `account_identities` and authenticated account state | Projected through JMAP/EWS/MAPI identity and submission paths; MAPI does not own a separate identity store. |
| Folder identity and hierarchy | `mailboxes`, built-in projected folder IDs, `search_folders`, and `mapi_object_identities` | Stable FIDs/source keys/change keys are reused across cached-mode sessions. Default-folder EntryIDs remain computed canonical projections. |
| Custom/shared collaboration folders | `contact_books`, `calendars`, `task_lists`, grants, and `mapi_object_identities` | Non-reserved Outlook-visible collaboration folders use kind-scoped deterministic canonical identity keys and durable store-allocated MAPI object IDs. LPE must not derive folder IDs from raw collection text, owner UUID suffixes, or fallback counters. |
| Named property IDs | `mapi_named_properties` | Durable per-account Outlook named-property ID mapping; session registry is only a cache. |
| Opaque item custom properties | `mapi_custom_property_values` | Stored only for canonical item/attachment objects where the value is not a canonical built-in property. |
| Navigation shortcuts | `mapi_navigation_shortcuts` | Common Views shortcut and group-header FAI rows are durable canonical profile-visible state for cached-mode profile creation and reopen. |
| Folder display flags | `mapi_folder_profile_property_values` | Outlook-written `PidTagExtendedFolderFlags` folder UI streams are persisted per account and MAPI folder id, then overlaid on folder open so display-option writes survive reconnect. This store is bounded to Outlook profile folder flags, not arbitrary Exchange folder truth. |
| Associated configuration FAI | `mapi_associated_config_messages` | Outlook-created folder associated/config messages are durable MAPI-only compatibility state for view/form/client configuration sync replay. Direct associated-message deletes are supported and folder-scoped incremental content sync exports associated-config delete idsets. |
| Sync checkpoints | `mapi_sync_checkpoints` | Durable EMSMDB/ICS cursors for hierarchy/content/read-state reuse; they do not store mailbox content. |
| IPM subtree OST identity | `mapi_profile_settings.ipm_subtree_ost_id` | Outlook-written cached-mode profile identity is persisted account-wide and reloaded on IPM subtree open after reconnect. |
| Default-folder EntryID writes | computed canonical folder projections | Valid writes are accepted for compatibility and stripped from session storage; invalid values are rejected. |

Normal message contents-table rows project Outlook-selected Inbox view columns
from canonical mail data, including creation time, normal importance,
sent-representing sender fields, and
`PidNameContentClass = urn:content-classes:message`.

## State-Management Invariants

### ICS State Encoding

- Final and checkpoint ICS state generated by LPE uses REPLGUID-scoped
  IDSET/CNSET encoding for `MetaTagIdsetGiven`, `MetaTagCnsetSeen`,
  `MetaTagCnsetSeenFAI`, and `MetaTagCnsetRead`.
- The REPLGUID in durable final/checkpoint state is the LPE replica GUID for the
  relevant mailbox or account scope.
- GLOBSET range commands carry six-byte GLOBCNT values in canonical
  byte-comparison order.
- Transient deleted/read/unread sets use REPLID-scoped IDSET/GLOBSET encoding.
  These transient sets must not be confused with durable REPLGUID checkpoint
  state.
- Content sync honors Outlook's extra flag contract for `Eid`, message size,
  and change number; when Outlook requests message size in the change header,
  LPE emits a non-zero value for projected normal and associated messages.
- Sync-upload saves that carry only sync identity/state properties and no
  user-visible message data are not persisted as Outlook-visible messages. A
  Deleted Items metadata-only upload that carries an out-of-range source key is
  acknowledged without canonical persistence so Outlook can drain client-local
  cache artifacts without creating user-visible LPE mail; a matching
  out-of-range Deleted Items import-delete is acknowledged as a no-op for the
  same reason. Out-of-range import-delete and read-state cleanup for
  non-persisted associated-message artifacts is also acknowledged as a no-op
  because those identifiers cannot map to canonical LPE state. Deleted Items
  uploads that include user-visible message data are canonical message imports,
  not metadata-only reports.
- Sync-upload saves with `import_associated=true` create or update bounded
  MAPI associated configuration rows when they target a regular folder. LPE
  persists and replays those rows through associated contents and FAI content
  sync, including `MetaTagCnsetSeenFAI`, but keeps them out of canonical mail
  storage and all non-MAPI user-visible surfaces.
- Hierarchy sync emits changed descendant folders of the configured
  synchronization root; it does not emit the synchronization root itself.
  Hierarchy final state scopes `MetaTagIdsetGiven` and `MetaTagCnsetSeen` to
  the emitted descendant folder changes.

### Checkpoint Selection and Advancement

- Zero-length client ICS state forces a baseline transfer.
- Non-empty uploaded client ICS state may select a delta transfer when it is
  parseable and compatible with the requested mailbox, folder, and sync scope.
- Uploaded client ICS state is input only. It must not be appended to, copied
  into, or substituted for server-generated final/checkpoint state.
- `RopSynchronizationGetTransferState` on an ICS upload collector returns
  server-generated checkpoint state. After successful imported message,
  note, journal, read-state, move, delete, or hierarchy changes, the collector
  state is advanced with the server-assigned object IDs and change numbers
  that Outlook must persist for the upload transaction. Successful delete and
  source-move uploads still produce an explicit server checkpoint so the
  transfer-state path does not fall back to a stale pre-upload folder snapshot.
- `RopSaveChangesMessage` for an Outlook-uploaded message with an imported
  `PidTagSourceKey`, including uploads into Deleted Items, persists the message
  through canonical mail storage and returns a server-assigned Message ID/change
  number. If the imported source key is a representable LPE replica GID in the
  persisted dynamic range, LPE reserves that identity. If it is system-reserved,
  already allocated, or outside LPE's representable persisted range, LPE assigns
  a new server identity and exposes the source key derived from that identity.
  LPE must not acknowledge a non-metadata ICS upload as saved while keeping it
  only as an unbacked client object, and it must not persist an Outlook-visible
  source key that conflicts with the assigned server Message ID.
- `mapi_sync_checkpoints` stores durable server cursor state: checkpoint kind,
  optional mailbox id, MAPI replica GUID, last canonical change sequence, last
  mail modseq, and a small JSON cursor.
- Hierarchy checkpoints are account-wide and usable only for the same sync root
  and hierarchy cursor version. Content and read-state checkpoints are
  mailbox/folder scoped. Canonical folders use the real mailbox id as the
  durable scope. Virtual special folders, including Calendar, Contacts, Tasks,
  and Reminders, use their stable projected folder UUID as the durable scope.
- On `RopSynchronizationConfigure`, the server reads the compatible checkpoint
  and replays canonical change log entries and tombstones after that cursor.
- The durable checkpoint advances only after `RopFastTransferSourceGetBuffer`
  drains the corresponding ICS download stream.
- Transfer-state handles from download sources retain their checkpoint sequence
  and modseq and must not regress `mapi_sync_checkpoints`.
- Upload/import collector handles mutate canonical mailbox state through the
  import path and must never advance download checkpoints.

### Canonical Change Tracking

- Content sync and folder object lookups select messages from canonical
  per-folder `mailbox_states`. Primary mailbox fields may be used only as a
  compatibility fallback where the canonical membership row is absent.
- Message change numbers include per-folder membership/state facts, not only
  message-body facts.
- Import, save, delete, move, copy, and read-state ROPs mutate canonical mailbox
  state and rely on the same change-log/tombstone path used by other protocols.
- MAPI state must remain consistent with JMAP and IMAP-visible state where those
  protocols expose the same user-visible fact.
- `RopModifyRules` is bounded to canonical Sieve-backed mailbox rules. The
  adapter accepts only generated bounded rule definitions that can be translated
  into canonical Sieve text and stored through the existing `sieve_scripts`
  mutation path: move/fileinto, delete/discard, forward/redirect where canonical
  sender rights allow submission, mark-read as a bounded canonical rule action,
  and stop-processing. Exchange-only rule condition/action blobs, client-only
  rules, delegate templates, provider-specific predicates, deferred-action
  provider data, and `RopUpdateDeferredActionMessages` return parseable ROP
  errors and must not create a MAPI-local rule store or activate Sieve.
  `RopGetRulesTable` remains a projection from canonical rule state.
- `RopModifyPermissions` is bounded to Outlook folder ACL rows that identify a
  same-tenant account member through `PidTagMemberId` and supply rights through
  `PidTagMemberRights`. Add and modify rows map read, write, delete, and share
  bits to the canonical `mailbox_delegation_grants` row for the target mailbox
  or the canonical `calendar_grants` row for the target calendar collection;
  remove rows delete that canonical grant. Custom and shared calendar folders
  are accepted only when canonical collection rights include share permission.
  Successful mutations write canonical audit and mail change-log rows and wake
  affected principals through the existing rights journal. Owner, `Default`, and
  `Anonymous` rows are accepted as non-mutating compatibility rows. Unsupported
  member identities, malformed rights, virtual folders, and non-canonical ACL
  data return ROP-specific errors without creating MAPI-local ACL state.

## Release Gates

### Readiness Terms

- Local harness pass means the `crates/lpe-exchange` tests and project live
  scripts pass, including
  `tools/rca_outlook_connectivity_check.py --outlook-rca-readiness`.
- RCA pass means Microsoft Remote Connectivity Analyzer Outlook Connectivity
  passes from the Internet against the same public host and account intended for
  Outlook testing.
- Real Outlook profile pass means Outlook 2016 and Outlook 2019 each create an
  Exchange profile, complete cached-mode synchronization, close and reopen
  without deleting the OST or repairing the profile, resolve NSPI, submit mail
  through canonical LPE submission, and show the authoritative canonical `Sent`
  item.

### Local Harness Gate

- Transport conformance tests cover required headers, `X-RequestId` echoing,
  `X-ClientInfo` echoing, `Content-Length`, `X-ResponseCode`, missing and
  malformed cookies, stale `Disconnect` cookies, duplicate replay, duplicate
  request id with a different body, and overlapping same-session invalid
  sequence behavior.
- EMSMDB tests cover supported bootstrap, hierarchy, contents, table, property,
  FastTransfer, submission, mutation, and unsupported/error paths.
- NSPI tests cover authenticated mailbox and visible-contact resolution, plus
  deterministic rejection of deferred mutation surfaces.
- ICS invariant tests prove REPLGUID final/checkpoint state, REPLID transient
  sets, baseline selection for zero-length client state, delta selection for
  non-empty uploaded state, non-regressing download checkpoints, and no
  checkpoint advancement from upload/import collectors.

### RCA Gate

- The public deployment uses the same host, TLS certificate, account, tenant,
  and endpoint flags intended for Outlook testing.
- Autodiscover publishes only endpoints that are implemented and exposed for the
  gate being tested.
- RCA Outlook Connectivity completes without requiring undocumented local
  registry edits, manual endpoint overrides, or protocol publication that is not
  backed by runtime behavior.

### Outlook 2016/2019 Cached-Mode Evidence

Each supported Outlook version must have separate evidence. A pass for Outlook
2016 does not imply a pass for Outlook 2019, and vice versa.
Use `docs/architecture/outlook-cached-mode-gate-evidence-template.md` to record
deployment, autodiscover, local harness, Microsoft RCA, real Outlook profile,
and log evidence. Completing the template does not by itself mark the gate
passed or authorize publication.

Success criteria for each version:

1. A clean Windows profile creates an Exchange account through the documented
   autodiscover path with MAPI over HTTP selected for the mailbox transport.
2. Initial cached-mode synchronization completes for the mailbox root, IPM
   subtree, Inbox, Sent, Drafts, Deleted Items or Trash, Calendar, Contacts,
   Tasks, Notes, Journal, and the supported built-in search/reminder folders.
3. Outlook closes and reopens at least twice without deleting the OST, repairing
   the profile, or forcing a full cache rebuild. The resumed session uses server
   checkpoint/delta behavior and does not duplicate, lose, or resurrect items.
4. NSPI resolves the authenticated mailbox and visible contacts while preserving
   tenant/account visibility boundaries.
5. Sending from Outlook uses canonical LPE submission. The sent item appears in
   authoritative canonical `Sent` and remains consistent when viewed through the
   supported non-MAPI protocols.
6. Cross-protocol changes for read/unread state, flags, moves, copies, deletes,
   drafts, attachments, and protected `Bcc` metadata agree with canonical LPE
   state.
7. Evidence records the Outlook version/build, Windows build, LPE commit/build,
   account and tenant used, public host, endpoint flags, autodiscover response,
   RCA result, local harness result, and relevant server/client logs.

The 2026-05-31 Outlook 16.0.20026 cached-mode audit in
`docs/audits/mapi-http-outlook-cached-mode-audit-2026-05-31.md` records partial
real-client evidence for autodiscover, NSPI bootstrap, EMSMDB profile/sync ROPs,
session cookies, and checkpoint storage. It does not include Microsoft RCA
exported output, does not identify separate Outlook 2016 and Outlook 2019 runs,
and does not prove close/reopen-twice or canonical send/`Sent` behavior. Treat it
as implementation evidence only, not a publication-gate pass.

Calendar troubleshooting diagnostics log the Calendar default folder contract,
projected canonical calendar counts, and hierarchy-sync `PidTagParentSourceKey`
role for each folder row. For a strict hierarchy sync stream that does not
include the configured sync root as a `folderChange`, a folder directly below
that sync root is represented by a zero-length `PidTagParentSourceKey` as
defined by Microsoft ICS. LPE's Outlook cached-mode bootstrap stream
intentionally emits the IPM subtree root row before its children; in that
emitted-root stream, direct children such as Inbox, Calendar, Contacts, and Sync
Issues use the emitted IPM subtree row's `PidTagSourceKey` as
`PidTagParentSourceKey` so Outlook can resolve the hierarchy from the row it just
received. Receive-folder table rows must
keep the fixed FolderId, MessageClass, and LastModificationTime property-row
wire shape, advertise `IPM.Appointment` before the generic `IPM` row, encode
MessageClass as String8, and derive LastModificationTime from canonical folder
change state, so Outlook can resolve the `IPM.Appointment` receive folder to
the advertised Calendar folder. `RopGetReceiveFolder`, `RopSetReceiveFolder`,
and `RopGetReceiveFolderTable` are valid only on the private mailbox logon
handle and return `ecNotSupported` for other handles. RCA diagnostics log the
receive-folder table row count, first message class, Calendar row presence, and
MessageClass wire type so Outlook startup traces can distinguish a missing
Calendar mapping from stale client cache behavior.
Root and IPM subtree `PidTagSubfolders` projections must remain true even in an
otherwise empty canonical mailbox because LPE's virtual Outlook special-folder
tree is always present below those folders; Outlook startup must be able to walk
that tree before any canonical mail rows or calendar events exist. Root and IPM
subtree rows must also project decodeable `PidTagEntryId` and
`PidTagInstanceKey` values so any cached hierarchy identity Outlook captures
during that walk can be reopened later. IPM subtree FastTransfer hierarchy sync
must emit the IPM subtree root row before child folders, with the same generic
folder type used by table/property projections and Root's source key as its
parent source key, so cached-mode Outlook can anchor child default-folder
EntryIDs under a present OST hierarchy parent.
Restricted hierarchy searches over those
rows must match the same display names and identity values that unrestricted
hierarchy `QueryRows` returns, including `"Top of Information Store"` for the
IPM subtree and exact account-scoped `PidTagEntryId` matches for the IPM subtree
and Calendar folder. Inbox hierarchy restriction matching must also evaluate
`PidTagIpmAppointmentEntryId` with the authenticated mailbox GUID for both real
canonical Inbox rows and synthetic virtual Inbox rows. FastTransfer/ICS
hierarchy folder-change rows for virtual parent folders such as Root, IPM
subtree, Sync Issues, and Recoverable Items root must also report
`PidTagSubfolders=true` based on the virtual
special-folder tree, not only on the subset of child rows included in the
current transfer.
`PidTagIpmAppointmentEntryId` projections from
canonical and synthetic Inbox hierarchy `QueryRows`/`FindRow` rows, direct Inbox
property reads, Root fallback reads, and store logon reads must use the
authenticated mailbox GUID consistently so Outlook does not see distinct
Calendar entry IDs for the same default folder. Opened Calendar folder
properties and hierarchy rows must also project decodeable `PidTagEntryId`,
`PidTagInstanceKey`, and `PidTagSourceKey` values for the same canonical folder
object, including `GetPropertiesSpecific`, `GetPropertiesAll`, property-list,
hierarchy-table probes, and ICS hierarchy folder-change rows unless the client
explicitly excludes the property. Calendar folders with `IPF.Appointment`
container class must also project `PidTagDefaultPostMessageClass` as
`IPM.Appointment` in both String8 and Unicode request forms so Outlook binds
the folder's default item type without falling back to generic mail semantics;
the Unicode form must be advertised by folder property enumeration and default
hierarchy column discovery and emitted in hierarchy FastTransfer/ICS folder
changes, not only returned for exact property probes. Hierarchy ICS exclusion
lists are matched through canonical string-property identity, so excluding the
String8 or Unicode form of a folder string property suppresses the same
underlying folder fact instead of reintroducing it through the alternate wire
type. Content ICS property include and exclude lists follow the same canonical
string-property matching, including Calendar `IPM.Appointment` message-class
rows, so String8 client filters and Unicode server projections remain aligned.
Hierarchy table rows must keep Calendar identity and classification on the same
row: `PidTagEntryId`, `PidTagInstanceKey`, `PidTagSourceKey`,
`PidTagContainerClass = IPF.Appointment`, and both String8 and Unicode
`PidTagDefaultPostMessageClass = IPM.Appointment` must describe the same
canonical folder object.
`RopGetReceiveFolder(IPM.Appointment)` must likewise resolve to the canonical
default Calendar FID and that FID must be immediately openable as
`IPF.Appointment`, even when canonical storage currently contains only custom
calendar collections.
`PidTagIpmAppointmentEntryId` must return an account-scoped EntryID whose
embedded long-term ID converts back to the same canonical Calendar FID; reopening
that FID must expose the same `IPF.Appointment`/`IPM.Appointment` folder
classification.
RCA diagnostics for Outlook Calendar startup must expose the exact folder
discovery path: whether `PidTagIpmAppointmentEntryId` was requested from Inbox
or Root, whether the Root fallback EntryID bytes match the Inbox EntryID bytes,
whether `RopGetReceiveFolder(IPM.Appointment)` resolves to the Calendar FID, and
whether the exact hierarchy-table property set requested by Outlook included
decodeable Calendar `PidTagEntryId`, `PidTagSourceKey`, and `PidTagFolderId`
values.
`RopGetPropertiesSpecific` on Root must return the same canonical
`PidTagIpmAppointmentEntryId` binary value as Inbox; advertising the property in
Root `GetPropertiesAll` or `GetPropertiesList` is insufficient if the specific
property read later returns a flagged missing value.
Outlook client Event Viewer entries such as `WebRequestSemaphore_Open_Error`
and `FindExtensionForRequestFailed` are not server-visible protocol failures by
themselves. Server-side RCA must correlate them against LPE Autodiscover and
MAPI HTTP logs by `x-requestid`, `client-request-id`, `x-trace-id`, user agent,
and MAPI publication fields. Autodiscover logs must show whether MapiHttp was
requested, gate-enabled, and selected, plus the published EMSMDB/NSPI URLs; MAPI
transport logs must show the matching client correlation headers for EMSMDB and
NSPI requests.
Outlook can issue FAI-only content sync (`syncFlags` normal content not
requested) against Deleted Items. When canonical normal messages exist in
Deleted Items, LPE completes the sync source but must not advance the normal
content checkpoint because the client-requested scope suppressed those messages.
Checkpoint storage diagnostics therefore report
`checkpoint_store_status=not_stored_partial_scope`, while the completed sync
summary reports `status=ok_partial_scope_no_checkpoint` together with
`all_sync_sources_completed=true` and an expected partial-scope
not-stored count. This is not a Calendar-folder lookup failure by itself.
Outlook can also upload transient collector artifacts into Deleted Items using
client-local source keys outside LPE's persisted MAPI identity range. LPE
acknowledges those saves with transient object identities but must not import
them into the canonical mailbox or they will surface as user-visible trash
messages.
Outlook Calendar startup can create Freebusy Data view/configuration messages
under the special Freebusy Data folder. Until LPE stores first-class writable
Freebusy Data FAI state, those creates are acknowledged as transient associated
messages and must not be routed into canonical mail or calendar storage. The
same transient handle remains readable for immediate Outlook
`RopGetPropertiesSpecific` calls after `RopSaveChangesMessage`.
Custom and shared calendar hierarchy rows must use owner-scoped decodeable
folder `PidTagEntryId` values rather than
nil-mailbox placeholders so Outlook can reopen the advertised folder identity,
including ICS hierarchy-sync folder-change rows. Custom and shared calendar
folders must also be present in IPM subtree hierarchy sync as `IPF.Appointment`
folder-change rows, not only in hierarchy tables.
Calendar content sync must load canonical calendar events for the Calendar folder
and emit them as normal
`IPM.Appointment` message changes with appointment timing/location properties,
`PidLidAppointmentStartWhole`, `PidLidAppointmentEndWhole`, all-day, busy status,
state flags, and stable `PidLidGlobalObjectId` / `PidLidCleanGlobalObjectId`
values when canonical events exist; a fresh Calendar folder with no events can
return state-only content sync, while a non-empty Calendar folder must not fall
back to generic-message-only projection. Low-LID Calendar named properties such
as `PidLidGlobalObjectId` must be exposed through
`RopGetPropertyIdsFromNames` with assigned named-property IDs in the
named-property range; the LID itself is the property name, not the wire property
ID. Outlook's MAPI Calendar property model also requires appointment start time
to be strictly earlier than end time, so zero-duration canonical events are
projected to MAPI with a minimum one-minute appointment window while leaving the
canonical event unchanged. Bounded MAPI calendar writes update only existing
canonical `calendar_events` columns: subject/display name, body, HTML body,
start/end through `PidTagStartDate`/`PidTagEndDate` and
`PidLidAppointmentStartWhole`/`PidLidAppointmentEndWhole` plus
`PidLidCommonStart`/`PidLidCommonEnd`, location, all-day,
busy-status-derived canonical status, organizer,
required attendees from display/To attendee properties, and optional attendees
from `PidTagDisplayCc` and `PidLidCcAttendeesString`, plus the bounded
`PidLidTimeZoneDescription` string into canonical `time_zone`. Bounded
`PidLidAppointmentStateFlags` writes map only the meeting/cancel bits; the
cancel bit updates canonical event status to `cancelled`, while unsupported
state bits are rejected without side effects. Calendar reads project those
canonical body, organizer, attendee, and timezone fields through
direct properties, requested contents columns, and FastTransfer/ICS message
properties, including common start/end aliases, the bounded
`PidLidAllAttendeesString`, `PidLidToAttendeesString`, and
`PidLidCcAttendeesString` plus `PidTagDisplayCc` projections from canonical attendee metadata and
timezone description/definition projections from canonical event timezone
state. Calendar content sync also projects canonical attachment presence from
`calendar_event_attachments` through `PidTagHasAttachments`; attachment table,
open, stream, create, and save paths use canonical calendar attachment rows.
Binary timezone payload writes remain rejected until parser-backed
canonical timezone mappings exist.
`PidLidAppointmentRecur` has a parser-backed bounded read/write mapping for
Gregorian daily, weekly, monthly-by-day including month-end, monthly-nth,
yearly-by-month-day, and yearly-nth recurrence patterns, including supported
yearly `BYMONTH` values, into canonical `recurrence_rule`, `recurrence_json`,
deleted-instance `recurrence_exceptions_json` fields, and modified-instance
exceptions that change the occurrence start/end time, subject, or location.
Direct property reads, contents rows that request the property, and
FastTransfer/ICS calendar sync can project the bounded recurrence blob back
from canonical event state. Appointment-like `IPM.Schedule.Meeting.Request`
payloads that contain only the bounded event property subset are canonicalized
as `calendar_events`; bounded meeting responses update canonical attendee
participation status on the existing event; and bounded cancellation payloads
delete the existing canonical event. Modified exceptions that override body,
reminder, busy status, attachment, or other per-instance fields, Hijri
recurrence, malformed recurrence blobs, unsupported meeting
response/cancel properties, and other binary meeting payloads remain
unsupported and are rejected with deterministic parseable errors instead of
being stored as opaque MAPI blobs.
Calendar attachments are projected only through canonical
`calendar_event_attachments`:
`PidTagHasAttachments`, `RopGetValidAttachments`, `RopGetAttachmentTable`, and
`RopOpenAttachment` read that table, while bounded
`RopCreateAttachment`/`RopSaveChangesAttachment` writes validated attachment
blobs into the same canonical event attachment state. `RopDeleteAttachment`
removes the canonical event attachment row and emits calendar change state.
Outlook-only attachment state is not stored.

Delegate/free-busy readiness additionally requires the canonical
`/api/mail/delegation/free-busy` layer to return delegate access objects and
merged non-overlapping availability blocks for the target mailbox calendar.
When no canonical delegate or free/busy state exists, the message-object list is
empty rather than a fabricated status object. MAPI delegate/free-busy message
objects are computed from canonical grants, sender rights, accounts, and
calendar events; LPE does not persist a MAPI-local delegate/free-busy message
table.
This follows the Microsoft MAPI over HTTP session model, the delegate calendar
constraints in MS-OXODLGT, the delegate-management contract in MS-OXWSDLGM, and
the Outlook free/busy block behavior described by Microsoft's Free/Busy API
documentation. Public MAPI publication still waits for the existing local, RCA,
and real-Outlook evidence gates.

### Publication Gate

- MAPI endpoint publication requires the local harness gate, RCA gate, and both
  Outlook 2016 and Outlook 2019 cached-mode evidence to pass for the deployment
  class being advertised.
- `LPE_AUTOCONFIG_MAPI_ENABLED` controls whether MAPI endpoints are advertised.
- `LPE_AUTOCONFIG_OUTLOOK_INTEROP_GATE_PASSED` records that the documented local,
  RCA, and real-Outlook evidence exists for the deployment.
- RPC/HTTP `EXPR` publication requires separate Outlook Anywhere evidence and
  must not be enabled by the MAPI gate alone.
