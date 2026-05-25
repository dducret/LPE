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
| Permission tables | Read-only canonical permission projection. Permission mutation is deferred. |
| Search and reminder folders | Persisted canonical built-in search-folder definitions and FAI rows for the bounded Outlook bootstrap surfaces. |

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
- Draft save, send, move, copy, delete, read/unread, flag, attachment, and
  protected-recipient behavior must map to canonical mailbox state.
- `Bcc` is protected metadata. It must not leak through MAPI search, AI-facing
  indexing, non-owner projections, or protocol shortcuts.
- NSPI resolves the authenticated mailbox and visible contacts from canonical
  account/contact visibility. `ModLinkAtt`, `ModProps`, and other NSPI mutation
  surfaces remain disabled until canonical write semantics are explicitly
  designed.
- Outlook default-folder properties must be projected from canonical folder
  identities. Generated special-folder binary identifiers use the documented
  46-byte folder EntryID form; cached 24-byte LongTermIDs and 46-byte folder
  EntryIDs written back by Outlook remain accepted and normalized.
- `RopIdFromLongTermId` advertises the canonical store replica GUID in
  `PidTagSerializedReplidGuidMap`, but it also accepts the authenticated
  mailbox account GUID byte layouts as legacy replica aliases so stale Outlook
  special-folder caches can resolve back to canonical LPE folder IDs. If a
  cached LongTermID carries another stale store GUID, LPE accepts it only when
  the global counter maps to an LPE-advertised MAPI special folder; normal
  mailbox items still require the canonical store replica GUID or authenticated
  mailbox GUID.
- Search folders are canonical persisted definitions plus folder-associated
  information rows. Bounded evaluators cover the Outlook bootstrap surfaces such
  as Common Views, To-Do, Tracked Mail Processing, and Contacts Search.
- Content synchronization emits long-term `PidTagEntryId` values for message
  and FAI rows using the documented private mailbox Message EntryID shape:
  mailbox account GUID as provider UID, canonical store replica GUIDs, and the
  folder/message global counters used by `PidTagSourceKey`. Outlook relies on
  this identity material when deriving local item-friendly identifiers during
  cached-mode sync.
- Private-mailbox `RopLogon` responses expose exactly the documented 13
  special-folder IDs before `ResponseFlags` and `MailboxGuid`; adding extra
  folder IDs shifts `MailboxGuid` and causes Outlook to construct malformed
  private-store EntryIDs.
- Reminder projection is a computed search-folder surface over canonical
  calendar/task/message data, not a protocol-local reminder store.
- `PidTagSwappedToDoData` uses the documented version-1 validation. Malformed
  blobs fail validation instead of being accepted into canonical task state.
- Journal and Notes data are canonical account-owned items. MAPI coverage must
  project and mutate them only through canonical item tables, APIs, and change
  tracking.

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
  message import/save, draft/send flows, read-state changes, deletes, moves,
  copies, and attachment reads/writes where backed by canonical state.
- Outlook's `PidTagAdditionalRenEntryIds` multi-binary special-folder cache is
  accepted as session-local folder metadata during cached-mode bootstrap.
- Outlook store bootstrap metadata includes the private-store marker, store
  state, mailbox owner, user GUID, server icons, and max submit message size.
- `RopGetReceiveFolder` maps Outlook `IPM.Appointment` probes to the canonical
  Calendar folder so cached-mode bootstrap does not fall back to Inbox.
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
  persisted definitions and bounded evaluators.
- Conversation action FAI rows and destroyed conversation actions are projected
  for the supported cached-mode sync path.
- Notes and Journal item projection uses canonical item state and must remain
  aligned with canonical API behavior.
- Session-scoped notification support can mark content and hierarchy changes as
  pending and replay canonical change cursors. Full notification registration
  and delivery parity remains deferred.

## Deferred Surfaces

| Surface | Status |
| --- | --- |
| Public folders | Public-folder logon and replica/per-user state are deferred. The server must return parseable unsupported responses without creating protocol-local public-folder state. |
| Outlook Anywhere / RPC over HTTP | Deferred legacy compatibility shim. `EXPR` publication requires a real `/rpc/rpcproxy.dll` path and separate evidence. |
| Cross-process MAPI session replay and load-balanced failover | Deferred production hardening. First lab gate may use single-node sticky sessions. |
| Client SMTP in core LPE | Forbidden. Submission must use canonical LPE submission, not a client SMTP endpoint in the core server. |
| Protocol-local Sent/Outbox | Forbidden. Sent and submission state must be canonical. |
| NSPI mutation | Deferred. Address-book writes and link-table mutation remain disabled. |
| Raw FastTransfer destination upload streams | Deferred except for bounded import behavior that mutates canonical mailbox state through supported ROPs. |
| Folder move/copy and whole-folder purge | Deferred until canonical folder lifecycle semantics and interoperability evidence are complete. |
| Full search-folder parity | Partially implemented. Full Microsoft template BLOB parity and secondary sender/recipient reminder promotion remain deferred. |
| Rules and deferred actions | Deferred until canonical rules/deferred-action state is designed. |
| Folder permission mutation | Deferred. Read-only permission table projection is supported. |
| Full notification registration and delivery | Partially implemented through pending session events and change-cursor replay; full parity remains deferred. |
| Outlook tolerance beyond the documented lab matrix | Unknown until captured through the release gates below. |

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
- Hierarchy final state scopes `MetaTagIdsetGiven` to emitted folder IDs, and
  `MetaTagCnsetSeen` covers emitted folder changes plus the sync root change
  counter.

### Checkpoint Selection and Advancement

- Zero-length client ICS state forces a baseline transfer.
- Non-empty uploaded client ICS state may select a delta transfer when it is
  parseable and compatible with the requested mailbox, folder, and sync scope.
- Uploaded client ICS state is input only. It must not be appended to, copied
  into, or substituted for server-generated final/checkpoint state.
- `mapi_sync_checkpoints` stores durable server cursor state: checkpoint kind,
  optional mailbox id, MAPI replica GUID, last canonical change sequence, last
  mail modseq, and a small JSON cursor.
- Hierarchy checkpoints are account-wide and usable only for the same sync root
  and hierarchy cursor version. Content and read-state checkpoints are
  mailbox/folder scoped. Virtual collaboration folders that do not map to a
  canonical mailbox do not persist content/read-state checkpoints.
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

Calendar troubleshooting diagnostics log the Calendar default folder contract,
projected canonical calendar counts, and hierarchy-sync `PidTagParentSourceKey`
role for each folder row. The hierarchy diagnostic follows the Microsoft ICS
rule that a folder directly below the configured hierarchy sync root is
represented by a zero-length `PidTagParentSourceKey`; this is expected for
Calendar when Outlook syncs the IPM subtree root. Calendar content sync must
load canonical calendar events for the Calendar folder and emit them as normal
`IPM.Appointment` message changes with appointment timing/location properties,
`PidLidAppointmentStartWhole`, `PidLidAppointmentEndWhole`, all-day/state flags,
and stable `PidLidGlobalObjectId` / `PidLidCleanGlobalObjectId` values; an
advertised Calendar folder with state-only or generic-message-only content sync
is not a valid Outlook interoperability result.

### Publication Gate

- MAPI endpoint publication requires the local harness gate, RCA gate, and both
  Outlook 2016 and Outlook 2019 cached-mode evidence to pass for the deployment
  class being advertised.
- `LPE_AUTOCONFIG_MAPI_ENABLED` controls whether MAPI endpoints are advertised.
- `LPE_AUTOCONFIG_OUTLOOK_INTEROP_GATE_PASSED` records that the documented local,
  RCA, and real-Outlook evidence exists for the deployment.
- RPC/HTTP `EXPR` publication requires separate Outlook Anywhere evidence and
  must not be enabled by the MAPI gate alone.
