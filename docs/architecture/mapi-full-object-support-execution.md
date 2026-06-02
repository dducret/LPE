# MAPI Full Object Support Execution

This document records the ordered execution plan for Microsoft object support
across `JMAP`, `IMAP`, `MAPI over HTTP`, and the client Web UI.

It is intentionally a correctness gate, not a compatibility promise. Public
MAPI publication still requires the local harness, Microsoft Remote
Connectivity Analyzer, and Outlook 2016 / 2019 cached-mode evidence described
in `docs/architecture/client-autoconfiguration.md`.

## Microsoft Source Log

Access date for all entries: 2026-05-21.

| Page title | URL | Topic or claim supported | Ambiguity |
| --- | --- | --- | --- |
| [MS-OXCMAPIHTTP]: MAPI Extensions for HTTP | https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxcmapihttp/d502edcf-0b22-42f2-8500-019f00d60245 | MAPI over HTTP carries MAPI payloads over HTTP between Outlook and Exchange servers. | The specification defines protocol flow, not an application storage model. |
| [MS-OXCDATA]: Overview | https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxcdata/cb617407-ceba-4c1b-9346-a9eb989f3ccf | MAPI data structures cover properties, folder and message identifiers, ROPs, and queries. | Identifier formats vary by protocol context, so canonical storage must not equal wire encoding. |
| [MS-OXCPRPT]: Getting Property IDs for Named Properties | https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxcprpt/33c1b19f-0664-4b53-a968-2ee0d674f72b | Property operations use a 32-bit tag made from a 16-bit property ID and 16-bit type; named properties are mapped with `RopGetPropertyIdsFromNames` and use IDs with the high bit set. The `PS_MAPI` property set is special: its LID is returned directly as the property ID instead of allocating a mailbox mapping. | Persistence scope for a non-Exchange implementation is an engineering decision; LPE must make mapping stable per mailbox/domain where Outlook caches require it. |
| [MS-OXPROPS]: Commonly Used Property Sets | https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxprops/cc9d955b-1492-47de-9dce-5bdea80a3323 | Common named property sets include mail, contact, calendar, task, note, journal, attachment, sync, sharing, and public string sets. | The master list is broad; LPE should implement object families by product scope, not claim every property has business semantics. |
| [MS-OXCMSG]: Per Message Object | https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxcmsg/040f3b96-2288-4813-9edf-a0e8c9e9199d | Message objects maintain per-message state such as read state, recipients, attachments, and related properties. | MAPI message state is richer than IMAP mail state; IMAP must remain a projection. |
| MAPI Service Provider Objects | https://learn.microsoft.com/en-us/office/client-developer/outlook/mapi/mapi-service-provider-objects | Provider object families include provider, logon, message store, folder, message, attachment, table, address book, messaging user, distribution list, status, and utility data/property/table objects. | Outlook provider documentation describes COM provider architecture; LPE maps these concepts to server-side HTTP/ROP state by engineering inference. |
| MAPI message store provider objects | https://learn.microsoft.com/en-us/office/client-developer/outlook/mapi/mapi-message-store-provider-objects | Message store providers implement provider/logon objects, message store, folders, messages, attachments, tables, and optional status objects. | The page is provider-facing; server-side support should expose equivalent behavior through EMSMDB ROPs, not COM objects. |
| MAPI address book provider objects | https://learn.microsoft.com/en-us/office/client-developer/outlook/mapi/mapi-address-book-provider-objects | Address book providers implement containers, distribution lists, messaging users, tables, status objects, and controls. | Controls/dialog behavior is client/provider UI behavior; LPE should support NSPI data semantics first. |
| Accessing Objects by Using the Session | https://learn.microsoft.com/en-us/office/client-developer/outlook/mapi/accessing-objects-by-using-the-session | A MAPI session opens message stores, address books, and entries by entry identifier. | LPE sessions are HTTP server sessions; client profile administration objects are not stored as canonical mailbox data. |

Access date for delegate/free-busy entries: 2026-05-26.

| Page title | URL | Topic or claim supported | Ambiguity |
| --- | --- | --- | --- |
| [MS-OXCMAPIHTTP]: Overview | https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxcmapihttp/beb85e42-8ecf-47f3-802a-d0e13914c6fd | MAPI over HTTP establishes server-side session context for mailbox and directory operations. | It defines transport/session flow, not how LPE should persist delegate or free/busy state. |
| [MS-OXODLGT]: Additional Constraints for Calendar Folder | https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxodlgt/22f09162-0382-4fe5-b767-c5417060fb8b | Calendar delegates need calendar Author/Editor-style permissions; delegates that receive meeting-related objects also need send-on-behalf and delegate-rule behavior. | LPE maps this to canonical calendar write plus `send-on-behalf`; full OP_DELEGATE rule parity remains deferred until canonical rule state exists. |
| [MS-OXWSDLGM]: Delegate Access Management Web Service Protocol | https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxwsdlgm/365cb220-56ba-4e2c-a244-b143a1f2eeac | Delegate access is server-managed mailbox information. | The protocol is EWS-oriented; LPE keeps one canonical delegate object layer for EWS and MAPI projections. |
| About the Free/Busy API | https://learn.microsoft.com/en-us/office/client-developer/outlook/auxiliary/about-the-free-busy-api | Outlook free/busy data is exposed as availability blocks and the Free/Busy API does not provide write access or delegate-account access. | LPE computes read-only blocks from canonical events; delegate management remains separate canonical ACL state. |

## Assumptions

- `LPE` remains the canonical system of record for mailboxes, contacts,
  calendars, tasks, rights, and user-visible collaboration state.
- `JMAP` is the primary protocol surface for modern object access.
- `IMAP` is mail-only compatibility. It must preserve mail invariants but does
  not become a Microsoft object API.
- `MAPI over HTTP` is the Outlook desktop Exchange-account path and can carry
  Microsoft-specific object properties that other protocols only project.
- Session, provider, subsystem, utility, status, and table objects are transport
  and protocol execution state unless a user-visible object or durable property
  requires canonical persistence.

## Success Criteria

Full support for the requested Microsoft object families is complete only when:

1. Durable MAPI identity and named-property mappings survive restart and are
   stable for cached Outlook profiles.
2. Message store objects, folders, messages, attachments, recipients, tables,
   permissions, and ICS state round-trip through MAPI over HTTP without creating
   protocol-local mailbox state.
3. Address book containers, users, distribution lists, and NSPI tables project
   canonical directory/contact data consistently.
4. JMAP exposes canonical mail/contact/calendar/task/note/journal/reminder data
   without modeling MAPI session or provider internals.
5. IMAP exposes only valid mail/folder projections and preserves UID,
   flag, delete, move, and Sent visibility when MAPI or JMAP mutate the same
   mailbox.
6. Web UI reads and writes the same canonical objects as the protocols and does
   not depend on protocol-local state.
7. Microsoft RCA and Outlook 2016 / 2019 cached-mode labs pass before public
   MAPI autodiscover is enabled.

## Ordered Execution

### 1. Canonical Microsoft Object And Property Model

Implement this before expanding protocol behavior.

- Add durable named-property mapping separate from session state.
- Add a durable custom property value store keyed by canonical object identity,
  MAPI property tag, property type, and object kind.
- Keep typed canonical columns for first-class LPE fields such as subject,
  recipients, read state, flags, dates, folder placement, contacts, events,
  tasks, notes, journals, reminders, permissions, and attachments.
- Store opaque or Outlook-specific properties only when they are required for
  Outlook round-trip behavior or object fidelity.
- Keep wire identifiers and entry IDs as projections over canonical IDs.
- Protect `Bcc` as metadata: do not index it in user search or expose it to
  user-facing AI pipelines.

Verification: restart-backed integration tests for named property allocation,
property set/get/delete, cross-session lookup, and JMAP/IMAP visibility of the
same canonical object.

### 2. JMAP Projection Gate

JMAP comes next because it is the cleanest canonical contract.

- Project Microsoft object data into JMAP only where JMAP has a real concept:
  mail, mailboxes, identities, contacts, calendars, tasks, notes, journals, and
  reminders.
- Do not expose MAPI session, subsystem, provider, support, status, table-data,
  or property-data objects as JMAP objects.
- Preserve custom MAPI properties through canonical storage, but expose them in
  JMAP only behind documented extension names.

Verification: JMAP change-state tests after MAPI-created, MAPI-updated, and
MAPI-deleted objects.

### 3. IMAP Mail Invariant Gate

IMAP is not a Microsoft object surface.

- Keep IMAP limited to mailbox, message, UID, flag, body, search, copy, move,
  expunge, and subscription behavior.
- Ensure MAPI-created mail has correct IMAP UID allocation and flags.
- Ensure IMAP moves/deletes update the same canonical objects MAPI sees.
- Do not project contacts, calendars, tasks, notes, journals, provider objects,
  named-property bags, or session state into IMAP.

Verification: cross-protocol tests for append, fetch, flags, move, delete,
expunge, UID validity, and Sent visibility across JMAP, IMAP, and MAPI.

### 4. MAPI Over HTTP EMSMDB Core

After the canonical model and projections are stable, complete the Outlook
mailbox route.

- Keep authenticated MAPI over HTTP endpoints behind the existing publication
  gates.
- Make EMSMDB connect, execute, disconnect, reconnect, context cookies, and
  async notifications resilient to restart boundaries where required by the
  lab gate.
- Implement property ROPs against canonical typed fields plus the durable MAPI
  property bag.
- Complete folder, message, attachment, recipient, table, stream, permissions,
  rules, search folder, and ICS/FastTransfer behavior for Outlook cached mode.
- Route send/draft flows through canonical LPE submission and Sent handling.

Verification: local ROP harness, Microsoft RCA, and Outlook 2016 / 2019 cached
mode profile, sync, send, reconnect, and restart tests.

### 5. Address Book Provider And NSPI

Complete address book support after EMSMDB object identity is stable.

- Map address book containers, messaging users, and distribution lists to
  canonical tenant directory, contacts, groups, and external recipients.
- Keep NSPI ephemeral row and table state separate from canonical objects.
- Persist only stable entry identifiers and required Outlook properties.
- Preserve permissions and tenant boundaries before adding broad lookup scope.

Verification: NSPI bind, query rows, seek, resolve names, display table, and
Outlook address book lookup tests across tenant boundaries.

### 6. Microsoft Object Family Completion

Complete the requested object families in this order:

1. Message store provider objects: store, folder, message, attachment, recipient,
   table, status, permissions, rules, search folders, and synchronization.
2. Address book provider objects: containers, users, distribution lists, tables,
   and status.
3. Session and subsystem objects: session, logon, context, status, notification,
   reconnect, and profile-facing behavior as protocol execution state.
4. Utility data objects: table data and property data semantics through ROP
   tables and the durable property bag; TNEF only when a real Outlook scenario
   requires it.

Engineering inference: because LPE is a server, not a COM MAPI provider DLL,
these are implemented as equivalent MAPI over HTTP, EMSMDB, NSPI, store, and
session semantics rather than literal provider objects.

Verification: object-family-specific interoperability matrix with captured
MAPI Inspector traces for unsupported or inferred behavior.

### 7. Notifications And Reconnect Hardening

- Deliver meaningful notification payloads for folder, message, table, and
  store changes.
- Make reconnect behavior deterministic for sticky single-node sessions first.
- Defer cross-process replay and load-balanced failover until the production
  hardening gate.

Verification: Outlook cached-mode idle, reconnect, network flap, and restart
tests with notification assertions.

### 8. Client Web UI

The Web UI must consume canonical services, not MAPI sessions.

- Surface mail, contacts, calendars, tasks, notes, journals, reminders,
  permissions, and search using shared canonical APIs.
- Show Microsoft-derived fields only where they are user-visible product state.
- Do not expose provider/session/subsystem diagnostics except in an admin
  troubleshooting surface.

Verification: Web UI CRUD and cross-protocol visibility tests for the same
canonical objects.

### 9. Publication Gate And Reassessment

- Keep MAPI autodiscover disabled by default until the evidence gates pass.
- Record local harness, Microsoft RCA, and Outlook lab results separately.
- Reassess readiness after each phase, with explicit gaps for unsupported ROPs,
  named properties, object families, permissions, notifications, and restart
  behavior.

Verification: public autodiscover emits MAPI only when implementation and lab
evidence match the advertised endpoints.

## Redone Assessment

`LPE` is not ready to claim full Microsoft object support yet.

Current code and documentation show meaningful MAPI over HTTP groundwork:
authenticated endpoints, EMSMDB and NSPI surfaces, session cookies, ROP
dispatch, handle tables, MAPI identity projection, table/property support,
ICS/FastTransfer work, permissions, and canonical store integration.

Durable Microsoft property/object fidelity is now split into concrete parts.
Named-property ID mappings are stored per account in `mapi_named_properties`,
and both name lookup and zero-count Logon enumeration load durable mappings
through storage instead of relying only on session-local allocation. A durable
`mapi_custom_property_values` table and storage runtime set/get/delete methods
exist for opaque Outlook-specific values, including attachment object values
where the canonical attachment identity is known and public-folder post item
values where the item has canonical identity. Existing supported object-property
ROP paths now round-trip custom named-property values through that store for
canonical object kinds without copying built-in/canonical fields into the custom
table. Full Exchange property-bag parity remains incomplete and must not be
claimed from this bounded round trip.

Readiness level after this plan:

- `JMAP`: suitable as the first canonical projection gate, but it must not grow
  MAPI session/provider objects.
- `IMAP`: suitable as a mail invariant gate only.
- `MAPI over HTTP`: promising but still guarded; correct next work is broader
  property-bag coverage, restart-safe session/reconnect behavior, fuller
  notification registration/delivery parity, and Outlook lab evidence.
- `NSPI`: bounded address book support is in place, but provider-family
  completeness requires directory/group semantics and tenant-bound lookup tests.
- `Client Web UI`: should wait for canonical object APIs rather than bind to
  MAPI-specific execution state.

The implementation order remains: canonical object/property model, JMAP
projection, IMAP invariant gate, MAPI over HTTP EMSMDB core, NSPI, Microsoft
object family completion, notifications/reconnect, Web UI, then publication
gate and reassessment.
