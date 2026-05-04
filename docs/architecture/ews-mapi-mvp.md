# EWS and MAPI MVP

### Objective

This document describes the `0.1.3` `Exchange` compatibility work in `LPE`.

The implementation is a deliberately scoped `EWS` adapter in `crates/lpe-exchange`. `IMAP` carried the initial desktop compatibility work through `0.1.2`; `0.1.3` moves the Exchange-style compatibility focus to `EWS`. Its goal is to let Exchange-style clients read and synchronize canonical mailbox, `Contacts`, and `Calendar` data from the `LPE` server without introducing a second collaboration or mailbox store.

`MAPI` implementation has started as a guarded `MAPI over HTTP` foundation for future Outlook desktop support. It is not Outlook-ready. `mapiHttp` autodiscover publication is available only through an explicit administrator interoperability-test switch. The current slice implements authenticated transport request classification, session-context cookies, and the first mailbox-folder bootstrap ROPs. Legacy `EXCH` / `EXPR` provider metadata for Outlook setup probes that do not yet send `X-MapiHttpCapability` requires a separate explicit interoperability-test switch; requests that do send that header receive the dedicated `mapiHttp` provider instead.

### Full-support boundary

For `LPE`, "full support" for Exchange compatibility means production-quality support for the client and interoperability surfaces that map cleanly onto the canonical `LPE` model. It does not mean becoming a complete Microsoft Exchange Server clone.

The intended supported surface is:

- `EWS` for mailbox folders, messages, contacts, calendars, tasks, attachments, search, availability, delegation discovery, and the common EWS client-library flows that can be backed by canonical `LPE` storage
- `MAPI over HTTP` for classic Outlook for Windows desktop profile creation, mailbox synchronization, cached-mode operation, address book lookup through `NSPI`, send and draft flows through canonical submission, attachments, delegated mailbox projection, and reconnect behavior
- autodiscover that publishes only the Exchange surfaces an administrator has explicitly enabled and that the interoperability matrix has proven
- mailbox `Basic`, mailbox app-password, and mailbox OAuth bearer authentication scoped through the existing mailbox-account model

The explicitly unsupported surface unless a later architecture document widens it is:

- Exchange administration APIs and Exchange control-plane compatibility
- public folders, archive mailbox parity, journaling, unified messaging, transport rules, litigation/eDiscovery parity, or Exchange Online service integration
- Outlook Anywhere, legacy RPC/HTTP, MAPI/RPC, POP-before-SMTP, or any direct client `SMTP` path inside the core `LPE` service
- cross-tenant directory or collaboration visibility
- Exchange-specific mailbox, contact, calendar, task, `Sent`, `Outbox`, GAL, or rights stores

This boundary is also the release gate. `EWS` can be administrator-published when its documented MVP limits are acceptable for a deployment. `MAPI over HTTP` remains an interoperability-test surface until Outlook desktop can create a profile, synchronize canonical mailbox state, resolve names through `NSPI`, send through canonical submission, reconnect after session loss, and keep the authoritative `Sent` view consistent.

### Milestone roadmap

1. Documentation and scope lock: resolve documentation divergence, define the support boundary above, and keep autodiscover publication gates explicit.
2. Exchange adapter foundation: harden shared SOAP/XML parsing, MAPI binary framing, error taxonomy, auth scopes, tenant isolation, structured logs, and golden protocol fixtures.
3. EWS mail and folder completion: add durable folder and item sync state with tombstones, message update/move/copy, soft and hard delete behavior, read-state and flag mutation, stable ids and change keys, and canonical `Sent` submission checks.
4. EWS attachments and protected metadata: implement attachment get/create/delete flows, MIME export, `Magika` validation for client-provided files, and protected `Bcc` handling that preserves compliance access without indexing or user-facing AI exposure.
5. EWS contacts, calendars, tasks, and availability: add recurrence, time zones, attendee response flows, meeting update/cancel behavior, reminders, free/busy, OOF, and tasks over canonical task storage.
6. EWS interoperability lab: gate publication with repeatable tests against EWS client libraries and real clients selected for the release, including day-two sync and mutation scenarios.
7. MAPI/HTTP transport and session correctness: harden HTTPS transport, common request/response framing, session-context cookies, handle lifetime, reconnect behavior, idle cleanup, request correlation, and `X-MapiHttpCapability` autodiscover behavior.
8. EMSMDB read/write mailbox store: implement the Outlook-critical ROPs for folder hierarchy, contents tables, property bags, body streams, message create/save/open, move/copy/delete, read state, flags, categories where mapped, recipient tables, attachments, named properties, and canonical change tracking.
9. EMSMDB sync and send semantics: implement Incremental Change Synchronization, notifications or pending event behavior, conflict handling, cached-mode friendliness, draft lifecycle, delegated send-as and send-on-behalf, and canonical `Sent` visibility.
10. NSPI and address book: implement tenant-scoped address list projection, stable entry IDs, legacy DN mapping, `ResolveNames`, `QueryRows`, `GetMatches`, restrictions, and property columns without a parallel GAL store.
11. Outlook desktop certification: verify classic Outlook for Windows desktop profile creation, first sync, day-two sync, cached mode, send/receive, attachments, delegated mailbox behavior, reconnects, and supported authentication modes before supported `mapiHttp` publication.
12. Operations, security, and support hardening: add Exchange-adapter dashboards and alerts, document supported clients and unsupported Exchange features, review any new dependencies against `LICENSE.md`, and keep public edge routing through `LPE-CT`.

### Architectural principles

- `JMAP` remains the primary modern protocol
- `ActiveSync` remains the flagship mobile/native-client layer for clients that actually support `Exchange ActiveSync`
- `EWS` is an adapter over canonical mailbox, `contacts`, and `calendar_events` storage
- `MAPI` is being introduced incrementally through `MAPI over HTTP`, starting with authenticated route and scope wiring plus opt-in autodiscover publication for testing
- `EWS` must not introduce parallel contact, calendar, mailbox, rights, `Sent`, or `Outbox` state
- `EWS` and `MAPI` authentication reuse mailbox-account authentication
- `EWS` must not perform or advertise `SMTP`; outbound transport remains in `LPE-CT`
- `MAPI` autodiscover publication must remain opt-in and clearly experimental until EMSMDB, NSPI, session context, and mailbox synchronization semantics are implemented against canonical `LPE` state; legacy `EXCH` / `EXPR` publication must remain separately opt-in so it cannot hijack Outlook desktop `IMAP` setup
- no `Stalwart` code is reused

### Endpoints

- `OPTIONS /EWS/Exchange.asmx`
- `POST /EWS/Exchange.asmx`
- `OPTIONS /ews/exchange.asmx`
- `POST /ews/exchange.asmx`

The lowercase path is accepted for tolerant reverse-proxy and client behavior. The canonical public path is `/EWS/Exchange.asmx`.

The first `MAPI over HTTP` implementation surface exists as authenticated transport and session wiring:

- `OPTIONS /mapi/emsmdb`
- `POST /mapi/emsmdb`
- `OPTIONS /mapi/nspi`
- `POST /mapi/nspi`

`/mapi/emsmdb` is reserved for mailbox ROP processing. `/mapi/nspi` is reserved for address book and name service provider interface behavior. `OPTIONS` returns the supported HTTP methods with `x-lpe-mapi-status: transport-session-ready`. `POST` requires mailbox authentication and `Content-Type: application/mapi-http`, and returns `application/mapi-http` responses with `X-RequestType`, `X-ResponseCode`, `X-RequestId`, and `X-ServerApplication`. Response bodies use the MAPI/HTTP common response framing, including the `PROCESSING` and `DONE` meta-tags before the request-specific binary response body, so strict Outlook and Remote Connectivity Analyzer clients do not parse raw binary as the transport envelope.

Implemented request types:

- EMSMDB `Connect`: creates an authenticated MAPI session context and returns an EMSMDB connect success body plus an implementation-owned session cookie
- EMSMDB `Disconnect`: consumes the authenticated EMSMDB session cookie and expires it
- NSPI `Bind`: creates an authenticated address book session context and returns an NSPI bind success body with a stable non-zero server GUID plus an implementation-owned session cookie
- NSPI `Unbind`: consumes the authenticated NSPI session cookie and expires it
- NSPI `GetAddressBookUrl`: returns the externally visible `/mapi/nspi/` URL using forwarded host/protocol headers when present
- NSPI `GetMailboxUrl`: returns the externally visible `/mapi/emsmdb/` URL using forwarded host/protocol headers when present
- NSPI `ResolveNames`: returns the authenticated mailbox identity as the first Check Name result, including display name, SMTP address, account minimal entry id, and legacy DN metadata
- NSPI bootstrap table and property requests `GetProps`, `GetPropList`, `QueryColumns`, `QueryRows`, `GetSpecialTable`, `GetMatches`, `SeekEntries`, `UpdateStat`, `DNToMId`, `CompareMIds`, `GetTemplateInfo`, and `ResortRestriction`: return protocol-success bootstrap data derived from the authenticated mailbox and a minimal Global Address List table so Outlook and Remote Connectivity Analyzer can continue past early address book probes without exposing a separate GAL store
- `PING`: returns a transport success response on either endpoint

`Execute` currently parses the EMSMDB Execute request body and validates the session cookie. It supports `RopRelease`, which intentionally emits no ROP response, the first private-mailbox `RopLogon` response with canonical account identity and fixed special-folder ids, `RopOpenFolder`, `RopOpenMessage`, `RopReadRecipients`, `RopGetHierarchyTable`, `RopGetContentsTable`, `RopSetColumns`, `RopQueryRows`, `RopGetPropertiesSpecific`, `RopGetPropertiesList`, `RopGetStatus`, `RopQueryPosition`, `RopResetTable`, `RopGetReceiveFolder`, `RopGetReceiveFolderTable`, and `RopGetStoreState` for early mailbox-store bootstrap. The implementation now keeps per-session ROP handle state and populates root and IPM-subtree hierarchy table rows from canonical JMAP mailbox folders, including display name, folder id, parent folder id, content count, unread count, and subfolder flags for the supported property tags. Contents tables expose the first read-only canonical message rows for a folder, including common subject, sender, recipient display, size, flags, attachment, message id, entry id, and instance-key columns. `RopOpenMessage` validates the requested message id against canonical mailbox data and creates a read-only message handle; `RopGetPropertiesSpecific` can return the same common message properties plus plain text body from that handle. `RopReadRecipients` returns read-only recipient rows for the canonical visible `To` and `Cc` recipients on an opened message. The current message-object slice is a bootstrap view only: rich body format negotiation, attachment tables, protected `Bcc` expansion, change synchronization, sorting, seeking, restrictions, and mutation ROPs are still not implemented. The common special-folder ids for `Inbox`, `Drafts`, `Sent`, and `Deleted` are also mapped back to canonical mailbox roles for opened-folder property reads, contents-table reads, message opens, and receive-folder discovery. Other ROPs currently return MAPI ROP error buffers with `MAPI_E_NO_SUPPORT`. NSPI responses remain a compatibility bootstrap only: they do not yet implement a real directory store, property restriction evaluation, or multi-recipient GAL search.

### Authentication

- mailbox-account `Basic` authentication is accepted
- mailbox `OAuth2` bearer access tokens are accepted through `Authorization: Bearer` when the token scope includes `ews`
- `MAPI over HTTP` bearer tokens require the separate `mapi` scope
- existing mailbox bearer-session authentication is accepted for internal integration and tests
- there is no separate Exchange account model outside the normal `LPE` mailbox account

### Supported EWS operations

The first `EWS` slice supports the read/sync surface needed to begin mailbox, contacts, and calendar interoperability:

- `FindFolder`
- `GetFolder`
- `SyncFolderHierarchy`
- `FindItem`
- `GetItem`
- `SyncFolderItems`
- `CreateItem` for `Message`, `Contact`, and `CalendarItem` items
- `UpdateItem` for canonical `Contact` and `CalendarItem` items
- `DeleteItem` for canonical `Message`, `Contact`, and `CalendarItem` ids
- `CreateFolder` and `DeleteFolder` for canonical custom mailbox folders

The adapter currently exposes:

- canonical owned and same-tenant shared contact collections as `Contacts` folders
- canonical owned and same-tenant shared calendar collections as `Calendar` folders
- contact items from `contacts`
- contact creation, update, and deletion through the canonical contacts model
- calendar items from `calendar_events`
- calendar item creation, update, and deletion through the canonical calendar model
- message creation through the canonical draft and submission model
- mailbox read and sync through the canonical JMAP mailbox model
- mailbox deletion through canonical hard-delete or move-to-trash behavior
- temporary/custom mailbox folder creation through the canonical JMAP mailbox model

The EWS distinguished folder ids `contacts` and `calendar` map to the canonical owned `default` contact and calendar collections. Shared collections keep explicit synthetic ids such as `shared-contacts-{owner_account_id}` and `shared-calendar-{owner_account_id}`.

The adapter returns a Basic authentication challenge for unauthenticated EWS requests and accepts `msgfolderroot` / `root` as lightweight root-folder discovery ids so clients can bootstrap folder traversal before requesting the supported contacts and calendar folders.
`SyncFolderItems` for contacts and calendar events includes deterministic item change keys in both item ids and a versioned server `SyncState`, derived from canonical item content plus the canonical row update marker.
This allows the adapter to return create, update, and delete changes without introducing an Exchange-specific collaboration store, including after bounded `UpdateItem` requests that touch EWS fields not represented as first-class LPE contact or calendar properties.
Legacy unversioned contact and calendar sync states from earlier `0.1.3` builds, including ID-only states and keyed states such as `contacts:default:{id}=ck-*`, are still accepted; matching current items are returned once as updates and the response advances the client to the `v2` change-key state format.

Folder responses include EWS `TotalCount` and `ChildFolderCount` properties so strict EWS clients can read requested folder properties during bootstrap. The current MVP returns conservative zero counts for these compatibility properties instead of deriving full mailbox-style counters for collaboration folders.

`CreateFolder` creates canonical custom mailbox folders, primarily for strict client connectivity tests that need temporary sync folders. `FindFolder` and `SyncFolderHierarchy` expose those custom mailbox folders. `DeleteFolder` removes those custom mailbox folders through the canonical JMAP mailbox deletion path, which rejects system folders and non-empty folders.

Mailbox folders, including system folders such as `Inbox`, `Drafts`, `Sent`, and `Deleted`, are exposed through canonical JMAP mailboxes. `FindItem`, `GetItem`, and `SyncFolderItems` return canonical messages from the requested mailbox. For temporary custom mailbox folders, `CreateItem SaveOnly` can import a `Message` into the requested canonical custom mailbox folder, so strict EWS connectivity tests can create, sync, read, delete, and resync items inside a temporary folder.

When a client requests unsupported distinguished folders such as `tasks` through this EWS adapter, the response remains an EWS-shaped `GetFolder` error with `ErrorFolderNotFound` instead of an HTTP transport failure. This keeps clients on the EWS negotiation path without advertising unsupported task synchronization through EWS.

The adapter also answers early client bootstrap probes for `GetServerTimeZones`, `ResolveNames`, and `GetUserAvailability`. `GetServerTimeZones` returns minimal `UTC` and `W. Europe Standard Time` definitions. `ResolveNames` returns an EWS no-results error because GAL resolution is not implemented. `GetUserAvailability` returns an EWS free/busy generation error because free/busy remains outside the current MVP.

`CreateItem` supports `Message`, `Contact`, and `CalendarItem` items. `Message` `SaveOnly` writes through the canonical Drafts path. `Message` `SendOnly` and `SendAndSaveCopy` write through the canonical submission path, which persists the canonical `Sent` copy before queueing outbound transport for `LPE-CT`. `Contact` creation writes to the requested canonical contacts collection, defaulting to the owned `default` address book. `CalendarItem` creation writes to the requested canonical calendar collection, defaulting to the owned `default` calendar. `CreateItem` does not implement task, attachment, meeting invitation, or folder writes.

Message ids returned by `CreateItem` and mail read operations are canonical mailbox ids wrapped in an EWS id prefix. Contact ids are canonical contact ids wrapped in the `contact:` EWS id prefix. Calendar item ids are canonical event ids wrapped in the `event:` EWS id prefix. `DeleteItem DeleteType="HardDelete"` permanently deletes the canonical message. `DeleteItem` without `HardDelete`, including `MoveToDeletedItems`, moves the canonical message to the `trash` mailbox when that mailbox exists; deleting a message that is already in `trash` permanently deletes it. `DeleteItem` for contact and event ids deletes through canonical collaboration rights and storage. This uses the same canonical mailbox and collaboration primitives as the other protocol layers and must not create EWS-only deletion state.

`UpdateItem` supports canonical `Contact` and `CalendarItem` ids and applies partial EWS field updates through canonical collaboration rights. Unsupported item ids, task updates, attachment mutations, and mailbox message updates return EWS-shaped `ErrorInvalidOperation` responses and must not mutate canonical data.

Other out-of-scope client bootstrap operations, including `GetUserOofSettings`, `GetRoomLists`, `FindPeople`, `ExpandDL`, `Subscribe`, `GetDelegate`, `GetUserConfiguration`, `GetSharingMetadata`, `GetSharingFolder`, `GetAttachment`, `Unsubscribe`, and `GetEvents`, also return EWS-shaped `ErrorInvalidOperation` responses instead of generic SOAP transport faults.

Any other unsupported EWS operation that can be identified as the first request element in the SOAP body also returns an operation-specific EWS-shaped `ErrorInvalidOperation` response. This keeps strict EWS client libraries from failing on transport faults or unknown response-code enum values while avoiding false success for unsupported mail, folder, rule, conversation, streaming, or conversion operations.

Request element names ending in `Request`, such as `GetUserOofSettingsRequest`, are normalized to their canonical operation name before response serialization so EWS Managed API clients receive the expected response element name.

### Current limitations

- `SyncFolderItems` uses compact server `SyncState` values over canonical item ids and deterministic change keys for contacts and calendar events; those keys include canonical update markers, but the adapter does not yet maintain a full EWS incremental change ledger with tombstone history beyond the previous client token
- `UpdateItem` is limited to contact and calendar field updates; message updates, attachment mutations, tasks, and meeting workflow updates are not implemented yet
- tasks, free/busy, recurrence expansion, alarms, meeting scheduling, extended properties, attachments, and GAL are not implemented through `EWS` yet
- autodiscover does not publish `EWS` by default; it is only published when explicitly enabled through `LPE_AUTOCONFIG_EWS_ENABLED`
- enabled `EWS` POX autodiscover publishes the configured EWS URL through a `WEB` protocol block with `ASUrl` for EWS-aware clients; top-level `EXCH` and `EXPR` provider sections remain reserved for explicit legacy Exchange autodiscover interoperability-test mode
- SOAP `GetUserSettings` autodiscover publishes the same configured `EWS` endpoint as `ExternalEwsUrl` and `InternalEwsUrl` for EWS clients that prefer SOAP autodiscover over POX
- `MAPI over HTTP` currently has authenticated transport, session-context wiring, a private-mailbox logon skeleton, read-only canonical mailbox-folder bootstrap ROPs, an initial read-only contents-table view over canonical message rows, read-only message open/property bootstrap, and visible recipient-row reads; it is not an Outlook-ready mailbox service and must advertise `mapiHttp` only when `LPE_AUTOCONFIG_MAPI_ENABLED` is explicitly enabled for interoperability testing, with legacy `EXCH` / `EXPR` provider sections requiring the additional `LPE_AUTOCONFIG_LEGACY_EXCHANGE_AUTODISCOVER_ENABLED` switch

### Completion priorities

The next EWS phase should focus on:

- real Outlook desktop compatibility testing for contacts and calendar discovery
- persistent incremental `SyncFolderItems` state over canonical contact and calendar change notifications
- deeper `UpdateItem` coverage for contacts and calendar events, routed through canonical collaboration rights

The next MAPI phase should focus on:

- complete the `EXHTTP` / `MapiHttp` autodiscover design and keep it behind `LPE_AUTOCONFIG_MAPI_ENABLED` until real Outlook login succeeds
- add attachment table bootstrap, body stream handling, protected sent-message `Bcc` handling, and synchronization ROPs over canonical mailbox data
- NSPI `GetSpecialTable`, `QueryRows`, `GetProps`, and `ResolveNames` without introducing a parallel GAL store
- binary protocol parsing and response serialization with focused conformance fixtures before any route is advertised to Outlook
