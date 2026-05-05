# EWS and MAPI MVP

### Objective

This document describes the `0.1.3` `Exchange` compatibility work in `LPE`.

The implementation is a deliberately scoped `EWS` adapter in `crates/lpe-exchange`. `IMAP` carried the initial desktop compatibility work through `0.1.2`; `0.1.3` moves the Exchange-style compatibility focus to `EWS`. Its goal is to let Exchange-style clients read and synchronize canonical mailbox, `Contacts`, `Calendar`, and `Tasks` data from the `LPE` server without introducing a second collaboration or mailbox store.

`MAPI` implementation is the `0.1.3` release path for full classic Outlook for Windows Exchange-account support over `MAPI over HTTP`. `mapiHttp` autodiscover publication is available only through an explicit administrator switch until the Outlook interoperability matrix proves profile creation, first sync, day-two sync, cached mode, `NSPI`, send, reconnect, and canonical `Sent` behavior. The current slice implements authenticated transport request classification, session-context cookies, and the first mailbox-folder bootstrap ROPs. Legacy `EXCH` / `EXPR` provider metadata for Outlook setup probes that do not yet send `X-MapiHttpCapability` requires a separate explicit interoperability-test switch and an explicitly published EWS or MAPI surface; requests that do send that header receive the dedicated `mapiHttp` provider instead.

The repeatable `EWS` live smoke and release-gate checks are tracked in `docs/architecture/ews-interoperability-matrix.md`.

### Full-support boundary

For `LPE`, "full support" for Exchange and Outlook compatibility is an explicit project goal. It means production-quality support for the client and interoperability surfaces that map cleanly onto the canonical `LPE` model. It does not mean becoming a complete Microsoft Exchange Server clone.

The intended supported surface is:

- `EWS` for mailbox folders, messages, contacts, calendars, tasks, attachments, search, availability, delegation discovery, and the common EWS client-library flows that can be backed by canonical `LPE` storage
- `MAPI over HTTP` for classic Outlook for Windows desktop profile creation, mailbox synchronization, cached-mode operation, address book lookup through `NSPI`, send and draft flows through canonical submission, attachments, delegated mailbox projection, and reconnect behavior
- autodiscover that publishes only the Exchange surfaces an administrator has explicitly enabled and that the interoperability matrix has proven, including legacy `EXCH` / `EXPR` provider metadata for RCA only when the legacy Exchange autodiscover switch is enabled
- mailbox `Basic`, mailbox app-password, and mailbox OAuth bearer authentication scoped through the existing mailbox-account model

The explicitly unsupported surface unless a later architecture document widens it is:

- Exchange administration APIs and Exchange control-plane compatibility
- public folders, archive mailbox parity, journaling, unified messaging, transport rules, litigation/eDiscovery parity, or Exchange Online service integration
- Outlook Anywhere, legacy RPC/HTTP, MAPI/RPC, POP-before-SMTP, or any direct client `SMTP` path inside the core `LPE` service
- cross-tenant directory or collaboration visibility
- Exchange-specific mailbox, contact, calendar, task, `Sent`, `Outbox`, GAL, or rights stores

This boundary is also the release gate. `EWS` can be administrator-published when its documented MVP limits are acceptable for a deployment. Full classic Outlook support in `0.1.3` requires `MAPI over HTTP` to create an Outlook profile, synchronize canonical mailbox state, resolve names through `NSPI`, send through canonical submission, reconnect after session loss, and keep the authoritative `Sent` view consistent.

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
- `MAPI` is the `0.1.3` full classic Outlook path through `MAPI over HTTP`, built incrementally but release-gated on real Outlook profile creation and day-two use
- `EWS` must not introduce parallel contact, calendar, mailbox, rights, `Sent`, or `Outbox` state
- `EWS` and `MAPI` authentication reuse mailbox-account authentication
- `EWS` must not perform or advertise `SMTP`; outbound transport remains in `LPE-CT`
- `MAPI` autodiscover publication must remain explicit until EMSMDB, NSPI, session context, and mailbox synchronization semantics are implemented against canonical `LPE` state and proven with Outlook; legacy `EXCH` / `EXPR` publication must remain separately opt-in so it cannot hijack Outlook desktop `IMAP` setup
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

`/mapi/emsmdb` is reserved for mailbox ROP processing. `/mapi/nspi` is reserved for address book and name service provider interface behavior. `OPTIONS` returns the supported HTTP methods with `x-lpe-mapi-status: transport-session-ready`. `POST` requires mailbox authentication and accepts `Content-Type: application/mapi-http`; it also accepts `application/octet-stream` for Outlook and Remote Connectivity Analyzer MAPI/HTTP probes such as `NSPI Bind`. Responses always use `application/mapi-http` with `X-RequestType`, `X-ResponseCode`, `X-RequestId`, `X-ServerApplication`, `X-ExpirationInfo`, and `X-PendingPeriod`, and they echo `X-ClientInfo` when the client sends it. Client-supplied non-empty `X-RequestId` values are echoed for diagnostics; if a client omits the header, `LPE` generates a non-zero per-request UUID instead of reusing a placeholder. Response bodies use the MAPI/HTTP common response framing, including the `PROCESSING` and `DONE` meta-tags before the request-specific binary response body, so strict Outlook and Remote Connectivity Analyzer clients do not parse raw binary as the transport envelope. For `0.1.3`, this endpoint pair must progress from bootstrap behavior to complete Outlook profile creation, mailbox synchronization, address book lookup, send/draft support, reconnect behavior, and cached-mode day-two usage.

For RCA and Outlook desktop diagnosis, the adapter emits `rca_debug=true`
structured log events through the normal `tracing` pipeline when `RUST_LOG`
allows `info` logs. Autodiscover logs include the response kind and mailbox
hint. EWS logs include the SOAP operation and EWS response code when available.
MAPI/HTTP logs include the endpoint, `X-RequestType`, `X-RequestId`,
`client-request-id`, `X-ClientInfo`, `X-ClientApplication`, response code, and
request/response payload sizes where available. These logs are operational
diagnostics only; they must not include authorization headers, credentials, or
request bodies by default. `LPE_RCA_DEBUG_PAYLOAD_PREVIEW_BYTES` can temporarily
enable a capped hexadecimal preview of MAPI/HTTP request and response payloads
for strict wire-format diagnosis; it is disabled by default and should be unset
after the RCA run because address-book payloads can contain mailbox or lookup
values. Microsoft RCA "Correlation ID" values are generated by RCA and are not
guaranteed to appear in inbound requests, so operators should correlate by
timestamp plus Outlook request identifiers.

Implemented request types:

- EMSMDB `Connect`: creates an authenticated MAPI session context and returns an EMSMDB connect success body plus an implementation-owned, HTTP-only, secure session cookie with a bounded `Max-Age`; server-side session state is pruned with the same idle lifetime
- EMSMDB `Disconnect`: consumes the authenticated EMSMDB session cookie and expires it
- NSPI `Bind`: creates an authenticated address book session context and returns an NSPI bind success body with a stable non-zero server GUID plus an implementation-owned, HTTP-only, secure session cookie with a bounded `Max-Age`; server-side session state is pruned with the same idle lifetime
- NSPI `Unbind`: consumes the authenticated NSPI session cookie and expires it
- NSPI `GetAddressBookUrl`: returns the externally visible `/mapi/nspi/` URL using forwarded host/protocol headers when present
- NSPI `GetMailboxUrl`: returns the externally visible `/mapi/emsmdb/` URL using forwarded host/protocol headers when present
- NSPI `ResolveNames`: returns the authenticated mailbox identity as the first Check Name result, reports the NSPI ANR marker `MID_RESOLVED` for the matched input name, honors the caller's requested address-book property columns when supplied, and falls back to the bootstrap display name, SMTP address, account minimal entry id, and legacy metadata columns otherwise
- NSPI bootstrap table and property requests `GetProps`, `GetPropList`, `QueryColumns`, `QueryRows`, `GetSpecialTable`, `GetMatches`, `SeekEntries`, `UpdateStat`, `DNToMId`, `CompareMIds`, `GetTemplateInfo`, and `ResortRestriction`: return protocol-success bootstrap data derived from the authenticated mailbox and a minimal Global Address List table so Outlook and Remote Connectivity Analyzer can continue past early address book probes without exposing a separate GAL store
- `PING`: returns a transport success response on either endpoint

`Execute` currently parses the EMSMDB Execute request body and validates the session cookie. It accepts both the legacy bare ROP payload used by the local compatibility tests and the `RPC_HEADER_EXT`-wrapped ROP payload used by Outlook/RCA, returning the same wrapped form when the request used it. It supports `RopRelease`, which intentionally emits no ROP response, the first private-mailbox `RopLogon` response with canonical account identity and fixed special-folder ids, `RopOpenFolder`, `RopOpenMessage`, `RopReadRecipients`, `RopGetHierarchyTable`, `RopGetContentsTable`, `RopSetColumns`, `RopQueryRows`, `RopGetPropertiesSpecific`, `RopGetPropertiesList`, `RopGetStatus`, `RopQueryPosition`, `RopResetTable`, `RopGetReceiveFolder`, `RopGetReceiveFolderTable`, and `RopGetStoreState` for early mailbox-store bootstrap. The implementation now keeps per-session ROP handle state and populates root and IPM-subtree hierarchy table rows from canonical JMAP mailbox folders, including display name, folder id, parent folder id, content count, unread count, and subfolder flags for the supported property tags. Contents tables expose the first read-only canonical message rows for a folder, including common subject, sender, recipient display, size, flags, attachment, message id, entry id, and instance-key columns. `RopOpenMessage` validates the requested message id against canonical mailbox data and creates a read-only message handle; `RopGetPropertiesSpecific` can return the same common message properties plus plain text body from that handle. `RopReadRecipients` returns read-only recipient rows for the canonical visible `To` and `Cc` recipients on an opened message. The current message-object slice is a bootstrap view only: rich body format negotiation, attachment tables, protected `Bcc` expansion, change synchronization, sorting, seeking, restrictions, and mutation ROPs are still not implemented. The common special-folder ids for `Inbox`, `Drafts`, `Sent`, and `Deleted` are also mapped back to canonical mailbox roles for opened-folder property reads, contents-table reads, message opens, and receive-folder discovery. Other ROPs currently return MAPI ROP error buffers with `MAPI_E_NO_SUPPORT`. NSPI responses remain a compatibility bootstrap only: they do not yet implement a real directory store, property restriction evaluation, or multi-recipient GAL search.

### Authentication

- mailbox-account `Basic` authentication is accepted
- mailbox `OAuth2` bearer access tokens are accepted through `Authorization: Bearer` when the token scope includes `ews`
- `MAPI over HTTP` bearer tokens require the separate `mapi` scope
- existing mailbox bearer-session authentication is accepted for internal integration and tests
- there is no separate Exchange account model outside the normal `LPE` mailbox account

### Supported EWS operations

The first `EWS` slice supports the read/sync surface needed to begin mailbox, contacts, calendar, and task interoperability:

- `FindFolder`
- `GetFolder`
- `SyncFolderHierarchy`
- `FindItem`
- `GetItem`
- `SyncFolderItems`
- `CreateItem` for `Message`, `Contact`, and `CalendarItem` items
- `UpdateItem` for canonical `Contact`, `CalendarItem`, and read/flag `Message` fields
- `DeleteItem` for canonical `Message`, `Contact`, `CalendarItem`, and `Task` ids
- `MoveItem` for canonical `Message` ids and canonical mailbox target folders
- `CopyItem` for canonical `Message` ids and canonical mailbox target folders
- `CreateFolder` and `DeleteFolder` for canonical custom mailbox folders
- `GetAttachment`, `CreateAttachment`, and `DeleteAttachment` for canonical message file attachments

The adapter currently exposes:

- canonical owned and same-tenant shared contact collections as `Contacts` folders
- canonical owned and same-tenant shared calendar collections as `Calendar` folders
- canonical owned and same-tenant shared task lists as `Tasks` folders
- contact items from `contacts`
- contact creation, update, and deletion through the canonical contacts model
- calendar items from `calendar_events`
- calendar item creation, update, and deletion through the canonical calendar model
- task items from canonical task storage
- task creation, update, and deletion through the canonical task model
- message creation through the canonical draft and submission model
- mailbox read and sync through the canonical JMAP mailbox model
- message attachment discovery and retrieval through canonical attachment rows and blob storage
- mailbox deletion through canonical hard-delete or move-to-trash behavior
- temporary/custom mailbox folder creation through the canonical JMAP mailbox model

The EWS distinguished folder ids `contacts`, `calendar`, and `tasks` map to the canonical owned `default` contact, calendar, and task collections. Shared collections keep explicit synthetic ids such as `shared-contacts-{owner_account_id}`, `shared-calendar-{owner_account_id}`, and `shared-tasks-{owner_account_id}`.

The adapter returns a Basic authentication challenge for unauthenticated EWS requests and accepts `msgfolderroot` / `root` as lightweight root-folder discovery ids so clients can bootstrap folder traversal before requesting the supported contacts, calendar, and task folders.
`SyncFolderItems` for contacts, calendar events, and tasks includes deterministic item change keys in both item ids and a versioned server `SyncState`, derived from canonical item content plus the canonical row update marker.
This allows the adapter to return create, update, and delete changes without introducing an Exchange-specific collaboration store, including after bounded `UpdateItem` requests that touch EWS fields not represented as first-class LPE contact or calendar properties.
Legacy unversioned contact and calendar sync states from earlier `0.1.3` builds, including ID-only states and keyed states such as `contacts:default:{id}=ck-*`, are still accepted; matching current items are returned once as updates and the response advances the client to the `v2` change-key state format.

Folder responses include EWS `TotalCount` and `ChildFolderCount` properties so strict EWS clients can read requested folder properties during bootstrap. The current MVP returns conservative zero counts for these compatibility properties instead of deriving full mailbox-style counters for collaboration folders.

`CreateFolder` creates canonical custom mailbox folders, primarily for strict client connectivity tests that need temporary sync folders. `FindFolder` and `SyncFolderHierarchy` expose those custom mailbox folders. `DeleteFolder` removes those custom mailbox folders through the canonical JMAP mailbox deletion path, which rejects system folders and non-empty folders.

Mailbox folders, including system folders such as `Inbox`, `Drafts`, `Sent`, and `Deleted`, are exposed through canonical JMAP mailboxes. `FindItem`, `GetItem`, and `SyncFolderItems` return canonical messages from the requested mailbox. When a message has canonical attachments, `GetItem` includes EWS `FileAttachment` references backed by canonical attachment ids, requested EWS `MimeContent` is reconstructed from canonical message, recipient, and attachment state, `GetAttachment` returns the stored blob content for those references, `CreateAttachment` validates client-provided file attachments with `Magika` before routing them through canonical attachment ingestion, and `DeleteAttachment` removes matching canonical attachment rows while updating message attachment state and search metadata. EWS MIME reconstruction follows the existing protected metadata rule: `Bcc` is included only for canonical `Drafts` and `Sent` messages, and is not exposed for normal mailbox reads. For temporary custom mailbox folders, `CreateItem SaveOnly` can import a `Message` into the requested canonical custom mailbox folder, so strict EWS connectivity tests can create, sync, read, delete, and resync items inside a temporary folder.

Task folders expose canonical task-list contents for `FindItem`, `GetItem`, `CreateItem`, `UpdateItem`, `DeleteItem`, and `SyncFolderItems`. The current task item mapping is intentionally narrow: id, parent folder, subject, text body, status, due date, completion date, and deterministic change key. Task create and update operations write through canonical task storage and canonical task-list rights. They do not introduce an EWS task store.

The adapter also answers early client bootstrap probes for `GetServerTimeZones`, `ResolveNames`, `GetUserAvailability`, `GetUserOofSettings`, and `SetUserOofSettings`. `GetServerTimeZones` returns minimal `UTC` and `W. Europe Standard Time` definitions. `ResolveNames` resolves only the authenticated mailbox when the unresolved entry matches that mailbox's display name or SMTP address; all other names still return an EWS no-results error until tenant address-book lookup and sharing policy are implemented. `GetUserAvailability` returns read-only busy blocks for the authenticated mailbox from canonical calendar events and the requested time window; availability requests for other mailboxes still return an EWS free/busy generation error until tenant address-book and sharing policy are implemented. `GetUserOofSettings` projects the authenticated account's canonical active `Sieve` vacation response as EWS OOF settings; malformed or non-vacation active scripts are reported as disabled rather than creating an Exchange-only OOF store. `SetUserOofSettings` maps enabled and disabled OOF states to the same canonical `Sieve` vacation script model used by JMAP; scheduled OOF remains unsupported until canonical scheduling fields exist.

`CreateItem` supports `Message`, `Contact`, `CalendarItem`, and `Task` items. `Message` `SaveOnly` writes through the canonical Drafts path. `Message` `SendOnly` and `SendAndSaveCopy` write through the canonical submission path, which persists the canonical `Sent` copy before queueing outbound transport for `LPE-CT`. `Contact` creation writes to the requested canonical contacts collection, defaulting to the owned `default` address book. `CalendarItem` creation writes to the requested canonical calendar collection, defaulting to the owned `default` calendar, preserves simple daily, weekly, absolute monthly, and absolute yearly EWS recurrence patterns as the canonical raw `RRULE`, and stores required/optional attendee identity plus accepted/tentative/declined/no-response status in canonical calendar participant metadata. `Task` creation writes to the requested canonical task list, defaulting to the owned default task list. `CreateItem` does not implement attachment creation embedded in the item payload, meeting invitation, or folder writes.

Message ids returned by `CreateItem` and mail read operations are canonical mailbox ids wrapped in an EWS id prefix. Contact ids are canonical contact ids wrapped in the `contact:` EWS id prefix. Calendar item ids are canonical event ids wrapped in the `event:` EWS id prefix. Task ids are canonical task ids wrapped in the `task:` EWS id prefix. `DeleteItem DeleteType="HardDelete"` permanently deletes the canonical message. `DeleteItem` without `HardDelete`, including `MoveToDeletedItems`, moves the canonical message to the `trash` mailbox when that mailbox exists; deleting a message that is already in `trash` permanently deletes it. `MoveItem` and `CopyItem` accept canonical `message:` ids and a single canonical mailbox target, either as `FolderId Id="mailbox:{uuid}"` or a supported distinguished mailbox folder such as `inbox`, `drafts`, `sentitems`, or `deleteditems`. `CopyItem` duplicates the canonical body, recipients, protected `Bcc` metadata, and attachment rows through the canonical mailbox copy primitive. `DeleteItem` for contact, event, and task ids deletes through canonical collaboration/task rights and storage. This uses the same canonical mailbox, collaboration, and task primitives as the other protocol layers and must not create EWS-only deletion, move, or copy state.

`UpdateItem` supports canonical `Contact`, `CalendarItem`, and `Task` ids and applies partial EWS field updates through canonical collaboration and task-list rights. Calendar updates can replace or delete the preserved raw recurrence rule for the same simple recurrence subset supported by `CreateItem`, and can replace the stored attendee metadata when the update carries required or optional attendee collections. Calendar reads render the stored attendee metadata back as EWS required and optional attendees. For canonical `Message` ids, `UpdateItem` is limited to `IsRead` and `FlagStatus`, mapping those fields to the canonical `unread` and `flagged` mailbox fields with normal mail-change emission. Unsupported item ids, attachment mutations, and other mailbox message updates return EWS-shaped `ErrorInvalidOperation` responses and must not mutate canonical data.

Other out-of-scope client bootstrap operations, including `GetRoomLists`, `FindPeople`, `ExpandDL`, `Subscribe`, `GetDelegate`, `GetUserConfiguration`, `GetSharingMetadata`, `GetSharingFolder`, `Unsubscribe`, and `GetEvents`, also return EWS-shaped `ErrorInvalidOperation` responses instead of generic SOAP transport faults.

Any other unsupported EWS operation that can be identified as the first request element in the SOAP body also returns an operation-specific EWS-shaped `ErrorInvalidOperation` response. This keeps strict EWS client libraries from failing on transport faults or unknown response-code enum values while avoiding false success for unsupported mail, folder, rule, conversation, streaming, or conversion operations.

Request element names ending in `Request`, such as `GetUserOofSettingsRequest`, are normalized to their canonical operation name before response serialization so EWS Managed API clients receive the expected response element name.

### Current limitations

- `SyncFolderItems` uses compact server `SyncState` values over canonical item ids and deterministic change keys for contacts, calendar events, and tasks; those keys include canonical update markers, but the adapter does not yet maintain a full EWS incremental change ledger with tombstone history beyond the previous client token
- `UpdateItem` message support is limited to read-state and flag mutation; attachment mutations and meeting workflow updates are not implemented yet
- EWS attachment support is limited to file attachments over `GetItem`, `GetAttachment`, `CreateAttachment`, and `DeleteAttachment`; item attachments, inline attachment metadata, attachment creation through `CreateItem` / `UpdateItem`, and byte-for-byte original RFC822 source replay are not implemented yet
- cross-mailbox free/busy, recurrence expansion, recurrence exceptions, detached recurrence instances, alarms, meeting scheduling, extended properties, and GAL are not implemented through `EWS` yet
- autodiscover does not publish `EWS` by default; it is only published when explicitly enabled through `LPE_AUTOCONFIG_EWS_ENABLED`
- enabled `EWS` POX autodiscover publishes the configured EWS URL through a `WEB` protocol block with `ASUrl` for EWS-aware clients; top-level `EXCH` and `EXPR` provider sections remain reserved for explicit legacy Exchange autodiscover interoperability-test mode and can be published for RCA validation by combining `LPE_AUTOCONFIG_EWS_ENABLED=true` with `LPE_AUTOCONFIG_LEGACY_EXCHANGE_AUTODISCOVER_ENABLED=true`
- SOAP `GetUserSettings` autodiscover publishes the same configured `EWS` endpoint as `ExternalEwsUrl` and `InternalEwsUrl` for EWS clients that prefer SOAP autodiscover over POX
- `MAPI over HTTP` currently has authenticated transport, session-context wiring, a private-mailbox logon skeleton, read-only canonical mailbox-folder bootstrap ROPs, an initial read-only contents-table view over canonical message rows, read-only message open/property bootstrap, and visible recipient-row reads; the remaining `0.1.3` work is to make this an Outlook-ready mailbox service before supported `mapiHttp` publication, with legacy `EXCH` / `EXPR` provider sections requiring the additional `LPE_AUTOCONFIG_LEGACY_EXCHANGE_AUTODISCOVER_ENABLED` switch

### Completion priorities

The next EWS phase should focus on:

- real Outlook desktop compatibility testing for contacts and calendar discovery
- persistent incremental `SyncFolderItems` state over canonical contact and calendar change notifications
- deeper `UpdateItem` coverage for contacts and calendar events, routed through canonical collaboration rights

The `0.1.3` MAPI/Outlook completion phase should focus on:

- complete the `EXHTTP` / `MapiHttp` autodiscover design and keep supported publication behind `LPE_AUTOCONFIG_MAPI_ENABLED` plus successful real Outlook login and sync testing
- add attachment table bootstrap, body stream handling, protected sent-message `Bcc` handling, and synchronization ROPs over canonical mailbox data
- NSPI `GetSpecialTable`, `QueryRows`, `GetProps`, and `ResolveNames` without introducing a parallel GAL store
- binary protocol parsing and response serialization with focused conformance fixtures before the route is documented as supported for Outlook
