# EWS and MAPI MVP

### Objective

This document describes the `0.1.3` `Exchange` compatibility work in `LPE`.

The implementation is a deliberately scoped `EWS` adapter in `crates/lpe-exchange`. `IMAP` carried the initial desktop compatibility work through `0.1.2`; `0.1.3` moves the Exchange-style compatibility focus to `EWS`. Its goal is to let Exchange-style clients read and synchronize canonical mailbox, `Contacts`, and `Calendar` data from the `LPE` server without introducing a second collaboration or mailbox store.

`MAPI` implementation has started as a guarded `MAPI over HTTP` foundation for future Outlook desktop support. It is not Outlook-ready and is not advertised by autodiscover. The current slice implements authenticated transport request classification and session-context cookies for the first mailbox and address book request types.

### Architectural principles

- `JMAP` remains the primary modern protocol
- `ActiveSync` remains the flagship mobile/native-client layer for clients that actually support `Exchange ActiveSync`
- `EWS` is an adapter over canonical mailbox, `contacts`, and `calendar_events` storage
- `MAPI` is being introduced incrementally through `MAPI over HTTP`, starting with authenticated, non-advertised route and scope wiring
- `EWS` must not introduce parallel contact, calendar, mailbox, rights, `Sent`, or `Outbox` state
- `EWS` and `MAPI` authentication reuse mailbox-account authentication
- `EWS` must not perform or advertise `SMTP`; outbound transport remains in `LPE-CT`
- `MAPI` must not be advertised to Outlook until EMSMDB, NSPI, session context, and mailbox synchronization semantics are implemented against canonical `LPE` state
- no `Stalwart` code is reused

### Endpoints

- `OPTIONS /EWS/Exchange.asmx`
- `POST /EWS/Exchange.asmx`
- `OPTIONS /ews/exchange.asmx`
- `POST /ews/exchange.asmx`

The lowercase path is accepted for tolerant reverse-proxy and client behavior. The canonical public path is `/EWS/Exchange.asmx`.

The first `MAPI over HTTP` implementation surface exists only as non-advertised authenticated transport and session wiring:

- `OPTIONS /mapi/emsmdb`
- `POST /mapi/emsmdb`
- `OPTIONS /mapi/nspi`
- `POST /mapi/nspi`

`/mapi/emsmdb` is reserved for mailbox ROP processing. `/mapi/nspi` is reserved for address book and name service provider interface behavior. `OPTIONS` returns the supported HTTP methods with `x-lpe-mapi-status: transport-session-ready`. `POST` requires mailbox authentication and returns `application/mapi-http` responses with `X-RequestType`, `X-ResponseCode`, `X-RequestId`, and `X-ServerApplication`.

Implemented request types:

- EMSMDB `Connect`: creates an authenticated MAPI session context and returns an EMSMDB connect success body plus an implementation-owned session cookie
- EMSMDB `Disconnect`: consumes the authenticated EMSMDB session cookie and expires it
- NSPI `Bind`: creates an authenticated address book session context and returns an NSPI bind success body plus an implementation-owned session cookie
- NSPI `Unbind`: consumes the authenticated NSPI session cookie and expires it
- `PING`: returns a transport success response on either endpoint

`Execute` currently parses the EMSMDB Execute request body and validates the session cookie. It supports `RopRelease`, which intentionally emits no ROP response, and the first private-mailbox `RopLogon` response with canonical account identity and fixed special-folder ids. Other ROPs currently return MAPI ROP error buffers with `MAPI_E_NO_SUPPORT`. NSPI table/query operations are not implemented yet.

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
- `DeleteItem` for canonical `Message`, `Contact`, and `CalendarItem` ids
- `CreateFolder` and `DeleteFolder` for canonical custom mailbox folders

The adapter currently exposes:

- canonical owned and same-tenant shared contact collections as `Contacts` folders
- canonical owned and same-tenant shared calendar collections as `Calendar` folders
- contact items from `contacts`
- contact creation and deletion through the canonical contacts model
- calendar items from `calendar_events`
- calendar item creation and deletion through the canonical calendar model
- message creation through the canonical draft and submission model
- mailbox read and sync through the canonical JMAP mailbox model
- mailbox deletion through canonical hard-delete or move-to-trash behavior
- temporary/custom mailbox folder creation through the canonical JMAP mailbox model

The EWS distinguished folder ids `contacts` and `calendar` map to the canonical owned `default` contact and calendar collections. Shared collections keep explicit synthetic ids such as `shared-contacts-{owner_account_id}` and `shared-calendar-{owner_account_id}`.

The adapter returns a Basic authentication challenge for unauthenticated EWS requests and accepts `msgfolderroot` / `root` as lightweight root-folder discovery ids so clients can bootstrap folder traversal before requesting the supported contacts and calendar folders.

Folder responses include EWS `TotalCount` and `ChildFolderCount` properties so strict EWS clients can read requested folder properties during bootstrap. The current MVP returns conservative zero counts for these compatibility properties instead of deriving full mailbox-style counters for collaboration folders.

`CreateFolder` creates canonical custom mailbox folders, primarily for strict client connectivity tests that need temporary sync folders. `FindFolder` and `SyncFolderHierarchy` expose those custom mailbox folders. `DeleteFolder` removes those custom mailbox folders through the canonical JMAP mailbox deletion path, which rejects system folders and non-empty folders.

Mailbox folders, including system folders such as `Inbox`, `Drafts`, `Sent`, and `Deleted`, are exposed through canonical JMAP mailboxes. `FindItem`, `GetItem`, and `SyncFolderItems` return canonical messages from the requested mailbox. For temporary custom mailbox folders, `CreateItem SaveOnly` can import a `Message` into the requested canonical custom mailbox folder, so strict EWS connectivity tests can create, sync, read, delete, and resync items inside a temporary folder.

When a client requests unsupported distinguished folders such as `tasks` through this EWS adapter, the response remains an EWS-shaped `GetFolder` error with `ErrorFolderNotFound` instead of an HTTP transport failure. This keeps clients on the EWS negotiation path without advertising unsupported task synchronization through EWS.

The adapter also answers early client bootstrap probes for `GetServerTimeZones`, `ResolveNames`, and `GetUserAvailability`. `GetServerTimeZones` returns minimal `UTC` and `W. Europe Standard Time` definitions. `ResolveNames` returns an EWS no-results error because GAL resolution is not implemented. `GetUserAvailability` returns an EWS free/busy generation error because free/busy remains outside the current MVP.

`CreateItem` supports `Message`, `Contact`, and `CalendarItem` items. `Message` `SaveOnly` writes through the canonical Drafts path. `Message` `SendOnly` and `SendAndSaveCopy` write through the canonical submission path, which persists the canonical `Sent` copy before queueing outbound transport for `LPE-CT`. `Contact` creation writes to the requested canonical contacts collection, defaulting to the owned `default` address book. `CalendarItem` creation writes to the requested canonical calendar collection, defaulting to the owned `default` calendar. `CreateItem` does not implement task, attachment, meeting invitation, or folder writes.

Message ids returned by `CreateItem` and mail read operations are canonical mailbox ids wrapped in an EWS id prefix. Contact ids are canonical contact ids wrapped in the `contact:` EWS id prefix. Calendar item ids are canonical event ids wrapped in the `event:` EWS id prefix. `DeleteItem DeleteType="HardDelete"` permanently deletes the canonical message. `DeleteItem` without `HardDelete`, including `MoveToDeletedItems`, moves the canonical message to the `trash` mailbox when that mailbox exists; deleting a message that is already in `trash` permanently deletes it. `DeleteItem` for contact and event ids deletes through canonical collaboration rights and storage. This uses the same canonical mailbox and collaboration primitives as the other protocol layers and must not create EWS-only deletion state.

Write operations that are outside the current MVP, including `UpdateItem`, return EWS-shaped `ErrorInvalidOperation` responses. Those unsupported operations must not mutate canonical contacts or calendar data until write support is explicitly designed and routed through canonical collaboration rights.

Other out-of-scope client bootstrap operations, including `GetUserOofSettings`, `GetRoomLists`, `FindPeople`, `ExpandDL`, `Subscribe`, `GetDelegate`, `GetUserConfiguration`, `GetSharingMetadata`, `GetSharingFolder`, `GetAttachment`, `Unsubscribe`, and `GetEvents`, also return EWS-shaped `ErrorInvalidOperation` responses instead of generic SOAP transport faults.

Any other unsupported EWS operation that can be identified as the first request element in the SOAP body also returns an operation-specific EWS-shaped `ErrorInvalidOperation` response. This keeps strict EWS client libraries from failing on transport faults or unknown response-code enum values while avoiding false success for unsupported mail, folder, rule, conversation, streaming, or conversion operations.

Request element names ending in `Request`, such as `GetUserOofSettingsRequest`, are normalized to their canonical operation name before response serialization so EWS Managed API clients receive the expected response element name.

### Current limitations

- the first `SyncFolderItems` implementation returns a full create-style snapshot for the requested folder and a compact server `SyncState`; it does not yet maintain a full EWS incremental change ledger
- write operations such as `UpdateItem` are not implemented yet
- tasks, free/busy, recurrence expansion, alarms, meeting scheduling, extended properties, attachments, and GAL are not implemented through `EWS` yet
- autodiscover does not publish `EWS` by default; it is only published when explicitly enabled through `LPE_AUTOCONFIG_EWS_ENABLED`
- enabled `EWS` POX autodiscover publishes the configured EWS URL through a `WEB` protocol block with `ASUrl` for EWS-aware clients; it intentionally does not publish top-level `EXCH` or `EXPR` mailbox protocol blocks because those imply a full Outlook desktop Exchange/MAPI route that is not implemented
- SOAP `GetUserSettings` autodiscover publishes the same configured `EWS` endpoint as `ExternalEwsUrl` and `InternalEwsUrl` for EWS clients that prefer SOAP autodiscover over POX
- `MAPI over HTTP` currently has authenticated transport and session-context wiring only; it is not an Outlook-ready mailbox service and must not be advertised

### Completion priorities

The next EWS phase should focus on:

- real Outlook desktop compatibility testing for contacts and calendar discovery
- persistent incremental `SyncFolderItems` state over canonical contact and calendar change notifications
- `CreateItem`, `UpdateItem`, and `DeleteItem` for contacts and calendar events, routed through canonical collaboration rights

The next MAPI phase should focus on:

- autodiscover design for `EXHTTP` / `MapiHttp` that remains disabled until real Outlook login succeeds
- read-only mailbox ROPs after logon, starting with `RopOpenFolder`, `RopGetHierarchyTable`, `RopSetColumns`, `RopQueryRows`, and `RopGetPropertiesSpecific`
- NSPI `GetSpecialTable`, `QueryRows`, `GetProps`, and `ResolveNames` without introducing a parallel GAL store
- binary protocol parsing and response serialization with focused conformance fixtures before any route is advertised to Outlook
