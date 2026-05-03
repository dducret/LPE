# EWS and MAPI MVP

### Objective

This document describes the first `Exchange` compatibility work in `LPE`.

The initial implementation is a deliberately narrow `EWS` adapter in `crates/lpe-exchange`. Its first goal is to let Exchange-style clients read and synchronize canonical `Contacts` and `Calendar` data from the `LPE` server without introducing a second collaboration store.

`MAPI` is not implemented in this phase.

### Architectural principles

- `JMAP` remains the primary modern protocol
- `ActiveSync` remains the flagship mobile/native-client layer for clients that actually support `Exchange ActiveSync`
- `EWS` is an adapter over canonical `contacts` and `calendar_events`
- `MAPI` remains deferred because it is a separate, much larger protocol surface
- `EWS` must not introduce parallel contact, calendar, mailbox, rights, `Sent`, or `Outbox` state
- `EWS` authentication reuses mailbox-account authentication
- `EWS` must not perform or advertise `SMTP`; outbound transport remains in `LPE-CT`
- no `Stalwart` code is reused

### Endpoints

- `OPTIONS /EWS/Exchange.asmx`
- `POST /EWS/Exchange.asmx`
- `OPTIONS /ews/exchange.asmx`
- `POST /ews/exchange.asmx`

The lowercase path is accepted for tolerant reverse-proxy and client behavior. The canonical public path is `/EWS/Exchange.asmx`.

### Authentication

- mailbox-account `Basic` authentication is accepted
- mailbox `OAuth2` bearer access tokens are accepted through `Authorization: Bearer` when the token scope includes `ews`
- existing mailbox bearer-session authentication is accepted for internal integration and tests
- there is no separate Exchange account model outside the normal `LPE` mailbox account

### Supported EWS operations

The first `EWS` slice supports only the collaboration read/sync surface needed to begin contacts and calendar interoperability:

- `FindFolder`
- `GetFolder`
- `SyncFolderHierarchy`
- `FindItem`
- `GetItem`
- `SyncFolderItems`

The adapter currently exposes:

- canonical owned and same-tenant shared contact collections as `Contacts` folders
- canonical owned and same-tenant shared calendar collections as `Calendar` folders
- contact items from `contacts`
- calendar items from `calendar_events`

The EWS distinguished folder ids `contacts` and `calendar` map to the canonical owned `default` contact and calendar collections. Shared collections keep explicit synthetic ids such as `shared-contacts-{owner_account_id}` and `shared-calendar-{owner_account_id}`.

The adapter returns a Basic authentication challenge for unauthenticated EWS requests and accepts `msgfolderroot` / `root` as lightweight root-folder discovery ids so clients can bootstrap folder traversal before requesting the supported contacts and calendar folders.

When a client requests unsupported distinguished folders such as `inbox` or `tasks` through this narrow EWS adapter, the response remains an EWS-shaped `GetFolder` error with `ErrorFolderNotFound` instead of an HTTP transport failure. This keeps clients on the EWS negotiation path without advertising unsupported mail or task synchronization through EWS.

The adapter also answers early client bootstrap probes for `GetServerTimeZones`, `ResolveNames`, and `GetUserAvailability`. `GetServerTimeZones` returns minimal `UTC` and `W. Europe Standard Time` definitions. `ResolveNames` returns an EWS no-results error because GAL resolution is not implemented. `GetUserAvailability` returns an EWS free/busy generation error because free/busy remains outside the current MVP.

### Current limitations

- the first `SyncFolderItems` implementation returns a full create-style snapshot for the requested folder and a compact server `SyncState`; it does not yet maintain a full EWS incremental change ledger
- write operations such as `CreateItem`, `UpdateItem`, and `DeleteItem` are not implemented yet
- mail, tasks, free/busy, recurrence expansion, alarms, meeting scheduling, extended properties, attachments, and GAL are not implemented through `EWS` yet
- autodiscover does not publish `EWS` by default; it is only published when explicitly enabled through `LPE_AUTOCONFIG_EWS_ENABLED`
- enabled `EWS` autodiscover publishes `EXCH` and `EXPR` protocol blocks only as discovery containers for the configured `EwsUrl` / `EmwsUrl`; this does not add `MAPI`, `RPC`, mail, submission, or outbox support
- SOAP `GetUserSettings` autodiscover publishes the same configured `EWS` endpoint as `ExternalEwsUrl` and `InternalEwsUrl` for EWS clients that prefer SOAP autodiscover over POX
- `MAPI` is not implemented and must not be advertised

### Completion priorities

The next EWS phase should focus on:

- real Outlook desktop compatibility testing for contacts and calendar discovery
- persistent incremental `SyncFolderItems` state over canonical contact and calendar change notifications
- `CreateItem`, `UpdateItem`, and `DeleteItem` for contacts and calendar events, routed through canonical collaboration rights
- explicit documentation before any mail or MAPI surface is introduced
