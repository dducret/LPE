# ActiveSync MVP

### Objective

This document describes the first `ActiveSync` adapter implemented in `LPE`.

The `crates/lpe-activesync` crate exposes a pragmatic subset of `Exchange ActiveSync` for mobile/native clients that actually support that protocol, such as Outlook mobile and iOS mail clients, without introducing any parallel `Sent`, `Outbox`, or direct `SMTP` bypass logic.

Outlook for Windows desktop is not an `ActiveSync` target and must not be forced to use this endpoint as an Exchange account. Until `EWS` or `MAPI` is implemented, Outlook for Windows desktop compatibility is handled through the `IMAP` adapter documented in `docs/architecture/imap-mvp.md`.

The concrete client interoperability matrix, prioritized defect risks, and automated test recommendations for this MVP are documented in `docs/architecture/activesync-interoperability-matrix.md`.

### Architectural principles

- the `ActiveSync` adapter is separated from `lpe-jmap` and from the business core
- authentication reuses the mailbox account already defined in `LPE`
- synchronization reads the canonical mailbox projection stored in `PostgreSQL`
- future task synchronization must reuse the canonical `tasks` model instead of introducing an `ActiveSync`-specific task store
- drafts reuse `save_draft_message` and `delete_draft_message`
- message submission reuses `submit_message`, which writes the authoritative `Sent` copy and then appends the `outbound_message_queue` row
- no `ActiveSync` endpoint performs direct Internet-facing `SMTP`
- `LPE-CT` remains the only component responsible for outbound `SMTP` relay
- `EWS` is not implemented

### Endpoints

- `OPTIONS /Microsoft-Server-ActiveSync`
- `POST /Microsoft-Server-ActiveSync`

Without the Debian reverse proxy, these routes are exposed directly by the Rust service.

With the documented Debian reverse proxy, they are published on `/Microsoft-Server-ActiveSync`. The historical `/api/Microsoft-Server-ActiveSync` path remains available only when a local front end still prefixes every upstream route under `/api/`.

### Authentication

- the MVP accepts mailbox-account `Basic` authentication
- mailbox `OAuth2` bearer access tokens are accepted through `Authorization: Bearer` when the token scope includes `activesync`
- existing mailbox bearer-session authentication is still supported for tests and internal integration
- there is no separate `ActiveSync` account model outside the normal `LPE` mailbox account

### Supported protocol commands

The MVP implements a focused `WBXML` codec for the code pages needed by the current scope, then supports:

- `Provision`
- `FolderSync`
- `Sync`
- `SendMail`
- `Search`
- `ItemOperations`
- `Ping`
- `SmartReply`
- `SmartForward`

`Sync` also tolerates multiple collections in the same request and accepts several common protocol options without failing even when the MVP does not yet implement their full semantics:

- `GetChanges`
- `DeletesAsMoves`
- `WindowSize`
- `Options`
- `BodyPreference`
- non-mutating draft-side `Fetch`

### Supported MVP scope

- account authentication
- minimal `Provision` flow with a lightweight device policy and `PolicyKey`
- base-folder synchronization for `Inbox`, `Sent`, and `Drafts`
- exposure of `Contacts` and `Calendar` collections
- message synchronization for `Inbox`, `Sent`, and `Drafts`
- same-tenant shared mailbox projection for delegated `Inbox`, `Sent`, and `Drafts`
- draft creation, update, and deletion through `Sync` on `Drafts`
- `Contacts` mutations through `Sync` on the `Contacts` collection
- `Calendar` mutations through `Sync` on the `Calendar` collection
- message submission through `SendMail`, wired to the canonical `LPE` submission workflow
- `SendMail` attachments validated through `Magika`, persisted through the canonical model, and exposed back through `Sync` + `ItemOperations`
- mailbox `Search` wired to the canonical `PostgreSQL` projection, including the v1 attachment text index already supported by `LPE`
- `ItemOperations` retrieval for messages and attachment payloads through canonical `FileReference` values
- `Ping` over synchronized folders by comparing current collection state against the device's latest persisted `SyncKey`
- `SmartReply` and `SmartForward` wired to canonical submission, reusing the canonical source message and forwarding source attachments when needed
- guarantee that a message sent from a native client becomes visible in the authoritative `Sent` view
- delegated mailbox submission through the same canonical sender-authorization model used by `JMAP`
- persistent `SyncKey` storage in `PostgreSQL` per account, device, and collection
- complete `Sync` pagination with `WindowSize` and `MoreAvailable`, including continuation of a server batch across multiple `SyncKey` values
- incremental `Sync` state tracking with compact per-item fingerprints instead of full serialized `ApplicationData` snapshots for large mailbox collections
- hardened `SendMail` parsing for native clients: folded headers, RFC 2047 encoded subjects and display names, `quoted-printable`, `base64`, and `multipart/alternative` text bodies

### Contacts and calendar

The MVP exposes `Contacts` and `Calendar` for downstream synchronization and now supports basic client-originated mutations through `Sync`:

- `Add`
- `Change`
- `Delete`

Those mutations still write directly into the canonical `contacts` and `calendar_events` models.

### Important rules

- `Sent` remains authoritative; `ActiveSync` does not write a parallel sent copy
- `SendMail` always finishes in canonical `LPE` submission
- canonical submission remains transactional: message stored, `Sent` copy written, outbound queue persisted, then relay delegated to `LPE-CT`
- delegated `SendMail`, `SmartReply`, and `SmartForward` resolve mailbox ownership and sender rights through canonical mailbox and sender grants
- if a delegated mailbox has only `send_on_behalf`, the adapter persists the authenticated account as `Sender`; if `send_as` is granted, the adapter may submit without a separate sender identity
- `Bcc` metadata is not reinjected into standard mailbox search
- the adapter does not reuse any `Stalwart` code

### Known limitations

- the MVP does not implement `EWS`
- the MVP does not expose the `Tasks` class yet; future task support must build on `docs/architecture/tasks-mvp.md`
- the `WBXML` parser is intentionally limited to the tags used by this MVP
- `Search` is intentionally limited to the canonical mailbox store; it does not cover `GAL`, `DocumentLibrary`, or richer search operators yet
- `ItemOperations` is currently limited to `Fetch` for messages and attachments; the other namespace operations are not exposed
- `Ping` is implemented as a lightweight delta detector against the device's latest persisted sync state; the MVP does not yet implement a sophisticated long-lived push loop
- `SmartReply` and `SmartForward` are targeted at the highest-priority Outlook/mobile flows; they reuse canonical submission and source-message data, but they do not yet cover every ComposeMail variant
- the `SendMail` `MIME` parser is still intentionally limited to MVP needs: it now covers common MIME attachments, but not the full MIME surface
- fine-grained client-originated mutation handling is currently focused on `Drafts`
- shared mailbox projection is currently limited to canonical mail folders; contacts and calendar continue to use their dedicated collaboration collections
- `Drafts` synchronization is targeted for `ActiveSync 16.1`; clients limited to older protocol versions should not be treated as fully supported for that capability
- the first `Sync` with `SyncKey = 0` uses a conservative priming round-trip before emitting the paged server changes; this is targeted for Outlook/mobile but has not yet been validated against the full diversity of `ActiveSync` clients
- `Sync` continuation is stabilized around a compact collection fingerprint set rather than persisted full payload snapshots; if an item targeted by an unfinished paged batch mutates before that page is emitted, the server may invalidate that continuation `SyncKey` and require a fresh sync instead of replaying a stale payload

### Current completion priorities

`ActiveSync` is the current flagship compatibility story for mobile/native clients that support `Exchange ActiveSync`.

The next phase must prioritize:

- structured Outlook mobile and iOS compatibility labs over additional protocol surface area
- `Ping` and long-poll stability so device refresh behavior is dependable under long-lived sessions
- `SendMail`, `SmartReply`, and `SmartForward` correctness so native-client submission always lands in canonical `Sent` without edge-case divergence
- `FolderSync` and `Sync` edge cases, especially around first sync, continuation, shared-mailbox behavior, and mixed folder collections


