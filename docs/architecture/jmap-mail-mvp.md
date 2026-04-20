# JMAP Mail MVP

### Objective

This document describes the `JMAP Mail` scope currently supported by `LPE` for the MVP.

`crates/lpe-jmap` acts as a `JMAP` adapter on top of the existing canonical `LPE` model implemented in `lpe-storage`. It does not create any parallel `Sent`, `Drafts`, `Outbox`, or transport-side `SMTP` logic.

### Authentication

- the `JMAP` client reuses the existing mailbox-account authentication
- login remains `/api/mail/auth/login`
- the existing account bearer token must then be sent to `/api/jmap/session`, `/api/jmap/api`, and `/api/jmap/ws`
- without the Debian reverse proxy, the same routes are exposed directly as `/jmap/session`, `/jmap/api`, and `/jmap/ws`

### Supported session capabilities

- `urn:ietf:params:jmap:core`
- `urn:ietf:params:jmap:mail`
- `urn:ietf:params:jmap:submission`
- `urn:ietf:params:jmap:websocket`

The `JMAP` session is real: it is built from the authenticated mailbox account and exposes that current `LPE` account as the active `accountId`.
The WebSocket capability is advertised only when the `/jmap/ws` endpoint is actually present in the running adapter.

### Supported methods

- `Mailbox/get`
- `Mailbox/query`
- `Mailbox/queryChanges`
- `Mailbox/changes`
- `Mailbox/set`
- `Email/query`
- `Email/queryChanges`
- `Email/get`
- `Email/changes`
- `Email/set` for draft creation, update, and deletion
- `Email/copy`
- `Email/import`
- `EmailSubmission/get`
- `EmailSubmission/set` for draft submission through the canonical `LPE` submission model
- `Identity/get`
- `Thread/query`
- `Thread/get`
- `Thread/changes`
- `Quota/get`
- `SearchSnippet/get`

Additional supported `JMAP` routes:

- `POST /api/jmap/upload/{accountId}` for temporary `JMAP` blob upload
- `GET /api/jmap/download/{accountId}/{blobId}/{name}` for temporary blob download
- `GET /api/jmap/ws` for `JMAP` over WebSocket with the `jmap` subprotocol

### Important MVP rules

- `Email/set` persists only in the `Drafts` mailbox
- `Email/set` accepts draft `keywords` for `$draft`, `$seen`, and `$flagged`; `$seen` and `$flagged` are mapped onto the canonical draft unread-flagged state without creating any parallel priority model
- `EmailSubmission/set` does not submit raw MIME or direct `SMTP`
- `EmailSubmission/set` takes an existing draft `emailId` and calls the canonical `LPE` submission workflow
- canonical submission creates the authoritative copy in `Sent`, marks the message as `queued`, inserts an `outbound_message_queue` row, then removes the source draft
- `JMAP` object `state` values and WebSocket `StateChange` payloads are derived from the same canonical mailbox, message, contact, and calendar projections already stored in `PostgreSQL`
- the WebSocket transport is notification and request transport only; it does not introduce a second mailbox cache, event journal, or submission model
- `Bcc` remains stored separately in `message_bcc_recipients`
- `Bcc` is not reinjected into search, `participants_normalized`, or `Email/query`
- `Email/get` may return `bcc` only when the `bcc` property is explicitly requested for the authenticated account's own sender-side draft or sent message

### Accepted MVP limitations

- `Email/query` and `Thread/query` support only descending `receivedAt` sort
- `Email/query` supports only the `inMailbox` filter
- `Email/queryChanges` and `Mailbox/queryChanges` use a stateless snapshot `queryState` token derived from the ordered result set instead of a durable per-query history table
- `queryChanges` compares the full ordered result set for the logical query even when the original `query` response was paginated, and is intended for incremental client refresh, not for long-lived durable sync cursors
- `Email/get` exposes a practical subset of `JMAP Mail` properties
- one `LPE` email currently belongs to one `LPE` mailbox, so `mailboxIds` contains one entry
- `EmailSubmission/set` currently supports only `create`
- `EmailSubmission/set` expects an existing draft through `emailId` or a resolved creation reference in the same request
- `Mailbox/set` cannot modify or delete system mailboxes (`Inbox`, `Sent`, `Drafts`, etc.)
- `Email/copy` currently supports only same-account copy
- `Email/import` consumes a validated `message/rfc822` blob, extracts visible multipart text with plaintext preference, preserves a first HTML body when available, validates each imported attachment with `Magika`, trims structural multipart boundary line endings from imported attachment bytes, and imports multipart attachments into the canonical attachment pipeline
- `Blob/upload` currently stores temporary upload blobs in `PostgreSQL`
- message `blobId` values now expose the canonical `mime_blob_ref` shape when one already exists, including `upload:{uuid}` for imported MIME uploads, and fall back to adapter-scoped opaque identifiers for messages that do not yet expose a persistent downloadable MIME blob
- no `JMAP Blob/get`, blob copy, or persistent message download contract is advertised yet; the current blob model is intentionally limited to uploaded-imported MIME reuse and internal canonical references
- the session keeps `eventSourceUrl` empty; this MVP uses `JMAP` over WebSocket rather than the older event-source transport
- WebSocket push uses `PostgreSQL` `LISTEN` / `NOTIFY` to wake the adapter on canonical storage commits, then recomputes canonical object states from `PostgreSQL` instead of introducing a durable push-history table or a second state database
- mail push state spans every mailbox account visible through canonical mailbox delegation so one authenticated session can receive `StateChange` payloads for owned and delegated mailboxes without a protocol-local sharing cache
- supported push data types are limited to `Mailbox`, `Email`, `Thread`, `AddressBook`, `ContactCard`, `Calendar`, `CalendarEvent`, `TaskList`, and `Task`

### Next methods to add

- `Blob/copy`
- `VacationResponse/get`
- persistent message-blob retrieval beyond temporary uploaded blobs
- durable server-side push cursors beyond the current per-connection database wakeup model for very large mailbox counts


