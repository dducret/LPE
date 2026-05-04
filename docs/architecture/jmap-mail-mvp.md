# JMAP Mail MVP

### Objective

This document describes the `JMAP Mail` scope currently supported by `LPE` for the MVP.

`crates/lpe-jmap` acts as a `JMAP` adapter on top of the existing canonical `LPE` model implemented in `lpe-storage`. It does not create any parallel `Sent`, `Drafts`, `Outbox`, or transport-side `SMTP` logic.

### Authentication

- the `JMAP` client reuses the existing mailbox-account authentication
- login remains `/api/mail/auth/login`
- the existing account bearer token must then be sent to `/api/jmap/session`, `/api/jmap/api`, and `/api/jmap/ws`
- without the Debian reverse proxy, the same routes are exposed directly as `/jmap/session`, `/jmap/api`, and `/jmap/ws`
- the Debian reverse proxy sets `X-Forwarded-Prefix: /api/jmap` so the `Session` document advertises public `/api/jmap/*` URLs instead of direct internal `/jmap/*` URLs

### Supported session capabilities

- `urn:ietf:params:jmap:core`
- `urn:ietf:params:jmap:mail`
- `urn:ietf:params:jmap:submission`
- `urn:ietf:params:jmap:websocket`

The `JMAP` session is real: it is built from the authenticated mailbox account and exposes that current `LPE` account as the active `accountId`.
Its session `state` is derived from the current accessible mailbox-account projection, so mailbox delegation or sender-right changes that alter advertised accounts or account capabilities change the session document state.
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

- `POST /api/jmap/upload/{accountId}` for temporary `JMAP` blob upload; the `Session` core capability advertises the enforced `maxSizeUpload` and concurrent upload limit for real-client interoperability
- `GET /api/jmap/download/{accountId}/{blobId}/{name}` for temporary blob download
- `GET /api/jmap/ws` for `JMAP` over WebSocket with the `jmap` subprotocol

### Important MVP rules

- `Email/set` persists only in the `Drafts` mailbox
- `Email/set` accepts draft `keywords` for `$draft`, `$seen`, and `$flagged`; `$seen` and `$flagged` are mapped onto the canonical draft unread-flagged state without creating any parallel priority model
- `EmailSubmission/set` does not submit raw MIME or direct `SMTP`
- `EmailSubmission/set` takes an existing draft `emailId` and calls the canonical `LPE` submission workflow
- for delegated mailbox accounts, `EmailSubmission/set` is available only when canonical mailbox write access and sender delegation grants allow draft-backed submission
- canonical submission creates the authoritative copy in `Sent`, marks the message as `queued`, inserts an `outbound_message_queue` row, then removes the source draft
- `JMAP` object `state` values and WebSocket `StateChange` payloads are derived from the same canonical mailbox, message, contact, and calendar projections already stored in `PostgreSQL`
- `JMAP` object `state` tokens use opaque fingerprints of canonical projection data; protected or message-visible fields such as `Bcc`, subject, and body content must not be serialized directly into client-visible state tokens
- delegated/shared mailbox `Email` and `Thread` state fingerprints exclude `Bcc`, so Bcc-only changes do not create observable state or push changes for delegated readers; owned sender-side state may still track Bcc for the authenticated owner's own draft and sent-message sync
- the WebSocket transport is notification and request transport only; it does not introduce a second mailbox cache, event journal, or submission model
- canonical change signaling stays inside `PostgreSQL`: `lpe-storage` emits account-scoped `LISTEN` / `NOTIFY` payloads after canonical commits, and `lpe-jmap` recomputes only the affected `JMAP` state scopes from canonical tables
- mail push wakeups are expanded through canonical mailbox delegation so a change in a shared mailbox wakes both the owner session and delegated reader sessions without a protocol-local sharing cache
- shared mailbox `Session` account flags, `Mailbox/get` `myRights`, and delegated `Identity/get` values are projected from the canonical mailbox delegation plus sender delegation grants rather than adapter-local ACL state
- shared mailbox `Session` `accountCapabilities` advertise mail access, and advertise submission only when canonical mailbox write access and sender delegation grants both allow draft-backed submission; collaboration and task capabilities remain on the authenticated principal account rather than being copied onto delegated mailbox accounts
- `Mailbox/set`, `Email/set`, `Email/copy`, `Email/import`, and draft-backed `EmailSubmission/set` reject writes when the selected shared mailbox account is not writable according to canonical mailbox delegation `may_write`; `EmailSubmission/set` also passes the authenticated submitter into the canonical sender-delegation check instead of trusting draft-local metadata
- shared `Drafts` writes are advertised and accepted only when mailbox write access and canonical sender delegation are both present, so draft creation, copy, and import cannot drift from the later canonical submission authorization check
- `Bcc` remains stored separately in `message_bcc_recipients`
- `Bcc` is not reinjected into search, `participants_normalized`, or `Email/query`
- `Email/get` may return `bcc` only when the `bcc` property is explicitly requested for the authenticated account's own sender-side draft or sent message

### Accepted MVP limitations

- `Email/query` and `Thread/query` support only descending `receivedAt` sort
- `Email/query` supports only the `inMailbox` filter
- `Email/queryChanges` and `Mailbox/queryChanges` use a stateless snapshot `queryState` token derived from the ordered result set instead of a durable per-query history table
- `queryChanges` compares the full ordered result set for the logical query even when the original `query` response was paginated, and is intended for incremental client refresh, not for long-lived durable sync cursors
- `changes` accepts `sinceState: "0"` as the explicit initial-sync state and otherwise requires an opaque state token bound to the requested account and method; malformed, cross-account, or cross-method tokens are rejected instead of being treated as a fresh initial sync
- `Email/get` exposes a practical subset of `JMAP Mail` properties
- one `LPE` email currently belongs to one `LPE` mailbox, so `mailboxIds` contains one entry
- `EmailSubmission/set` currently supports only `create`
- `EmailSubmission/set` expects an existing draft through `emailId` or a resolved creation reference in the same request
- `Identity/get` exposes the standard MVP fields plus `LPE`-specific delegated-sender metadata for clients that request it
- `Mailbox/set` cannot modify or delete system mailboxes (`Inbox`, `Sent`, `Drafts`, etc.)
- `Email/copy` currently supports only same-account copy
- `Email/import` consumes a validated `message/rfc822` blob, extracts visible multipart text with plaintext preference, preserves a first HTML body when available, validates each imported attachment with `Magika`, trims structural multipart boundary line endings from imported attachment bytes, and imports multipart attachments into the canonical attachment pipeline
- `Blob/upload` is supported both through the core HTTP upload endpoint and through the `urn:ietf:params:jmap:blob` inline method; both paths validate the resulting bytes with `Magika` and store temporary upload blobs in `PostgreSQL`
- message `blobId` values now expose the canonical `mime_blob_ref` shape when one already exists, including `upload:{uuid}` for imported MIME uploads, and fall back to stable `message:{emailId}` identifiers for canonical message downloads
- `GET /api/jmap/download/{accountId}/{blobId}/{name}` can return temporary upload blobs and reconstructed `message/rfc822` downloads for canonical message blob IDs; delegated/shared mailbox downloads never include protected `Bcc` metadata
- `Blob/copy` copies readable upload or canonical message blobs into the destination account's temporary upload blob store; it does not introduce a separate durable blob store
- `Blob/get` returns requested byte ranges from readable temporary upload blobs or reconstructed canonical message blobs; non-owner delegated/shared mailbox reads use the same Bcc-safe reconstruction path as HTTP download
- `Blob/lookup` is limited to canonical mail references for `Mailbox`, `Thread`, and `Email`; it never exposes inaccessible blobs and does not inspect unrelated collaboration stores or introduce a separate blob-reference index
- the session keeps `eventSourceUrl` empty; this MVP uses `JMAP` over WebSocket rather than the older event-source transport
- WebSocket push uses canonical `PostgreSQL` signaling end to end: `lpe-storage` writes a canonical change-journal row and emits principal-filtered `LISTEN` / `NOTIFY` wakeups after canonical commits, while `lpe-jmap` replays bounded missed reconnect work from that journal and recomputes only the affected canonical object states without introducing a second mailbox state engine
- when reconnect recovery or a live push wakeup sees the canonical journal cursor advance without a subscribed object-state fingerprint change, `lpe-jmap` may emit a `StateChange` with an empty `changed` map so clients receive a fresher `pushState` cursor and avoid unnecessary future full-snapshot fallbacks
- mail push state spans every mailbox account visible through canonical mailbox delegation so one authenticated session can receive `StateChange` payloads for owned and delegated mailboxes without a protocol-local sharing cache
- collaboration and task push stay principal-scoped: shared contacts, calendars, and task lists notify every affected principal account, while mailbox push still spans the canonical owner plus delegated mailbox readers
- supported push data types are limited to `Mailbox`, `Email`, `Thread`, `AddressBook`, `ContactCard`, `Calendar`, `CalendarEvent`, `TaskList`, and `Task`
- `VacationResponse/get` and `VacationResponse/set` project the authenticated account's canonical active `Sieve` script; `set` writes a bounded `jmap-vacation` Sieve script or disables the active script and does not introduce a separate `JMAP` vacation store

### Next methods to add

- journal retention, pruning, and resumable push cursors beyond the current bounded reconnect-replay window for very large mailbox counts

### Current completion priorities

Before broadening `JMAP` method surface further, the current priority is to finish protocol depth and interoperability:

- complete canonical `state`, `changes`, and `queryChanges` behavior so refresh and resync semantics stay coherent under concurrent mailbox operations
- harden WebSocket reliability, including wakeup delivery, reconnect behavior, principal filtering, and delegated-mailbox push consistency
- validate mailbox delegation and shared collection behavior so `Session`, `Mailbox`, `Identity`, and push views stay aligned
- add interoperability tests against real `JMAP` clients and keep those tests focused on canonical-state correctness rather than synthetic method-count growth


