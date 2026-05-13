# JMAP Mail

## Current State/Functionality Overview

`lpe-jmap` exposes JMAP Mail over canonical `LPE` mailbox state. It uses canonical draft, message, blob, submission, and push state and never bypasses `LPE-CT` for transport.

## Implementation/Usage

- Endpoints:
  - `GET /api/jmap/session`
  - `POST /api/jmap/api`
  - `POST /api/jmap/upload/{accountId}`
  - `GET /api/jmap/download/{accountId}/{blobId}/{name}`
  - `GET /api/jmap/ws`
  - `GET /api/jmap/events`
  - `GET /.well-known/jmap`
- Authentication:
  - mailbox login through `/api/mail/auth/login`
  - account-scoped JMAP session
- Supported methods:
  - `Mailbox/get`
  - `Mailbox/changes`
  - `Mailbox/query`
  - `Email/get`
  - `Email/query`
  - `Email/changes`
  - `Email/queryChanges`
  - `Email/set`
  - `Email/copy`
  - `Email/import`
  - `EmailSubmission/set`
  - `Blob/copy`
- Push:
  - WebSocket at `/api/jmap/ws`
  - event stream at `/api/jmap/events?types={types}&closeafter={closeafter}`
  - PostgreSQL `LISTEN` / `NOTIFY` wakes the adapter after canonical commits
  - owned and delegated mailboxes participate in canonical push state
- State:
  - `Email/changes`, `Thread/changes`, and `Mailbox/changes` carry the
    canonical mail change cursor in state tokens
  - `Email/queryChanges` and `Mailbox/queryChanges` store ordered query
    snapshots in `jmap_query_states` and expose only resumable query-state
    references to clients
- Mailboxes:
  - `Mailbox.parentId` hierarchy is persisted in canonical mailbox state and exposed through `Mailbox/get`, `Mailbox/query`, `Mailbox/changes`, and `Mailbox/set`
  - `Mailbox/set` validates that `parentId` belongs to the same mailbox account and rejects self-parenting or ancestor cycles
  - `isSubscribed` reflects canonical persisted subscription state in `mailbox_subscriptions`, shared with IMAP `SUBSCRIBE`, `UNSUBSCRIBE`, and `LSUB`
  - `Mailbox/set` preflights create and update `name` values through the shared mailbox-name policy before persistence, including NFC display normalization, reserved-name protection, `/` rejection inside JMAP names, and deterministic method-level `invalidArguments` errors for invalid names
  - canonical-equivalent duplicate rejection uses the shared mailbox canonical-key policy within the target parent, so siblings must be unique while the same leaf name may exist under different parents
  - standard mailbox names such as `INBOX`, `Sent`, and `Trash` remain canonical backend names; localized labels are client UI presentation driven by JMAP `role`, not stored as translated role identities
  - custom `Mailbox/set` create and update names must not collide with reserved role names or accepted IMAP compatibility aliases such as `INBOX`, `Sent Items`, `Deleted Items`, `Spam`, or `Archive`
- Submission:
  - `EmailSubmission/set` loads a persisted draft
  - canonical submission creates authoritative `Sent`
  - outbound relay stays in `LPE-CT`
- Upload/import:
  - uploaded files use canonical blob handling
  - external or client-provided files require validation
- Safety:
  - `Bcc` must not appear in standard search or user-facing projections
  - no JMAP-specific mailbox state engine
  - no direct `SMTP`

## Reference Table/List

| Surface | Path |
| --- | --- |
| session | `GET /api/jmap/session` |
| API | `POST /api/jmap/api` |
| upload | `POST /api/jmap/upload/{accountId}` |
| download | `GET /api/jmap/download/{accountId}/{blobId}/{name}` |
| WebSocket | `GET /api/jmap/ws` |
| event stream | `GET /api/jmap/events` |
| discovery | `GET /.well-known/jmap` |

| Capability | Canonical source |
| --- | --- |
| mailboxes | mailbox tables |
| messages | `messages`, `message_bodies`, recipients, blobs |
| drafts | `Drafts` mailbox messages |
| submission | `/api/mail/messages/submit` workflow |
| push state | canonical push journal |
