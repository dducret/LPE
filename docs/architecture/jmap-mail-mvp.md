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
