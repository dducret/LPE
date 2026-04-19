# IMAP MVP

## Purpose

The first `IMAP` server in `LPE` is a mailbox compatibility adapter over the canonical mailbox model already used by `JMAP`, `ActiveSync`, and the web client.

It does not introduce a parallel mailbox store, a parallel sent-message workflow, or a protocol-specific draft model.

## Implemented scope

- account authentication through `LOGIN` with the existing mailbox account credentials
- `CAPABILITY`, `NOOP`, `LOGOUT`
- `LIST` for the canonical system mailboxes `Inbox`, `Sent`, and `Drafts`
- `SELECT` on `Inbox`, `Sent`, and `Drafts`
- minimal `FETCH` over canonical message state
- minimal `STORE` for `\Seen` and `\Flagged`
- minimal `SEARCH`
- `UID FETCH`, `UID STORE`, and `UID SEARCH`
- `APPEND` to `Drafts` only, persisted through the canonical draft workflow

## Canonical model alignment

- mailbox reads are served from the canonical `messages`, `message_bodies`, `message_recipients`, and protected `message_bcc_recipients` data already used by `JMAP` and `ActiveSync`
- `\Seen` maps to the canonical `unread` flag
- `\Flagged` maps to the canonical `flagged` flag
- `APPEND` to `Drafts` reuses `save_draft_message`
- no `IMAP` path creates a parallel `Sent`, `Drafts`, or `Outbox`
- `Bcc` stays out of `SEARCH`; it is only rendered back in `Drafts` and `Sent` header reconstruction for the authenticated owner view

## File validation

`APPEND` validates MIME attachments with Google `Magika` before the draft is persisted, following the same architecture rule already applied to `JMAP` uploads and `ActiveSync` MIME submission.

## Current limitations

- no message submission or `APPEND` to `Sent`; outbound submission remains canonical through `JMAP`, `ActiveSync`, and the web/API submission workflow
- no mailbox creation, rename, delete, subscribe, hierarchy management, `COPY`, `MOVE`, `EXPUNGE`, `IDLE`, `STATUS`, `NAMESPACE`, or `AUTHENTICATE`
- only the `Inbox`, `Sent`, and `Drafts` system mailboxes are exposed by the MVP
- the supported `FETCH` body sections are limited to header, text body, and reconstructed full message body without attachment MIME reserialization
- `SEARCH` supports only a minimal subset: `ALL`, `SEEN`, `UNSEEN`, `FLAGGED`, `UNFLAGGED`, `TEXT`, `SUBJECT`, `FROM`, and `TO`
- `UIDVALIDITY` is fixed for the MVP and message `UID`s come from a stable numeric projection column, not from a per-mailbox replication model yet

## Runtime

- the listener is started by `lpe-cli`
- the bind address is configured through `LPE_IMAP_BIND_ADDRESS`
- the default bind is `127.0.0.1:1143`
