# IMAP MVP

## Purpose

The first `IMAP` server in `LPE` is a mailbox compatibility adapter over the canonical mailbox model already used by `JMAP`, `ActiveSync`, and the web client.

It does not introduce a parallel mailbox store, a parallel sent-message workflow, or a protocol-specific draft model.

## Implemented scope

- account authentication through `LOGIN` with the existing mailbox account credentials
- `AUTHENTICATE XOAUTH2` with the mailbox `OAuth2` bearer access token
- `CAPABILITY`, `NOOP`, `LOGOUT`
- `LIST` for the canonical system mailboxes `Inbox`, `Sent`, and `Drafts`
- `STATUS` for mailbox counters and stable UID metadata
- flat mailbox management through `CREATE`, `RENAME`, and `DELETE` for custom user mailboxes
- `SELECT` on `Inbox`, `Sent`, and `Drafts`
- minimal `FETCH` over canonical message state
- minimal `STORE` for `\Seen` and `\Flagged`
- `COPY` and `UID COPY` into `Inbox` or custom mailboxes
- richer `SEARCH`
- `UID FETCH`, `UID STORE`, and `UID SEARCH`
- `APPEND` to `Drafts` only, persisted through the canonical draft workflow

## Canonical model alignment

- mailbox reads are served from the canonical `messages`, `message_bodies`, `message_recipients`, and protected `message_bcc_recipients` data already used by `JMAP` and `ActiveSync`
- `\Seen` maps to the canonical `unread` flag
- `\Flagged` maps to the canonical `flagged` flag
- `APPEND` to `Drafts` reuses `save_draft_message`
- custom IMAP mailbox creation and rename reuse the canonical mailbox records already exposed through `JMAP`
- `COPY` reuses canonical message-copy persistence and creates a new canonical message row in the target mailbox instead of introducing mailbox replication state
- no `IMAP` path creates a parallel `Sent`, `Drafts`, or `Outbox`
- `Bcc` stays out of `SEARCH`; it is only rendered back in `Drafts` and `Sent` header reconstruction for the authenticated owner view

## File validation

`APPEND` validates MIME attachments with Google `Magika` before the draft is persisted, following the same architecture rule already applied to `JMAP` uploads and `ActiveSync` MIME submission.

## Current limitations

- no message submission or `APPEND` to `Sent`; outbound submission remains canonical through `JMAP`, `ActiveSync`, and the web/API submission workflow
- no subscribe state, hierarchy management, `MOVE`, `EXPUNGE`, `IDLE`, `NAMESPACE`, or SASL mechanisms other than `XOAUTH2`
- mailbox management remains a flat namespace for now; hierarchical folder trees are not implemented yet
- the supported `FETCH` body sections are limited to header, text body, and reconstructed full message body without attachment MIME reserialization
- `COPY` intentionally rejects `Sent` and `Drafts` as source or target mailboxes so the adapter cannot become an alternate sent-message or draft workflow
- `SEARCH` now supports `ALL`, `SEEN`, `UNSEEN`, `FLAGGED`, `UNFLAGGED`, `TEXT`, `SUBJECT`, `FROM`, `TO`, `CC`, `BODY`, `HEADER`, `BEFORE`, `ON`, `SINCE`, `LARGER`, `SMALLER`, `NOT`, `OR`, sequence-set criteria, and `UID`

## UID and sync tradeoffs

- `UIDVALIDITY` remains fixed at `1` for now because the adapter still sits on one canonical mailbox store rather than a dedicated per-mailbox synchronization engine
- message `UID`s come from the stable `messages.imap_uid` projection column and are globally monotonic for the account data set, not reallocated from a mailbox-local replication log
- `UIDNEXT` is derived from the highest currently visible message `UID` in the mailbox projection plus one; gaps are expected after copies or future deletions
- `COPY` returns a new canonical row with a new `UID` in the target mailbox; it does not create a shared multi-mailbox identity or hidden replication record
- because there is no dedicated IMAP sync state yet, `IDLE`, `MOVE`, and `EXPUNGE` stay deferred until delete and change-notification semantics are promoted to first-class canonical operations
- `Bcc` remains protected in those tradeoffs as well: it is preserved in protected storage for owner reconstruction in `Drafts` and `Sent`, but never added to IMAP search matching

## Runtime

- the listener is started by `lpe-cli`
- the bind address is configured through `LPE_IMAP_BIND_ADDRESS`
- the default bind is `127.0.0.1:1143`
