# IMAP MVP

## Purpose

The first `IMAP` server in `LPE` is a mailbox compatibility adapter over the canonical mailbox model already used by `JMAP`, `ActiveSync`, and the web client.

It does not introduce a parallel mailbox store, a parallel sent-message workflow, or a protocol-specific draft model.

## Implemented scope

- account authentication through `LOGIN` with the existing mailbox account credentials
- `AUTHENTICATE XOAUTH2` with the mailbox `OAuth2` bearer access token
- `CAPABILITY`, `NOOP`, `LOGOUT`
- tolerant `ID`
- `NAMESPACE` for the flat personal namespace
- `LIST` for the canonical system mailboxes `Inbox`, `Sent`, and `Drafts`
- `SPECIAL-USE` folder flags on listed system mailboxes
- tolerant `LSUB`, `SUBSCRIBE`, and `UNSUBSCRIBE` for Outlook compatibility; subscription state is not persisted yet
- `STATUS` for mailbox counters and stable UID metadata
- flat mailbox management through `CREATE`, `RENAME`, and `DELETE` for custom user mailboxes
- `SELECT` on `Inbox`, `Sent`, and `Drafts`
- `FETCH` over canonical message state, including `ENVELOPE`, `BODYSTRUCTURE`,
  `BODY.PEEK[HEADER.FIELDS (...)]`, body sections, and partial literals
- minimal `STORE` for `\Seen` and `\Flagged`
- `IDLE` on a selected mailbox, with periodic refresh against canonical mailbox state
- `CONDSTORE` mailbox sync primitives: `HIGHESTMODSEQ`, per-message `MODSEQ`, and conditional `STORE` with `UNCHANGEDSINCE`
- `COPY` and `UID COPY` into `Inbox` or custom mailboxes
- `MOVE` and `UID MOVE` between `Inbox` and custom user mailboxes
- richer `SEARCH`
- `UID FETCH`, `UID STORE`, and `UID SEARCH`
- `APPEND` to `Drafts` only, persisted through the canonical draft workflow
- `UIDPLUS` response codes where the current canonical workflow can supply them directly
- `ACL` admin commands `GETACL`, `MYRIGHTS`, `LISTRIGHTS`, `SETACL`, and `DELETEACL` projected from canonical mailbox and sender delegation grants

## Canonical model alignment

- mailbox reads are served from the canonical `messages`, `message_bodies`, `message_recipients`, and protected `message_bcc_recipients` data already used by `JMAP` and `ActiveSync`
- `\Seen` maps to the canonical `unread` flag
- `\Flagged` maps to the canonical `flagged` flag
- `APPEND` to `Drafts` reuses `save_draft_message`
- `APPEND` returns `APPENDUID` using the canonical draft row written into `messages`
- custom IMAP mailbox creation and rename reuse the canonical mailbox records already exposed through `JMAP`
- `COPY` reuses canonical message-copy persistence and creates a new canonical message row in the target mailbox instead of introducing mailbox replication state
- `MOVE` reuses a canonical mailbox move on the existing message row, updates the target mailbox projection, and allocates a fresh destination `UID` so the destination mailbox still receives the moved message at the tail of its IMAP order
- `CONDSTORE` reuses a canonical account-level mail change watermark plus canonical per-message `imap_modseq` values stored on `messages`; the adapter does not maintain an `IMAP`-only sync journal
- `ACL` reuses canonical `mailbox_delegation_grants` and `sender_delegation_grants`; the adapter does not maintain a separate IMAP ACL store
- no `IMAP` path creates a parallel `Sent`, `Drafts`, or `Outbox`
- `Bcc` stays out of `SEARCH`; it is only rendered back in `Drafts` and `Sent` header reconstruction for the authenticated owner view

## File validation

`APPEND` validates MIME attachments with Google `Magika` before the draft is persisted, following the same architecture rule already applied to `JMAP` uploads and `ActiveSync` MIME submission.

## Current limitations

- no message submission or `APPEND` to `Sent`; outbound submission remains canonical through `JMAP`, `ActiveSync`, and the web/API submission workflow
- no durable subscribe state, hierarchy management, standalone `EXPUNGE`, `QRESYNC`, or SASL mechanisms other than `XOAUTH2`
- mailbox management remains a flat namespace for now; hierarchical folder trees are not implemented yet
- `FETCH BODYSTRUCTURE` and MIME section rendering are compatibility projections over the canonical message text and sanitized HTML fields; attachment MIME reserialization remains deferred
- `COPY` intentionally rejects `Sent` and `Drafts` as source or target mailboxes so the adapter cannot become an alternate sent-message or draft workflow
- `MOVE` uses the same guardrail and only supports `Inbox` plus custom user mailboxes
- `SEARCH` now supports `ALL`, `SEEN`, `UNSEEN`, `FLAGGED`, `UNFLAGGED`, `TEXT`, `SUBJECT`, `FROM`, `TO`, `CC`, `BODY`, `HEADER`, `BEFORE`, `ON`, `SINCE`, `LARGER`, `SMALLER`, `NOT`, `OR`, sequence-set criteria, and `UID`
- `IDLE` currently refreshes by polling canonical mailbox state for the selected mailbox; it now coexists with a reusable canonical mail change watermark, but still does not publish `QRESYNC`-grade vanished history
- the current `ACL` slice is administrative only for the authenticated owner mailbox namespace; delegated mailbox projection through IMAP remains deferred even though the grants are canonical today

## UID and sync tradeoffs

- `UIDVALIDITY` remains fixed at `1` for now because the adapter still sits on one canonical mailbox store rather than a dedicated per-mailbox synchronization engine
- message `UID`s come from the stable `messages.imap_uid` projection column and are globally monotonic for the account data set, not reallocated from a mailbox-local replication log
- `UIDNEXT` is derived from the highest currently visible message `UID` in the mailbox projection plus one; gaps are expected after copies or future deletions
- each canonical message now carries a stored `imap_modseq`, and each account carries a canonical mail `HIGHESTMODSEQ` watermark that advances on draft persistence, inbound delivery, copy, move, flag updates, and canonical draft deletion
- `HIGHESTMODSEQ` is account-scoped canonical mail state rather than an `IMAP`-local shadow counter, so it stays monotonic across mailbox-local deletions and moves; clients may therefore observe a mailbox `HIGHESTMODSEQ` advance because of other mail changes in the same account
- `COPY` returns a new canonical row with a new `UID` in the target mailbox; it does not create a shared multi-mailbox identity or hidden replication record
- `MOVE` updates the canonical message row in place but still assigns a new destination `UID`, preserving `UIDPLUS` mapping and keeping the destination mailbox append-like from an IMAP client perspective
- `FETCH MODSEQ` and `STORE ... (UNCHANGEDSINCE n)` operate directly on those canonical values; mixed conditional `STORE` batches may partially apply and return `MODIFIED` for the stale subset
- `IDLE` only reports selected-mailbox changes that can be observed from canonical mailbox refreshes, such as flag changes, additions, and removals
- because there is still no canonical vanished-history journal, standalone `EXPUNGE` and `QRESYNC` stay deferred even though `CONDSTORE` now uses canonical first-class change anchors
- `ACL` rights are a truthful projection over canonical delegation: mailbox access rights map to mailbox visibility and mutation, `p` maps to canonical `send-as`, and `b` is an `LPE`-specific right for canonical `send-on-behalf`
- `Bcc` remains protected in those tradeoffs as well: it is preserved in protected storage for owner reconstruction in `Drafts` and `Sent`, but never added to IMAP search matching

## Runtime

- the listener is started by `lpe-cli`
- the bind address is configured through `LPE_IMAP_BIND_ADDRESS`
- the default bind is `127.0.0.1:1143`
- public `IMAPS` on `993` is terminated by `LPE-CT`; in a split `DMZ` / `LAN`
  deployment, the core `LPE` listener must bind to a private LAN address such
  as `192.168.1.25:1143`, and firewall policy must allow only `LPE-CT` to
  reach that clear internal `IMAP` upstream

## Current completion priorities

Before expanding `IMAP` breadth, the current priority is to improve correctness and interoperability of the implemented slice:

- tighten sync correctness across `SELECT`, `FETCH`, `STORE`, `COPY`, `MOVE`, `IDLE`, and `CONDSTORE`
- validate that `UID`, `UIDNEXT`, `UIDVALIDITY`, and `MODSEQ` behavior remain coherent under realistic mailbox operations
- ensure flag handling stays consistent with the canonical message model across concurrent protocol activity
- add compatibility testing against common real-world `IMAP` clients and mailbox workflows instead of broadening protocol surface prematurely
