# IMAP and JMAP Implementation Matrix

## Current State/Functionality Overview

This matrix classifies IMAP and JMAP work by product priority. It is an
implementation planning document, not a replacement for the protocol RFCs.

`MUST`, `SHOULD`, `COULD`, and `IGNORE` in this document are LPE priority
labels:

- `MUST`: required for the protocol surface to be advertised as supported.
- `SHOULD`: expected for broad client interoperability after the required path is
  stable.
- `COULD`: useful only after the canonical implementation and tests are mature.
- `IGNORE`: intentionally out of scope unless architecture is explicitly revised.

All protocol adapters use canonical LPE mailbox, message, draft, sent,
submission, rights, search, contact, calendar, task, blob, and sync state. They
must not introduce protocol-local canonical stores, search indexes, `Sent`,
`Drafts`, or `Outbox` behavior.

IMAP and JMAP discovery are scoped to their own implemented endpoints. Publishing
either surface must not promote Exchange-style MAPI autodiscover, which remains a
separate opt-in compatibility gate.

## IMAP

IMAP is a mailbox compatibility layer. It exposes mail folders, messages,
headers, bodies, flags, and synchronization behavior over canonical LPE mailbox
state. It is not a collaboration, calendar, contact, task, or send protocol.

| Priority | Function, task, or data | LPE requirement |
| --- | --- | --- |
| `MUST` | Authentication and TLS policy | Authenticate mailbox users before mailbox access; publish only configured TLS endpoints; reject anonymous or guest mailbox access. |
| `MUST` | `CAPABILITY` accuracy | Advertise only implemented IMAP commands and extensions. Capability changes must track behavior and tests. |
| `MUST` | Client discovery boundary | IMAP autoconfiguration may remain the Outlook desktop fallback path while Exchange-style MAPI autodiscover stays gated; IMAP discovery must publish only an implemented and exposed `LPE-CT` IMAPS endpoint, must not point clients at the private core `LPE` IMAP listener, and must not imply a client `SMTP` endpoint or MAPI publication. |
| `MUST` | Mailbox discovery | `LIST`, `XLIST` compatibility where needed, `STATUS`, and special mailbox aliases must map to canonical folders. |
| `MUST` | Mailbox selection | `SELECT` and `EXAMINE` must expose stable `UIDVALIDITY`, `UIDNEXT`, message counts, recent state, and mod-sequence metadata where advertised. |
| `MUST` | Message read | `FETCH` and `UID FETCH` must read canonical message metadata, headers, bodies, HTML/plain body variants, MIME structure, flags, dates, sizes, byte lengths, and partial body ranges. Other compatibility protocols, including MAPI body streams and stream-size probes, must derive message bodies from the same canonical content. |
| `MUST` | Flag mutation | `STORE` and `UID STORE` must update canonical read, unread, flagged, answered, draft, and deleted state. |
| `MUST` | Message import | `APPEND` must create canonical messages or drafts in the target mailbox and use the canonical validation path for client-provided content. |
| `MUST` | Copy, move, and delete | `COPY`, `UID COPY`, move flows, `EXPUNGE`, `UID EXPUNGE`, `CLOSE`, and trash aliases must mutate canonical mailbox state without protocol-local trash or sent state. A move is source membership expunge plus target membership creation: the source `mailbox_messages` row keeps its original UID and is tombstoned, while the target row receives a new UID from the target mailbox. |
| `MUST` | Search | `SEARCH` and `UID SEARCH` must use canonical indexed fields and must not expose or match protected `Bcc` metadata in user-visible search. |
| `MUST` | Refresh and long-lived sessions | `NOOP`, `CHECK`, `IDLE`, selected-mailbox refresh, and reconnect behavior must observe canonical changes without stale duplicate state. |
| `MUST` | Conditional sync if advertised | `CONDSTORE` and `QRESYNC` state must come from canonical change state and must reject invalid or stale tokens predictably. |
| `MUST` | Client transcripts | Outlook, Thunderbird, and other target-client transcripts must cover first login, folder list, select, refresh, partial fetch, delete, draft, and large mailbox flows. |
| `SHOULD` | `SPECIAL-USE` and folder role compatibility | Prefer standard special-use roles while keeping compatibility aliases where real clients require them. |
| `SHOULD` | `MOVE` and `UID MOVE` | Support native move commands when exposed, backed by the same canonical source-expunge plus target-membership mutation path as copy/delete flows. `COPYUID`/move responses and QRESYNC replay must use the target UID for the new membership and the source UID for expunge/tombstone replay. |
| `SHOULD` | `SORT`, `THREAD`, and `ESEARCH` | Implement through canonical query/search primitives when needed by target clients. |
| `SHOULD` | Quota and mailbox size probes | Report canonical quota and size data for clients that probe setup or account state. |
| `SHOULD` | Robust MIME/bodystructure handling | Preserve MIME fidelity for nested multiparts, attachments, charsets, inline content, and partial body fetches. |
| `SHOULD` | ACL and shared mailbox visibility | Expose only canonical rights-backed shared mailboxes; do not create IMAP-local rights data. |
| `COULD` | `COMPRESS=DEFLATE` | Add only after license review and transport tests show meaningful benefit. |
| `COULD` | `NOTIFY`, `OBJECTID`, `SAVEDATE`, `PREVIEW`, and `SEARCHRES` | Consider for advanced clients if each maps cleanly to canonical change, message, and search state. |
| `COULD` | IMAP metadata extensions | Consider only when backed by canonical mailbox properties and rights. |
| `IGNORE` | IMAP send semantics | IMAP must not submit outbound mail or create protocol-local `Outbox` behavior. |
| `IGNORE` | Calendar, contacts, tasks, and collaboration data | These belong to JMAP, ActiveSync, EWS, MAPI, DAV, or web APIs, not IMAP. |
| `IGNORE` | Protocol-local mailbox database or search index | All durable state remains canonical LPE state. |
| `IGNORE` | Protected `Bcc` search/projection | `Bcc` must not leak through IMAP search, fetch projections, logs, or diagnostics. |
| `IGNORE` | Non-standard extensions without client evidence | Do not add vendor extensions without documented target-client need and tests. |

## JMAP

JMAP is the primary modern client protocol. It exposes mail and, where enabled,
contacts, calendars, tasks, blobs, push, and submission over canonical LPE state.

| Priority | Function, task, or data | LPE requirement |
| --- | --- | --- |
| `MUST` | Session discovery | `/.well-known/jmap` and the JMAP session object must publish only implemented endpoints, accounts, upload/download URLs, state, and capabilities; JMAP discovery must not advertise Exchange/MAPI compatibility endpoints or change MAPI autodiscover publication gates. |
| `MUST` | Core request handling | Request batching, method-call ordering, result references, created-id references, and method-level errors must follow JMAP Core semantics. |
| `MUST` | Mailbox data | `Mailbox/get`, `Mailbox/query`, `Mailbox/changes`, and `Mailbox/set` where exposed must use canonical mailbox state and roles. |
| `MUST` | Email read and query | `Email/get`, `Email/query`, `Email/changes`, and `Email/queryChanges` must read canonical messages, threads, keywords, all visible mailbox memberships, per-mailbox membership state, text and HTML body values, body sizes, blobs, and state tokens. Unscoped queries return one id per canonical message; mailbox-scoped queries filter by the selected visible membership. Compatibility protocol projections, including IMAP body fetch and MAPI body streams, must not maintain divergent body data. |
| `MUST` | Email write/import/copy | `Email/set`, `Email/import`, and `Email/copy` must mutate canonical messages, including text and HTML body values, and validate uploaded content before persistence. Compatibility draft/write paths such as MAPI pending-message body streams and ICS imported-message save flows must feed the same canonical message/draft model, while read-only projections must reject mutation with protocol-specific errors instead of silently forking state. |
| `MUST` | Drafts | Draft creation and update must persist in canonical `Drafts`; no JMAP-local draft store is allowed. |
| `MUST` | Submission | `EmailSubmission/set` must submit through canonical LPE submission, create the authoritative `Sent` copy before relay or handoff, and never bypass LPE-CT for transport. Compatibility transport/spooler probes must not create IMAP-local or JMAP-local submission queues, Outbox state, or advisory event stores. |
| `MUST` | Blob upload/download/copy | Blob methods and endpoints must use canonical blob storage, attachment deduplication, permissions, validation, and blob-kind constraints. Raw RFC 5322 message blobs are database-backed `raw_message` blobs; MIME and attachment durable blobs must prove tenant/domain ownership and may use external placement only through active placement metadata. |
| `MUST` | Threads and snippets | `Thread/*` and `SearchSnippet/get` must be derived from canonical message/search state and must not expose `Bcc`. Protected `Bcc` access is owner-only and must use an explicit storage boundary rather than the default fetch/search paths. Current thread projection may use message-level `thread_id`; add a first-class `threads` table when thread lifecycle, MAPI conversation IDs, or retained `Thread/changes` require stable durable thread identity. |
| `MUST` | Push and reconnect | WebSocket and event-stream push must resume from canonical push state or fall back to safe full state when replay is unavailable. |
| `MUST` | Contacts, calendars, and tasks where exposed | JMAP contacts, calendars, and tasks must use canonical LPE collaboration state and rights. |
| `MUST` | Sharing/delegation | Shared mailbox and delegated account visibility must be rights-backed and tenant-bound. |
| `MUST` | State safety | `state`, `oldState`, `newState`, and `queryState` tokens must be account, method, filter, and sort scoped. |
| `SHOULD` | Quotas | Expose canonical quota state through standard JMAP quota support when quota enforcement is mature. |
| `SHOULD` | Identity management | `Identity/*` should reflect canonical account identities, delegation, and sender permissions. |
| `SHOULD` | Vacation response | `VacationResponse/*` should be implemented only when backed by canonical sieve/vacation state. |
| `SHOULD` | MDN and parse helpers | Add message disposition notification and parse behavior when backed by canonical message/submission rules. |
| `SHOULD` | Mailbox filtering and rules | Any future JMAP mailbox-rule surface must use canonical core LPE mailbox rule state, such as Sieve-backed user rules. Perimeter filtering, antispam policy, quarantine retention, and quarantine actions remain LPE-CT-owned. Compatibility rule probes and deferred-action message updates must not create IMAP-local or JMAP-local rule state. |
| `SHOULD` | Advanced search and sort | Expand filters, sort comparators, and pagination only through canonical search/query primitives. |
| `SHOULD` | Cross-account copy | Support only when canonical rights and blob/mailbox ownership rules permit it. |
| `SHOULD` | Import/export MIME fidelity | Preserve raw message fidelity, attachment references, charsets, and recipient metadata without leaking protected fields. |
| `COULD` | JMAP Sieve | Consider after ManageSieve and canonical sieve storage are implemented. |
| `COULD` | JSContact and JSCalendar alignment | Consider when LPE contact and calendar schemas are ready for the newer standards. |
| `COULD` | File storage and preview extensions | Consider only if backed by canonical blob rights, validation, and non-leaking preview generation. |
| `COULD` | Push transport enhancements | Compression or alternate push transports can be added after baseline push replay is stable. |
| `IGNORE` | JMAP-specific canonical stores | Do not create JMAP-only mailbox, blob, search, rights, draft, sent, or submission state. |
| `IGNORE` | Direct SMTP submission | JMAP submission must not bypass canonical LPE submission or LPE-CT relay/handoff. |
| `IGNORE` | Protected `Bcc` search/projection | `Bcc` must not appear in snippets, shared mailbox projections, normal user responses, AI-facing data, or logs. |
| `IGNORE` | Non-standard methods without documented need | Do not publish private JMAP capabilities as supported release surface without architecture and test evidence. |
| `IGNORE` | Remote-AI assumptions | JMAP data exposure must remain compatible with local-only AI execution and protected metadata rules. |

## Canonical Data Mapping

| Data | Canonical owner | IMAP mapping | JMAP mapping |
| --- | --- | --- | --- |
| Mailboxes and roles | Core LPE mailbox state | `LIST`, `XLIST`, `STATUS`, `SELECT`, `EXAMINE`; compatibility receive-folder and folder move/copy probes must map back to canonical mailbox roles rather than IMAP-local routing or hierarchy state | `Mailbox/*`; compatibility receive-folder and folder move/copy probes must map back to canonical mailbox roles rather than JMAP-local routing or hierarchy state |
| Messages and MIME bodies | Core LPE message state | `FETCH`, `UID FETCH`, `APPEND`; body and HTML variants are canonical projections, not IMAP-local data | `Email/*`, upload/download; text and HTML body values are canonical projections reused by MAPI body streams |
| Flags and keywords | Core LPE message state | system flags and keywords | `keywords` |
| Drafts | Core LPE draft/mailbox state | `APPEND` to canonical drafts; delete/move through canonical mutations | `Email/set` in canonical `Drafts` |
| Sent mail | Core LPE submission state | visible as canonical `Sent`; not created by IMAP send; compatibility transport/spooler probes do not create IMAP-local Outbox or queue state | `EmailSubmission/set` creates canonical `Sent`; compatibility transport/spooler probes do not create JMAP-local Outbox or queue state |
| Attachments and blobs | Core LPE blob state | MIME body fetch/import | Blob endpoints and `Email` body parts |
| Search | Core LPE search state | `SEARCH`, `UID SEARCH` without `Bcc` | `Email/query`, `SearchSnippet/get` without `Bcc` |
| Change/sync state | Core LPE change and push state | UID/mod-sequence/IDLE refresh and QRESYNC-style expunge replay from canonical `mail_change_log` and `tombstones`; compatibility sync/version and local-replica checkpoint probes must not create IMAP-local state | `changes`, `queryChanges`, push from retained canonical change cursors; replay rows must have valid object shape, and stale or unmappable cursors fall back to current-state diff rather than protocol-local replicas |
| Compatibility table views | Core LPE mailbox/message/attachment projections | IMAP list/status/fetch views and MAPI table set/query/status/reset probes must validate the selected canonical view instead of reporting success for unrelated object handles | JMAP mailbox/message/blob query projections and MAPI table set/query/status/reset probes must validate the selected canonical view instead of reporting success for unrelated object handles |
| Public-folder compatibility probes | Core LPE mailbox/public-folder state when implemented | Private-mailbox public-folder ghost checks may return non-ghosted compatibility metadata, but replica topology probes must not create IMAP-local public-folder state | Private-mailbox public-folder ghost checks may return non-ghosted compatibility metadata, but replica topology probes must not create JMAP-local public-folder state |
| Filtering and rules | Core LPE filtering/rule state | IMAP has no canonical rule store; compatibility rule probes must not create IMAP-local rule or deferred-action state | Future JMAP filtering surfaces must use canonical rule state; compatibility rule probes must not create JMAP-local rule or deferred-action state |
| Rights and shared visibility | Core LPE rights state | shared folders only when rights-backed | delegated/shared accounts and objects |
| Contacts/calendars/tasks | Core LPE collaboration state | ignored | JMAP collaboration methods where exposed |
| Client discovery | Autoconfiguration policy | IMAP may be published only as implemented mailbox access, with client `SMTP` only when real authenticated submission exists | JMAP publishes only JMAP endpoints; Exchange/MAPI discovery remains separately gated and opt-in |

## References

- Microsoft Learn: POP3 and IMAP4 in Exchange Server.
- Microsoft Learn: Enable and configure IMAP4 on an Exchange server.
- RFC 9051: Internet Message Access Protocol (IMAP) Version 4rev2.
- RFC 8620: The JSON Meta Application Protocol (JMAP).
- RFC 8621: The JSON Meta Application Protocol (JMAP) for Mail.
- RFC 9404: JMAP Blob Management Extension.
