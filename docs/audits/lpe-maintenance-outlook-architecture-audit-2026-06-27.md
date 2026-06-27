# LPE Maintenance And Outlook Architecture Audit - 2026-06-27

## Scope

This audit reviewed the repository architecture and implementation shape against
the primary product objective: Outlook must be able to connect as an Exchange
client and provide full Outlook functionality.

Read before auditing:

- `ARCHITECTURE.md`
- `docs/architecture/initial-architecture.md`
- `LICENSE.md`
- `docs/architecture/ews-mapi-mvp.md`
- `docs/architecture/mapi-over-http-implementation-plan.md`
- `docs/architecture/mapi-full-object-support-execution.md`
- `docs/architecture/client-autoconfiguration.md`
- `docs/architecture/edge-and-protocol-exposure.md`

## Executive Findings

1. The documented long-term objective and the current bounded implementation
   scope are not aligned. The core docs correctly identify MAPI over HTTP as
   the Outlook desktop Exchange route, but several detailed docs still say
   LPE is not a complete Exchange server or declare Exchange behaviors out of
   scope. If full Outlook functionality is the objective, those statements
   should become temporary readiness gaps, not permanent scope exclusions.
2. The Exchange/MAPI implementation has source files that are too large for
   safe maintenance. `dispatch.rs`, `tables.rs`, `properties.rs`, `rop.rs`,
   and `service.rs` are each carrying multiple responsibilities. This is now
   the highest maintenance risk in the Outlook-critical path.
3. Several low-level helpers are duplicated across protocol crates. Date
   conversion, MIME header sanitization/rendering, hashing/HMAC helpers, and
   address normalization should move into shared crates before more protocol
   behavior is added.
4. The architecture correctly avoids protocol-local canonical mailbox state,
   but the amount of MAPI compatibility state is growing. It needs clearer
   ownership boundaries: canonical collaboration state, durable Outlook
   compatibility metadata, and session-only transport state must be separated
   in code as clearly as they are described in the docs.
5. Existing release gates are sound. They should not be removed. They are the
   mechanism that prevents LPE from claiming Outlook compatibility before local
   harness, Microsoft RCA, and real Outlook 2016/2019 cached-mode evidence
   agree.

## Instructions Or Documentation That Conflict With Full Outlook Functionality

These do not all mean the current behavior is wrong. Some are prudent current
limits. The conflict is in wording them as permanent scope limits while the
product objective is full Outlook/Exchange functionality.

| Location | Conflict |
| --- | --- |
| `docs/architecture/ews-mapi-mvp.md` | Says `lpe-exchange` is "not a complete Exchange server". That directly conflicts with a 100% Outlook functionality objective if retained as a durable rule. |
| `docs/architecture/ews-mapi-mvp.md` | Marks push notifications, full Exchange streaming affinity, full property-bag parity, rich compose edge cases, spooler advisory events, arbitrary public-folder per-user blobs, deferred actions, full search-folder criteria, full notification delivery, and multiple recovery/dumpster behaviors as out of scope or unsupported. These should be tracked as missing Outlook parity work, not architectural exclusions. |
| `docs/architecture/mapi-over-http-implementation-plan.md` | The unsupported matrix lists full public-folder replication, full search-folder parity, Exchange rules/deferred actions, full notification delivery, full Exchange dumpster behavior, and other areas as deferred/unsupported. That is acceptable only if "deferred" means "not yet implemented". |
| `docs/architecture/mapi-full-object-support-execution.md` | Says LPE is not ready to claim full Microsoft object support. This is true as an assessment, not a contradiction, but it should remain a readiness statement rather than a product boundary. |
| `docs/i18n.md` | Says MAPI over HTTP is out of scope. That conflicts with current architecture and should be removed or marked as obsolete planning text. |
| `docs/architecture/public-folders-mapi-mvp.md` | Says recipient-bearing public-folder conversion and Exchange-compatible binary per-user blobs remain out of scope. If Outlook parity requires those behaviors, this needs to become a staged implementation gap. |
| `docs/architecture/sieve-managesieve-mvp.md` and `docs/architecture/sql-schema-v2.md` | State Exchange rule blobs, client-only rules, delegate templates, provider-specific predicates, and deferred actions are unsupported and must not activate Sieve. The safety rule is valid, but full Outlook support eventually needs a canonical model for these, not a permanent rejection. |

No conflict was found in the high-level architecture that makes MAPI over HTTP
secondary to IMAP or ActiveSync. The top-level docs consistently say IMAP is a
compatibility path, ActiveSync is mobile/native only, and MAPI over HTTP is the
classic Outlook desktop Exchange route.

## Oversized Files

The largest files are concentrated in the Outlook-critical path:

| Lines | File | Audit note |
| ---: | --- | --- |
| 48,988 | `crates/lpe-exchange/src/tests/mapi_over_http.rs` | Test coverage is valuable, but this needs scenario-based test modules. A failure here is hard to localize and expensive to review. |
| 33,851 | `crates/lpe-exchange/src/mapi/dispatch.rs` | Critical risk. Dispatch owns ROP routing, object mutations, debug summaries, compatibility probes, and many tests. Split by object family and ROP family. |
| 17,206 | `crates/lpe-exchange/src/mapi/tables.rs` | Table projections should be split into hierarchy, contents, associated contents, attachments, permissions, public folders, search/reminder, and row codecs. |
| 16,211 | `crates/lpe-exchange/src/service.rs` | EWS HTTP/SOAP service and MIME rendering are too coupled. Split operation dispatch, XML parsing/rendering, MIME helpers, and each operation family. |
| 15,473 | `crates/lpe-exchange/src/mapi/properties.rs` | Property constants, codecs, object mappings, calendar/task/contact properties, recurrence, and debug helpers should not live together. |
| 12,932 | `crates/lpe-exchange/src/mapi/rop.rs` | Wire codecs, ROP parsers, response builders, debug formatters, and tests should become smaller protocol modules. |
| 10,128 | `LPE-CT/src/smtp.rs` | Sorting-center SMTP state machine, tests, queue behavior, and policy handling are too large for edge-security work. |
| 8,687 | `crates/lpe-exchange/src/store.rs` | Store adapter is accumulating protocol-specific translation. Extract object-family stores or canonical service facades. |
| 7,796 | `crates/lpe-exchange/src/mapi_mailstore.rs` | Mailbox snapshot/projection should be split by identity, folder tree, message contents, associated contents, and sync facts. |
| 6,471 | `crates/lpe-exchange/src/mapi_store.rs` | MAPI compatibility persistence should be split by profile settings, named/custom properties, associated config, shortcuts, and sync checkpoints. |
| 5,593 | `LPE-CT/web/app.js` | Admin UI logic should be modularized by page/feature instead of one shared app file. |

Recommended hard rule: production source files should stay below 1,500 lines
unless they are generated or data tables. Test files should stay below 2,500
lines per scenario family. Anything above that needs an explicit split plan.

## Duplication To Centralize

### Date And Time Conversion

Repeated civil-date conversion helpers exist in:

- `crates/lpe-jmap/src/upload.rs`
- `crates/lpe-activesync/src/service.rs`
- `crates/lpe-activesync/src/snapshot.rs`
- `crates/lpe-exchange/src/mapi_mailstore.rs`
- `crates/lpe-exchange/src/mapi/tables.rs`
- `crates/lpe-exchange/src/mapi/properties.rs`
- `crates/lpe-exchange/src/mapi/transport.rs`
- `crates/lpe-storage/src/storage_backend.rs`

Create a shared `lpe-domain::time` or `lpe-core::time` module for calendar-safe
UTC/date conversion and protocol formatting primitives. Protocol-specific wire
formats can stay in protocol crates, but date arithmetic should not.

### MIME And Header Rendering

MIME/header helpers are duplicated between JMAP upload/download rendering and
EWS rendering:

- `crates/lpe-jmap/src/upload.rs`
- `crates/lpe-exchange/src/service.rs`

Centralize header value sanitization, display-name quoting, RFC 5322 date
formatting, MIME boundary generation, and body newline normalization in a shared
mail formatting module. This matters for Outlook because different protocol
paths must not render subtly different messages or leak protected metadata.

### Hashing And HMAC Helpers

SHA-256 and HMAC helpers exist in:

- `crates/lpe-storage/src/util.rs`
- `crates/lpe-storage/src/storage_backend.rs`
- `crates/lpe-admin-api/src/totp.rs`
- `crates/lpe-exchange/src/mapi/rop.rs`

Create a small shared crypto utility module for hex encoding, SHA-256, HMAC,
and preview hashing. Keep protocol-specific signing algorithms local, but do
not duplicate primitive wrappers.

### Address And Identity Normalization

Normalization is split across:

- `crates/lpe-storage/src/util.rs`
- `crates/lpe-mail-auth/src/auth.rs`
- `crates/lpe-storage/src/calendar.rs`
- `crates/lpe-jmap/src/contacts.rs`
- `crates/lpe-exchange/src/mapi/nspi.rs`
- `crates/lpe-exchange/src/service.rs`

Email/domain/account/contact lookup normalization should be centralized in
`lpe-domain`, with protocol-specific wrappers only where Microsoft wire
behavior requires special handling.

## Unnecessary Or Suspect Functions

This audit did not delete code, but these areas deserve ruthless follow-up:

- `stub-local` AI provider wiring in `crates/lpe-ai/src/provider.rs`,
  `crates/lpe-core/src/service.rs`, and storage/admin projections should be
  reviewed. If it is runtime-visible placeholder behavior, remove it or confine
  it to tests/dev-only fixtures.
- Empty Outlook placeholder suppression helpers in `mapi_store.rs`,
  `dispatch.rs`, and `tables.rs` are probably legitimate compatibility
  filters, but the naming and spread make them hard to reason about. Centralize
  them under an associated-configuration policy module.
- Debug summary and Outlook trace helper functions inside `dispatch.rs` and
  `rop.rs` are useful during interoperability work, but they should be split
  into diagnostics modules so core dispatch logic stays readable.
- Large schema contract tests are valuable, but `runtime_schema_drift.rs` and
  `schema_contract.rs` now cover too many subsystems in one file. Split by
  storage area to reduce review cost.

## Recommended Architecture

The current crate map is directionally good, but Outlook parity needs stricter
layers:

1. `lpe-domain`
   - Shared value types, canonical IDs, email/domain normalization, date/time
     primitives, MIME/header primitives, and permission bit models.
   - No SQL, HTTP, or protocol dispatch.
2. `lpe-core`
   - Canonical application services: submission, mailbox mutation, contacts,
     calendar, tasks, rules, recoverable items, public folders, rights, and
     audit/change events.
   - Protocols call these services instead of hand-building canonical writes.
3. `lpe-storage`
   - PostgreSQL repositories only. Keep SQL here, but avoid protocol-specific
     behavior leaking in except through explicitly named compatibility metadata
     repositories.
4. `lpe-outlook-model` or a clearly separated `lpe-exchange::outlook_model`
   - Durable Outlook compatibility model: named properties, custom properties,
     entry IDs, source keys, profile settings, associated config, ICS
     checkpoints, navigation shortcuts, and canonical object mapping.
   - This is not canonical mailbox truth; it is compatibility metadata needed
     for Outlook round trips.
5. `lpe-exchange`
   - Protocol adapters only:
     - `ews/` for SOAP operation parsing/rendering and operation handlers.
     - `mapi_http/transport/` for HTTP, headers, cookies, sequence, replay.
     - `mapi_http/rop/` for wire parsing/serialization.
     - `mapi_http/emsmdb/` for mailbox ROP execution by object family.
     - `mapi_http/nspi/` for address book behavior.
     - `mapi_http/ics/` for FastTransfer/ICS.
     - `diagnostics/` for RCA/Outlook trace summaries.
6. Protocol crates
   - JMAP, IMAP, ActiveSync, DAV, and ManageSieve remain projections or
     compatibility adapters over canonical services.
   - They must not duplicate submission, Sent, Outbox, rights, or search state.
7. `LPE-CT`
   - Keep edge SMTP, quarantine, filtering, retry, and relay custody isolated.
   - Split `smtp.rs` into protocol state machine, queue/custody, policy,
     delivery bridge, DSN/bounce, and tests.

## Outlook Parity Roadmap

To keep the primary objective unchanged, treat every permanent unsupported
Exchange/Outlook behavior as a debt item with one of three outcomes:

1. Implement canonical model and protocol projection.
2. Prove with Microsoft docs and real Outlook traces that Outlook does not need
   the behavior for supported versions.
3. Explicitly decide that LPE will not provide 100% Outlook functionality.

Current priority areas:

- Complete and modularize MAPI over HTTP before adding new protocol breadth.
- Convert "out of scope" Outlook behaviors into tracked implementation gaps.
- Keep public MAPI autodiscover behind the existing evidence gates.
- Add a compatibility metadata boundary so Outlook-specific state is durable
  where required but never becomes mailbox truth.
- Add cross-protocol tests for every Outlook mutation path: create, edit,
  delete, move, copy, read state, flags, draft, send, recipients, Bcc, calendar,
  contacts, tasks, rules, public folders, recoverable items, and permissions.

## Verification Performed

- Read required architecture and license documents.
- Read Exchange/MAPI, MAPI over HTTP, full object support, autoconfiguration,
  and edge exposure architecture documents.
- Scanned source files and identified the largest implementation/test files.
- Searched for duplicated helper functions across Rust and web code.
- Searched for "out of scope", "unsupported", "placeholder", and Outlook/MAPI
  readiness language in docs and source.
- Confirmed high-level docs consistently identify MAPI over HTTP as the Outlook
  desktop Exchange route and do not promote ActiveSync as the desktop route.

No tests were run because this was an audit/reporting task and no runtime code
was changed.
