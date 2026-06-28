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

## Post-Maintenance Follow-Up - 2026-06-27

This section records the state after the maintenance refactors present in the
current working tree. It supersedes the line-count and duplication observations
below where the same files are mentioned.

### Oversized Files Resolved Or Reduced

The original 49k-line MAPI-over-HTTP test file was resolved as a single-file
hotspot: `crates/lpe-exchange/src/tests/mapi_over_http.rs` is now a 677-line
module hub, with scenario modules under
`crates/lpe-exchange/src/tests/mapi_over_http/`.

The following original hotspots were reduced but remain above the 1,500-line
production-source target:

| File | Previous audit lines | Current lines | Current state |
| --- | ---: | ---: | --- |
| `crates/lpe-exchange/src/mapi/tables.rs` | 17,206 | 8,351 | Reduced, still oversized. Small `attachments.rs` and `row_codecs.rs` modules exist, but most table logic remains in the hub. |
| `crates/lpe-exchange/src/mapi/properties.rs` | 15,473 | 7,961 | Reduced, still oversized. `tags.rs` and `values.rs` were extracted; most object mapping and property behavior remains in the hub. |
| `crates/lpe-exchange/src/mapi/rop.rs` | 12,932 | 7,603 | Reduced, still oversized. `buffer.rs`, `errors.rs`, and `serialize.rs` were extracted; parsing/responses/tests still dominate. |
| `crates/lpe-exchange/src/service.rs` | 16,211 | 14,742 | Slightly reduced, still a major EWS/MAPI HTTP service hotspot. |
| `LPE-CT/src/smtp.rs` | 10,128 | 4,915 | Reduced, still oversized. Protocol/session/audit/bridge helpers were extracted; policy, queue, outbound, DSN, and tests need more separation. |
| `crates/lpe-exchange/src/mapi/dispatch.rs` | 33,851 | 32,853 | Still the highest-risk file; only marginally reduced. |

New oversized-file check output also identifies additional production hotspots
that were not the first maintenance targets: `crates/lpe-storage/src/protocols.rs`,
`crates/lpe-storage/src/blob_store.rs`, `LPE-CT/src/main.rs`,
`crates/lpe-activesync/src/service.rs`, `crates/lpe-exchange/src/mapi/transport.rs`,
`crates/lpe-exchange/src/mapi/store_adapter.rs`, `crates/lpe-exchange/src/mapi/nspi.rs`,
`crates/lpe-admin-api/src/workspace.rs`, and `crates/lpe-exchange/src/mapi/sync.rs`.

### Duplicated Helpers Removed Or Reduced

Primitive crypto wrappers were centralized in `lpe-domain`:

- `crates/lpe-domain/src/crypto.rs` owns `hex_lower`, `sha256_hex`,
  `sha256_hex_prefix`, `hmac_sha256`, and `hmac_sha256_hex`.
- `crates/lpe-storage/src/storage_backend.rs`,
  `crates/lpe-admin-api/src/totp.rs`,
  `crates/lpe-storage/src/util.rs`, and
  `crates/lpe-exchange/src/mapi/rop.rs` now use those shared helpers instead
  of owning duplicate primitive wrappers.
- S3 signing logic remains local to storage backend code, as intended.

Normalization helpers were centralized in `lpe-domain`:

- `crates/lpe-domain/src/normalization.rs` owns mailbox/domain/email,
  calendar email, login-name, SMTP lookup, and trimmed-lowercase helpers.
- `crates/lpe-storage/src/util.rs` and `crates/lpe-storage/src/calendar.rs`
  still expose compatibility wrappers, but their behavior delegates to
  `lpe-domain`.
- `crates/lpe-mail-auth/src/auth.rs`,
  `crates/lpe-jmap/src/contacts.rs`,
  `crates/lpe-exchange/src/service.rs`, and
  `crates/lpe-exchange/src/mapi/nspi.rs` now call shared normalization for the
  semantically identical cases.
- Microsoft-specific NSPI lookup behavior remains local where it is a protocol
  rule rather than generic normalization.

Message read/flag mutation has an initial canonical shared path:

- `crates/lpe-storage/src/mail_items.rs` owns `MessageFlagUpdate` and
  `update_message_flags`.
- `crates/lpe-activesync/src/store.rs` and `crates/lpe-exchange/src/store.rs`
  route their real storage adapters through this shared path.
- Protocol-specific response mapping and fake test stores remain local.

Still not resolved: broad MAPI debug hex/preview formatting remains duplicated
inside diagnostics-heavy Exchange code. Those helpers are not the same as the
primitive SHA-256/HMAC wrappers and should be extracted only with the MAPI
diagnostics split.

### Remaining Outlook Parity Contradictions

The strongest direct contradiction from the original audit appears resolved:
`docs/i18n.md` no longer says MAPI over HTTP is out of scope; it now states
that MAPI over HTTP is part of the Outlook desktop Exchange-account objective.

Remaining wording conflicts are softer but still important:

- `docs/architecture/ews-mapi-mvp.md` correctly says full Outlook
  functionality is the target, but the same document still describes many
  behaviors as incomplete, unsupported, or not implemented yet. That is
  acceptable only while the wording stays framed as current readiness state,
  not a permanent product boundary.
- `docs/architecture/mapi-over-http-implementation-plan.md` still contains
  deferred/unsupported matrices for public folders, raw FastTransfer streams,
  full search-folder parity, rules/deferred actions, notification delivery, and
  transport/spooler behavior. These must remain tracked parity gaps until
  Microsoft docs, Outlook traces, or canonical LPE requirements prove they are
  unnecessary.
- `docs/architecture/public-folders-mapi-mvp.md`,
  `docs/architecture/sieve-managesieve-mvp.md`, and
  `docs/architecture/sql-schema-v2.md` correctly reject MAPI-local state for
  Exchange-only blobs. They become contradictions only if interpreted as
  permanent refusal to model Outlook-required behavior canonically.

### Remaining Permanent Unsupported Exchange Behaviors

These are still intentionally unsupported or deferred because there is no
canonical LPE model yet, or because accepting them as opaque Exchange state
would violate the architecture:

- `RopSetSpooler`, `RopSpoolerLockMessage`, and `RopTransportNewMail`: parsed
  but unsupported until LPE has canonical advisory state. They must not create
  client-spooler custody or transport state outside LPE/LPE-CT ownership.
- `RopSetReceiveFolder`: bounded compatibility acknowledgement exists only for
  writes that confirm the fixed canonical receive-folder map. Arbitrary
  configurable receive-folder routing remains unsupported until it has a
  canonical model.
- `RopUpdateDeferredActionMessages`, Exchange rule blobs, provider-specific
  predicates, client-only rules, delegate rule templates, and deferred-action
  provider data: rejected until canonical rule/deferred-action semantics exist.
  They must not activate Sieve or create a MAPI-local rule store.
- Raw Exchange FastTransfer marker/subobject destination streams outside the
  bounded canonical-object upload path: parseable errors without side effects.
- `RopLockRegionStream` and `RopUnlockRegionStream`: parsed but not implemented
  for stream locking semantics.
- Cross-process notification replay and full Exchange notification payload
  parity: session-local behavior remains acceptable for the first sticky-session
  lab gate, but production parity needs either documented sticky-session
  requirements or durable replay semantics.
- Full public-folder Exchange replication, recipient-bearing public-folder item
  conversion, and arbitrary Exchange per-user binary blobs: deferred until a
  canonical public-folder model exists for each behavior.
- Full Microsoft search-folder template BLOB parity and arbitrary restriction
  trees: bounded JSON-mappable search criteria exist, but full parity remains
  deferred.

### Next Highest-Risk Files

The next refactor targets should be selected by Outlook risk and current line
count:

1. `crates/lpe-exchange/src/mapi/dispatch.rs` - still 32,853 lines and owns too
   much ROP execution, mutation routing, diagnostics, and compatibility logic.
   The 2026-06-28 oversized-source check reports 30,180 lines after additional
   helper extraction, but it remains the top source hotspot.
2. `crates/lpe-exchange/src/service.rs` - still 15,596 lines and remains the
   main EWS service hotspot despite small MIME/XML/notification extractions.
3. `crates/lpe-exchange/src/store.rs` - 8,693 lines of Exchange storage facade
   and canonical/compatibility translation.
4. `crates/lpe-exchange/src/mapi/tables.rs` - 8,674 lines; table projections
   are Outlook-critical and still mostly centralized.
5. `crates/lpe-exchange/src/mapi/properties.rs` - 8,335 lines; property
   mapping remains a high-risk source of wire-compatibility regressions.
6. `crates/lpe-exchange/src/mapi/rop.rs` - 7,959 lines; parsing, response
   serialization, unsupported behavior, and tests need continued separation.
7. `crates/lpe-exchange/src/mapi_mailstore.rs` and
   `crates/lpe-exchange/src/mapi_store.rs` - both remain large and define the
   Outlook metadata/canonical projection boundary.
8. `crates/lpe-storage/src/protocols.rs` - 5,832 lines and used broadly by
   protocol adapters.
9. `LPE-CT/src/smtp.rs` and `LPE-CT/src/main.rs` - still large in the
   perimeter-security path.

### Follow-Up Verification

Commands run for this follow-up:

- `python tools/check_oversized_sources.py`
- `rg -n "not a complete Exchange server|out of scope|unsupported|not implemented|does not support|MAPI over HTTP is out of scope|public MAPI autodiscover|autodiscover" docs/architecture docs/audits docs/i18n.md -g "*.md"`
- `rg -n "fn (sha256_hex|hmac_sha256|hmac_sha256_hex|hex_lower|hex_encode|to_hex|normalize_mailbox_email|normalize_mailbox_domain|normalize_email|normalize_domain_name|normalize_calendar_email)" crates/lpe-storage/src crates/lpe-admin-api/src crates/lpe-exchange/src crates/lpe-jmap/src crates/lpe-mail-auth/src crates/lpe-domain/src -g "*.rs"`
- `rg -n "lpe_domain::(crypto|normalization)|use lpe_domain::crypto|use lpe_domain::normalization|pub use lpe_domain::crypto|normalization::normalize" crates/lpe-storage/src crates/lpe-admin-api/src crates/lpe-exchange/src crates/lpe-jmap/src crates/lpe-mail-auth/src crates/lpe-domain/src -g "*.rs"`
- `rg -n "RopSetSpooler|RopSpoolerLockMessage|RopTransportNewMail|RopSetReceiveFolder|RopUpdateDeferredActionMessages|Exchange rule blobs|client-only rules|deferred-action|raw Exchange marker|RopLockRegionStream|RopUnlockRegionStream" docs/architecture/ews-mapi-mvp.md docs/architecture/mapi-over-http-implementation-plan.md crates/lpe-exchange/src -g "*.md" -g "*.rs"`

### Verification Sweep - 2026-06-28

The current non-cargo verification sweep confirms that the maintenance work is
still incomplete but better tracked:

- `python tools/check_oversized_sources.py` still exits successfully in warning
  mode and reports 298 checked production source files. The top current
  offenders are `crates/lpe-exchange/src/mapi/dispatch.rs` at 30,180 lines,
  `crates/lpe-exchange/src/service.rs` at 15,596 lines,
  `crates/lpe-exchange/src/store.rs` at 8,693 lines,
  `crates/lpe-exchange/src/mapi/tables.rs` at 8,674 lines,
  `crates/lpe-exchange/src/mapi/properties.rs` at 8,335 lines, and
  `crates/lpe-exchange/src/mapi/rop.rs` at 7,959 lines.
- Primitive crypto helper definitions are centralized in
  `crates/lpe-domain/src/crypto.rs`. The `rg` helper scan finds no duplicate
  `sha256_hex`, `hmac_sha256`, `hmac_sha256_hex`, or `hex_lower` definitions
  outside `lpe-domain`.
- Normalization definitions remain in `crates/lpe-domain/src/normalization.rs`,
  with compatibility wrappers still present in `crates/lpe-storage/src/util.rs`
  and `crates/lpe-storage/src/calendar.rs`. Those wrappers are intentionally
  delegated compatibility APIs, not independent normalization logic.
- Receive-folder, spooler advisory, rule/deferred-action, public-folder,
  Search Folder/Common Views, and notification replay gaps now have focused
  architecture or audit follow-ups:
  `docs/architecture/mapi-receive-folder-routing.md`,
  `docs/architecture/mapi-spooler-advisory-model.md`,
  `docs/architecture/exchange-rule-deferred-action-canonical-model.md`,
  `docs/audits/public-folder-outlook-parity-follow-up-2026-06-28.md`,
  `docs/audits/mapi-search-folder-common-views-parity-2026-06-28.md`, and
  `docs/audits/outlook-notification-replay-parity-2026-06-28.md`.
- Full cargo verification is not current. A prior `cargo test -p
  lpe-exchange` run remained alive in the workspace process table and had
  already shown failures before hanging, so this sweep deliberately avoided
  launching another cargo test until that process contention is cleared.

## Instructions Or Documentation That Conflict With Full Outlook Functionality

These do not all mean the current behavior is wrong. Some are prudent current
limits. The conflict is in wording them as permanent scope limits while the
product objective is full Outlook/Exchange functionality.

| Location | Original conflict | Current status |
| --- | --- | --- |
| `docs/architecture/ews-mapi-mvp.md` | Historically described `lpe-exchange` as not a complete Exchange server and marked many Outlook-visible behaviors as unsupported. | Current wording frames incomplete behavior as readiness gaps and bounded canonical projections. The remaining unsupported rows must stay tracked as parity gaps, not durable refusals. |
| `docs/architecture/mapi-over-http-implementation-plan.md` | The unsupported matrix lists full public-folder replication, full search-folder parity, Exchange rules/deferred actions, full notification delivery, full Exchange dumpster behavior, and other deferred areas. | Acceptable while "deferred" means "not yet implemented." The major gap families now have focused follow-up docs or audits. |
| `docs/architecture/mapi-full-object-support-execution.md` | Says LPE is not ready to claim full Microsoft object support. | Still valid as a readiness assessment, not a product boundary. |
| `docs/i18n.md` | Previously said MAPI over HTTP was out of scope. | Resolved before this sweep; current text says MAPI over HTTP is part of the Outlook desktop Exchange-account objective. |
| `docs/architecture/public-folders-mapi-mvp.md` | Recipient-bearing public-folder conversion and Exchange-compatible binary per-user blobs remain bounded or deferred. | Tracked as staged public-folder parity gaps in `docs/audits/public-folder-outlook-parity-follow-up-2026-06-28.md`. |
| `docs/architecture/sieve-managesieve-mvp.md` and `docs/architecture/sql-schema-v2.md` | Exchange rule blobs, client-only rules, delegate templates, provider-specific predicates, and deferred actions are unsupported and must not activate Sieve. | Safety rule remains valid. The canonical model needed for wider parity is now tracked in `docs/architecture/exchange-rule-deferred-action-canonical-model.md`. |

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
