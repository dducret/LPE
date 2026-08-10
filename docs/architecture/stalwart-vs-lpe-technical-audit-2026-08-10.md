# Stalwart vs LPE Technical Audit - 2026-08-10

## Assumptions

- This is a technical architecture and implementation-maturity benchmark, not a
  runtime performance benchmark. No workload, mailbox corpus, hardware profile,
  deployment topology, or SLA target was provided.
- Stalwart is a benchmark only. Its source code must not be reused in `LPE`,
  and its all-in-one topology must not be treated as directly reusable.
- `LPE` remains Apache-2.0 and follows `LICENSE.md`.
- The core `LPE` / `LPE-CT` split remains a hard architectural boundary.

## Success Criteria

- Reassess current `LPE` 0.5.2 against current public Stalwart materials.
- Separate implemented progress from documented intent.
- Identify deliberate `LPE` differences versus real gaps.
- Record local verification results, including failures.
- Produce actionable priorities for the next engineering cycle.

## Verification

Commands run from `C:\Development\LPE` on 2026-08-10:

| Command | Result |
| --- | --- |
| `git status --short` before writing this file | clean |
| `git ls-remote https://github.com/stalwartlabs/stalwart.git HEAD` | `7401fc27c7398ed6f3044e98c0dd4fe8391b05b8` |
| `git ls-remote https://github.com/stalwartlabs/stalwart.git refs/tags/v0.16.14` | `425d034ae71bceaa715daf05ba872069302992bc` |
| `python tools/check_oversized_sources.py` | warning only; 27 tracked production files over 1,500 lines |
| `cargo test` | failed in `lpe-exchange`; 2,048 passed, 81 failed before the workspace stopped |
| `cargo test` in `LPE-CT/` | passed; 87 passed, 19 ignored |
| `python tools/check_repository.py` | completed with the same oversized-source warnings |

The full local workspace is not release-clean today. The failure cluster is
concentrated in `lpe-exchange` MAPI/EWS behavior: MAPI identity, hierarchy,
FastTransfer/ICS, public folders, permissions, reminders, contacts, root
hierarchy, receive-folder handling, and Microsoft protocol gap currency.

Implementation density snapshot:

| Component | Rust files | Lines |
| --- | ---: | ---: |
| `crates/lpe-exchange/src` | 315 | 273,941 |
| `crates/lpe-storage/src` | 59 | 47,196 |
| `crates/lpe-jmap/src` | 32 | 30,962 |
| `crates/lpe-activesync/src` | 25 | 13,227 |
| `crates/lpe-admin-api/src` | 30 | 12,805 |
| `crates/lpe-imap/src` | 16 | 8,978 |
| `LPE-CT/src/smtp` | 21 | 8,392 |
| `crates/lpe-dav/src` | 11 | 3,336 |

Oversized production files above 1,500 lines include protocol-critical files
such as `crates/lpe-exchange/src/mapi/dispatch/tables.rs`,
`crates/lpe-exchange/src/mapi/transport/diagnostics.rs`,
`crates/lpe-exchange/src/mapi/dispatch.rs`,
`crates/lpe-exchange/src/service/ews/items.rs`,
`crates/lpe-exchange/src/mapi/rop.rs`, `crates/lpe-exchange/src/mapi/nspi.rs`,
and `crates/lpe-storage/src/submission.rs`.

## Stalwart Benchmark Baseline

Current public Stalwart material presents Stalwart as a feature-complete,
all-in-one mail and collaboration server moving toward 1.0, with support for
JMAP, IMAP4rev2/rev1, POP3, SMTP, CalDAV, CardDAV, WebDAV, JMAP extensions,
broad Sieve support, spam/phishing controls, automated DNS and DKIM lifecycle,
multi-tenant administration, and observability.

Key current benchmark points:

- Stalwart is dual licensed under AGPL-3.0 and Stalwart Enterprise License v2.
- Stalwart's current public release evidence includes `v0.16.14` from
  2026-07-20.
- Stalwart splits storage into four independent stores: data, blob, search, and
  in-memory. Backend variants include RocksDB, FoundationDB, PostgreSQL, MySQL,
  SQLite, S3-compatible, filesystem, ElasticSearch, Meilisearch, Redis, Azure
  Blob Storage, and composite sharding/read-replica options.
- Stalwart clustering allows any cluster node to handle IMAP, SMTP, JMAP, or
  WebDAV requests, with distributed SMTP queues and coordination through
  peer-to-peer mode or Kafka, NATS, Redis, and related backends.
- Stalwart outbound queues are distributed and fault-tolerant, with virtual
  queues for concurrency, priority, retry, routing, and control-traffic
  isolation.
- Stalwart telemetry covers tracing/logging, metrics, webhooks, live telemetry,
  alerts, and persisted history.
- Stalwart management includes WebUI, CLI, queue/report management,
  declarative bulk operations, configuration export, database maintenance, and
  report visualization.

The benchmark lesson is not "make `LPE` all-in-one." The relevant bar is that
operators need protocol depth, queue custody, storage recovery, observability,
and repeatable administration to work together under failure.

## LPE Current Position

`LPE` has advanced substantially since the earlier 0.2-era framing:

- The repository is now aligned to `0.5.2`.
- `0.5.2` deliberately requires a fresh empty PostgreSQL database initialized
  from the canonical `0.5.2-sql` schema, with no in-place upgrade path.
- MAPI over HTTP publication is now enabled for capable Outlook desktop clients
  on new 0.5.2 installations when `LPE_AUTOCONFIG_MAPI_ENABLED` is true and the
  client sends a positive `X-MapiHttpCapability`.
- `LPE_AUTOCONFIG_OUTLOOK_INTEROP_GATE_PASSED` is now reserved for legacy
  `EXPR` / RPC over HTTP, not for MAPI over HTTP publication.
- EWS has a broad bounded dispatcher surface: documentation records 96/96
  Microsoft catalog operations routed to canonical implementations, all marked
  partial rather than full Exchange parity.
- Public folders now have canonical storage, APIs, permissions, replicas,
  per-user state, replay facts, and bounded EWS/MAPI projection.
- Notes, journal entries, reminders, search folders, recoverable items, mailbox
  rules, Outlook profile summaries, and public-folder APIs are now in the
  canonical model.
- `LPE-CT` remains the SMTP edge and passes its local custody/security tests.
- The `LPE` / `LPE-CT` bridge contract now emphasizes signed raw RFC 822/MIME
  custody for outbound handoff and inbound delivery, with Bcc kept as protected
  envelope metadata.
- Outlook diagnostic tracing exists, including sanitized/default-off traces and
  separately gated raw payload capture.

The largest current concern is that implementation breadth has outrun the local
green test baseline. The architecture is coherent, but the current branch must
not be described as locally release-clean while `lpe-exchange` fails.

## Benchmark Matrix

| Area | Stalwart benchmark | Current `LPE` position | Assessment |
| --- | --- | --- | --- |
| License | AGPL-3.0 / SELv2 | Apache-2.0 with strict dependency policy | Direct reuse remains forbidden |
| Topology | all-in-one clustered server | split core plus `LPE-CT` DMZ edge | deliberate difference; preserve split |
| SMTP/MTA | integrated MTA, distributed virtual queues, reports | `LPE-CT` owns ingress, submission, relay, quarantine, queue custody | ownership is right; not Stalwart-scale cluster yet |
| JMAP | broad mail/collaboration/file/Sieve extensions | mail, contacts, calendars, tasks, notes, journal, reminders, blobs, push | strong modern axis; private extensions need clear support boundary |
| IMAP | IMAP4rev2/rev1 plus many extensions | compatibility layer with Outlook/Thunderbird-focused behavior | acceptable depth path; lower extension breadth |
| POP3 | supported | not planned in current order | acceptable gap; do not add before protocol depth stabilizes |
| ActiveSync | not a core differentiator | first-class mobile/native adapter, protocol 16.1 | LPE differentiator; needs recurring real-device lab evidence |
| EWS | not Stalwart's focus | 96-operation bounded dispatcher, all partial | broad but not full Exchange parity; tests currently blocked by exchange failures |
| MAPI over HTTP | not Stalwart's focus | primary Outlook desktop Exchange path | strategic differentiator; current failure cluster is P0 |
| DAV/WebDAV | CalDAV, CardDAV, WebDAV, file storage | CalDAV/CardDAV/tasks; no general WebDAV file store | acceptable current scope |
| Public folders | WebDAV/JMAP sharing model, not Exchange PF focus | canonical public-folder model plus bounded EWS/MAPI projection | strong LPE-specific progress; still guarded |
| Sieve | broad Sieve and JMAP/ManageSieve surface | ManageSieve plus bounded canonical Sieve rules/vacation | useful MVP, less complete |
| Storage | four independent stores, many backends, sharding/read replicas | PostgreSQL authority, BlobStore boundary, S3-compatible blobs, raw RFC 822 still DB-backed | improving; behind Stalwart flexibility |
| Search | pluggable search backends and language breadth | PostgreSQL default and limited attachment indexing | aligned with policy, less scalable |
| Clustering | any node can serve protocols; distributed queues | HA scripts, core PostgreSQL authority, `LPE-CT` local custody | smaller HA story; not equivalent horizontal cluster |
| Security filtering | broad spam/phishing, DNSBL, reports, DKIM rotation, TLS reporting | SPF/DKIM/DMARC tests, greylisting, Bayes, reputation, quarantine, antivirus hooks | good foundation; reporting/lifecycle still behind |
| Observability | OTel, Prometheus, webhooks, alerts, live telemetry, history | metrics, logs, trace IDs, diagnostics, Outlook traces | useful but lacks full alert/history/live operations parity |
| Admin operations | WebUI/CLI, declarative bulk operations, queue/report management | admin API/UI, LPE-CT UI, install/update checks | needs idempotent declarative operations |
| Maintainability | mature broad codebase | very large protocol modules and failing suite | architectural risk now significant |

## Priority Findings

### P0: Restore The Local Test Baseline

The full workspace `cargo test` currently fails in `lpe-exchange` with 81
failures. That blocks any credible release-readiness claim for the current
tree. The failures align with the highest-risk project area: Outlook/MAPI,
EWS, ICS/FastTransfer, public folders, permissions, hierarchy, and identity.

Action:

1. Run `cargo test -p lpe-exchange --lib` as the immediate failing gate.
2. Group failures by shared root cause before editing.
3. Fix protocol identity/hierarchy failures through canonical MAPI identity and
   store logic, not test-specific workarounds.
4. Re-run `cargo test -p lpe-exchange --lib`, then full `cargo test`.

### P0: Keep MAPI Publication Claims Aligned With Evidence

The docs now say new 0.5.2 installations can publish MAPI over HTTP when the
deployment flag is enabled and the client capability probe succeeds. That is a
major shift from earlier guarded publication. Because the local exchange suite
is failing, the immediate audit position is:

- code may contain broad MAPI behavior,
- public deployment policy may intentionally publish it,
- but the current checkout is not verified clean locally.

Action: do not widen Outlook support claims until local `lpe-exchange`, the
bounded public-host Gate 1 harness, Microsoft RCA, and real Outlook cached-mode
runs are separately recorded for the exact deployed revision.

### P1: Preserve The Split Architecture

Stalwart's all-in-one design is a product strength, but it is the wrong target
topology for `LPE`. `LPE-CT` must remain the public SMTP/perimeter custody
component, and core `LPE` must remain the canonical mailbox and collaboration
state owner.

Action: benchmark Stalwart outcomes: queue durability, operational reporting,
transport policy traceability, and recovery. Do not move public SMTP or
quarantine state into core `LPE`.

### P1: Maintainability Is Now A Product Risk

The previous audit risk was missing breadth. The current risk is breadth,
failing protocol regressions, and large files. `lpe-exchange` is now large
enough that local reasoning cost is material. The repository's own rule says
production source files should stay below 1,500 lines or have explicit split
plans.

Action:

- Split oversized protocol files only when touching them for failing tests.
- Keep entry modules as thin dispatchers.
- Prioritize `mapi/dispatch/tables.rs`, `mapi/transport/diagnostics.rs`,
  `service/ews/items.rs`, `mapi/rop.rs`, `mapi/nspi.rs`, and
  storage MAPI/contact/submission modules as split-plan candidates.

### P1: Storage Progress Is Real, But Stalwart Still Sets A Higher Bar

`LPE` has a real `BlobStore` boundary, explicit placement metadata, migration
jobs, cleanup guards, and S3-compatible durable attachment/MIME-part blobs.
That closes a major gap from the older audit. Stalwart still has a more mature
four-store model and more backend options, including dedicated search,
in-memory state, Azure Blob, sharding, and read replicas.

Action: finish provider-specific restore and degraded-pool evidence before
adding more backend breadth. Raw RFC 822 message blob movement and search-store
separation should remain documented future boundaries.

### P1: LPE-CT Custody Is The Cleanest Current Subsystem

`LPE-CT` passes locally and covers custody, replay, quarantine, greylisting,
Bayesian scoring, reputation, DKIM foundations, SMTP policy, and STARTTLS. This
is the strongest operational area relative to its scope.

Action: promote deployment-level drills: crash during `DATA`, crash after
accept before bridge delivery, bridge timeout after core commit, repeated
outbound handoff after relay success, quarantine node replacement, and spool
restore from backup.

### P2: Security Reporting And Lifecycle Still Trail Stalwart

`LPE-CT` has deterministic filtering foundations. Stalwart remains ahead in
DNSBL breadth, DMARC/TLS report analysis, DKIM key automation, phishing
defenses, web reporting, and operator workflows.

Action order:

1. DKIM key lifecycle and rotation.
2. DMARC aggregate/failure report ingestion and visualization.
3. TLS-RPT and MTA-STS validation/reporting.
4. DNSBL/reputation lifecycle with false-positive controls.
5. Optional local-only LLM filtering after deterministic controls are stable.

### P2: Observability Needs SLOs And History

LPE has logs, metrics, trace IDs, diagnostics, and Outlook trace artifacts.
Stalwart's bar includes alerts, webhooks, live telemetry, and persisted
history. `LPE` should define SLOs for message and protocol custody rather than
only service health.

Recommended SLOs:

- SMTP `DATA` accepted to core final delivery.
- Authenticated submission to canonical `Sent`.
- Canonical `Sent` to `LPE-CT` handoff.
- Outbound queue age by state.
- Quarantine decision latency and operator action latency.
- JMAP WebSocket reconnect recovery.
- ActiveSync `Ping` reconnect recovery.
- MAPI reconnect and request replay behavior.

### P2: Declarative Administration Remains The Largest Operator Gap

Stalwart's WebUI/CLI and declarative bulk operations give operators a stronger
repeatable operations model. LPE has install/update checks and UI/API
surfaces, but no equivalent plan/diff/apply model.

Action: after the exchange suite is green, design a small declarative admin
plan format for domains, accounts, aliases, accepted domains, DKIM policy,
LPE-CT routing policy, and storage policy.

## Performance Benchmark Plan

No runtime benchmark was run. The smallest useful benchmark suite should be
added only after the local test baseline is green:

| Scenario | Metric |
| --- | --- |
| JMAP sync | session load, `Email/query`, `Email/queryChanges`, push reconnect |
| IMAP refresh | `LOGIN`, `LIST`, `SELECT`, `UID FETCH`, `SEARCH`, IDLE propagation |
| ActiveSync | `FolderSync`, paged `Sync`, `Ping`, stale-key recovery |
| EWS/MAPI | profile bootstrap, folder sync, content sync, reconnect, request replay |
| SMTP ingress | `DATA` accept, bridge final delivery, defer/retry latency |
| Submission | client submission to canonical `Sent`, then `LPE-CT` handoff |
| Queue recovery | duplicate suppression and terminal-state stability after restart |
| Storage | blob write/read/stat/verify across DB and S3-compatible placements |
| Search | realistic mailbox search over message bodies and indexed attachments |

Run each at 1k, 10k, and 100k messages per mailbox, with shared mailbox,
delegated sender, public-folder, and Bcc-protection cases.

## Recommended Roadmap

1. Restore `cargo test -p lpe-exchange --lib`.
2. Restore full `cargo test`.
3. Use the failing MAPI/EWS cluster to drive targeted split plans for oversized
   protocol modules.
4. Re-run public-host MAPI Gate 1, Microsoft RCA, and real Outlook cached-mode
   evidence for the exact 0.5.2 revision.
5. Promote `LPE-CT` custody/recovery drills into deployment CI where possible.
6. Define operator SLO dashboards and alert thresholds.
7. Finish provider-specific storage restore/degraded-pool evidence.
8. Add a minimal declarative admin plan/diff/apply model.
9. Only then consider new protocol breadth.

## Sources

- Stalwart README: <https://github.com/stalwartlabs/stalwart>
- Stalwart raw README, license and feature list: <https://raw.githubusercontent.com/stalwartlabs/stalwart/main/README.md>
- Stalwart email protocol docs: <https://stalw.art/docs/email/>
- Stalwart storage docs: <https://stalw.art/docs/storage/>
- Stalwart clustering docs: <https://stalw.art/docs/cluster/>
- Stalwart outbound queue docs: <https://stalw.art/docs/mta/outbound/queue/>
- Stalwart telemetry docs: <https://stalw.art/docs/telemetry/>
- Stalwart release page: <https://github.com/stalwartlabs/stalwart/releases>
- Local `LPE` docs: `ARCHITECTURE.md`,
  `docs/architecture/initial-architecture.md`, `LICENSE.md`, `README.md`,
  `docs/releases/0.5.2.md`, `docs/architecture/client-autoconfiguration.md`,
  `docs/architecture/lpe-ct-integration.md`,
  `docs/architecture/mailbox-storage-pools-roadmap.md`,
  `docs/architecture/observability.md`,
  `docs/architecture/mapi-over-http-implementation-plan.md`,
  `docs/architecture/mapi-full-object-support-execution.md`,
  `docs/architecture/public-folders-mapi-mvp.md`,
  `docs/architecture/notes-journal-reminders.md`, and
  `docs/architecture/ews-interoperability-matrix.md`
