# Stalwart vs LPE Technical Audit - 2026-05-08

## Success Criteria

- Compare Stalwart and `LPE` at product, architecture, protocol, operations, and performance-readiness levels.
- Preserve the `LPE` architecture: core `LPE` owns canonical mailbox and collaboration state; `LPE-CT` owns SMTP edge, relay, quarantine, and perimeter security.
- Treat Stalwart as a benchmark only. Do not copy Stalwart source code or adopt license-incompatible implementation details.
- Verify current `LPE` build and tests before making conclusions about implementation state.
- Produce actionable gaps that can guide `0.1.3` and post-`0.1.3` work.

## Scope

This audit used:

- local `LPE` architecture documents, especially `ARCHITECTURE.md`, `docs/architecture/initial-architecture.md`, and `LICENSE.md`
- local implementation structure in `crates/`, `LPE-CT/`, `web/`, and `installation/`
- public Stalwart product documentation and README, not Stalwart source code

Stalwart is dual licensed under `AGPL-3.0` / Stalwart Enterprise License. `LPE` remains `Apache-2.0`, with the dependency restrictions in `LICENSE.md`. Therefore Stalwart may inform target behavior and operational expectations, but not implementation reuse.

## Verification

Commands run from this workspace:

| Command | Result |
| --- | --- |
| `cargo check` from repository root | passed |
| `cargo check` from `LPE-CT/` | passed |
| `cargo test` from repository root | passed: 506 passed, 9 ignored |
| `cargo test` from `LPE-CT/` | passed: 76 passed, 19 ignored |

Implementation density snapshot:

| Component | Rust files | Lines |
| --- | ---: | ---: |
| `lpe-exchange` | 13 | 45,826 |
| `LPE-CT` | 13 | 19,536 |
| `lpe-jmap` | 22 | 18,560 |
| `lpe-storage` | 20 | 14,268 |
| `lpe-admin-api` | 22 | 7,634 |
| `lpe-imap` | 15 | 6,653 |
| `lpe-activesync` | 12 | 6,434 |
| `lpe-dav` | 10 | 3,254 |

Test distribution is strongest around `lpe-exchange`, `lpe-jmap`, `LPE-CT`, and `lpe-admin-api`, which matches the current `0.1.3` focus. Some env-sensitive and benchmark tests are intentionally ignored in normal `cargo test`.

## Stalwart Benchmark Baseline

Stalwart is the more mature and broader reference system:

- all-in-one mail and collaboration server with JMAP, IMAP, POP3, SMTP, CalDAV, CardDAV, and WebDAV
- built-in MTA capabilities including DKIM, SPF, DMARC, ARC, DANE, MTA-STS, SMTP TLS reporting, filtering, throttling, queues, routing, and delivery policy
- broad protocol/RFC implementation matrix, including IMAP4rev2, many IMAP extensions, JMAP Blob and Quotas, POP3, extensive Sieve extensions, SMTP extensions, mail authentication, calendaring, contacts, and WebDAV
- storage abstraction split into data, blob, search, and in-memory stores, each selectable across backends such as RocksDB, FoundationDB, PostgreSQL, MySQL, SQLite, S3-compatible storage, Redis, and search engines
- native clustering model where any node can serve IMAP, SMTP, JMAP, or WebDAV, with distributed SMTP queues and coordination via peer-to-peer mode, Kafka, NATS, or Redis
- v0.16 management direction: WebUI and CLI rebuilt around JMAP management objects, external OIDC support, declarative apply flows, automated DNS management, and automated DKIM rotation

The benchmark lesson is not "make `LPE` all-in-one." The relevant lesson is that mail systems become operationally credible when protocol correctness, queue custody, storage separation, admin workflows, telemetry, and recovery behavior are tested as one system.

## LPE Observed State

`LPE` has a narrower but deliberate architecture:

- core `LPE` and `LPE-CT` are separate responsibility zones
- PostgreSQL is the canonical core store
- canonical state tables exist for accounts, domains, mailboxes, messages, Bcc-protected metadata, outbound queue, contacts, calendar events, task lists, tasks, collaboration grants, sender grants, JMAP uploads, ActiveSync sync state, Sieve scripts, attachment blobs, and local AI projections
- `LPE-CT` implements SMTP ingress, submission, outbound relay, quarantine/history APIs, DKIM signing foundations, filtering policy, diagnostics, metrics, and signed bridge calls to core `LPE`
- protocol adapters exist for JMAP Mail, JMAP Contacts/Calendars/Tasks, IMAP, ActiveSync, EWS, MAPI over HTTP, DAV, and ManageSieve
- autodiscover/autoconfiguration is intentionally gated so `EWS`, `MAPI over HTTP`, `EXCH`, and `EXPR` metadata are only published by explicit opt-in
- Magika validation exists as a first-class boundary for external/client-provided files
- Bcc handling has explicit storage and tests for search/projection exclusion

The main implementation caution is that `LPE` is already broad. The current risk is not lack of ambition; it is carrying too many client surfaces before the flagship protocol paths have real-client interoperability depth.

## Benchmark Matrix

| Area | Stalwart benchmark | Current `LPE` position | Audit assessment |
| --- | --- | --- | --- |
| License posture | AGPL/enterprise dual-license project | Apache-2.0 project with strict dependency policy | `LPE` cannot reuse Stalwart code; benchmark only |
| Deployment shape | all-in-one horizontally scalable cluster | split core plus replaceable `LPE-CT` edge | `LPE` split is coherent and should be preserved |
| SMTP/MTA | mature integrated MTA, queues, policy, reports | `LPE-CT` owns SMTP ingress, submission, relay, quarantine, retry, trace | Correct architectural ownership; needs more distributed-custody validation |
| JMAP | broad RFC coverage including Blob, Quotas, Sieve, WebSocket | substantial Mail/Contacts/Calendar/Tasks, upload/download, WebSocket/events, canonical journal | strongest modern protocol path; continue depth and state recovery testing |
| IMAP | IMAP4rev2/rev1 plus extensive extensions | compatibility layer with common commands, UID, flags, search, append, idle | sufficient MVP; gap remains extension breadth and formal interoperability matrix |
| POP3 | supported | not planned in current protocol order | acceptable gap; do not add before current protocol depth |
| ActiveSync | not a Stalwart differentiator | first-class mobile/native compatibility adapter | `LPE` differentiator; needs real Outlook mobile/iOS lab evidence |
| EWS/MAPI | not Stalwart's main target | major `0.1.3` focus for Outlook/Exchange compatibility | strategic differentiator; still bounded by unsupported EWS operations and MAPI ROP gaps |
| DAV/WebDAV | CalDAV, CardDAV, WebDAV, file storage | CalDAV/CardDAV/tasks over canonical models; no WebDAV file storage | acceptable for current scope; file storage should wait for documented need |
| Sieve | broad Sieve and ManageSieve extensions | basic ManageSieve plus fileinto/discard/redirect/vacation | adequate MVP; far behind Stalwart extension surface |
| Storage | pluggable data/blob/search/memory stores | PostgreSQL primary, attachment blobs in DB, local `LPE-CT` technical state | simpler and maintainable; scalability bottleneck is blob/search/ephemeral separation |
| Search | pluggable search engines and language breadth | PostgreSQL search and selected attachment indexing | aligned with current policy; less scalable and less multilingual than Stalwart |
| Clustering | any node can serve protocols; distributed queues | core state anchored in PostgreSQL; replaceable `LPE-CT`; HA scripts | credible small/medium HA path, not Stalwart-class horizontal cluster |
| Admin UX | rebuilt WebUI/CLI around JMAP management API | admin API, web admin, `LPE-CT` management UI and diagnostics | functional direction; needs stronger declarative/idempotent operations story |
| Observability | OpenTelemetry/Prometheus/logging/webhooks/live telemetry | metrics, trace IDs, structured logs, dashboards, diagnostics | good foundations; missing full SLO dashboards and production trace playbooks |
| AI/security filtering | built-in spam/phishing, statistical, reputation, LLM analysis | local-only AI-compatible model; `LPE-CT` filtering and Magika validation | better privacy constraint; less complete filtering maturity |

## Priority Findings

### P0: Preserve The Split Architecture

Stalwart's biggest product strength is also the wrong architecture for `LPE`: one clustered server can own SMTP, mailbox, collaboration, management, and edge behavior. `LPE` explicitly separates core canonical state from `LPE-CT` perimeter custody. Closing gaps by collapsing SMTP back into core `LPE` would violate local architecture, weaken DMZ separation, and create parallel custody semantics.

Action: benchmark Stalwart's operational outcomes, not its topology. Required outcome parity is queue custody, replay safety, policy traceability, admin recovery, and high availability, implemented through the `LPE` / `LPE-CT` bridge.

### P1: Prove Real-Client Interoperability Before Protocol Breadth

`LPE` has many protocol surfaces in code. Stalwart's advantage is mature standards depth across fewer client-adoption priorities. For `LPE`, Outlook paths are first-class: IMAP for current desktop compatibility, ActiveSync for mobile, EWS and MAPI over HTTP for Exchange-style compatibility.

Action: prioritize repeatable interoperability labs:

- Outlook desktop IMAP first-login, UID stability, folder rename/delete/copy, flag sync, large mailbox refresh
- Outlook mobile and iOS ActiveSync provisioning, sync keys, send, smart reply/forward, attachment retrieval, long-poll reconnect
- EWS and MAPI over HTTP profile creation, mailbox sync, NSPI resolution, draft/send, reconnect, and authoritative `Sent`
- JMAP session, query/changes/queryChanges, WebSocket reconnect, shared mailbox rights, delegated identity, and Bcc-safe projections

### P1: `LPE-CT` Needs Distributed Custody Benchmarks

Stalwart's distributed SMTP queues are a high bar. `LPE-CT` has queue, quarantine, trace, retry, release, delete, and bridge tests, but normal tests still skip several env-sensitive custody paths.

Action: add deployment-level tests that exercise:

- accepted inbound message survives `LPE-CT` restart before core delivery
- outbound handoff idempotency across repeated `trace_id` and `remote_message_ref`
- terminal queue states never regress after process restart
- quarantine release/reject/delete behavior across node replacement
- no duplicate final delivery when bridge timeouts race with successful core commit

### P1: Storage Scalability Needs A Clear Next Boundary

Stalwart's four-store model is operationally mature: metadata, blobs, search, and ephemeral state can scale independently. `LPE` currently keeps the architecture simpler with PostgreSQL as the primary store and database-backed attachment blobs.

Action: do not add pluggable backends prematurely. Instead define one concrete next boundary:

- keep PostgreSQL as metadata authority
- formalize canonical blob storage abstraction for attachment and MIME blobs
- keep PostgreSQL search as default, but document the trigger point for a dedicated search backend
- keep `LPE-CT` local technical stores non-canonical

### P2: Management Should Become More Declarative

Stalwart v0.16 moved management to unified JMAP objects and an idempotent CLI apply flow. `LPE` should not copy that surface, but should absorb the operational lesson: administrators need repeatable reconciliation, not only hand-driven API/UI mutations.

Action: add an `LPE` declarative admin plan model after `0.1.3` Outlook gates. Start with domains, accounts, aliases, accepted domains, DKIM/publication policy, and `LPE-CT` routing policy.

### P2: Security Filtering Is Behind Stalwart's Breadth

`LPE-CT` has important perimeter foundations: Magika validation, quarantine, greylisting, reputation-oriented state, DKIM signing, policy scoring, trace logs, and structured transport results. Stalwart is broader in DNSBL, spam/phishing, reporting, reputation, DKIM rotation, DANE, MTA-STS, TLS reporting, and automated DNS workflows.

Action: close the gap in this order:

- traceable SPF/DKIM/DMARC decision model
- DMARC aggregate and failure report ingestion/visualization
- DKIM key lifecycle and rotation
- MTA-STS/TLS-RPT/DANE publication and validation
- reputation and Bayesian training lifecycle
- optional local-only LLM filtering after deterministic controls are solid

### P2: Observability Needs Operator-Facing SLOs

Stalwart's benchmark is not merely exposing metrics; it is making live operations understandable. `LPE` exposes health, metrics, trace IDs, dashboard data, and diagnostics, but the audit did not find a complete SLO set for mail-flow custody.

Action: define SLOs for:

- submission accepted to canonical `Sent`
- canonical `Sent` to `LPE-CT` handoff
- Internet accepted to core final delivery
- quarantine decision latency
- outbound retry aging
- JMAP push reconnect recovery
- ActiveSync long-poll reconnect recovery

## Product Gap Summary

`LPE` should not try to match Stalwart feature-for-feature in the next cycle. The strongest defensible `LPE` position is:

- privacy-preserving, Apache-2.0, local-first architecture
- explicit DMZ sorting center rather than all-in-one public exposure
- PostgreSQL-backed canonical state with clear Bcc and attachment validation rules
- Outlook and native-client compatibility as a differentiator
- JMAP depth as the modern protocol axis

The highest-risk gaps are operational, not code volume:

- real-client interoperability proof
- cross-process queue custody proof
- node replacement and restore drills
- declarative operations and policy reconciliation
- production-grade mail security reporting

## Recommended Roadmap

1. Finish `0.1.3` protocol depth gates:
   - JMAP state/change/push consistency
   - IMAP Outlook/Thunderbird compatibility transcripts
   - ActiveSync mobile/iOS compatibility lab
   - EWS bounded operation matrix
   - MAPI over HTTP profile creation and mailbox sync

2. Turn `LPE-CT` custody into an audited invariant:
   - durable inbound spool tests
   - outbound handoff replay tests
   - quarantine release/reject/delete recovery tests
   - duplicate-suppression tests under bridge timeout

3. Add operations benchmarks:
   - cold start
   - mailbox list/query latency
   - JMAP queryChanges and WebSocket reconnect latency
   - IMAP SELECT/FETCH/SEARCH latency on realistic mailbox sizes
   - ActiveSync Sync/Ping latency
   - SMTP DATA acceptance to final delivery
   - outbound retry throughput

4. Document next storage boundary:
   - canonical blob abstraction
   - PostgreSQL default search retention
   - future dedicated search backend trigger
   - non-canonical `LPE-CT` local store limits

5. Add declarative administration after the Outlook release gates:
   - idempotent plan format
   - diff/preview/apply workflow
   - rollback guidance
   - explicit license review for any new dependency

## Sources

- Stalwart repository README: <https://github.com/stalwartlabs/stalwart>
- Stalwart RFC implementation documentation: <https://stalw.art/docs/development/rfcs/>
- Stalwart storage documentation: <https://stalw.art/docs/storage/>
- Stalwart clustering documentation: <https://stalw.art/docs/cluster/>
- Stalwart v0.16 release blog: <https://stalw.art/blog/stalwart-0-16/>
