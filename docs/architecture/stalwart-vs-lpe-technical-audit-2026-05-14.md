# Stalwart vs LPE Technical Audit - 2026-05-14

## Assumptions

- This is an architecture, implementation-maturity, and operations benchmark, not a synthetic runtime load test.
- Stalwart is used only as a public product and architecture benchmark. Its source code is not reused in `LPE`.
- `LPE` remains Apache-2.0 and must continue to follow `LICENSE.md`.
- The `LPE` / `LPE-CT` split is intentional and remains non-negotiable.

## Success Criteria

- Compare Stalwart and `LPE` across licensing, topology, protocols, storage, SMTP custody, security, operations, observability, and performance readiness.
- Identify where `LPE` is deliberately different from Stalwart rather than behind it.
- Identify gaps that matter for `0.2.0` and post-`0.2.0` production readiness.
- Verify the current `LPE` local test baseline.

## Verification

Commands run from `C:\Development\LPE` on 2026-05-14:

| Command | Result |
| --- | --- |
| `cargo test` | passed |
| `cargo test` in `LPE-CT/` | passed |
| `git status --short` | clean before audit document creation |

Observed test totals:

| Suite | Passed | Ignored | Notes |
| --- | ---: | ---: | --- |
| Core workspace | 531 | 12 | ignored cases are benchmark or environment-sensitive tests |
| `LPE-CT` | 85 | 19 | ignored cases are benchmark or environment-sensitive tests |

Implementation density snapshot:

| Component | Rust files | Lines |
| --- | ---: | ---: |
| `crates/lpe-exchange/src` | 20 | 44,932 |
| `crates/lpe-storage/src` | 26 | 28,305 |
| `crates/lpe-jmap/src` | 23 | 21,176 |
| `LPE-CT/src/smtp.rs` | 1 | 9,545 |
| `crates/lpe-imap/src` | 15 | 8,846 |
| `crates/lpe-admin-api/src` | 24 | 8,408 |
| `crates/lpe-activesync/src` | 12 | 6,749 |
| `crates/lpe-dav/src` | 11 | 3,250 |

The current Stalwart `main` HEAD observed with `git ls-remote` was `e049f9ff4fecdb51e0d2c3ee908a93b70e932b31`.

## Stalwart Benchmark Baseline

Stalwart is a mature all-in-one mail and collaboration server. Public upstream material presents it as supporting JMAP, IMAP4, POP3, SMTP, CalDAV, CardDAV, and WebDAV; SMTP authentication and transport security features including SPF, DKIM, DMARC, ARC, DANE, MTA-STS, and TLS reporting; broad spam and phishing controls; pluggable storage backends; clustering; OpenTelemetry/Prometheus observability; and WebUI/CLI management.

Important benchmark properties:

- Stalwart is dual licensed under AGPL-3.0 and the Stalwart Enterprise License, which makes direct source reuse incompatible with `LPE`.
- Stalwart's deployment shape is an integrated cluster where any node can serve core protocols.
- Its storage model separates data, blob, search, and in-memory stores.
- Its SMTP queue model is distributed and fault-tolerant across cluster nodes.
- Its administration model includes WebUI, CLI, declarative deployment concepts, alerts, reports, and live telemetry.

The useful benchmark lesson is operational outcome parity, not topology parity. `LPE` should measure itself against Stalwart's protocol correctness, custody, observability, storage recovery, and administration outcomes without collapsing SMTP edge duties into the core server.

## LPE Observed State

`LPE` has a narrower but strategically different architecture:

- Core `LPE` owns canonical mailbox, collaboration, rights, search, submission, and user-visible state.
- `LPE-CT` owns public SMTP ingress, authenticated submission, outbound relay, quarantine, filtering, DKIM, queue custody, and perimeter policy.
- PostgreSQL remains the canonical metadata authority.
- JMAP, IMAP, ActiveSync, EWS, MAPI over HTTP, DAV, and ManageSieve adapters exist and are tested locally.
- Exchange compatibility is a major differentiator: `lpe-exchange` is the largest crate, and MAPI/NSPI coverage is extensive for a `0.2.0` target.
- Bcc protection, canonical `Sent`, signed `LPE` / `LPE-CT` bridges, Magika validation, and storage-pool boundaries are explicit architectural invariants with tests.
- Storage pools now include database-backed and S3-compatible durable attachment and MIME-part blobs behind a `BlobStore` boundary; raw RFC 5322 message blobs remain database-backed.

The current risk is no longer absence of protocol surfaces. The risk is proving that all those surfaces stay correct under real clients, realistic mailbox sizes, restart/replay conditions, and operator recovery workflows.

## Benchmark Matrix

| Area | Stalwart benchmark | Current `LPE` position | Assessment |
| --- | --- | --- | --- |
| License | AGPL-3.0 / enterprise dual license | Apache-2.0 with strict dependency policy | Direct reuse forbidden; architecture benchmarking only |
| Topology | all-in-one clustered mail/collaboration server | split core plus `LPE-CT` DMZ sorting center | Deliberate difference; preserve split |
| SMTP/MTA | integrated SMTP, distributed virtual queues, routing, reports | `LPE-CT` edge, relay, quarantine, bridge custody, replay tests | Correct ownership; needs production multi-node drills |
| JMAP | broad JMAP family support | mail, contacts, calendars, tasks, blobs/uploads, WebSocket, state replay | Strong path; continue live state/change/push evidence |
| IMAP | IMAP4rev2/rev1 and many extensions | compatibility layer with UID, flags, search, append, IDLE, CONDSTORE coverage | Good MVP; lower extension breadth |
| POP3 | supported | not in current protocol order | Acceptable gap; do not add before current depth gates |
| ActiveSync | not a primary Stalwart differentiator | first-class mobile/native compatibility adapter | `LPE` differentiator; real-device lab remains key evidence |
| EWS/MAPI | not Stalwart's focus | major `0.2.0` Outlook path | Strategic differentiator; keep bounded and gated |
| DAV/WebDAV | CalDAV, CardDAV, WebDAV, JMAP file storage | CalDAV/CardDAV/tasks; no general WebDAV file storage | Acceptable for current scope |
| Sieve | broad Sieve and ManageSieve surface | ManageSieve plus focused script execution support | Adequate MVP; far less complete |
| Storage | separate data/blob/search/memory stores, many backends | PostgreSQL metadata, BlobStore boundary, S3-compatible durable blobs | Simpler and coherent; search and raw message blob split remain future work |
| Search | pluggable search engines, multilingual FTS claims | PostgreSQL search default, selected attachment text indexing | Aligned with LPE policy; less scalable |
| Clustering | any node can handle protocols; distributed queues | HA scripts, core PostgreSQL authority, `LPE-CT` local custody | credible smaller HA path, not Stalwart-class horizontal cluster |
| Security filtering | broad spam/phishing, DNSBL, reputation, reports, DKIM rotation | SPF/DKIM/DMARC policy tests, greylisting, Bayesian scoring, quarantine, antivirus hook, Magika | improved since prior audit; still behind breadth of reporting and automated DNS/security lifecycle |
| Observability | OTel, Prometheus, webhooks, alerts, live telemetry, history | structured logs, metrics, trace IDs, dashboard/diagnostics | foundations exist; alerts/live telemetry/history are weaker |
| Admin operations | WebUI, CLI, declarative deployment, queue/report management | admin API/UI, `LPE-CT` management UI, install scripts | needs idempotent declarative operations story |

## Priority Findings

### P0: Do Not Collapse `LPE-CT` Into Core `LPE`

Stalwart's integrated MTA is a strength for Stalwart, but the wrong answer for `LPE`. `LPE` should benchmark Stalwart's custody and operations outcomes while preserving the DMZ sorting center. Moving public SMTP or canonical perimeter policy back into core `LPE` would violate the documented architecture and weaken isolation.

### P1: Convert Local Protocol Breadth Into Real-Client Evidence

The local protocol test surface is strong, especially JMAP, MAPI, ActiveSync, and `LPE-CT` custody. The next credibility threshold is repeatable live evidence:

- Outlook desktop IMAP, MAPI over HTTP, reconnect, cached mode, and canonical `Sent`.
- Outlook mobile and iOS ActiveSync enrollment, send, sync, long-poll, stale-key, and attachment flows.
- JMAP shared/delegated state, WebSocket reconnect, query changes, and Bcc-safe projections.
- EWS bounded matrix with unsupported operations remaining parseable and explicit.

### P1: Treat `LPE-CT` Custody As A Release Gate

The 2026-05-14 tree has better custody evidence than the earlier audit: accepted inbound spool restart, bridge failure retention, outbound replay suppression, terminal-state non-regression, and quarantine node-replacement tests exist. The next step is deployment-level proof under actual multi-process or multi-node conditions:

- crash during `DATA` acceptance,
- crash after accept before bridge delivery,
- bridge timeout after successful core commit,
- repeated outbound handoff after relay success,
- quarantine release/reject/delete during node replacement,
- replay after local spool restore from backup.

### P1: Storage Boundary Is Moving In The Right Direction

The prior audit called out lack of clear blob separation. The current roadmap and tests show a concrete `BlobStore` boundary, placement metadata, migration jobs, quota stability, cleanup guards, and S3-compatible durable blobs for attachments and MIME parts. This should continue incrementally. Do not introduce a broad Stalwart-style backend matrix until restore and degradation behavior are proven for the existing boundary.

### P2: Security Filtering Still Needs Reporting And Lifecycle Depth

`LPE-CT` now has meaningful tests around SPF/DKIM/DMARC decisions, greylisting, reputation, Bayesian scoring, quarantine, and antivirus output parsing. Stalwart remains ahead in breadth: DNSBLs, TLS reporting, DMARC report analysis, phishing defenses, DKIM rotation, automated DNS, and operator-facing report workflows.

Recommended order:

1. DKIM key lifecycle and rotation.
2. DMARC aggregate/failure report ingestion and visualization.
3. TLS-RPT and MTA-STS validation/reporting.
4. DNSBL and reputation lifecycle with false-positive controls.
5. Local-only LLM filtering only after deterministic controls are operationally proven.

### P2: Observability Needs Mail-Flow SLOs

`LPE` and `LPE-CT` expose logs, metrics, trace IDs, health, and diagnostics. Stalwart's benchmark is broader: alerts, webhooks, live telemetry, and retained history. `LPE` should define SLOs and dashboards around custody, not just component health:

- accepted SMTP message to core delivery,
- authenticated submission to canonical `Sent`,
- canonical `Sent` to `LPE-CT` handoff,
- outbound queue age by state,
- quarantine decision latency,
- JMAP push reconnect recovery,
- ActiveSync long-poll reconnect recovery,
- MAPI reconnect and request replay behavior.

### P2: Declarative Administration Is The Largest Operator Gap

Stalwart's management story is stronger because it gives operators repeatable configuration and reconciliation paths. `LPE` should add an idempotent plan/apply model after the Outlook gates, starting with domains, accounts, aliases, accepted domains, DKIM policy, routing policy, storage policy, and `LPE-CT` edge publication.

## Performance Benchmark Plan

No runtime load benchmark was run because no workload, mailbox corpus, deployment topology, hardware, or SLA target was provided. The smallest useful benchmark suite should measure:

| Scenario | Metric |
| --- | --- |
| JMAP mailbox sync | session load, `Email/query`, `Email/queryChanges`, WebSocket reconnect latency |
| IMAP client refresh | `LOGIN`, `LIST`, `SELECT`, `UID FETCH`, `SEARCH`, IDLE change propagation |
| ActiveSync mobile sync | `FolderSync`, paged `Sync`, `Ping`, stale-key recovery |
| EWS/MAPI Outlook path | profile creation, folder sync, content sync, reconnect, request replay |
| SMTP ingress | `DATA` accept latency, bridge delivery latency, deferred retry latency |
| Authenticated submission | submission to canonical `Sent`, then handoff to `LPE-CT` |
| Queue recovery | duplicate suppression and terminal-state stability after restart |
| Storage | attachment read/write/stat/verify across database-backed and S3-compatible placements |
| Search | common mailbox search over realistic message counts and attachment indexes |

Each scenario should run at 1k, 10k, and 100k messages per test mailbox, with at least one shared mailbox and one delegated sender case.

## Recommended Roadmap

1. Keep `0.2.0` focused on protocol depth and Outlook/native-client proof.
2. Promote `LPE-CT` spool recovery and custody tests into CI where environment allows.
3. Add the performance benchmark suite above before adding new protocol families.
4. Finish storage Milestone 7 only through the existing `BlobStore` boundary.
5. Add operator SLO dashboards and alert thresholds for custody and protocol sync.
6. Design declarative admin apply after the Outlook release gates.
7. Document every new dependency or external implementation idea against `LICENSE.md`.

## Sources

- Stalwart README: https://github.com/stalwartlabs/stalwart
- Stalwart license statement in README: https://raw.githubusercontent.com/stalwartlabs/stalwart/main/README.md
- Stalwart email protocol documentation: https://stalw.art/docs/email/
- Stalwart storage documentation: https://stalw.art/docs/storage/
- Stalwart clustering documentation: https://stalw.art/docs/cluster/
- Stalwart outbound queue documentation: https://stalw.art/docs/mta/outbound/queue/
- Stalwart telemetry documentation: https://stalw.art/docs/telemetry/
- Local `LPE` documents: `ARCHITECTURE.md`, `docs/architecture/initial-architecture.md`, `LICENSE.md`, `docs/architecture/0.2.0-protocol-depth-gates.md`, `docs/architecture/lpe-ct-integration.md`, `docs/architecture/lpe-ct-local-data-stores.md`, `docs/architecture/observability.md`, `docs/architecture/mailbox-storage-pools-roadmap.md`
