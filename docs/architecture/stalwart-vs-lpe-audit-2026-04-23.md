# Stalwart vs LPE Technical Audit

Date: 2026-04-23

## Scope

This audit compares the current `LPE` repository state against `Stalwart` as a product and architecture benchmark only.

No `Stalwart` code is reused here. `LPE` remains constrained by `Apache-2.0` project licensing and the repository dependency policy documented in `LICENSE.md`.

## Primary Inputs

### Local `LPE`

- `ARCHITECTURE.md`
- `docs/architecture/initial-architecture.md`
- `docs/architecture/lpe-ct-integration.md`
- `docs/architecture/edge-and-protocol-exposure.md`
- `docs/architecture/high-availability.md`
- `LICENSE.md`
- workspace code and tests under `crates/` and `LPE-CT/`

### External `Stalwart`

- repository root and README: <https://github.com/stalwartlabs/stalwart>
- workspace manifest: <https://github.com/stalwartlabs/stalwart/blob/main/Cargo.toml>
- docs: <https://stalw.art/docs/install/get-started/>
- docs: <https://stalw.art/docs/cluster/overview/>
- docs: <https://stalw.art/docs/auth/overview/>
- docs: <https://stalw.art/docs/collaboration/overview/>

## Executive Summary

`Stalwart` is materially ahead of `LPE` in protocol breadth, edge-mail maturity, clustering, storage flexibility, operational completeness, and market-readiness.

`LPE` is materially ahead of a generic greenfield mail project in one specific area: it already enforces a coherent canonical-state architecture around `PostgreSQL`, `Sent` authority, protected `Bcc`, delegated submission rights, and a strict separation between the core collaboration system and the `DMZ` sorting center.

The practical conclusion is:

- if the target is a production-ready all-in-one mail platform today, `Stalwart` is the benchmark to beat and `LPE` is not there yet
- if the target is a deliberately split core-plus-sorting-center architecture with strict canonical-state guarantees and Outlook/mobile-first convergence rules, `LPE` already has a defensible architectural identity that is meaningfully different from `Stalwart`

## Observed Repository Maturity

### `LPE`

- local commit count: `176`
- Rust source files under `crates/`: `126`
- Rust source size under `crates/`: about `1.8 MB`
- `LPE-CT` Rust source files: `4`
- `LPE-CT` Rust source size: about `310 KB`
- workspace crates: `14`
- detected Rust unit/integration-style test attributes under `crates/`: `192`
- `cargo test --workspace --lib`: passed
- `cargo test` in `LPE-CT`: passed

### `Stalwart`

- GitHub repository page shows `1,371` commits on `main`
- workspace members in `Cargo.toml`: `27`
- repository claims feature completeness and positions the next milestone as schema/performance refinement toward `1.0`

## Architecture Comparison

### 1. System shape

`Stalwart` is a unified all-in-one server. It handles `SMTP`, `JMAP`, `IMAP`, `POP3`, collaboration, filtering, administration, and clustering in one integrated product surface.

`LPE` is intentionally split:

- `LPE` core owns canonical mailbox, contacts, calendars, tasks, rights, and user-visible state
- `LPE-CT` owns Internet-facing `SMTP`, relay, quarantine, perimeter policy, and traceability

Audit result:

- `Stalwart` wins on deployment simplicity and integrated operations
- `LPE` wins on boundary clarity for security zoning and canonical-state discipline

### 2. Canonical message model

`LPE` has a strong and explicit canonical submission contract:

- authoritative `Sent` copy exists before transport handoff
- `Bcc` stays protected and out of user search projections
- all submission paths are intended to converge on one internal model
- outbound queue state is stored canonically in `PostgreSQL`

This is visible in both docs and tests, especially in `lpe-storage` and protocol-adapter test coverage.

`Stalwart` is broader and more mature overall, but from the examined public sources it presents itself primarily as a unified mail server rather than as a core-vs-edge canonical-state split.

Audit result:

- `LPE` has a sharper explicit model for canonical user-visible state versus edge transport state
- this is one of the strongest differentiators in the current repository

### 3. Protocol surface

`Stalwart` publicly claims support for:

- `JMAP`
- `IMAP4rev1` and `IMAP4rev2`
- `POP3`
- `SMTP`
- `ManageSieve`
- `CalDAV`
- `CardDAV`
- `WebDAV`
- `JMAP` collaboration and sharing extensions

`LPE` currently implements and tests:

- `JMAP` mail, contacts, calendars, tasks, uploads, WebSocket push
- `IMAP`
- `ActiveSync`
- `ManageSieve`
- `CardDAV` / `CalDAV` / `VTODO`
- webmail and admin APIs
- `LPE-CT` authenticated client submission and edge `SMTP`

Audit result:

- `LPE` is already unusually broad for a pre-1.0 codebase
- `Stalwart` still leads clearly on protocol completeness and breadth, especially `POP3`, WebDAV/file-storage, SMTP feature depth, and documented extension support

### 4. Edge mail and transport

This is the largest gap.

`Stalwart` positions edge mail as a first-class integrated subsystem with:

- built-in `DMARC`, `DKIM`, `SPF`, `ARC`
- DANE, `MTA-STS`, TLS reporting
- distributed queues
- routing, throttling, filtering, greylisting, reputation, and anti-phishing

`LPE-CT` already contains real code and tests for:

- relay routing and throttling
- quarantine and deferred/bounce classification
- `SPF` / `DKIM` / `DMARC`-aligned auth summaries
- `DNSBL`, greylisting, reputation, bayesian scoring, antivirus-provider parsing
- authenticated SMTP submission bridged back into canonical `LPE`

But `LPE-CT` is still much smaller and less operationally proven than Stalwart’s MTA stack.

Audit result:

- architecturally, `LPE-CT` is credible
- operationally and functionally, `Stalwart` remains far ahead

### 5. Storage and scale

`Stalwart` documents pluggable storage backends and native clustering for nodes that can all serve client protocols.

`LPE` intentionally standardizes on:

- `PostgreSQL` as the canonical store
- `PostgreSQL` search by default
- active/standby core writes only
- optional local technical stores only on the `LPE-CT` side

Audit result:

- `Stalwart` wins decisively on horizontal scale story and backend flexibility
- `LPE` wins on operational determinism and architectural simplicity for canonical-state correctness

### 6. High availability

`Stalwart` documents native clustering where any node can serve protocol traffic, plus distributed queue handling.

`LPE` explicitly rejects active/active core writers and supports:

- active/standby core
- primary/standby PostgreSQL outside the app
- active/standby or horizontally replaceable `LPE-CT`
- role-file readiness gating

Audit result:

- `Stalwart` is stronger for large-scale HA and fault tolerance
- `LPE` has a coherent first-step HA model, but it is intentionally conservative and less capable

### 7. Identity and authorization

`Stalwart` documents internal plus external directory/authentication support, including `OIDC`, `LDAP`, `SQL`, roles, permissions, and ACLs.

`LPE` currently includes:

- mailbox and admin authentication separation
- password auth, `OIDC`, `TOTP`, app-password paths
- delegated mailbox submission rights
- collaboration ACL and sender-delegation models in storage and JMAP/DAV projections

Audit result:

- `Stalwart` is broader as an identity platform
- `LPE` is already solid in the parts that directly support canonical mailbox workflows

## Strongest `LPE` Assets

### 1. Clear architectural identity

The `LPE` versus `LPE-CT` split is not superficial. It is reinforced by docs, runtime topology, queue handoff, readiness rules, and tests.

### 2. Canonical-state rigor

The repository consistently protects:

- authoritative `Sent`
- canonical draft and submission workflows
- protected `Bcc`
- delegation-aware sender identity
- protocol adapters as views over the same core state

### 3. Real protocol implementation depth

The crate and test footprint shows this is not a mock architecture:

- `JMAP` has significant behavioral coverage
- `IMAP`, `ActiveSync`, `DAV`, and `ManageSieve` have passing tests
- `LPE-CT` has real transport-policy logic, not only configuration placeholders

## Weakest `LPE` Areas Relative to Stalwart

### 1. Production readiness gap

The codebase is far smaller, younger, and less field-proven than `Stalwart`.

### 2. Edge operational depth

`LPE-CT` is promising but still far from Stalwart’s integrated MTA maturity in breadth, deployment guidance, and likely real-world hardening.

### 3. Scaling model

The deliberate active/standby core model protects correctness, but it also makes `LPE` less competitive for large multi-node environments.

### 4. Integrated admin and ops completeness

`Stalwart` exposes a much more complete all-in-one operations story: admin UI, queue management, DNS automation, observability, alerts, storage choices, and clustering guidance.

### 5. Protocol breadth outside the core priority path

`LPE` has chosen the right depth-first strategy for its architecture, but against `Stalwart` it still lacks breadth in completed protocol surface and associated interoperability evidence.

## Benchmark Scorecard

Scores are relative to the current repository state and use a 1-5 scale.

| Area | LPE | Stalwart | Notes |
| --- | --- | --- | --- |
| Canonical mailbox architecture | 5 | 4 | `LPE` is unusually explicit and disciplined here |
| Edge SMTP and filtering maturity | 3 | 5 | `Stalwart` is materially ahead |
| Protocol breadth | 4 | 5 | `LPE` is broad; `Stalwart` is broader and more complete |
| Protocol maturity | 3 | 5 | `LPE` shows credible tests; `Stalwart` appears much further along |
| Outlook/mobile strategy | 4 | 3 | `LPE` explicitly prioritizes `ActiveSync`; `Stalwart` public materials emphasize other protocols |
| Horizontal scale and clustering | 2 | 5 | `LPE` intentionally avoids active/active core |
| Operational simplicity | 3 | 5 | integrated product advantage for `Stalwart` |
| Security zoning clarity | 5 | 3 | `LPE` split architecture is stronger here |
| Storage flexibility | 2 | 5 | `LPE` is intentionally opinionated around `PostgreSQL` |
| License fit for `LPE` benchmarking | 5 | 1 | `Stalwart` is useful as a benchmark, not as reusable implementation material |

## Strategic Conclusions

### Where `LPE` should not try to imitate `Stalwart`

- do not collapse `LPE` and `LPE-CT` into one all-in-one server
- do not trade canonical-state guarantees for protocol or deployment breadth
- do not follow Stalwart’s licensing model or dependency posture
- do not add active/active core writing just to match a clustering checklist

### Where `LPE` should learn from `Stalwart`

- operational polish
- install and upgrade ergonomics
- admin observability depth
- explicit interoperability matrices
- performance characterization and benchmarking discipline
- clearer productized queue, trace, and policy-management workflows

## Priority Recommendations For `LPE`

### Tier 1

1. Finish protocol depth before adding new surfaces, especially:
   - `JMAP` state/change correctness and push recovery
   - `IMAP` sync and `UID` edge cases
   - `ActiveSync` long-poll and Outlook/iOS labs
2. Harden `LPE-CT` into an operationally serious sorting center:
   - spool recovery drills
   - queue inspection and retry UX
   - policy trace observability
   - integration-failure recovery tests
3. Publish reproducible benchmarks for:
   - message submission latency
   - inbound delivery throughput
   - IMAP sync behavior
   - JMAP push reconnect and replay

### Tier 2

1. Strengthen deployment artifacts for the active/standby model.
2. Add broader real-client interoperability documentation and automated fixtures.
3. Tighten admin/product surface around traceability, queue state, and delegated send flows.

### Tier 3

1. Expand `LPE-CT` transport sophistication only where it strengthens the existing split.
2. Delay new protocol families until the current ones are operationally convincing.

## Bottom Line

`Stalwart` is the current benchmark leader for an all-in-one Rust mail and collaboration server.

`LPE` is not competitive with `Stalwart` yet on overall product maturity, clustering, or integrated MTA completeness.

However, `LPE` is not just a smaller copy. Its strongest differentiator is a deliberate architecture:

- canonical user-visible state in the core
- strict edge isolation in `LPE-CT`
- no parallel `Sent` or mailbox models
- explicit protection of sensitive metadata like `Bcc`

If `LPE` continues to execute against that design and improves operational maturity, it can become a strong differentiated system even while `Stalwart` remains ahead in total feature completeness.
