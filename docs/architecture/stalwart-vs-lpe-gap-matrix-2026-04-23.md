# Stalwart vs LPE Gap Matrix

Date: 2026-04-23

This companion document turns the broader audit into a subsystem-by-subsystem gap matrix with explicit implementation priority.

Primary sources are the same as in [stalwart-vs-lpe-audit-2026-04-23.md](C:/Development/LPE/docs/architecture/stalwart-vs-lpe-audit-2026-04-23.md).

## Scoring

- `LPE status`: current repository position, from `1` to `5`
- `Stalwart benchmark`: benchmark strength, from `1` to `5`
- `Gap`: `Stalwart benchmark - LPE status`
- `Priority`: recommended implementation order for `LPE`

## Matrix

| Subsystem | LPE status | Stalwart benchmark | Gap | Priority | Assessment |
| --- | --- | --- | --- | --- | --- |
| Canonical submission and `Sent` authority | 5 | 4 | -1 | 1 | `LPE` is already stronger in explicit canonical-state discipline |
| Protected `Bcc` handling | 5 | 4 | -1 | 1 | strong differentiator, already reflected in tests and schema |
| `JMAP` mail core semantics | 4 | 5 | 1 | 1 | real implementation exists, but depth and interoperability still need work |
| `JMAP` push / reconnect / replay | 4 | 5 | 1 | 1 | good local signal, still a critical reliability area |
| `IMAP` sync correctness and compatibility | 3 | 5 | 2 | 1 | implemented, but still behind a mature server benchmark |
| `ActiveSync` Outlook / mobile behavior | 3 | 3 | 0 | 1 | strategically important for `LPE`; must be finished even if not Stalwart’s headline strength |
| `DAV` correctness | 3 | 4 | 1 | 2 | useful and real, but behind higher-priority mail paths |
| `ManageSieve` and mailbox `Sieve` | 3 | 5 | 2 | 2 | credible implementation, lower urgency than core sync paths |
| Edge `SMTP` ingress / relay / policy | 3 | 5 | 2 | 1 | biggest practical product gap |
| Queue traceability and operator workflow | 3 | 5 | 2 | 1 | architecture is there; operational polish is not |
| Anti-abuse and filtering depth | 3 | 5 | 2 | 2 | `LPE-CT` has real features but far less maturity |
| Admin and operations UX | 3 | 5 | 2 | 2 | `Stalwart` is much more complete operationally |
| Installation and upgrade path | 2 | 5 | 3 | 2 | major gap for deployability and adoption |
| Observability and diagnostics | 3 | 5 | 2 | 2 | foundations exist; productized telemetry is behind |
| High availability and failover | 2 | 5 | 3 | 3 | `LPE` is intentionally conservative here |
| Horizontal scaling and clustering | 2 | 5 | 3 | 4 | not the near-term identity for `LPE` |
| Storage backend flexibility | 2 | 5 | 3 | 4 | intentionally out of scope for now |
| Identity backend breadth | 3 | 5 | 2 | 3 | sufficient for core paths, but narrower |
| Collaboration breadth beyond current adapters | 3 | 5 | 2 | 3 | lower priority than mail correctness |
| WebDAV / file-storage collaboration | 1 | 5 | 4 | 5 | large gap, but outside current depth-first strategy |
| `POP3` | 1 | 4 | 3 | 5 | intentionally not a current product priority |

## Priority Order

### Priority 1: finish the current flagship paths

1. `JMAP` state, change, and push reliability
2. `IMAP` sync correctness and client compatibility
3. `ActiveSync` Outlook and mobile labs
4. `LPE-CT` edge relay, retry, quarantine, and queue operator workflow
5. preserve canonical submission guarantees while hardening these paths

Reason:

This is the shortest path to turning `LPE` from a promising architecture into a convincing product for real mailbox workloads.

### Priority 2: make operations credible

1. admin observability
2. trace and queue tooling
3. install and upgrade discipline
4. diagnostics around filtering, relay, and handoff failures
5. `DAV` plus `Sieve` completion after the mail-critical paths stabilize

Reason:

This is where `Stalwart` is visibly ahead as an operator-facing product.

### Priority 3: broaden support around the stabilized core

1. stronger identity backend options
2. richer collaboration behavior
3. more complete HA drills and runbooks

Reason:

These matter, but they do not fix the highest-risk product gaps first.

### Priority 4: scale architecture

1. stronger `LPE-CT` horizontal deployment patterns
2. more advanced failover automation
3. optional non-default storage patterns if they become strategically necessary

Reason:

`LPE` should not optimize for distributed topology before it fully proves canonical correctness and client interoperability.

### Priority 5: deliberately deferred breadth

1. `POP3`
2. broad WebDAV/file-storage scope
3. storage-backend plurality for its own sake

Reason:

These would dilute the current architecture and delivery strategy.

## Recommended 12-Week Focus

### Track A: protocol correctness

1. Close the highest-risk `JMAP` state and WebSocket recovery edge cases.
2. Expand `IMAP` sync tests around `UID`, flags, mailbox membership changes, and real-client replay patterns.
3. Run and document `ActiveSync` interoperability labs with Outlook and iOS.

### Track B: sorting-center hardening

1. Strengthen `LPE-CT` spool inspection, replay, and quarantine workflows.
2. Add failure-injection tests for `LPE <-> LPE-CT` handoff and recovery.
3. Improve operator-visible diagnostics for routing, throttling, and DSN classification.

### Track C: productization

1. Tighten install, upgrade, and rollback guidance.
2. Expand metrics and admin views around queue state and sync health.
3. Publish benchmark numbers for submission, inbound delivery, `JMAP` push recovery, and `IMAP` refresh behavior.

## What To Measure Against Stalwart

For future audits, compare these concrete metrics instead of broad feature claims:

- median and p95 canonical submission latency
- inbound final-delivery latency into `Inbox`
- outbound retry and duplicate-handoff correctness
- `JMAP` reconnect recovery time after push interruption
- `IMAP` resync cost after flag and mailbox mutations
- `ActiveSync` long-poll stability and resend behavior
- operator time to identify and replay a quarantined or deferred message
- active/standby failover recovery time for both `LPE` and `LPE-CT`

## Bottom Line

The correct competitive move for `LPE` is not to mimic `Stalwart` feature-for-feature.

The correct move is:

1. preserve `LPE`’s stronger canonical-state and security-zone architecture
2. finish the already implemented protocol family to a much higher degree of interoperability
3. turn `LPE-CT` into an operationally serious sorting center
4. only then consider broader scale or broader protocol surface
