# LPE-CT Mail Filtering Operations

## Purpose

This document describes the operational controls now enforced by `LPE-CT` on inbound mail before final delivery to `LPE`.

`LPE-CT` remains the Internet-facing `SMTP` edge, the quarantine owner, and the perimeter decision point.

## Active controls

Inbound messages now go through these stages:

1. raw SMTP ingestion and trace creation
2. optional drain-mode hold
3. attachment validation with `Magika`
4. greylisting on `(source IP, MAIL FROM, first RCPT TO)`
5. DNSBL lookups on the connecting IP
6. SPF, DKIM, and DMARC evaluation
7. simple sender reputation weighting
8. final decision: `accept`, `defer`, `quarantine`, or `reject`

The resulting decision trace is persisted with the queued message JSON in the local spool.

## Main environment variables

`LPE_CT_GREYLISTING_ENABLED`

- default: `true`
- first-seen triplets are deferred for `90` seconds before a later retry is accepted

`LPE_CT_REQUIRE_SPF`

- default: `true`
- if SPF fails and no DKIM pass is available, `LPE-CT` can reject at the perimeter

`LPE_CT_REQUIRE_DKIM_ALIGNMENT`

- default: `false`
- when enabled, missing aligned DKIM causes quarantine

`LPE_CT_REQUIRE_DMARC_ENFORCEMENT`

- default: `true`
- DMARC `reject` dispositions are enforced as SMTP rejects

`LPE_CT_DEFER_ON_AUTH_TEMPFAIL`

- default: `true`
- if SPF, DKIM, or DMARC returns a temporary authentication failure, `LPE-CT` replies with `451` instead of silently failing open

`LPE_CT_DNSBL_ENABLED`

- default: `true`
- enables DNSBL/RBL lookups on the source IP

`LPE_CT_DNSBL_ZONES`

- default: `zen.spamhaus.org,bl.spamcop.net`
- comma-separated zones queried in order

`LPE_CT_REPUTATION_ENABLED`

- default: `true`
- enables a local reputation score per `(source IP, sender domain)`

`LPE_CT_REPUTATION_QUARANTINE_THRESHOLD`

- default: `-4`
- a sender reputation at or below this threshold forces quarantine before final delivery

`LPE_CT_REPUTATION_REJECT_THRESHOLD`

- default: `-8`
- a sender reputation at or below this threshold forces an SMTP reject

`LPE_CT_SPAM_QUARANTINE_THRESHOLD`

- default: `5.0`
- messages at or above this score are quarantined

`LPE_CT_SPAM_REJECT_THRESHOLD`

- default: `9.0`
- messages at or above this score are rejected

## Spool and policy state

`LPE_CT_SPOOL_DIR` now contains additional policy data:

- `greylist/`: first-seen triplets and release timestamps
- `policy/reputation.json`: simple accumulated reputation counters
- `policy/<rule>.json`: local throttling windows keyed by routing/throttle rule

Message trace files now also persist:

- `spam_score`
- `security_score`
- `reputation_score`
- `dnsbl_hits`
- `auth_summary`
- `decision_trace`

`auth_summary` is now derived from structured SPF/DKIM/DMARC results rather than string matching on debug output. The decision trace also records:

- SPF domain used for evaluation
- RFC 5322 `From` domain used for `DMARC`
- SPF and DKIM alignment gaps
- temporary DNS or authentication failures that caused `defer`

This trace is the primary operational artifact for explaining why a message was deferred, quarantined, rejected, or accepted.

Outbound trace files now also persist:

- applied routing rule and selected relay target
- local throttling decisions
- last SMTP phase and reply code
- enhanced status code when available
- `DSN` action and diagnostic for deferred/bounced deliveries

Permanent outbound failures are copied to `bounces/` while the operational queue copy remains in `held/`.

## SMTP outcomes

Typical perimeter outcomes are:

- `250 queued as <trace>`: accepted for delivery or quarantine ownership
- `250 quarantined as <trace>`: accepted but retained locally in quarantine
- `451 message temporarily deferred by perimeter policy (trace <trace>)`: greylisting or transient auth/DNS conditions
- `554 message rejected by perimeter policy (trace <trace>)`: hard reject, for example enforced DMARC reject

The decision matrix is now intentionally stricter:

- DMARC `reject` enforces SMTP reject when `LPE_CT_REQUIRE_DMARC_ENFORCEMENT=true`
- DMARC `quarantine` enforces quarantine when DMARC enforcement is enabled
- SPF `fail` rejects only when no aligned DKIM pass compensates
- DKIM alignment can independently force quarantine when `LPE_CT_REQUIRE_DKIM_ALIGNMENT=true`
- authentication temporary failures can produce `451` when `LPE_CT_DEFER_ON_AUTH_TEMPFAIL=true`
- poor sender/IP reputation can force quarantine or reject before spam-score thresholds are reached

## Operational recommendations

- keep recursive DNS resolvers reachable and low-latency, because SPF, DKIM, DMARC, and DNSBL now depend on them
- monitor the size of `greylist/` and `policy/reputation.json` during the first production weeks
- tune `LPE_CT_SPAM_QUARANTINE_THRESHOLD` and `LPE_CT_SPAM_REJECT_THRESHOLD` conservatively before tightening
- review quarantined trace files before enabling `LPE_CT_REQUIRE_DKIM_ALIGNMENT=true`
- keep at least one DNSBL zone highly reliable; too many low-quality zones increase false positives and latency

## Current scope

This implementation executes inbound SPF, DKIM, and DMARC verification, DNSBL checks, greylisting, reputation weighting, and detailed trace persistence.

It also now executes outbound routing selection, local throttling, and SMTP-result classification into `relayed`, `deferred`, `bounced`, or `failed`, with structured `DSN`/technical trace feedback for the `LPE` worker.

If those policy artifacts outgrow flat files, they may move into a dedicated private `LPE-CT` database on non-public `5432`, but only as technical perimeter state and never as canonical mailbox state.

It does not yet add:

- outbound DKIM signing
- inbound or outbound `ARC`
- `MTA-STS` policy fetch and TLS-policy enforcement
- `TLS-RPT` report generation and sending
- advanced reputation feeds beyond the local `(IP, sender domain)` spool state
- DNSSEC validation or DANE for SMTP transport

The current lot prepares those follow-ups by persisting structured authentication outcomes, richer technical status, and explicit defer/quarantine/reject reasons in the spool trace.
