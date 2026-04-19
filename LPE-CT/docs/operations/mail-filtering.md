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

`LPE_CT_DNSBL_ENABLED`

- default: `true`
- enables DNSBL/RBL lookups on the source IP

`LPE_CT_DNSBL_ZONES`

- default: `zen.spamhaus.org,bl.spamcop.net`
- comma-separated zones queried in order

`LPE_CT_REPUTATION_ENABLED`

- default: `true`
- enables a local reputation score per `(source IP, sender domain)`

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

Message trace files now also persist:

- `spam_score`
- `security_score`
- `reputation_score`
- `dnsbl_hits`
- `auth_summary`
- `decision_trace`

This trace is the primary operational artifact for explaining why a message was deferred, quarantined, rejected, or accepted.

## SMTP outcomes

Typical perimeter outcomes are:

- `250 queued as <trace>`: accepted for delivery or quarantine ownership
- `250 quarantined as <trace>`: accepted but retained locally in quarantine
- `451 message temporarily deferred by perimeter policy (trace <trace>)`: greylisting or transient auth/DNS conditions
- `554 message rejected by perimeter policy (trace <trace>)`: hard reject, for example enforced DMARC reject

## Operational recommendations

- keep recursive DNS resolvers reachable and low-latency, because SPF, DKIM, DMARC, and DNSBL now depend on them
- monitor the size of `greylist/` and `policy/reputation.json` during the first production weeks
- tune `LPE_CT_SPAM_QUARANTINE_THRESHOLD` and `LPE_CT_SPAM_REJECT_THRESHOLD` conservatively before tightening
- review quarantined trace files before enabling `LPE_CT_REQUIRE_DKIM_ALIGNMENT=true`
- keep at least one DNSBL zone highly reliable; too many low-quality zones increase false positives and latency

## Current scope

This implementation executes inbound SPF, DKIM, and DMARC verification, DNSBL checks, greylisting, reputation weighting, and detailed trace persistence.

It does not yet add outbound DKIM signing. That remains an explicit `LPE-CT` responsibility to complete in a later increment.
