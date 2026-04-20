# LPE-CT Mail Filtering Operations

## Purpose

This document describes the operational controls now enforced by `LPE-CT` on inbound mail before final delivery to `LPE`.

`LPE-CT` remains the Internet-facing `SMTP` edge, the quarantine owner, and the perimeter decision point.

## Active controls

Inbound messages now go through these stages:

1. raw SMTP ingestion and trace creation
2. SMTP envelope/protocol capture (`HELO`, `MAIL FROM`, `RCPT TO`, `DATA`)
3. optional drain-mode hold
4. attachment validation with `Magika`
5. greylisting on `(source IP, MAIL FROM, first RCPT TO)`
6. `DNSBL` / `RBL` lookups on the connecting IP
7. SPF, DKIM, and DMARC evaluation
8. `bayespam` scoring on subject, visible text, sender, and `HELO`
9. antivirus provider chain execution on extracted MIME attachments and a raw `.eml` artifact
10. sender reputation weighting and final score calculation
11. final decision: `accept`, `defer`, `quarantine`, or `reject`

The resulting decision trace is persisted with the queued message JSON in the local spool.
When the optional local PostgreSQL store is enabled, quarantined messages are also indexed into `quarantine_messages` for operational search and release workflows while the spool remains the payload owner.

## Main environment variables

`LPE_CT_GREYLISTING_ENABLED`

- default: `true`
- first-seen triplets are deferred for `90` seconds before a later retry is accepted

`LPE_CT_BAYESPAM_ENABLED`

- default: `true`
- enables the local Bayesian spam scorer in the edge pipeline

`LPE_CT_BAYESPAM_AUTO_LEARN`

- default: `true`
- auto-trains the local corpus from inbound `accept` as ham and inbound `quarantine` / `reject` as spam

`LPE_CT_BAYESPAM_SCORE_WEIGHT`

- default: `6.0`
- maximum contribution applied by the Bayesian classifier to `spam_score`

`LPE_CT_BAYESPAM_MIN_TOKEN_LENGTH`

- default: `3`
- minimum token length retained in the Bayesian corpus

`LPE_CT_BAYESPAM_MAX_TOKENS`

- default: `256`
- caps the number of unique tokens used per message for training and scoring

`LPE_CT_ANTIVIRUS_ENABLED`

- default: `true` in the Debian example environment
- enables the antivirus provider chain inside the `LPE-CT` SMTP pipeline

`LPE_CT_ANTIVIRUS_FAIL_CLOSED`

- default: `true`
- quarantines the message when an enabled antivirus chain is misconfigured or a provider execution fails

`LPE_CT_ANTIVIRUS_PROVIDER_CHAIN`

- default: `takeri`
- ordered comma-separated provider ids; `LPE-CT` executes them in sequence and stops on the first suspicious or infected result

`LPE_CT_ANTIVIRUS_TAKERI_BIN`

- default: `/opt/lpe-ct/bin/Shuhari-CyberForge-CLI`
- `takeri` preset executable built from the upstream Git checkout

`LPE_CT_ANTIVIRUS_TAKERI_ARGS`

- default: `takeri,scan`
- argument prefix passed before the scan target path for the `takeri` preset

`LPE_CT_ANTIVIRUS_TAKERI_REPO_URL`

- default: `https://github.com/AnimeForLife191/Shuhari-CyberForge.git`
- upstream repository synchronized by the Debian helper script

`LPE_CT_ANTIVIRUS_TAKERI_BRANCH`

- default: `main`
- upstream branch used by the Git synchronization helper

`LPE_CT_ANTIVIRUS_TAKERI_SYNC_DIR`

- default: `/opt/lpe-ct/vendor/takeri-src`
- sparse Git checkout used to rebuild the `takeri` CLI during install and update

Additional command-style providers can be chained after `takeri` with:

- `LPE_CT_ANTIVIRUS_<PROVIDER>_BIN`
- `LPE_CT_ANTIVIRUS_<PROVIDER>_ARGS`
- `LPE_CT_ANTIVIRUS_<PROVIDER>_INFECTED_MARKERS`
- `LPE_CT_ANTIVIRUS_<PROVIDER>_SUSPICIOUS_MARKERS`
- `LPE_CT_ANTIVIRUS_<PROVIDER>_CLEAN_MARKERS`

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

`LPE_CT_LOCAL_DB_URL`

- default: none
- required only when `LPE_CT_LOCAL_DB_ENABLED=true`
- PostgreSQL connection string used to persist technical quarantine metadata

## Spool and policy state

`LPE_CT_SPOOL_DIR` now contains additional policy data:

- `greylist/`: first-seen triplets and release timestamps
- `policy/reputation.json`: simple accumulated reputation counters
- `policy/bayespam.json`: local Bayesian corpus (`ham_messages`, `spam_messages`, token counts)
- `policy/<rule>.json`: local throttling windows keyed by routing/throttle rule

Message trace files now also persist:

- `spam_score`
- `security_score`
- `reputation_score`
- `dnsbl_hits`
- `auth_summary`
- `decision_trace`

When the optional local PostgreSQL store is active, `LPE-CT` also upserts one row per quarantined trace into `quarantine_messages` with:

- `trace_id`, direction, status, sender, recipients, subject, and `Message-Id`
- spool path for payload custody
- `spam_score`, `security_score`, `reputation_score`
- `DNSBL`, auth summary, and decision trace JSON
- `Magika` summary / decision when applicable

`auth_summary` is now derived from structured SPF/DKIM/DMARC results rather than string matching on debug output. The decision trace also records:

- SPF domain used for evaluation
- RFC 5322 `From` domain used for `DMARC`
- SPF and DKIM alignment gaps
- temporary DNS or authentication failures that caused `defer`

This trace is the primary operational artifact for explaining why a message was deferred, quarantined, rejected, or accepted.

`bayespam` is spool-first in `v1.2.0`: the corpus lives in `policy/bayespam.json`, while optional private PostgreSQL remains available for quarantine metadata and later corpus indexing work.

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
- multiple triggered causes are accumulated in the final decision trace instead of keeping only the first matching rule
- antivirus detections or suspicious provider outcomes force quarantine in `LPE-CT`
- when `LPE_CT_ANTIVIRUS_FAIL_CLOSED=true`, provider execution failures also force quarantine
- quarantined or rejected spam sessions can be terminated immediately after the final `SMTP` reply so the edge stops the transaction cleanly
- outbound handoff now also runs through `bayespam`; high-scoring outbound content is quarantined before relay
- outbound handoff now also runs through the same antivirus provider chain before relay

## Operational recommendations

- keep recursive DNS resolvers reachable and low-latency, because SPF, DKIM, DMARC, and DNSBL now depend on them
- monitor the size of `greylist/` and `policy/reputation.json` during the first production weeks
- monitor `policy/bayespam.json` growth and back it up with the rest of the technical edge state
- tune `LPE_CT_SPAM_QUARANTINE_THRESHOLD` and `LPE_CT_SPAM_REJECT_THRESHOLD` conservatively before tightening
- keep `LPE_CT_BAYESPAM_AUTO_LEARN=true` only if quarantine/reject decisions are already reasonably clean
- review quarantined trace files before enabling `LPE_CT_REQUIRE_DKIM_ALIGNMENT=true`
- keep at least one DNSBL zone highly reliable; too many low-quality zones increase false positives and latency

## Current scope

This implementation executes inbound SPF, DKIM, and DMARC verification, DNSBL checks, greylisting, Bayesian spam scoring with spool-first auto-learning, a configurable antivirus provider chain, reputation weighting, and detailed trace persistence.

It also now executes outbound routing selection, local throttling, retry backoff aware of the previous attempt count, and SMTP-result classification into `relayed`, `deferred`, `bounced`, or `failed`, with structured `DSN`/technical trace feedback for the `LPE` worker.

If those policy artifacts outgrow flat files, they may move into a dedicated private `LPE-CT` database on non-public `5432`, but only as technical perimeter state and never as canonical mailbox state.

It does not yet add:

- outbound DKIM signing
- inbound or outbound `ARC`
- `MTA-STS` policy fetch and TLS-policy enforcement
- `TLS-RPT` report generation and sending
- advanced reputation feeds beyond the local `(IP, sender domain)` spool state
- DNSSEC validation or DANE for SMTP transport

The current lot prepares those follow-ups by persisting structured authentication outcomes, richer technical status, and explicit defer/quarantine/reject reasons in the spool trace.
