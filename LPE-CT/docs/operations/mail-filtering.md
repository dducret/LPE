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

`LPE-CT` now also retains perimeter mail-flow history in `policy/transport-audit.jsonl` and mirrors that retained stream into a dedicated private `mail_flow_history` database index for management-side history search, quarantine inspection, and retained reporting artifacts without creating a canonical mailbox search index.

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

`LPE_CT_POLICY_ALLOW_SENDERS` / `LPE_CT_POLICY_BLOCK_SENDERS`

- default: empty
- comma-separated exact sender addresses or bare domains
- applied on inbound `MAIL FROM`, authenticated submission `MAIL FROM`, and outbound relay handoff

`LPE_CT_POLICY_ALLOW_RECIPIENTS` / `LPE_CT_POLICY_BLOCK_RECIPIENTS`

- default: empty
- comma-separated exact recipient addresses or bare domains
- applied on inbound `RCPT TO`, authenticated submission `RCPT TO`, and outbound relay handoff

`LPE_CT_RECIPIENT_VERIFICATION_ENABLED`

- default: `false`
- when enabled, inbound `SMTP` recipient acceptance is checked against an internal `LPE` verification API and cached locally

`LPE_CT_RECIPIENT_VERIFICATION_FAIL_CLOSED`

- default: `true`
- when enabled, internal verification bridge failures return temporary `451` instead of silently accepting unverifiable recipients

`LPE_CT_RECIPIENT_VERIFICATION_CACHE_TTL_SECONDS`

- default: `300`
- local cache TTL for positive and negative recipient-verification results

`LPE_CT_OUTBOUND_DKIM_ENABLED`

- default: `false`
- enables DKIM signing in the outbound relay flow before `SMTP` delivery

`LPE_CT_OUTBOUND_DKIM_KEYS`

- default: empty
- semicolon-separated `domain|selector|private-key-path` entries
- only sender domains with a configured key are signed

`LPE_CT_OUTBOUND_DKIM_HEADERS`

- default: `from,to,cc,subject,mime-version,content-type,message-id`
- signed header list passed into the DKIM signer

`LPE_CT_ATTACHMENT_ALLOW_EXTENSIONS` / `LPE_CT_ATTACHMENT_BLOCK_EXTENSIONS`

- default: empty
- comma-separated attachment filename extensions enforced after MIME parsing

`LPE_CT_ATTACHMENT_ALLOW_MIME_TYPES` / `LPE_CT_ATTACHMENT_BLOCK_MIME_TYPES`

- default: empty
- comma-separated declared or detected MIME types enforced by policy

`LPE_CT_ATTACHMENT_ALLOW_DETECTED_TYPES` / `LPE_CT_ATTACHMENT_BLOCK_DETECTED_TYPES`

- default: empty
- comma-separated Magika detected-type labels enforced after file-type detection

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

The retained `policy/transport-audit.jsonl` stream now also captures:

- `Message-Id`
- queue and disposition transitions
- `DNSBL` hits
- structured auth summary (`SPF`, `DKIM`, `DMARC`)
- `Magika` summary / decision when present
- structured technical relay and `DSN` detail when present
- operator release, retry, and quarantine deletion actions

When the private local PostgreSQL store is enabled, those same retained events are also inserted into `mail_flow_history` with indexed trace, time, direction, queue, and retained technical metadata so history search does not depend on scanning the raw JSONL artifact. The current schema also adds `GIN` full-text indexing on `search_text` and, when `pg_trgm` can be enabled on the local PostgreSQL instance, a trigram index for substring-heavy operator lookups.

When the optional local PostgreSQL store is active, `LPE-CT` also upserts one row per quarantined trace into `quarantine_messages` with:

- `trace_id`, direction, status, sender, recipients, subject, and `Message-Id`
- spool path for payload custody
- `spam_score`, `security_score`, `reputation_score`
- `DNSBL`, auth summary, and decision trace JSON
- `Magika` summary / decision when applicable

The same technical store now mirrors the current management-plane policy state into private tables:

- `policy_address_rules` for sender and recipient allow/block entries
- `attachment_policy_rules` for extension, MIME, and detected-type controls
- `digest_settings` and `digest_recipients` for quarantine digest scheduling and targets
- `recipient_verification_settings` and `recipient_verification_cache` for internal recipient-validation behavior plus short-lived cache rows
- `dkim_domain_configs` for enabled sender-domain signing references and key-path metadata

Those rows remain technical only. They exist to keep the sorting-center backend searchable and cluster-friendly; the durable admin source remains `state.json`, and queue ownership remains in the spool.

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

## Quarantine operations and reporting

The management API and UI now use the retained perimeter artifacts for these operational workflows:

- search quarantine by trace, sender, recipient, subject, `Message-Id`, direction, and domain
- inspect a quarantined or retained trace with headers, body excerpt, decision trace, and retained flow history
- release, retry, or delete a quarantined trace while keeping retained perimeter audit evidence
- search inbound and outbound flow history by trace id, sender, recipient, subject, route, disposition, and policy evidence

`LPE-CT` also now generates scheduled quarantine digest reports from sorting-center-owned data only.

Current digest controls cover:

- global enable or disable
- global interval in minutes
- maximum items per digest
- retained history window in days
- domain default recipients
- mailbox-specific user overrides when a narrower digest target is needed

Generated digest artifacts are stored under `policy/digest-reports/` as technical operational reports surfaced in the management UI. They are not canonical mailbox messages and do not require direct access to the core `LPE` database.

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
- explicit sender or recipient block-list hits reject the transaction before final acceptance
- when recipient verification is enabled, invalid local recipients are rejected during inbound `RCPT TO`
- attachment policies can quarantine inbound messages or reject authenticated client submissions based on extension, MIME type, or detected file type
- quarantined or rejected spam sessions can be terminated immediately after the final `SMTP` reply so the edge stops the transaction cleanly
- outbound handoff now also runs through `bayespam`; high-scoring outbound content is quarantined before relay
- outbound handoff now also runs through the same antivirus provider chain before relay
- outbound relay can add a DKIM signature for sender domains that have an explicit configured key

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

- inbound or outbound `ARC`
- `MTA-STS` policy fetch and TLS-policy enforcement
- `TLS-RPT` report generation and sending
- advanced reputation feeds beyond the local `(IP, sender domain)` spool state
- DNSSEC validation or DANE for SMTP transport

The current lot prepares those follow-ups by persisting structured authentication outcomes, richer technical status, and explicit defer/quarantine/reject reasons in the spool trace.
