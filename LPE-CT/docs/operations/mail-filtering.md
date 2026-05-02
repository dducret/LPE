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

For fast live troubleshooting, Debian deployments also enable a Postfix-style
operator log at `/var/log/lpe-ct/mail.log` through `LPE_CT_MAIL_LOG_PATH`.
Those lines are derived from the same transport audit events and include
`trace_id`, queue, status, sender, recipients, peer, `Message-Id`, size, relay,
`DSN`, reason, reply, and subject. Use this file for quick `tail -f` diagnosis;
use `transport-audit.jsonl` or the local `mail_flow_history` index for complete
retained evidence.

## Main environment variables

`LPE_CT_POSTFIX_MAIL_LOG_ENABLED`

- default: `true` in the Debian example environment
- writes a human-readable Postfix-style diagnostic line for each retained transport audit event

`LPE_CT_MAIL_LOG_PATH`

- default: `/var/log/lpe-ct/mail.log` in Debian installs
- file path used by the Postfix-style diagnostic stream

`LPE_CT_HOST_LOG_DIR`

- default: `/var/log/lpe-ct` in Debian installs
- host-log browser directory for `mail.log` and rotated `mail.log.*` files

`LPE_CT_GREYLISTING_ENABLED`

- default: `true`
- first-seen triplets are deferred for `90` seconds before a later retry is accepted
- accepted-domain settings can disable greylisting for a specific verified recipient domain; when disabled there, a normal plaintext inbound message for that domain proceeds to final delivery instead of receiving the first-seen greylist `451`

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
- when disabled, `LPE-CT` intentionally uses deferred local-part validation for verified accepted domains: any syntactically valid recipient at a verified accepted domain is accepted at `RCPT TO`, and final mailbox existence is left to the private `LPE` final-delivery bridge
- if the final-delivery bridge later rejects an accepted local recipient, `LPE-CT` treats that as a local deferred-delivery condition and retains the message in sorting-center custody; it does not generate an outbound bounce to the reverse path from that post-acceptance rejection
- if no accepted domains are configured and verified, inbound `RCPT TO` is rejected so the edge cannot become an open relay

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
- outbound signing prefers the RFC 5322 `From` domain and falls back to a distinct `Sender` domain when delegated sending is used

`LPE_CT_OUTBOUND_DKIM_HEADERS`

- default: `from,sender,to,cc,subject,mime-version,content-type,message-id`
- signed header list passed into the DKIM signer
- the runtime normalizes this list to keep `sender` present so delegated send-on-behalf flows remain covered

`LPE_CT_ATTACHMENT_ALLOW_EXTENSIONS` / `LPE_CT_ATTACHMENT_BLOCK_EXTENSIONS`

- default: empty
- comma-separated attachment filename extensions enforced after MIME parsing
- extension matching is normalized case-insensitively and treats `exe` and `.exe` as equivalent rules

`LPE_CT_ATTACHMENT_ALLOW_MIME_TYPES` / `LPE_CT_ATTACHMENT_BLOCK_MIME_TYPES`

- default: empty
- comma-separated declared or detected MIME types enforced by policy

`LPE_CT_ATTACHMENT_ALLOW_DETECTED_TYPES` / `LPE_CT_ATTACHMENT_BLOCK_DETECTED_TYPES`

- default: empty
- comma-separated Magika detected-type labels enforced after file-type detection

`LPE_CT_LOCAL_DB_URL`

- default: none
- required only when `LPE_CT_LOCAL_DB_ENABLED=true`
- PostgreSQL connection string used to persist the default indexed technical `LPE-CT` state, including quarantine metadata, retained history indexes, `bayespam`, greylisting, reputation, throttling, digest settings, recipient-verification cache rows, and DKIM-domain references
- when this is missing or the private PostgreSQL store is temporarily unreachable, `LPE-CT` now keeps the management API online in a degraded mode instead of making the `nginx` front end return `502`

## Spool and policy state

`LPE_CT_SPOOL_DIR` now contains these durable operational artifacts:

- `greylist/`: legacy first-seen triplets that can still be migrated into the private PostgreSQL store
- `policy/transport-audit.jsonl`: retained perimeter audit stream used for reporting and rebuild
- `policy/digest-reports/`: generated digest artifacts
- queue files under `incoming/`, `outbound/`, `deferred/`, `held/`, `quarantine/`, `bounces/`, and `sent/`

When `LPE_CT_LOCAL_DB_ENABLED=true`, the active indexed policy state no longer lives primarily in flat spool JSON files:

- greylisting uses `greylist_entries`
- reputation uses `reputation_entries`
- `bayespam` uses `bayespam_corpora`
- throttling uses `throttle_windows`

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

Synthetic Internet tests must not use a protected local author domain unless
the expected result is a hard authentication reject. For example, a message
with `MAIL FROM:<smtp-test@l-p-e.ch>` and `From: smtp-test@l-p-e.ch` sent from
an unauthorized external source is expected to fail strict `DMARC` for
`l-p-e.ch` when no aligned `SPF` pass or aligned `DKIM` signature is present.
Use a controlled external sender domain with valid aligned `SPF` or `DKIM`, or
a neutral sender domain that does not publish a rejecting `DMARC` policy, when
the goal is to validate successful inbound delivery.

The current default profile stores `bayespam` in the private PostgreSQL `bayespam_corpora` table. Legacy `policy/bayespam.json` content is still migrated when present so older technical state can be carried forward during rollout.

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
- inspect a quarantined, queued, or retained trace with headers, body excerpt, decision trace, and retained flow history
- release, retry, or delete an eligible current queue-custody trace while keeping retained perimeter audit evidence
- search inbound and outbound flow history by trace id, sender, recipient, subject, route, disposition, and policy evidence
- operate sender and recipient allow/block rules from full-width policy lists with drawer-based create, edit, and delete flows
- operate attachment-policy rules by extension, MIME type, and detected file type from the same policy workspace
- review recipient-verification mode, fail-open or fail-closed posture, cache backend, and cache TTL without leaving the management shell
- review DKIM signing posture, per-domain selectors, and key-path status in the same operator-facing policy workspace
- manage digest schedule, domain defaults, mailbox overrides, and retained digest artifacts from one reporting section

The current backend search surface is intentionally operator-oriented rather than dashboard-oriented.

Quarantine search now supports explicit filtering on:

- free text across trace id, sender, recipients, subject, `Message-Id`, route target, remote references, and retained policy evidence
- exact trace id, sender, recipient, `Message-Id`, route target, and retained reason
- direction, status, and domain
- minimum `spam_score` and minimum `security_score`

Retained mail-flow history search now supports explicit filtering on:

- free text across trace id, sender, recipients, subject, route target, peer, `Message-Id`, and retained evidence
- exact trace id, sender, recipient, `Message-Id`, peer, route target, and retained reason
- direction, queue, disposition, and domain
- minimum `spam_score` and minimum `security_score`

Returned quarantine and history detail payloads now expose additional technical evidence useful during incident handling and operator triage:

- peer IP and `HELO`
- `DNSBL` hit set
- structured auth summary
- `Magika` summary and final `Magika` decision when present
- latest retained decision summary
- route target and remote message reference when relay work already happened
- structured technical status and `DSN` detail for retained outbound outcomes

Policy-status views are now operational rather than purely declarative:

- recipient verification reports `disabled`, `misconfigured`, `degraded`, `bridge-misconfigured`, or `active` based on bridge and cache-store readiness
- DKIM reports `disabled`, `misconfigured`, or `active` based on enabled domains with readable key material
- update failures that cannot be mirrored into the private technical store roll back to the previous durable dashboard state instead of leaving split-brain state between `state.json` and the technical store

`LPE-CT` also now generates scheduled quarantine digest reports from sorting-center-owned data only.

Current digest controls cover:

- global enable or disable
- global interval in minutes
- maximum items per digest
- retained history window in days
- retained digest-artifact window in days
- domain default recipients
- mailbox-specific user overrides when a narrower digest target is needed

Generated digest artifacts are stored under `policy/digest-reports/` as technical operational reports surfaced in the management UI. They are not canonical mailbox messages and do not require direct access to the core `LPE` database.

Each generated digest now carries a bounded and explainable technical summary derived only from retained sorting-center data, including:

- inbound and outbound item counts
- highest retained `spam_score` and `security_score`
- oldest and newest retained event in the covered window
- top retained reasons
- per-status counts
- per-domain counts in the retained details payload

Digest generation remains predictable:

- the generator reads only the retained history window
- digest files are written to the sorting-center spool
- digest configuration is mirrored into the private local database for management use
- stored digest artifacts are pruned independently from retained history according to the dedicated digest retention setting

Retention is explicit and bounded:

- `policy/transport-audit.jsonl` is pruned to the configured history-retention window
- mirrored `mail_flow_history` rows are pruned to the same history-retention window
- retained digest artifacts under `policy/digest-reports/` are pruned to their own configured digest-retention window
- current quarantine search indexes are rebuilt from the live `quarantine/` spool at startup when the private local database is enabled

Those retention rules apply only to sorting-center-owned technical evidence and reporting artifacts. They do not change queue custody or make the private database canonical.

## SMTP outcomes

SMTP envelope parsing is strict for the currently implemented ESMTP surface:

- the `220` banner and `EHLO` response use the configured `System Setup` / `Mail relay` / `SMTP Settings` EHLO name, normally the public MX FQDN such as `mail.l-p-e.ch`
- command lines longer than the SMTP command limit return `500 command line too long`
- `MAIL FROM` and `RCPT TO` require bracketed reverse-path or forward-path syntax such as `MAIL FROM:<sender@example.test>`
- malformed envelope paths return `501`
- unsupported `MAIL FROM` or `RCPT TO` parameters return `555`
- only the `MAIL FROM` `SIZE=` parameter is implemented; values larger than `LPE_CT_MAX_MESSAGE_SIZE_MB` return `552`
- each transaction accepts at most 25 recipients before returning `452 too many recipients`
- public port `25` does not advertise or accept `AUTH`; authenticated client submission must stay on the dedicated submission surface, not public inbound SMTP

Typical inbound outcomes are:

- `250 queued as <trace>`: accepted for delivery or quarantine ownership
- `250 quarantined as <trace>`: accepted but retained locally in quarantine
- `451 message temporarily deferred by greylisting (trace <trace>)`: intentional first-seen greylisting
- `451 message temporarily deferred by authentication dependency (trace <trace>)`: transient `SPF` / `DKIM` / `DMARC` dependency failure
- `451 core final delivery temporarily unavailable (trace <trace>)`: accepted edge policy, but the internal `LPE` final-delivery bridge is not configured or reachable
- `554 message rejected by perimeter policy: <reason> (trace <trace>)`: hard reject, for example enforced DMARC reject, SPF fail without aligned DKIM, explicit block-list hit, spam reject threshold, or reputation reject threshold

`LPE_CT_CORE_DELIVERY_BASE_URL` must point at the private core `LPE` listener that exposes `/internal/lpe-ct/inbound-deliveries`. If it is empty, invalid, or unreachable, `LPE-CT` keeps the message in deferred custody and returns the explicit core-final-delivery `451` above rather than a generic perimeter-policy deferral.

Recommended external validation commands:

Inbound `STARTTLS` is advertised only when `System Setup -> Mail relay -> SMTP
Settings` has an active public TLS profile with usable certificate and key
material. If no profile is selected, the validation should confirm that EHLO
does not include `STARTTLS`.

```bash
openssl s_client -starttls smtp -crlf -connect mail.l-p-e.ch:25 -servername mail.l-p-e.ch
```

After the `STARTTLS` handshake, send a fresh `EHLO`, then verify recipient
domain filtering:

```smtp
EHLO validator.example
MAIL FROM:<internet-check@external.example>
RCPT TO:<test@l-p-e.ch>
RCPT TO:<test@sdic.ch>
RCPT TO:<relay-test@example.net>
```

The expected results are `250 recipient accepted` for `test@l-p-e.ch` and `550
recipient domain is not accepted by this sorting center` for `test@sdic.ch`
and `relay-test@example.net`. A successful DATA test should use a matching
neutral or controlled external `From` header. A spoofed `l-p-e.ch` author from
an unauthorized source should return `554 message rejected by perimeter
policy: DMARC policy requested reject; SPF failed and no aligned DKIM signature
passed (trace <trace>)`.

## MTA-STS and TLS-RPT deployment notes

`LPE-CT` does not currently automate `MTA-STS` DNS publication, HTTPS policy hosting, or `TLS-RPT` report generation. Operators can still deploy the public policy artifacts alongside the SMTP TLS profile:

- publish `_mta-sts.<domain>` as a `TXT` record such as `v=STSv1; id=2026050201`
- host `https://mta-sts.<domain>/.well-known/mta-sts.txt`
- use a policy body such as:

```text
version: STSv1
mode: enforce
mx: mail.l-p-e.ch
max_age: 86400
```

- publish `_smtp._tls.<domain>` as a `TXT` record such as `v=TLSRPTv1; rua=mailto:tlsrpt@<domain>`
- keep the `mx:` value aligned with the configured `LPE-CT` public EHLO/banner FQDN and the certificate SAN used by inbound `STARTTLS`
- start with `mode: testing` until STARTTLS, certificate renewal, and external delivery telemetry are verified; switch to `enforce` only after the MX hostname, certificate, and policy file are stable

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
- outbound delegated senders are checked against sender allow/block policy in addition to the RFC 5321 / RFC 5322 author address
- when recipient verification is enabled, invalid local recipients are rejected during inbound `RCPT TO`
- attachment policies can quarantine inbound messages or reject authenticated client submissions based on extension, MIME type, or detected file type
- quarantined or rejected spam sessions can be terminated immediately after the final `SMTP` reply so the edge stops the transaction cleanly
- outbound handoff now also runs through `bayespam`; high-scoring outbound content is quarantined before relay
- outbound handoff now also runs through the same antivirus provider chain before relay
- outbound relay can add a DKIM signature for sender domains that have an explicit configured key

The management UI exposes these inbound reject/quarantine knobs under `Filtering` -> `Spam` -> `Edit settings`, including SPF enforcement, DMARC reject enforcement, aligned-DKIM requirement, authentication temporary-failure deferral, Bayesian spam scoring, reputation scoring, and the spam/reputation quarantine and reject thresholds.

Operator trace actions now behave defensively:

- retry, release, or delete returns a conflict instead of a false success when the trace is not eligible for that action
- retry and release clear stale relay-specific fields such as prior remote references, technical status, DSN, route, and throttle state before the message re-enters a live queue
- retry, release, and delete each append retained transport-audit history so reporting and trace inspection stay consistent
- delete is available from the quarantine list and from the mail-history trace drawer for current `incoming`, `outbound`, `deferred`, `held`, `quarantine`, and `bounces` spool items; retained `sent` history is not deleted from that action

## Operational recommendations

- keep recursive DNS resolvers reachable and low-latency, because SPF, DKIM, DMARC, and DNSBL now depend on them
- monitor private PostgreSQL growth for `greylist_entries`, `reputation_entries`, `bayespam_corpora`, `throttle_windows`, `quarantine_messages`, and `mail_flow_history` during the first production weeks
- keep the private local database backup policy aligned with the retained spool artifacts, because the runtime now splits technical evidence between PostgreSQL indexes and spool-owned payload/report files
- tune `LPE_CT_SPAM_QUARANTINE_THRESHOLD` and `LPE_CT_SPAM_REJECT_THRESHOLD` conservatively before tightening
- keep `LPE_CT_BAYESPAM_AUTO_LEARN=true` only if quarantine/reject decisions are already reasonably clean
- review quarantined trace files before enabling `LPE_CT_REQUIRE_DKIM_ALIGNMENT=true`
- keep at least one DNSBL zone highly reliable; too many low-quality zones increase false positives and latency

## Current scope

This implementation executes inbound SPF, DKIM, and DMARC verification, DNSBL checks, greylisting, Bayesian spam scoring with private-PostgreSQL-backed corpus state by default, a configurable antivirus provider chain, reputation weighting, and detailed trace persistence.

It also now executes outbound routing selection, local throttling, retry backoff aware of the previous attempt count, and SMTP-result classification into `relayed`, `deferred`, `bounced`, or `failed`, with structured `DSN`/technical trace feedback for the `LPE` worker.

If those policy artifacts outgrow flat files, they may move into a dedicated private `LPE-CT` database on non-public `5432`, but only as technical perimeter state and never as canonical mailbox state.

It does not yet add:

- inbound or outbound `ARC`
- `MTA-STS` policy fetch and TLS-policy enforcement
- `TLS-RPT` report generation and sending
- advanced reputation feeds beyond the local `(IP, sender domain)` spool state
- DNSSEC validation or DANE for SMTP transport

The current lot prepares those follow-ups by persisting structured authentication outcomes, richer technical status, and explicit defer/quarantine/reject reasons in the spool trace.
