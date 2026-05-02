# DMZ Sorting Center

### Role

`LPE-CT` is an edge component for an `LPE` deployment where the sorting server resides in a `DMZ` separate from the `LAN`.

The sorting center:

- receives publicly exposed SMTP traffic
- applies filtering, quarantine, and drain policies
- delivers accepted inbound messages to the core `LPE` services on the `LAN` through an explicit internal API
- also receives outbound handoffs emitted by `LPE`, then executes `SMTP` relay
- exposes a management interface through `nginx` and a local Rust API

The functional v1 includes a minimal SMTP listener, local spool, simple quarantine, drain mode, an internal handoff endpoint for outbound work, and HTTP final delivery into `LPE` for inbound mail. `mTLS` remains a configuration policy but is not enabled by default in this v1 until the license-compliant TLS choice is documented.

The perimeter pipeline now also executes:

- `Magika` validation for attachments
- greylisting
- `DNSBL/RBL` lookups
- `SPF`, `DKIM`, and `DMARC` verification
- local `bayespam` scoring with the dedicated private PostgreSQL store as the default indexed backend, plus migration from legacy spool artifacts when present
- a configurable antivirus provider chain, with `takeri` as the default Git-synchronized Debian profile
- anti-spam scoring and simple local reputation
- detailed decision tracing persisted in the spool

When dedicated local PostgreSQL is enabled, `LPE-CT` now also persists technical quarantine metadata into a private `quarantine_messages` table and retained flow-history events into a private `mail_flow_history` table. Payload custody remains in the spool and quarantine directories.
The same private store now also holds technical management-plane metadata for `policy_address_rules`, `attachment_policy_rules`, `digest_settings`, `digest_recipients`, `recipient_verification_cache`, `recipient_verification_settings`, and `dkim_domain_configs`.
It also materializes accepted-domain relay configuration from the private PostgreSQL `dashboard_state` row, including each accepted domain, destination server, verification type, per-domain `RBL` / `SPF` / greylisting toggles, and whether the domain has been operator-verified.
The management surface now also uses sorting-center-owned retained artifacts plus those dedicated technical indexes for quarantine search, retained mail-flow history, address-policy administration, digest scheduling, recipient-verification cache inspection, and DKIM-domain configuration references generated entirely from `LPE-CT` state.

Operational indexing is now deliberately evidence-oriented:

- `quarantine_messages` indexes the current quarantine set by retained receipt time, sender domain, recipient domains, route target, remote message reference, and combined search text
- `mail_flow_history` indexes retained perimeter events by trace, time, direction, queue, disposition, and bounded technical evidence
- full-text search is enabled for retained operator text lookups, with optional trigram acceleration when `pg_trgm` is available on the private local PostgreSQL instance

Those indexes exist only to improve sorting-center operations. They must stay rebuildable from current spool artifacts, retained audit history, and the active PostgreSQL dashboard state.

For the P1 management interface, those operator workflows are exposed as one coherent web surface instead of separate technical screens:

- full-width quarantine and retained-history lists with search and trace drawers
- full-width allow/block and attachment-rule lists with drawer-based create, edit, and delete flows
- recipient-verification settings and status in the same policy workspace
- DKIM signing profile plus per-domain selector and key-reference management in the same policy workspace
- accepted domains under `System Setup` / `Mail relay` / `Domains`, with drawer-based add, import, edit, test, and delete workflows; this dynamic PostgreSQL-backed list, not an environment variable, controls inbound `RCPT TO` domain acceptance, and a successful signed bridge test marks the domain as verified
- digest scheduling, domain defaults, mailbox overrides, and retained digest artifacts under one reporting workspace

That web surface stays bounded to sorting-center-owned state. It must not imply canonical mailbox ownership, direct `LPE` database access, or any parallel user-visible state model in `LPE-CT`.

`LPE-CT` may also use dedicated local technical data stores for perimeter-owned state such as Bayesian filtering, reputation, greylisting, quarantine indexes, or cluster coordination. Those stores must remain local to the sorting center and must not become canonical mailbox or collaboration storage.

In the current repository state, the runtime now uses:

- private PostgreSQL for management configuration and bootstrap state
- the local spool for inbound and outbound queue ownership, quarantine payload custody, retained transport audit artifacts, and generated digest reports
- the private dedicated PostgreSQL store as the default backend for indexed technical state such as greylisting, reputation, `bayespam`, throttling, quarantine metadata, retained history indexes, recipient-verification cache rows, and DKIM-domain references

Legacy flat-file policy artifacts in the spool are still migrated forward when present so older state can be retained during rollout, but the default indexed backend is no longer spool-only for greylisting, reputation, throttling, or `bayespam`.

For outbound `LPE -> LPE-CT` handoff, the sorting center now also covers:

- local outbound routing rules
- local outbound throttling
- outbound DKIM signing for configured sender domains
- sender and recipient allow/block policy enforcement
- retry backoff informed by the upstream attempt count supplied by `LPE`
- classification of `SMTP` replies into `relayed`, `deferred`, `bounced`, or `failed`
- structured technical and `DSN` detail for the latest attempt

Inbound `SMTP` recipient verification remains an `LPE`-backed internal decision. `LPE-CT` may cache the result locally, but it must not replace that contract with public callback-verification tricks or invent canonical mailbox state in the `DMZ`. If recipient verification is disabled, `LPE-CT` intentionally uses deferred local-part validation for verified accepted domains: it accepts syntactically valid recipients at those domains as a catch-all edge policy and leaves final mailbox existence to the internal delivery bridge. An empty accepted-domain set rejects inbound `RCPT TO` instead of acting as an open relay.

### Architecture position

`LPE-CT` does not replace the core `LPE` stack:

- canonical business data remains in the core `LPE` services
- `PostgreSQL` remains the primary store for the core product
- `LPE-CT` keeps only minimal local operational state for configuration and operations
- the modern product axis remains `JMAP` on the core `LPE` side

The dedicated local-store rules for `LPE-CT` are documented in `docs/architecture/lpe-ct-local-data-stores.md`.

Technical rebuild expectations remain strict:

- queue and quarantine payload custody stay in the spool
- local PostgreSQL indexes may be repopulated from the active dashboard state, retained audit history, and current spool artifacts
- recipient-verification cache rows are disposable and expire by TTL
- no private table in `LPE-CT` becomes authoritative mailbox or tenant state

### Network flows

Flows allowed from the Internet to the `DMZ`:

- inbound SMTP to `LPE-CT`
- administration HTTPS or HTTP depending on the chosen exposure policy

Flows allowed from the `DMZ` to the `LAN`:

- authenticated HTTP final delivery toward designated core nodes
- management traffic strictly limited to authorized addresses and segments

Flows denied by default:

- direct `DMZ` access to the core `PostgreSQL` databases
- exposing the core back office on the DMZ server
- sending data to an external AI service

If `LPE-CT` uses a local `PostgreSQL` service for technical state, `5432` must remain private to the local host, backend segment, or dedicated `LPE-CT` cluster network and must never be Internet-facing.

### Management interface

The `LPE-CT` management interface covers:

- node identity and public bind addresses
- primary and secondary relays toward the `LAN`
- network surface policy and authorized CIDRs
- drain mode, quarantine, and `SPF` / `DKIM` / `DMARC` controls
- quarantine search, trace inspection, release, retry, and delete workflows
- retained mail-flow history search by trace, sender, recipient, subject, route, and disposition
- scheduled quarantine digest configuration with domain defaults and mailbox-specific overrides
- Git-first update source and maintenance window

Operator-facing retained payloads now also include the technical evidence needed to explain a decision without re-reading raw spool files:

- peer IP and `HELO`
- `DNSBL` hits
- structured auth summary
- `Magika` outcome when applicable
- latest retained decision summary
- route target, remote message reference, technical status, and `DSN` detail for outbound traces

### Debian installation

The `Debian Trixie` scripts for `LPE-CT`:

- install system prerequisites
- clone the Git repository into `/opt/lpe-ct/src`
- build the `lpe-ct` binary
- create the `/var/spool/lpe-ct` spool
- deploy the static interface and `nginx` configuration
- install and restart `lpe-ct.service`

The systemd service grants `CAP_NET_BIND_SERVICE` so the binary can listen on SMTP port `25` without running the service as root.

### Product coherence

This split keeps:

- the core `LPE` services on the `LAN` for business data and `JMAP`
- the sorting center in the `DMZ` for exposable traffic
- a future local AI path without data leaving the infrastructure


