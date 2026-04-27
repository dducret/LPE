# `LPE-CT` Local Data Stores

### Goal

This document formalizes the storage boundary for `LPE-CT`.

It defines:

- the current repository state
- the target architecture for dedicated local `LPE-CT` databases
- the data classes allowed in those stores
- the data classes forbidden because they remain canonical in `LPE`
- the exact network rules for local database traffic, including private `5432`

### Non-negotiable split

The split remains strict:

- `LPE` is the sole system of record for mailboxes, contacts, calendars, tasks, rights, and all user-visible state
- `LPE-CT` owns only perimeter transport and security state for `SMTP` ingress, outbound relay, quarantine, filtering, traceability, throttling, and cluster coordination
- `LPE-CT` must not create a parallel `Inbox`, `Sent`, `Drafts`, or `Outbox`
- `LPE-CT` must not depend on direct `DMZ` access to the core `LPE` `PostgreSQL` database

### Current repository state

The current `LPE-CT` implementation now uses a dedicated private `PostgreSQL` store as its default technical state backend.

It currently persists local state in three ways:

- `LPE_CT_STATE_FILE`, default `/var/lib/lpe-ct/state.json`
- `LPE_CT_SPOOL_DIR`, default `/var/spool/lpe-ct`
- private `PostgreSQL` technical tables on the dedicated `LPE-CT` local database

The active spool layout in the current code is:

- `incoming/`
- `deferred/`
- `quarantine/`
- `held/`
- `bounces/`
- `sent/`
- `outbound/`
- `policy/`
- `greylist/`

The currently implemented technical local state already includes:

- management configuration and audit metadata in `state.json`
- queued inbound and outbound message traces as JSON files in the spool
- quarantine ownership in `quarantine/`
- greylisting triplets in `greylist_entries`
- reputation counters in `reputation_entries`
- throttling window artifacts in `throttle_windows`
- `bayespam` corpus state in `bayespam_corpora`
- quarantine metadata in `quarantine_messages`
- bounded decision traces attached to queued message files

This keeps indexed perimeter state in a private database while leaving payload custody and queue ownership in the spool.

### Target architecture

`LPE-CT` keeps spool custody for transport artifacts, but the default deployment path now also includes a private dedicated `PostgreSQL` service for indexed technical state.

`LPE-CT` may still add additional dedicated local databases later when operational pressure justifies it, but they remain strictly technical stores owned by the sorting center.

The target storage model is therefore:

- `state.json` for local management configuration and bootstrap state
- spool files for raw queue ownership, raw message payload traces, quarantine payload custody, and replay-oriented artifacts
- dedicated local database stores for higher-churn technical indexes and coordination state

The preferred first dedicated store is an `LPE-CT`-owned local `PostgreSQL` service used only for technical state.

The current implementation now persists the default indexed perimeter state into private tables such as `greylist_entries`, `reputation_entries`, `bayespam_corpora`, `throttle_windows`, and `quarantine_messages` when `LPE_CT_LOCAL_DB_ENABLED=true` and `LPE_CT_LOCAL_DB_URL` is configured. Those tables are technical only; payload custody remains in the spool and quarantine directories.
Retained mail-flow history and scheduled quarantine digest artifacts remain sorting-center-owned technical state as well; the current implementation stores retained history in the spool under `policy/transport-audit.jsonl` and digest artifacts under `policy/digest-reports/`, while the private PostgreSQL store now also persists dedicated technical indexes and configuration mirrors for:

- `mail_flow_history`
- `policy_address_rules`
- `attachment_policy_rules`
- `digest_settings`
- `digest_recipients`
- `recipient_verification_cache`
- `recipient_verification_settings`
- `dkim_domain_configs`
- `accepted_domains`

Typical target domains for that local database are:

- Bayesian classifier tokens, corpus metadata, and training history
- sender, domain, and IP reputation history
- greylisting indexes and cleanup-friendly timestamps
- quarantine metadata and operational search indexes
- retained perimeter mail-flow history indexes and reporting metadata
- technical admin policy metadata for allow/block lists, attachment controls, digest schedules, and DKIM domain references
- recipient-verification cache state and verification-policy materialization
- accepted-domain relay policy, including destination server, recipient-verification mode, and per-domain perimeter check toggles
- throttling counters and routing coordination metadata
- cluster membership, failover coordination, and shared perimeter state across `LPE-CT` nodes

### Allowed data in local `LPE-CT` stores

The following data is explicitly allowed in `LPE-CT` local databases because it is perimeter-owned technical state:

- Bayesian token statistics
- Bayesian training metadata
- sender, domain, relay, and IP reputation
- greylisting triplets and their timers
- DNSBL, authentication, and policy decision caches
- quarantine metadata, operational indexes, and release workflow references
- throttling windows and counters
- relay routing hints and local transport history
- bounded traceability indexes keyed by `trace_id`, peer IP, relay, or policy result
- cluster membership and failover coordination metadata between `LPE-CT` nodes

`LPE-CT` may also keep:

- raw `SMTP` payloads in spool or quarantine storage
- raw inbound or outbound technical traces
- bounded operational correlation identifiers

Those artifacts remain technical evidence and replay material. They do not become canonical product state.

### Forbidden data because it is canonical in `LPE`

The following data is forbidden in dedicated local `LPE-CT` databases as authoritative state:

- mailbox hierarchy
- canonical `Inbox`
- canonical `Sent`
- canonical drafts
- canonical `Outbox`
- message ownership, mailbox rights, and user-visible flags
- contacts
- calendars
- tasks
- tenant administration state
- user rights, ACLs, delegations, or sharing state
- canonical search indexes for user-visible content
- protected `Bcc` business storage

Temporary possession is allowed only where the perimeter function requires it, for example:

- raw inbound `SMTP` bytes before final delivery
- quarantine copies retained by the sorting center
- bounded transport metadata required for `DSN`, bounce handling, replay, or incident review

Even in those cases, `LPE-CT` remains non-canonical.

### Rebuild and retention expectations

The local PostgreSQL store must remain operationally discardable.

That means the expected rebuild behavior is:

- `greylist_entries`, `reputation_entries`, `bayespam_corpora`, and `throttle_windows` may be rebuilt or relearned from future traffic plus any retained spool artifacts
- `quarantine_messages` may be rebuilt from the current quarantine spool because payload custody remains in `quarantine/`
- `mail_flow_history` may be partially rebuilt from retained `policy/transport-audit.jsonl` within the configured retention window
- `policy_address_rules`, `attachment_policy_rules`, `digest_settings`, `digest_recipients`, `recipient_verification_settings`, and `dkim_domain_configs` are mirrors of `state.json` management configuration and can be repopulated from that file at startup
- `accepted_domains` is a mirror of `state.json` management configuration and can be repopulated from that file at startup
- `recipient_verification_cache` is disposable short-lived materialized state and may be dropped without changing canonical mailbox truth

Retention remains bounded:

- queue custody stays in the spool
- digest artifacts remain technical reports under `policy/digest-reports/`
- retained `policy/transport-audit.jsonl` follows the configured history window
- `mail_flow_history` rows follow the same retained history window
- digest artifacts under `policy/digest-reports/` follow a dedicated digest-report retention window
- recipient-verification cache rows expire automatically by TTL and must not become durable mailbox-directory truth

The current reporting and quarantine implementation also makes the rebuild boundary explicit:

- `quarantine_messages` is treated as an operational index over the live quarantine spool and is reindexed from current `quarantine/` artifacts at startup when the private local database is enabled
- retained history pruning removes only bounded technical evidence, never canonical mailbox data
- digest artifacts are derived from retained sorting-center history and may be deleted or regenerated without affecting queue ownership or user-visible state

### Storage assignment by function

Recommended storage ownership for the target architecture is:

- `state.json`: node identity, relay settings, management bootstrap, local admin audit trail, declared local-store topology
- spool: inbound/outbound queue files, raw message traces, quarantine payload custody, replay-oriented artifacts
- local `PostgreSQL`: Bayesian state, reputation, greylisting, quarantine metadata indexes, throttling counters, cluster metadata

This keeps the spool authoritative for immediate transport custody while allowing indexed technical state to move away from flat files when needed.

### Exact `5432` policy

Port `5432` is allowed for `LPE-CT` only under these rules:

- it must refer to an `LPE-CT`-owned technical database, not the core `LPE` product database
- it must never be Internet-facing
- it must never be published on the public `DMZ` edge
- it must never be used as a path from the `DMZ` toward the core `LPE` `PostgreSQL` service
- it must stay on one of these scopes only:
  - loopback on a single `LPE-CT` node
  - a private backend segment behind the `DMZ`
  - a dedicated `LPE-CT` cluster network between sorting-center nodes

The accepted network scopes are therefore:

- `host-local`
- `private-backend`
- `lpe-ct-cluster`

The following uses of `5432` are explicitly forbidden:

- `0.0.0.0:5432` or equivalent wildcard exposure on the public edge
- perimeter firewall rules exposing `5432` to the Internet
- direct connections from `LPE-CT` nodes to the core `LPE` `PostgreSQL` writer across the `DMZ`
- treating an `LPE-CT` local database as an extension schema of the core product database

### Allowed network flows

The allowed network flows for local database and clustering traffic are:

- `LPE-CT` process to loopback `5432` on the same node
- `LPE-CT` node to private `5432` on another `LPE-CT` node when a documented cluster topology requires it
- management and backup traffic to the local database from explicitly authorized internal addresses

The allowed `LPE` and `LPE-CT` cross-zone flows remain:

- `LPE -> LPE-CT` outbound handoff over the integration API
- `LPE-CT -> LPE` final delivery over the internal delivery API

No local database flow changes that contract.

### Separation from the core `LPE` database

If `LPE-CT` adopts local `PostgreSQL`, that database remains wholly separate from core `LPE` `PostgreSQL`.

That implies:

- separate schema ownership
- separate lifecycle and retention policy
- separate backup and restore procedures
- no shared tables with the core product
- no assumption that failure of the local `LPE-CT` database changes canonical mailbox truth

The local `LPE-CT` database should remain operationally replaceable.

Rebuild sources may include:

- the active spool
- current quarantine artifacts
- current perimeter traffic
- bounded retained technical history

### Clustering guidance

For single-node deployments, spool plus JSON state remains acceptable.

For active/passive or future clustered `LPE-CT`, a dedicated local database becomes preferable for:

- shared quarantine metadata
- shared greylisting timers
- shared reputation history
- shared throttling windows
- node-role and coordination metadata

Even in clustered mode, the state must stay:

- technical rather than canonical
- bounded by retention rules
- isolated from the core tenant and mailbox database
- documented so operators know how to rebuild or discard it safely

### Current implementation decision

The repository now uses private dedicated `PostgreSQL` as the default persisted state for indexed `LPE-CT` technical data.

That current default profile covers:

- Bayesian filtering
- reputation
- greylisting
- quarantine metadata
- cluster coordination
- outbound throttling state

For reporting and operator search, the private store now also carries dedicated technical indexing fields so perimeter workflows do not depend on full spool scans:

- retained quarantine receipt time
- sender and recipient domains
- route target and remote message reference
- combined operator search text with full-text indexing and optional trigram acceleration
- retained mail-flow event timestamps and correlation metadata

The spool remains authoritative for queue ownership, raw message traces, quarantine payload custody, and replay-oriented artifacts.

### Decision

`LPE-CT` is explicitly allowed to use dedicated local technical databases, including private `PostgreSQL` on `5432`, provided that:

- `LPE` remains the only system of record for user-visible product state
- `LPE-CT` databases remain limited to perimeter-owned technical data
- `5432` stays private and documented
- no direct `DMZ` path to the core `LPE` database is introduced
- spool and quarantine ownership stay under `LPE-CT`
