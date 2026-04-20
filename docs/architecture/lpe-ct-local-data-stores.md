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

The current `LPE-CT` implementation does not use a relational database yet.

It currently persists local state in three ways:

- `LPE_CT_STATE_FILE`, default `/var/lib/lpe-ct/state.json`
- `LPE_CT_SPOOL_DIR`, default `/var/spool/lpe-ct`
- JSON policy artifacts inside the spool

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
- greylisting triplets in `greylist/<triplet>.json`
- reputation counters in `policy/reputation.json`
- throttling window artifacts in `policy/<rule>.json`
- bounded decision traces attached to queued message files

This means the repository already proves the need for sorting-center-local persistence, but it still uses flat files rather than a dedicated local database.

### Target architecture

`LPE-CT` keeps the current spool-first model as the default deployment path.

When operational pressure justifies it, `LPE-CT` may add one or more dedicated local databases that remain strictly technical stores owned by the sorting center.

The target storage model is therefore:

- `state.json` for local management configuration and bootstrap state
- spool files for raw queue ownership, raw message payload traces, quarantine payload custody, and replay-oriented artifacts
- optional dedicated local database stores for higher-churn technical indexes and coordination state

The preferred first dedicated store is an `LPE-CT`-owned local `PostgreSQL` service used only for technical state.

The current implementation may now upsert quarantined-message metadata into a private `quarantine_messages` table when `LPE_CT_LOCAL_DB_ENABLED=true` and `LPE_CT_LOCAL_DB_URL` is configured. That table is technical only; payload custody remains in the spool and quarantine directories.
The first `bayespam` implementation remains spool-first through `policy/bayespam.json`, which keeps the classifier operational even when the optional local PostgreSQL service is disabled.

Typical target domains for that local database are:

- Bayesian classifier tokens, corpus metadata, and training history
- sender, domain, and IP reputation history
- greylisting indexes and cleanup-friendly timestamps
- quarantine metadata and operational search indexes
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

The repository now keeps the existing spool-first runtime behavior.

At the same time, the management state and readiness model explicitly describe the future optional dedicated `LPE-CT` PostgreSQL profile with these purposes:

- Bayesian filtering
- reputation
- greylisting
- quarantine metadata
- cluster coordination

That is a foundation only. It does not yet move current runtime policy data out of the spool.

### Decision

`LPE-CT` is explicitly allowed to use dedicated local technical databases, including private `PostgreSQL` on `5432`, provided that:

- `LPE` remains the only system of record for user-visible product state
- `LPE-CT` databases remain limited to perimeter-owned technical data
- `5432` stays private and documented
- no direct `DMZ` path to the core `LPE` database is introduced
- spool and quarantine ownership stay under `LPE-CT`
