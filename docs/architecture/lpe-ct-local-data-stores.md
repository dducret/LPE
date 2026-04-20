# `LPE-CT` Local Data Stores

### Goal

This document defines which local data stores `LPE-CT` may use on its own `DMZ` host or cluster, and which data must remain canonical in the core `LPE` platform.

The intent is to allow `LPE-CT` to operate independently in the perimeter while preserving the non-negotiable split between:

- `LPE`, the system of record for user-visible state
- `LPE-CT`, the sorting center for `SMTP`, perimeter policy, quarantine, relay, and traceability

### Core rule

`LPE-CT` may use dedicated local stores for technical edge needs.

Those stores are allowed only for `LPE-CT`-owned operational data such as:

- local spool metadata
- quarantine indexes
- anti-spam and Bayesian classifier state
- greylisting state
- sender, domain, and IP reputation
- routing and throttling state
- bounded decision traces and operational correlation indexes
- cluster coordination and failover metadata between `LPE-CT` nodes

Those stores must not become a second canonical mailbox or collaboration database.

### Canonical data forbidden in `LPE-CT`

The following data remains canonical in `LPE` and must not be reimplemented or promoted to authoritative state in `LPE-CT` local databases:

- mailboxes and mailbox hierarchy
- authoritative `Inbox`, `Sent`, drafts, and user-visible message state
- contacts
- calendars
- tasks
- user rights, ACLs, delegations, and tenant administration state
- canonical search indexes for user-visible content
- protected `Bcc` business storage

`LPE-CT` may temporarily hold raw `SMTP` payloads, quarantine copies, and bounded transport metadata, but those remain perimeter-operational artifacts rather than canonical product state.

### Allowed local storage patterns

The current v1 implementation already uses:

- a local JSON state file for management configuration
- a local disk spool for inbound, deferred, quarantine, held, sent, and policy artifacts

That model may evolve toward one or more dedicated local databases when operational pressure justifies it.

Examples of acceptable local stores include:

- a local Bayesian database for spam classification tokens and training state
- a local `PostgreSQL` instance for quarantine indexes, routing rules, throttling counters, or cluster coordination
- a local embedded store for greylisting or reputation caches

The decisive rule is ownership of the data, not the storage engine.

### `PostgreSQL` on port `5432`

`PostgreSQL` is acceptable on `LPE-CT` when it is used as a dedicated local technical store for the sorting center.

Typical acceptable uses:

- Bayesian classifier persistence
- reputation and greylisting state
- cluster membership and coordination metadata
- quarantine search and operational lookups

Network constraints:

- `5432` must never be publicly exposed on the `DMZ` edge
- `5432` traffic must remain limited to the local host, a private backend segment, or a dedicated `LPE-CT` cluster network
- `LPE-CT` must not require direct `DMZ` access to the core `LPE` `PostgreSQL` database
- if multiple `LPE-CT` nodes coordinate through `5432`, that flow must be documented as sorting-center-internal traffic, not as core-product database access

### Separation from the core `LPE` store

If `LPE-CT` uses a local `PostgreSQL` database, it must be treated as a sorting-center database, not as an extension of the core `LPE` database.

That means:

- schema ownership remains local to `LPE-CT`
- lifecycle, retention, and rebuild policies remain local to `LPE-CT`
- data must stay reconstructible or replaceable from spool, perimeter traffic, and bounded operational history where practical
- failure of the local `LPE-CT` database must not redefine canonical user-visible mailbox truth

### Clustering guidance

When `LPE-CT` runs as a cluster, a dedicated local database may be preferable to flat files for:

- shared quarantine indexes
- shared Bayesian state
- reputation scoring history
- throttling windows across nodes
- election or active/passive coordination metadata

Even in clustered mode, the system should keep the perimeter state bounded and operationally replaceable.

Clustered `LPE-CT` state must therefore remain:

- technical rather than canonical
- documented with explicit retention and rebuild rules
- isolated from the core `LPE` tenant and mailbox database

### Current implementation status

In the current repository state, `LPE-CT` does not yet use a relational database.

It currently relies on:

- `LPE_CT_STATE_FILE` for local management state
- `LPE_CT_SPOOL_DIR` for queue, quarantine, and policy artifacts

This is compatible with the target architecture, but it does not yet implement a dedicated Bayesian or cluster database.

### Decision

The architecture explicitly allows `LPE-CT` to adopt dedicated local technical databases, including a local `PostgreSQL` service on private `5432`, provided that:

- `LPE` remains the sole system of record for user-visible product state
- `LPE-CT` stores remain limited to perimeter-owned technical data
- no direct `DMZ` dependency on the core `LPE` database is introduced
- `5432` remains non-public and tightly scoped
