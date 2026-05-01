# High Availability

This document defines the supported high-availability strategy for `LPE` and `LPE-CT`.

The strategy is intentionally incremental:

- the default deployment remains one `LPE` node and one `LPE-CT` node
- the core `LPE` pattern is explicitly `active/standby`, not `active/active`
- `LPE-CT` supports either `active/standby` edge publication or replaceable horizontal nodes behind stable publication
- the design must stay compatible with `Debian Trixie`
- the `DMZ` / core split remains strict

## Supported patterns

The supported HA patterns are now explicit:

- core `LPE`: `active/standby` only
- `PostgreSQL`: primary plus standby, promoted outside the application
- `LPE-CT`: either one active node plus one standby, or a replaceable horizontal pool of equivalent nodes

Unsupported patterns remain:

- `active/active` core writers
- more than one traffic-writing core node at a time
- shared canonical mailbox state outside core `PostgreSQL`
- any `LPE-CT` topology that makes the sorting center canonical for mailbox state

The core reason for that split is different failure semantics:

- the core owns canonical mailbox and protocol state, so write leadership must stay singular
- `LPE-CT` owns perimeter transport and technical state, so nodes must be replaceable and horizontally repeatable

## Implemented MVP Status

The current codebase now implements the first operational HA step:

- `systemd` restart remains the default local recovery line on both nodes
- `LPE` and `LPE-CT` expose `/health`, `/health/live`, and `/health/ready`
- readiness now supports optional HA role gating through a local role file
- the role file accepts `active`, `standby`, `drain`, or `maintenance`
- when HA role gating is enabled, only `active` returns `ready`
- the `LPE` outbound worker now pauses automatically on non-active nodes
- `JMAP` push publication must follow the active core node only
- authenticated submission through `LPE-CT` now follows the currently published edge node only
- the core inbound-delivery API now rejects `LPE-CT` traffic on non-active nodes
- `LPE-CT` now rejects direct `SMTP` ingress and outbound handoff traffic on non-active nodes
- Debian examples now include readiness probes, role-switch scripts, and `keepalived` example configurations for both zones
- Debian examples now also include HA validation and spool-recovery scripts

This keeps the single-node majority path unchanged while making the first `active/passive` failover implementable.

## Why This Strategy

Most installations will run with a single `LPE` and a single `LPE-CT`.

That means the first HA strategy must not impose a cluster manager, shared filesystem, or distributed coordination layer on every deployment.

The chosen baseline is therefore:

- single-node by default
- `systemd` restart for local process recovery
- persistent queues and persistent database state
- optional warm standby nodes with a floating IP or equivalent front-end failover

This keeps the bootstrap path simple while still enabling a serious recovery plan for larger environments.

## Zones And Failover Boundaries

High availability is split by architectural zone:

- core zone: `LPE`, internal HTTP APIs, `JMAP`, `IMAP`, `ActiveSync`, `DAV`, web UI, outbound queue worker, `PostgreSQL`
- `DMZ` zone: `LPE-CT`, exposed `SMTP`, perimeter controls, quarantine, local spool, final-delivery handoff to the core

The two zones fail over independently.

Non-negotiable rules:

- the public Internet still reaches only `LPE-CT` or an equivalent edge publication layer
- `LPE` stays on the `LAN` side
- no public `SMTP` listener is moved into `LPE`
- failover must not collapse the `DMZ` and core roles into one node by default

## Deployment Modes

### Mode 1: Default single-node

This remains the reference deployment for most sites:

- `1 x LPE`
- `1 x PostgreSQL`
- `1 x LPE-CT`

Recovery is local:

- `systemd` restarts failed processes
- `LPE` keeps authoritative user state in `PostgreSQL`
- `LPE-CT` keeps local transport state in `/var/spool/lpe-ct`
- operators restore the node from backup if the host is lost

### Mode 2: Supported HA baseline

This is the first supported HA topology:

- `2 x LPE` nodes on the `LAN`
- `PostgreSQL` primary + standby, with promotion outside the application
- `2+ x LPE-CT` nodes in the `DMZ`
- one floating IP or equivalent stable endpoint per zone

The service model is:

- `LPE`: active/standby
- `LPE-CT`: active/standby or replaceable horizontal publication
- `PostgreSQL`: primary/standby

The application remains stateless enough for this:

- `LPE` durable state is in `PostgreSQL`
- the binaries and web assets are installed from the same Git revision on both nodes
- the internal `LPE <-> LPE-CT` contract already uses stable HTTP endpoints that can sit behind a VIP or edge load-balancer

### Mode 3: Replaceable horizontal `LPE-CT`

This is the preferred scale-out shape for the sorting center once a site needs more than one spare node:

- one active core `LPE` node plus one standby core node
- primary plus standby `PostgreSQL`
- multiple equivalent `LPE-CT` nodes published behind stable `MX`, NAT, or load-balancer entry points
- optional shared `LPE-CT` technical database or coordination layer for perimeter-only state

The architectural rules stay unchanged:

- queue payload custody remains in node-local spool unless a deployment explicitly adds replicated transport storage
- horizontal `LPE-CT` does not create distributed canonical mailbox state
- any node may be replaced, rebuilt, or reintroduced after manual spool review

This pattern is supported because `LPE-CT` is a perimeter service, not because the core has become multi-writer.

## Core HA Strategy

### Core critical components

The core critical path is:

- `PostgreSQL` writer availability
- `lpe.service`
- local `nginx`
- the `LPE` integration secret

The first HA strategy for the core is:

- one active `LPE` node bound to a core-side VIP or stable internal endpoint
- one passive `LPE` node installed and configured the same way
- one `PostgreSQL` primary and one standby

The passive `LPE` node does not need shared local storage because:

- mailbox state is canonical in `PostgreSQL`
- the web UIs are static assets
- outbound queue items are already stored in `PostgreSQL`

That means failover is:

1. promote the standby `PostgreSQL` node
2. move the core VIP to the standby `LPE` node
3. start or confirm `lpe.service` and `nginx` on that node

`LPE` does not need distributed locking in this first strategy because the intent is one active core node at a time.

## `LPE-CT` HA Strategy

### `DMZ` critical components

The `DMZ` critical path is:

- `lpe-ct.service`
- the SMTP listener on port `25`
- the local spool under `/var/spool/lpe-ct`
- the `LPE` integration secret
- the core delivery base URL

The first supported publication strategy for `LPE-CT` is active/standby:

- one active `LPE-CT` node receives Internet SMTP and handles outbound relay
- one standby `LPE-CT` node is preinstalled and synchronized by configuration management
- the core uses the `LPE-CT` VIP for outbound handoff
- public `MX` or perimeter NAT points to the active `LPE-CT` endpoint

This baseline strategy deliberately keeps the spool local to the node that accepted the traffic.

That has two consequences:

- a service failover is simple because the standby node can start receiving immediately
- a host loss on one `LPE-CT` node can leave some in-flight spool data on that node until it comes back

This tradeoff is accepted for the first HA iteration because it avoids introducing:

- shared `DMZ` storage
- cross-node spool replication
- a custom transport quorum layer

When a failed `LPE-CT` node returns, operators must inspect and replay `deferred`, `held`, or `quarantine` items as needed.

### Replaceable horizontal `LPE-CT` behavior

When `LPE-CT` is run as a horizontal pool:

- inbound `SMTP` may land on any published healthy edge node
- authenticated submission may land on any published healthy submission node
- outbound handoff from the core may target any healthy published `LPE-CT` integration endpoint
- each node owns the spool items it accepted unless the deployment adds an explicit transport-replication layer

This means horizontal scale improves perimeter availability and node replaceability, but it does not remove the need for per-node spool recovery after host loss.

## Failover behavior by function

### Core outbound queue worker

The outbound queue worker is a core-only active-node function.

Rules:

- only the active core node may poll and dispatch `outbound_message_queue`
- standby core nodes must stay paused
- after core failover, the promoted node resumes dispatch from canonical `PostgreSQL` queue state
- duplicate dispatch must be prevented by the single-active-core rule, not by assuming safe multi-writer behavior

### `JMAP` push listeners

`JMAP` push publication and WebSocket-adjacent notification wakeups are active-core functions.

Rules:

- only the active core node may be published as traffic-ready for `JMAP`
- after core failover, clients must reconnect to the newly active endpoint
- long-lived listener state must be treated as disposable; canonical mailbox state remains in `PostgreSQL`
- failover correctness is based on replay from canonical state tokens, not on preserving in-memory listener sessions

### `ActiveSync` long-poll listeners

`ActiveSync` long-poll listeners are also active-core functions.

Rules:

- long-poll requests on the failed node may terminate during failover
- clients are expected to reconnect to the active node
- correctness depends on canonical sync state in the core database, not on preserving the original TCP session

### Authenticated submission

Authenticated client submission is an edge-publication function on `LPE-CT`.

Rules:

- only healthy published `LPE-CT` nodes may accept client submission
- if a submission node fails before canonical acceptance by `LPE`, the client receives a temporary failure and must retry
- if canonical submission in `LPE` succeeded before the edge failure, the authoritative `Sent` copy already exists and outbound relay continues through the normal queue path
- submission failover must never create a second non-canonical submission path

## Health And Readiness

The first implementation standardizes three classes of health endpoint:

- `/health`: compatibility endpoint
- `/health/live`: process liveness
- `/health/ready`: readiness for local takeover and traffic

Readiness is intentionally conservative:

- a critical local dependency failure returns `failed`
- a remote dependency issue is reported as a warning when the node can still safely accept traffic

### `LPE` readiness checks

`LPE` currently validates:

- `PostgreSQL` reachability
- integration secret validity
- optional HA role activation state from `LPE_HA_ROLE_FILE`
- optional reachability of the `LPE-CT` API

`LPE-CT` reachability is reported as a warning, not a hard readiness failure, because the core can still accept user traffic and queue outbound work while the `DMZ` side recovers.

### `LPE-CT` readiness checks

`LPE-CT` currently validates:

- integration secret validity
- optional HA role activation state from `LPE_CT_HA_ROLE_FILE`
- presence of the local state file
- presence of required spool directories
- outbound delivery mode: direct recipient-domain `MX` by default, with optional upstream smart-host reachability when configured
- configured core delivery base URL
- optional reachability of the core `LPE` API

Core reachability is reported as a warning, not a hard readiness failure, because `LPE-CT` can continue to receive and queue mail locally during a temporary LAN-side outage.

## Recovery Model

### Local process recovery

For both products:

- `systemd` restart is the first line of recovery
- health endpoints expose whether the restarted process is actually ready
- when HA role gating is enabled, the local role file must be switched to `active` on the promoted node
- non-active nodes now also stop accepting new active traffic at the application edge

### Core node recovery

If the active core node is lost:

1. promote the `PostgreSQL` standby
2. move the core VIP
3. set the promoted node role file to `active`
4. set the former active node role file to `standby` or `maintenance` before reusing it
5. verify `curl http://<core-vip>:8080/health/ready`
6. verify web and protocol publication through `nginx`
7. verify outbound queue draining toward the `LPE-CT` VIP

The outbound worker resumes naturally in this model because queue state stays in `PostgreSQL` and only the `active` node is considered ready for traffic.

### `DMZ` node recovery

If the active `LPE-CT` node is lost:

1. move the `DMZ` VIP or perimeter publication
2. set the promoted or replacement node role file to `active`
3. set the former active node role file to `standby` or `maintenance` before reusing it
4. verify `curl http://<lpe-ct-vip>:8380/health/ready`
5. verify SMTP banner and accepted spool writes
6. verify final delivery toward the core VIP
7. when the failed node returns, inspect its spool before reusing it

### `LPE-CT` spool return to service

When a failed `LPE-CT` node returns, the local spool must be treated as transport custody, not as disposable cache.

The tracked recovery line is:

1. keep the node in `maintenance` or `standby`
2. inventory the queues with `lpe-ct-spool-recover.sh summary`
3. inspect suspicious traces with `lpe-ct-spool-recover.sh show <trace-id>`
4. requeue safe `deferred` or `held` items with `lpe-ct-spool-recover.sh requeue ...`
5. keep `quarantine` under manual review
6. only then return the node to service as `standby` or `active`

## Debian Trixie Operating Shape

The first practical `Debian Trixie` operating model is:

- one `keepalived` instance for the core VIP
- one separate `keepalived` instance for the `DMZ` VIP
- `check-lpe-ready.sh` and `check-lpe-ct-ready.sh` as tracked readiness probes
- `lpe-ha-set-role.sh` and `lpe-ct-ha-set-role.sh` as role-switch hooks
- `test-ha-core-active-passive.sh` and `test-ha-lpe-ct-active-passive.sh` as local HA validation scenarios
- `lpe-ct-spool-recover.sh` and `test-lpe-ct-spool-recovery.sh` as first spool-return tooling
- one stable writer endpoint for `PostgreSQL`, promoted outside the application

The example files are intentionally limited to the first HA step. They do not attempt to solve:

- database promotion
- fencing
- split-brain prevention policy
- perimeter routing specifics

## Explicit Non-Goals Of This First Iteration

This supported HA strategy does not yet implement:

- `active/active` `LPE`
- queue-replicated `LPE-CT` with zero-loss host failover guarantees
- replicated `DMZ` spool storage
- automatic `PostgreSQL` failover inside the application
- application-managed fencing or split-brain control

These topics can be added later, but they are intentionally deferred until the simpler active/passive model is exercised in production-like deployments.
