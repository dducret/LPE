# High Availability

This document defines the first realistic high-availability strategy for `LPE` and `LPE-CT`.

The strategy is intentionally incremental:

- the default deployment remains one `LPE` node and one `LPE-CT` node
- the first HA target is `active/passive`, not `active/active`
- the design must stay compatible with `Debian Trixie`
- the `DMZ` / core split remains strict

## Implemented MVP Status

The current codebase now implements the first operational HA step:

- `systemd` restart remains the default local recovery line on both nodes
- `LPE` and `LPE-CT` expose `/health`, `/health/live`, and `/health/ready`
- readiness now supports optional HA role gating through a local role file
- the role file accepts `active`, `standby`, `drain`, or `maintenance`
- when HA role gating is enabled, only `active` returns `ready`
- Debian examples now include readiness probes, role-switch scripts, and `keepalived` example configurations for both zones

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

### Mode 2: First HA target

This is the first supported HA topology:

- `2 x LPE` nodes on the `LAN`
- `PostgreSQL` primary + standby, with promotion outside the application
- `2 x LPE-CT` nodes in the `DMZ`
- one floating IP or equivalent stable endpoint per zone

The service model is:

- `LPE`: active/passive
- `LPE-CT`: active/passive
- `PostgreSQL`: primary/standby

The application remains stateless enough for this:

- `LPE` durable state is in `PostgreSQL`
- the binaries and web assets are installed from the same Git revision on both nodes
- the internal `LPE <-> LPE-CT` contract already uses stable HTTP endpoints that can sit behind a VIP

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

The first HA strategy for `LPE-CT` is also active/passive:

- one active `LPE-CT` node receives Internet SMTP and handles outbound relay
- one passive `LPE-CT` node is preinstalled and synchronized by configuration management
- the core uses the `LPE-CT` VIP for outbound handoff
- public `MX` or perimeter NAT points to the active `LPE-CT` endpoint

This first strategy deliberately keeps the spool local to the active node.

That has two consequences:

- a service failover is simple because the passive node can start receiving immediately
- a host loss on the active `LPE-CT` can leave some in-flight spool data on the failed node until it comes back

This tradeoff is accepted for the first HA iteration because it avoids introducing:

- shared `DMZ` storage
- cross-node spool replication
- a custom transport quorum layer

When the failed `LPE-CT` node returns, operators must inspect and replay `deferred`, `held`, or `quarantine` items as needed.

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
- configured primary relay target
- configured core delivery base URL
- optional reachability of the core `LPE` API

Core reachability is reported as a warning, not a hard readiness failure, because `LPE-CT` can continue to receive and queue mail locally during a temporary LAN-side outage.

## Recovery Model

### Local process recovery

For both products:

- `systemd` restart is the first line of recovery
- health endpoints expose whether the restarted process is actually ready
- when HA role gating is enabled, the local role file must be switched to `active` on the promoted node

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
2. set the promoted node role file to `active`
3. set the former active node role file to `standby` or `maintenance` before reusing it
4. verify `curl http://<lpe-ct-vip>:8380/health/ready`
5. verify SMTP banner and accepted spool writes
6. verify final delivery toward the core VIP
7. when the failed node returns, inspect its spool before reusing it

## Debian Trixie Operating Shape

The first practical `Debian Trixie` operating model is:

- one `keepalived` instance for the core VIP
- one separate `keepalived` instance for the `DMZ` VIP
- `check-lpe-ready.sh` and `check-lpe-ct-ready.sh` as tracked readiness probes
- `lpe-ha-set-role.sh` and `lpe-ct-ha-set-role.sh` as role-switch hooks
- one stable writer endpoint for `PostgreSQL`, promoted outside the application

The example files are intentionally limited to the first HA step. They do not attempt to solve:

- database promotion
- fencing
- split-brain prevention policy
- perimeter routing specifics

## Explicit Non-Goals Of This First Iteration

This first HA strategy does not yet implement:

- `active/active` `LPE`
- `active/active` `LPE-CT`
- replicated `DMZ` spool storage
- automatic `PostgreSQL` failover inside the application
- application-managed fencing or split-brain control

These topics can be added later, but they are intentionally deferred until the simpler active/passive model is exercised in production-like deployments.
