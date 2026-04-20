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
- anti-spam scoring and simple local reputation
- detailed decision tracing persisted in the spool

For outbound `LPE -> LPE-CT` handoff, the sorting center now also covers:

- local outbound routing rules
- local outbound throttling
- classification of `SMTP` replies into `relayed`, `deferred`, `bounced`, or `failed`
- structured technical and `DSN` detail for the latest attempt

### Architecture position

`LPE-CT` does not replace the core `LPE` stack:

- canonical business data remains in the core `LPE` services
- `PostgreSQL` remains the primary store for the core product
- `LPE-CT` keeps only minimal local operational state for configuration and operations
- the modern product axis remains `JMAP` on the core `LPE` side

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

### Management interface

The `LPE-CT` management interface covers:

- node identity and public bind addresses
- primary and secondary relays toward the `LAN`
- network surface policy and authorized CIDRs
- drain mode, quarantine, and `SPF` / `DKIM` / `DMARC` controls
- Git-first update source and maintenance window

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


