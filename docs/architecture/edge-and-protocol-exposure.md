# Edge and Protocol Exposure

### Goal

This document describes the boundary between `LPE` and `LPE-CT` for network exposure, protocol publication, and internal transport.

### Core rule

`LPE-CT` is the unique external exposure point.

The core `LPE` server must not be directly reachable from the public Internet and does not need to be exposed for the target architecture to work.

### Baseline external exposure

`LPE-CT` publishes:

- inbound `SMTP` on port `25`
- the `LPE` web client over `HTTPS` on `443` under `/mail`
- `ActiveSync` over `HTTPS` under `/activesync`
- exposed `JMAP` endpoints over `TLS` toward `LPE`
- secure `JMAP` WebSockets over `TLS` under the same published `JMAP` origin when the `JMAP` WebSocket endpoint is enabled
- `IMAPS`
- `ManageSieve` over `TLS` on `4190` when enabled
- `SMTPS`

For secure client submission, the baseline target prefers implicit TLS on port `465`, aligned with `RFC 8314`.

When published, the `JMAP` WebSocket endpoint remains a reverse-proxied `LPE` protocol adapter behind `LPE-CT`; it does not change the rule that `LPE-CT` is the only external exposure point.

### Separation between publication and protocol logic

- `LPE` owns mailbox and collaboration protocol logic
- `LPE` also owns mailbox `Sieve` execution and the related `ManageSieve` service
- `LPE-CT` owns external exposure, reverse proxying, TCP/TLS proxying, and edge security posture

`LPE-CT` edge policies remain distinct from end-user mailbox `Sieve` rules. `Sieve` must not become a vehicle for perimeter filtering, anti-spam decisions, quarantine handling, relay routing, or throttling policies, which stay under sorting-center control.

### ActiveSync

`ActiveSync` is chatty and commonly relies on long-polling behavior.

The `LPE-CT` front layer must therefore support:

- long timeouts
- protocol-aware connection handling
- no premature disconnects for Outlook and iOS during long-held sync waits

`JMAP` WebSockets require similar edge treatment:

- support for `HTTP` upgrade handling and persistent `TLS` sessions
- no premature idle timeout while a mailbox session is waiting for state changes
- publication only on the externally documented `LPE-CT` hostname, never by exposing the core `LPE` bind address directly

### LPE-CT as stateless as possible

`LPE-CT` should remain as stateless as possible in order to simplify:

- `DNS` load balancing
- `VRRP`
- horizontal node replacement

Necessary edge state such as spool or quarantine must remain bounded, explicit, and operationally replaceable.

`LPE-CT` may still use dedicated local technical databases when bounded file-based state is no longer sufficient, for example for Bayesian filtering, reputation, or cluster coordination. Those databases remain sorting-center-local stores and must not become a second canonical product database.

When such a database is used, private `5432` remains acceptable only on loopback, a private backend segment, or a dedicated `LPE-CT` cluster network. It must never be published on the public `DMZ` edge.

### Internal transport `LPE-CT <-> LPE`

The target protocol for internal `LPE-CT` to `LPE` exchanges is `gRPC`.

This choice is strictly limited to the internal backbone and does not change the externally exposed client protocols.

The preferred Rust implementation for that internal layer is `tonic`.

The current functional v1 contract remains documented separately in `docs/architecture/lpe-ct-integration.md`.

The dedicated local-store boundary for `LPE-CT`, including private `5432` use, is documented in `docs/architecture/lpe-ct-local-data-stores.md`.


