# Edge and Protocol Exposure

### Goal

This document describes the boundary between `LPE` and `LPE-CT` for network exposure, protocol publication, and internal transport.

### Core rule

`LPE-CT` is the unique external exposure point.

The core `LPE` server must not be directly reachable from the public Internet and does not need to be exposed for the target architecture to work.

### Baseline external exposure

`LPE-CT` publishes:

- inbound `SMTP` on port `25`, with `STARTTLS` advertised only when an active
  public certificate and private-key profile is configured in the `LPE-CT`
  management console under `System Setup -> Mail relay -> SMTP Settings`
- authenticated client `SMTP` submission on implicit `TLS` port `465` when configured
- the `LPE` web client and `LPE-CT` management publication over `HTTPS` on `443`
- plain `HTTP` on port `80` only as a redirect to the `HTTPS` edge; port `80`
  must not be configured as the `HTTPS` listener
- `ActiveSync` over `HTTPS` under `/activesync`
- exposed `JMAP` endpoints over `TLS` toward `LPE` under `/api/jmap/*`
- secure `JMAP` WebSockets over `TLS` under the same published `JMAP` origin when the `JMAP` WebSocket endpoint is enabled
- `IMAPS` on port `993`, with `LPE-CT` terminating client `TLS` and proxying the internal clear IMAP stream to the core `LPE` IMAP adapter
- `ManageSieve` over `TLS` on `4190` when enabled
- `SMTPS`

For secure client submission, the baseline target prefers implicit TLS on port `465`, aligned with `RFC 8314`.

The same public certificate chain may be reused for `HTTPS` `443`, implicit
`TLS` submission `465`, and `IMAPS` `993` on the same public hostname. `LPE-CT`
stores selectable public `SMTP` `STARTTLS` certificate profiles in its private
management state; operators upload a PEM certificate chain and matching PEM
private key, then select the active profile in the management console. Debian
installations may still bootstrap the initial public profile through:

- `LPE_CT_PUBLIC_TLS_CERT_PATH` / `LPE_CT_PUBLIC_TLS_KEY_PATH` to bootstrap the
  active inbound `SMTP` `STARTTLS` profile on `25`
- `LPE_CT_PUBLIC_TLS_CERT_PATH` / `LPE_CT_PUBLIC_TLS_KEY_PATH` for `nginx` `443`
- `LPE_CT_SUBMISSION_TLS_CERT_PATH` / `LPE_CT_SUBMISSION_TLS_KEY_PATH` for `465`
- `LPE_CT_IMAPS_TLS_CERT_PATH` / `LPE_CT_IMAPS_TLS_KEY_PATH` for `993`

Port `25` is a plaintext `SMTP` listener that upgrades with `STARTTLS`; it is
not implicit `TLS`. External validation must use `openssl s_client -starttls
smtp -connect <mx-host>:25 -servername <mx-host>`. After the server replies
`220 ready to start TLS` and the TLS handshake succeeds, clients must send a
fresh `EHLO` before `MAIL FROM`, `RCPT TO`, or `DATA`.

The `HTTPS` publication must redirect accidental plain `HTTP` traffic to the
configured TLS origin, including nginx's plain-HTTP-on-HTTPS-port case. This
avoids presenting the default nginx `400 Bad Request` page when an administrator
types `http://host:443` instead of `https://host`. Debian deployments render the
redirect with `LPE_CT_NGINX_LISTEN_PORT` so a non-standard HTTPS port is explicit
instead of silently redirecting to a closed `443`.

When client submission is enabled, `LPE-CT` terminates the external `TLS` session, performs `AUTH`, and forwards the raw RFC 822 message plus envelope to the internal canonical `LPE` submission workflow. `LPE-CT` does not create the authoritative `Sent` copy itself, and the internal `LPE -> LPE-CT` outbound relay remains a backend-only transport.

When published, the `JMAP` WebSocket endpoint remains a reverse-proxied `LPE` protocol adapter behind `LPE-CT`; it does not change the rule that `LPE-CT` is the only external exposure point.

The public `JMAP` paths published by `LPE-CT` are:

- `/api/jmap/session`
- `/api/jmap/api`
- `/api/jmap/upload/{accountId}`
- `/api/jmap/download/{accountId}/{blobId}/{name}`
- `/api/jmap/ws`

`/api/jmap/ws` must support `HTTP` upgrade and long-lived `WSS` sessions. `EmailSubmission/set` remains a `JMAP` adapter over the canonical `LPE` submission model and must never hand mail directly to `SMTP`.

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

The implemented v1 HTTP topology is:

- Internet `SMTP` port `25` -> `LPE-CT` -> `POST ${LPE_CT_CORE_DELIVERY_BASE_URL}/internal/lpe-ct/inbound-deliveries` on `LPE`, default `LPE` port `8080`
- inbound `RCPT TO` verification -> `POST ${LPE_CT_CORE_DELIVERY_BASE_URL}/internal/lpe-ct/recipient-verification` on `LPE`
- client `SMTP` submission port `465` -> `LPE-CT` -> `POST ${LPE_CT_CORE_DELIVERY_BASE_URL}/internal/lpe-ct/submission-auth` and `POST ${LPE_CT_CORE_DELIVERY_BASE_URL}/internal/lpe-ct/submissions` on `LPE`
- core outbound queue -> `LPE` worker -> `POST ${LPE_CT_API_BASE_URL}/api/v1/integration/outbound-messages` on `LPE-CT`, default `LPE-CT` API port `8380`

Port `2525` is not part of this canonical `LPE <-> LPE-CT` bridge. If used, it is only a configured technical `SMTP` upstream relay target owned by `LPE-CT`.

The dedicated local-store boundary for `LPE-CT`, including private `5432` use, is documented in `docs/architecture/lpe-ct-local-data-stores.md`.


