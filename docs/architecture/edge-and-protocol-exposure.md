# Edge and Protocol Exposure

## Current State/Functionality Overview

`LPE-CT` is the public edge and DMZ sorting center; the core `LPE` service is not directly Internet-facing. Client-facing protocols must route through exposed edge paths and converge on canonical `LPE` state.

## Implementation/Usage

- Public edge responsibilities:
  - Internet `SMTP` ingress
  - outbound relay
  - authenticated client submission where deployed
  - quarantine
  - perimeter filtering
  - public HTTPS/WSS proxying
  - traceability
- Core `LPE` responsibilities:
  - mailboxes
  - contacts
  - calendars
  - tasks
  - rights
  - canonical `Sent`
  - user-visible state
- `LPE` must not expose Internet-facing `SMTP`.
- `LPE-CT` must not store canonical mailbox, collaboration, rights, or user-visible state.
- Public HTTPS routes may include:
  - `/mail/`
  - `/admin/`
  - `/assets/`
  - `/api/mail/auth/login`
  - `/api/jmap/session`
  - `/api/jmap/api`
  - `/api/jmap/upload/{accountId}`
  - `/api/jmap/download/{accountId}/{blobId}/{name}`
  - `/api/jmap/ws`
  - `/api/jmap/events`
  - `/Microsoft-Server-ActiveSync`
  - `/EWS/Exchange.asmx`
  - `/mapi/`
  - `/rpc/rpcproxy.dll`
  - `/autoconfig/mail/config-v1.1.xml`
  - `/.well-known/autoconfig/mail/config-v1.1.xml`
  - `/autodiscover/autodiscover.xml`
- Public non-HTTPS client ports may include:
  - `993` IMAPS, terminated by `LPE-CT` and proxied to the private core `lpe-imap` listener
  - `465` authenticated client submission, terminated by `LPE-CT` only when deployed
- Internal routes:
  - `/api/v1/integration/outbound-messages`
  - `/internal/lpe-ct/inbound-deliveries`
  - `/internal/lpe-ct/recipient-verification`
  - `/internal/lpe-ct/submission-auth`
  - `/internal/lpe-ct/submissions`
- Edge publication is separate from protocol implementation.
- The web client uses an HttpOnly, `SameSite=Strict` same-origin mailbox-session cookie. It has the `Secure` attribute whenever the public edge forwards HTTPS; the local HTTP development/LAN route omits that attribute because browsers otherwise discard the session. The public WSS route must forward the browser upgrade and cookie; clients must not put bearer credentials in a native WebSocket header.
- Autodiscovery must publish only implemented and exposed endpoints.
- IMAP autodiscovery/autoconfiguration must publish the public `LPE-CT` IMAPS hostname only after the `LPE-CT` IMAPS proxy is configured and verified. It must not publish the private core `LPE` IMAP listener.
- Autodiscovery/autoconfiguration must not publish client `SMTP` unless `LPE-CT` exposes real authenticated client submission; the internal `LPE -> LPE-CT` handoff is not client submission.
- An edge route alone is not Autodiscover publication evidence. `mapiHttp` requires its MAPI/HTTP transport and MAPI gate; `EXCH` requires the authenticated `/rpc/rpcproxy.dll` route and EXCH gate; `EXPR` requires that same RPC/HTTP route and its separate Outlook Anywhere gate. A client `X-MapiHttpCapability` header only negotiates an already approved MAPI/HTTP response ([MS-OXDSCLI] §§2.2.2.1, 2.2.4.1.1.2.6, 2.2.4.1.1.2.46, and 3.2.5.1).

## Reference Table/List

| Surface | Public component | Core component |
| --- | --- | --- |
| inbound `SMTP` | `LPE-CT` | none |
| outbound relay | `LPE-CT` | outbound queue worker |
| authenticated client submission | `LPE-CT` | canonical submission API |
| `JMAP` | `LPE-CT` HTTPS/WSS proxy | `lpe-jmap` |
| `IMAP` | `LPE-CT` TLS proxy when exposed | `lpe-imap` |
| `ActiveSync` | `LPE-CT` HTTPS proxy | `lpe-activesync` |
| `EWS` / `MAPI` | `LPE-CT` HTTPS proxy | `lpe-exchange` |
