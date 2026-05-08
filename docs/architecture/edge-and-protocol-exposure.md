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
- Internal routes:
  - `/api/v1/integration/outbound-messages`
  - `/internal/lpe-ct/inbound-deliveries`
  - `/internal/lpe-ct/recipient-verification`
  - `/internal/lpe-ct/submission-auth`
  - `/internal/lpe-ct/submissions`
- Edge publication is separate from protocol implementation.
- Autodiscovery must publish only implemented and exposed endpoints.

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
