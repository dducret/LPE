# Client Autoconfiguration

## Current State/Functionality Overview

`LPE` publishes client autoconfiguration only for endpoints that are implemented and explicitly exposed. Exchange-style discovery blocks remain gated so Outlook desktop is not forced away from the default `IMAP` path.

## Implementation/Usage

- Publish public autoconfiguration through `LPE-CT` HTTPS. The core `LPE` service may render the response behind the proxy, but public clients must not be directed to a core `LPE` listener.
- Publish `IMAP` only when public IMAPS is exposed by `LPE-CT`; set `LPE_AUTOCONFIG_IMAP_HOST` to that public `LPE-CT` IMAPS hostname. Leaving it unset suppresses IMAP blocks in Thunderbird autoconfig and Outlook POX Autodiscover.
- Publish `SMTP` submission only when a real authenticated client-submission listener is exposed by `LPE-CT`.
- Never advertise the internal `LPE -> LPE-CT` relay as client `SMTP`.
- Publish `ActiveSync` only for clients that support `Exchange ActiveSync`.
- Do not advertise `ActiveSync` as the Outlook for Windows desktop Exchange route.
- Publish `EWS` only when `LPE_AUTOCONFIG_EWS_ENABLED` is true.
- Publish `mapiHttp` only when the MAPI profile, sync, reconnect, request-id replay, live RCA, and real Outlook desktop profile-creation gates pass and `LPE_AUTOCONFIG_MAPI_ENABLED` is true. A client `X-MapiHttpCapability` probe never publishes MAPI by itself.
- Publish top-level `EXCH` only when `LPE_AUTOCONFIG_EXCH_AUTODISCOVER_ENABLED` is true and an Exchange-style surface is enabled.
- Publish top-level `EXPR` only when `LPE_AUTOCONFIG_EXPR_AUTODISCOVER_ENABLED` and `LPE_AUTOCONFIG_RPC_PROXY_ENABLED` are true and `/rpc/rpcproxy.dll` is implemented and exposed.
- Publish SOAP `GetUserSettings` only when `LPE_AUTOCONFIG_SOAP_EXCHANGE_AUTODISCOVER_ENABLED` is true and an `EWS` or `MAPI` surface is enabled.
- `/.well-known/jmap` redirects to the configured public JMAP session URL.
- `EmailSubmission/set` must load a draft and call canonical `LPE` submission; it must not hand mail directly to `SMTP`.

## Reference Table/List

| Endpoint | Purpose |
| --- | --- |
| `GET /autoconfig/mail/config-v1.1.xml` | Thunderbird autoconfig |
| `GET /.well-known/autoconfig/mail/config-v1.1.xml` | Thunderbird autoconfig |
| `GET /autodiscover` | Outlook compatibility alias |
| `POST /autodiscover` | Outlook compatibility alias |
| `GET /autodiscover/autodiscover.xml` | Outlook POX autodiscover |
| `POST /autodiscover/autodiscover.xml` | Outlook POX autodiscover |
| `GET /autodiscover/autodiscover.json/v1.0/{email}` | Autodiscover v2 JSON |
| `GET /Autodiscover` | case-compatible alias |
| `POST /Autodiscover` | case-compatible alias |
| `GET /Autodiscover/Autodiscover.xml` | case-compatible POX autodiscover |
| `POST /Autodiscover/Autodiscover.xml` | case-compatible POX autodiscover |
| `GET /Autodiscover/Autodiscover.json/v1.0/{email}` | case-compatible v2 JSON |
| `OPTIONS /EWS/Exchange.asmx` | EWS probe |
| `POST /EWS/Exchange.asmx` | EWS SOAP |
| `OPTIONS /ews/exchange.asmx` | EWS lowercase probe |
| `POST /ews/exchange.asmx` | EWS lowercase SOAP |
| `OPTIONS /mapi/emsmdb` | MAPI/HTTP EMSMDB probe |
| `POST /mapi/emsmdb` | MAPI/HTTP EMSMDB |
| `OPTIONS /mapi/nspi` | MAPI/HTTP NSPI probe |
| `POST /mapi/nspi` | MAPI/HTTP NSPI |
| `OPTIONS /Microsoft-Server-ActiveSync` | ActiveSync probe |
| `POST /Microsoft-Server-ActiveSync` | ActiveSync |
| `GET /api/jmap/session` | JMAP session |
| `POST /api/jmap/api` | JMAP API |
| `POST /api/jmap/upload/{accountId}` | JMAP upload |
| `GET /api/jmap/download/{accountId}/{blobId}/{name}` | JMAP download |
| `GET /api/jmap/ws` | JMAP WebSocket |
| `GET /api/jmap/events` | JMAP event stream |
| `GET /.well-known/jmap` | JMAP service discovery |

| Setting | Default / behavior |
| --- | --- |
| `LPE_PUBLIC_SCHEME` | `https` |
| `LPE_PUBLIC_HOSTNAME` | inferred from `Host` or `X-Forwarded-Host` when unset |
| `LPE_AUTOCONFIG_IMAP_HOST` | optional; enables published `IMAP` blocks and must name the public `LPE-CT` IMAPS endpoint, not the core `LPE` listener |
| `LPE_AUTOCONFIG_IMAP_PORT` | `993` when `LPE_AUTOCONFIG_IMAP_HOST` is set |
| `LPE_AUTOCONFIG_SMTP_HOST` | optional; enables the published `SMTP` block only for real authenticated client submission |
| `LPE_AUTOCONFIG_SMTP_PORT` | `465` |
| `LPE_AUTOCONFIG_SMTP_SOCKET_TYPE` | `SSL` |
| `LPE_AUTOCONFIG_EWS_ENABLED` | true values: `true`, `1`, `yes`, `on` |
| `LPE_AUTOCONFIG_EWS_URL` | `{public_scheme}://{public_host}/EWS/Exchange.asmx` |
| `LPE_AUTOCONFIG_MAPI_ENABLED` | true values: `true`, `1`, `yes`, `on` |
| `LPE_AUTOCONFIG_EXCH_AUTODISCOVER_ENABLED` | true values: `true`, `1`, `yes`, `on` |
| `LPE_AUTOCONFIG_EXPR_AUTODISCOVER_ENABLED` | true values: `true`, `1`, `yes`, `on` |
| `LPE_AUTOCONFIG_RPC_PROXY_ENABLED` | true values: `true`, `1`, `yes`, `on` |
| `LPE_AUTOCONFIG_SOAP_EXCHANGE_AUTODISCOVER_ENABLED` | true values: `true`, `1`, `yes`, `on` |
| `LPE_AUTOCONFIG_MAPI_EMSMDB_URL` | `{public_scheme}://{public_host}/mapi/emsmdb/?MailboxId={email}` |
| `LPE_AUTOCONFIG_MAPI_NSPI_URL` | `{public_scheme}://{public_host}/mapi/nspi/?MailboxId={email}` |
| `LPE_AUTOCONFIG_ACTIVESYNC_URL` | `{public_scheme}://{public_host}/Microsoft-Server-ActiveSync` |
| `LPE_AUTOCONFIG_JMAP_SESSION_URL` | `{public_scheme}://{public_host}/api/jmap/session` |

| Autodiscover protocol request | Response rule |
| --- | --- |
| `Protocol=AutoDiscoverV1` | canonical POX URL |
| `Protocol=EWS` | configured EWS URL only when `LPE_AUTOCONFIG_EWS_ENABLED` is true |
| `Protocol=MapiHttp` | configured EMSMDB URL only when `LPE_AUTOCONFIG_MAPI_ENABLED` is true and the MAPI release/publication gate has passed |
| `Protocol=ActiveSync` / `MobileSync` | ActiveSync endpoint for mobile-client probes |
| `Protocol=JMAP` | configured public JMAP session URL |

| Readiness command | Scope |
| --- | --- |
| `python tools/rca_outlook_connectivity_check.py --outlook-rca-readiness --allow-mutating-fixtures` | `IMAP`, `EWS`, `EXCH`, `EXPR`, `mapiHttp`, canonical `Sent`, `NSPI`, RPC proxy |
| `python tools/rca_outlook_connectivity_check.py --ews-readiness --allow-mutating-fixtures` | EWS autodiscover, authentication, canonical send-to-`Sent`, contact/calendar create-read-delete |
