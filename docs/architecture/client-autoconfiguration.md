# Client Autoconfiguration

## Current State/Functionality Overview

`LPE` publishes client autoconfiguration only for endpoints that are implemented and explicitly exposed. New 0.5.1 installations enable MAPI over HTTP for capable Outlook desktop clients; legacy Exchange provider blocks remain separately gated.

## Implementation/Usage

- Publish public autoconfiguration through `LPE-CT` HTTPS. The core `LPE` service may render the response behind the proxy, but public clients must not be directed to a core `LPE` listener.
- Publish `IMAP` only when public IMAPS is exposed by `LPE-CT`; set `LPE_AUTOCONFIG_IMAP_HOST` to that public `LPE-CT` IMAPS hostname. Leaving it unset suppresses IMAP blocks in Thunderbird autoconfig and Outlook POX Autodiscover.
- Publish `SMTP` submission only when a real authenticated client-submission listener is exposed by `LPE-CT`.
- Never advertise the internal `LPE -> LPE-CT` relay as client `SMTP`.
- Publish `ActiveSync` only for clients that support `Exchange ActiveSync`.
- Do not advertise `ActiveSync` as the Outlook for Windows desktop Exchange route.
- The ActiveSync endpoint currently supports and advertises exactly protocol
  version `16.1`; autodiscover may publish the endpoint for mobile
  ActiveSync/MobileSync probes, but it must not imply support for older
  ActiveSync protocol versions.
- Publish `EWS` only when `LPE_AUTOCONFIG_EWS_ENABLED` is true.
- Publish `mapiHttp` when `LPE_AUTOCONFIG_MAPI_ENABLED` is true and the client sends a supported positive `X-MapiHttpCapability` value. New 0.5.1 installations enable this setting. The capability header never enables a deployment-disabled endpoint, and legacy `EXCH` / `EXPR` metadata is suppressed only when `mapiHttp` metadata is actually emitted.
- Publish top-level `EXCH` only when `LPE_AUTOCONFIG_EXCH_AUTODISCOVER_ENABLED` is true and an Exchange-style surface is enabled.
- Publish top-level `EXPR` only when `LPE_AUTOCONFIG_EXPR_AUTODISCOVER_ENABLED`, `LPE_AUTOCONFIG_RPC_PROXY_ENABLED`, and `LPE_AUTOCONFIG_OUTLOOK_INTEROP_GATE_PASSED` are true and `/rpc/rpcproxy.dll` is implemented and exposed.
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
| `LPE_AUTOCONFIG_MAPI_ENABLED` | true values: `true`, `1`, `yes`, `on`; new 0.5.1 installations set `true` |
| `LPE_AUTOCONFIG_OUTLOOK_INTEROP_GATE_PASSED` | legacy `EXPR`/RPC over HTTP release gate; it does not control MAPI over HTTP publication |
| `LPE_AUTOCONFIG_EXCH_AUTODISCOVER_ENABLED` | true values: `true`, `1`, `yes`, `on` |
| `LPE_AUTOCONFIG_EXPR_AUTODISCOVER_ENABLED` | true values: `true`, `1`, `yes`, `on` |
| `LPE_AUTOCONFIG_RPC_PROXY_ENABLED` | true values: `true`, `1`, `yes`, `on` |
| `LPE_AUTOCONFIG_SOAP_EXCHANGE_AUTODISCOVER_ENABLED` | true values: `true`, `1`, `yes`, `on` |
| `LPE_AUTOCONFIG_MAPI_EMSMDB_URL` | `{public_scheme}://{public_host}/mapi/emsmdb/?MailboxId={email}` |
| `LPE_AUTOCONFIG_MAPI_NSPI_URL` | `{public_scheme}://{public_host}/mapi/nspi/?MailboxId={email}` |
| `LPE_AUTOCONFIG_ACTIVESYNC_URL` | `{public_scheme}://{public_host}/Microsoft-Server-ActiveSync` |
| `LPE_AUTOCONFIG_WEBMAIL_URL` | `{public_scheme}://{public_host}/mail/`; used as POX `WEB` / `External` / `OWAUrl` and must not point at EWS |
| `LPE_AUTOCONFIG_JMAP_SESSION_URL` | `{public_scheme}://{public_host}/api/jmap/session` |

| Autodiscover protocol request | Response rule |
| --- | --- |
| `Protocol=AutoDiscoverV1` | canonical POX URL |
| `Protocol=EWS` | configured EWS URL only when `LPE_AUTOCONFIG_EWS_ENABLED` is true |
| `Protocol=MapiHttp` | configured EMSMDB URL when `LPE_AUTOCONFIG_MAPI_ENABLED` is true |
| `Protocol=ActiveSync` / `MobileSync` | ActiveSync endpoint for mobile-client probes |

Microsoft Autodiscover v2 JSON does not advertise `JMAP`. Use `/.well-known/jmap` for JMAP service discovery.

| Readiness command | Scope |
| --- | --- |
| `python tools/rca_outlook_connectivity_check.py --outlook-rca-readiness --allow-mutating-fixtures` | Project-owned scripted readiness harness for `IMAP`, `EWS`, `EXCH`, `mapiHttp`, canonical `Sent`, `NSPI`, and RPC proxy checks when legacy `EXPR` / RPC publication is being validated; this is not Microsoft RCA or real Outlook profile evidence |
| `python tools/rca_outlook_connectivity_check.py --check-mapi-empty-deleted-items --allow-mutating-fixtures --dangerously-empty-deleted-items` | Project-owned destructive fixture check that creates a message in `Deleted Items`, empties `Deleted Items` through MAPI `RopEmptyFolder`, and verifies disappearance through EWS and JMAP; IMAP absence is covered by local storage/runtime tests until this harness has an IMAP connection helper |
| `python tools/rca_outlook_connectivity_check.py --ews-readiness --allow-mutating-fixtures` | EWS autodiscover, authentication, canonical send-to-`Sent`, contact/calendar create-read-delete |

## Outlook Release Evidence Checklist

Record these checks for the exact public host used for a 0.5.x release. The checklist no longer acts as a second MAPI runtime switch; `LPE_AUTOCONFIG_OUTLOOK_INTEROP_GATE_PASSED` remains reserved for legacy `EXPR`/RPC over HTTP:

- Microsoft MAPI/HTTP and Autodiscover references have been checked for the release, including `MS-OXCMAPIHTTP` transport, `MS-OXDSCLI` `X-MapiHttpCapability` handling, and the MapiHttp response shape.
- `cargo test -p lpe-admin-api` and `cargo test -p lpe-exchange` pass for the exact revision being deployed.
- `tools/rca_outlook_connectivity_check.py --outlook-rca-readiness --allow-mutating-fixtures` passes against the public `LPE-CT` HTTPS edge.
- Microsoft Remote Connectivity Analyzer Outlook Connectivity passes from the Internet against the same account and host.
- Outlook 2016 and Outlook 2019 each create an Exchange account profile through Autodiscover, perform cached-mode mailbox synchronization, close and reopen without a full-cache wipe, resolve address-book entries through NSPI, send mail through canonical submission, and show the authoritative message in `Sent`.
- RCA server logs for hierarchy sync include `rca debug mapi hierarchy row` entries with `folder_id`, `parent_folder_id`, `source_key_len`, `parent_source_key_len`, `display_name`, `container_class`, and `change_number` for the rows used as publication evidence.
- Single-node sticky MAPI sessions are acceptable for the first Outlook 2016 / 2019 lab gate. Cross-process session replay remains production hardening, not a blocker for the first lab gate.
- `/rpc/rpcproxy.dll` is routed through the public edge with streaming proxy settings and passes authenticated mailbox-store endpoint checks before legacy `EXPR` metadata is enabled; this is a later legacy compatibility gate, not the first MAPI over HTTP publication path.
