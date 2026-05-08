# EWS and MAPI

## Current State/Functionality Overview

`lpe-exchange` exposes bounded EWS compatibility and guarded MAPI over HTTP endpoints over canonical `LPE` mailbox, contacts, calendar, task, address-book, and submission state. It is not a complete Exchange server and must not introduce Exchange-specific storage.

## Implementation/Usage

- EWS endpoints:
  - `OPTIONS /EWS/Exchange.asmx`
  - `POST /EWS/Exchange.asmx`
  - `OPTIONS /ews/exchange.asmx`
  - `POST /ews/exchange.asmx`
- MAPI over HTTP endpoints:
  - `OPTIONS /mapi/emsmdb`
  - `POST /mapi/emsmdb`
  - `OPTIONS /mapi/nspi`
  - `POST /mapi/nspi`
- Outlook Anywhere / RPC over HTTP endpoint:
  - `/rpc/rpcproxy.dll`
- Authentication:
  - mailbox account authentication
  - authenticated endpoints only
- EWS adapter rules:
  - reads and writes canonical mailbox, contacts, calendar, and task state
  - uses canonical submission for sending
  - records authoritative `Sent`
  - never implements client `SMTP`
  - never stores Exchange-only mailbox state
- MAPI rules:
  - `EMSMDB` maps mailbox synchronization to canonical mailbox state
  - `NSPI` maps address-book behavior to canonical account/contact visibility
  - session context must remain authenticated and bounded to the mailbox principal
  - send and draft flows must use canonical submission and draft persistence
- Autodiscover:
  - `EWS` publication requires `LPE_AUTOCONFIG_EWS_ENABLED`
  - `mapiHttp` publication requires `LPE_AUTOCONFIG_MAPI_ENABLED`
  - legacy `EXCH` publication requires `LPE_AUTOCONFIG_EXCH_AUTODISCOVER_ENABLED`
  - legacy `EXPR` publication requires `LPE_AUTOCONFIG_EXPR_AUTODISCOVER_ENABLED` and `LPE_AUTOCONFIG_RPC_PROXY_ENABLED`

## Reference Table/List

| EWS operation group | Current support |
| --- | --- |
| Folder discovery | `FindFolder`, `GetFolder`, `SyncFolderHierarchy` |
| Mail item discovery | `FindItem`, `GetItem`, `SyncFolderItems` |
| Mail mutation | selected `CreateItem`, selected `DeleteItem` |
| Folder mutation | `CreateFolder`, `DeleteFolder` |
| Contacts | canonical contact read/write operations |
| Calendar | canonical event read/write operations |
| Tasks | canonical task read/write operations where exposed |
| Submission | canonical send flow with authoritative `Sent` |

| MAPI component | Canonical mapping |
| --- | --- |
| `EMSMDB` | mailbox tables, message content, flags, folders, sync state |
| `NSPI` | account/contact address-book visibility |
| `/rpc/rpcproxy.dll` | authenticated RPC/HTTP mailbox transport path |

| Unsupported | Rule |
| --- | --- |
| client `SMTP` in core `LPE` | forbidden |
| Exchange-specific mailbox store | forbidden |
| parallel `Sent` / `Outbox` | forbidden |
| unauthenticated MAPI endpoints | forbidden |
