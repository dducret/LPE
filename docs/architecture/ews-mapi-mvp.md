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
  - profile creation must complete authenticated `Connect`, private mailbox `Logon`, hierarchy/content table probes, and NSPI address-book binding without publishing MAPI autodiscover until the gate passes
  - mailbox synchronization must use canonical message, folder, flag, attachment, and sync-state mappings; import/read/delete/move operations must mutate canonical state only
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
| Notifications | bounded pull-subscription compatibility through `Subscribe`, `GetEvents`, and `Unsubscribe`; `GetEvents` projects visible mailbox messages as `CreatedEvent` / `NewMailEvent` and push or streaming notifications remain out of scope |

| MAPI component | Canonical mapping |
| --- | --- |
| `EMSMDB` | mailbox tables, message content, flags, folders, sync state |
| `NSPI` | account/contact address-book visibility |
| `/rpc/rpcproxy.dll` | authenticated RPC/HTTP mailbox transport path |

| MAPI gate | Required behavior |
| --- | --- |
| profile creation | `OPTIONS`, `Connect`, private mailbox `Logon`, hierarchy table, receive-folder table, and store-state probes succeed after authentication |
| address book | `NSPI Bind`, row lookup, seek/query rows, resolve names, mailbox URL, and address-book URL behavior use canonical account/contact visibility |
| mailbox sync | `RopSynchronizationConfigure`, fast-transfer buffer, upload transfer state, import message/read/delete/move/hierarchy changes, and local replica IDs remain bounded to canonical mailbox state |
| folder and message tables | open folder, hierarchy table, contents table, set columns, sort/restrict/seek/query rows, and query position return canonical folder/message data |
| draft/send | create/open message, set properties, recipients, save changes, submit, and canonical `Sent` visibility use core submission |
| reconnect | session cookies and request IDs maintain bounded authenticated state; reconnect can reissue `Connect`, `Logon`, and sync probes |
| RPC proxy | `/rpc/rpcproxy.dll` authenticates and maps Outlook Anywhere mailbox transport probes to the same canonical MAPI path |

| Unsupported | Rule |
| --- | --- |
| client `SMTP` in core `LPE` | forbidden |
| Exchange-specific mailbox store | forbidden |
| parallel `Sent` / `Outbox` | forbidden |
| unauthenticated MAPI endpoints | forbidden |
