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
| Notifications | bounded pull-subscription compatibility through `Subscribe`, `GetEvents`, and `Unsubscribe`; `Subscribe` honors request watermarks against a short-lived in-process mailbox event journal, mailbox `CreateItem` and `DeleteItem` calls are recorded as `CreatedEvent`, `NewMailEvent`, and `DeletedEvent` candidates, `GetEvents` returns queued notifications before falling back to visible-message projections, emits mailbox-scoped compatibility `CreatedEvent` probes when no durable event queue is available, and push or streaming notifications remain out of scope |

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
| reconnect | session cookies, idle `PING`, and request IDs maintain bounded authenticated state; reconnect can reissue `Connect`, `Logon`, and sync probes |
| RPC proxy | `/rpc/rpcproxy.dll` authenticates and maps Outlook Anywhere mailbox transport probes to the same canonical MAPI path |

| Exchange / MAPI behavior | Current LPE implementation | Test coverage | Remaining gap |
| --- | --- | --- | --- |
| Outlook desktop profile creation | Authenticated `OPTIONS`, EMSMDB `Connect`, private mailbox `RopLogon`, root/IPM subtree open, hierarchy and contents table bootstrap, receive folder / receive folder table, store-state, named-property, no-event `NotificationWait`, idle `PING`, mailbox URL, address-book URL, and RPC/HTTP probe paths are implemented over canonical state | `mapi_over_http_connect_creates_emsmdb_session`, `mapi_over_http_notification_wait_refreshes_emsmdb_session_cookie`, `mapi_over_http_ping_requires_and_refreshes_session_cookie`, `mapi_over_http_ping_refreshes_nspi_session_cookie`, `mapi_over_http_execute_returns_private_mailbox_logon`, hierarchy/content table tests, `mapi_over_http_execute_returns_receive_folder_and_store_state`, named-property tests, NSPI URL tests, RPC proxy EMSMDB tests | Full Outlook desktop profile creation remains release-gated on live RCA and real-client evidence before public `mapiHttp` publication |
| Mailbox synchronization | `RopSynchronizationConfigure`, chunked `RopFastTransferSourceGetBuffer` continuation, upload state stream begin/continue/end, transfer-state readback, local replica IDs, content-sync manifests scoped to the opened folder, hierarchy-sync manifests scoped to folder changes, stable change-key/predecessor-list, read/flag-state, visible-recipient, and attachment-aware content change facts, import message change, read-state import, delete import, move import, hierarchy import, stable source/change keys, and Bcc-safe message manifests are implemented against canonical folders/messages | `mapi_over_http_sync_configure_returns_canonical_manifest_buffer`, `mapi_over_http_fast_transfer_get_buffer_resumes_across_execute_requests`, `mapi_over_http_sync_configure_separates_content_and_hierarchy_manifests`, `mapi_over_http_sync_manifest_includes_stable_change_key_facts_without_bcc`, `mapi_over_http_sync_manifest_includes_canonical_read_flag_state`, `mapi_over_http_sync_manifest_includes_visible_recipient_facts_without_bcc`, `mapi_over_http_sync_manifest_includes_attachment_change_facts_without_bcc`, upload-state, import message/read/delete/move/hierarchy, local replica ID, cached-mode property tests | Fast-transfer content remains a bounded LPE manifest, not a complete Exchange ICS encoding for every property stream and conflict case |
| NSPI / address book | `Bind`, `Unbind`, `DNToMId`, `QueryRows`, `SeekEntries`, `ResolveNames`, `GetMatches`, `GetProps`, `GetNamesFromIDs`, URL discovery, and tenant-bound directory/contact visibility are implemented | NSPI bind/unbind, DN-to-minimal-ID, query rows, seek/resolve/get matches/get props/get names, tenant visibility, hidden self-resolution tests | Distribution lists, ambiguous-name ranking parity, and full address-book template semantics remain bounded |
| Draft/send | MAPI create/open/set properties, recipients, attachments, save-draft/import, submit, and opened-draft submit call canonical LPE draft/submission paths; no protocol-local `Outbox` or `Sent` state is introduced | create/set/save/import, recipients, attachment stream, submit pending, submit opened draft, end-to-end mail lifecycle tests | Complete Outlook compose edge cases such as rich body fidelity and every recipient property are still bounded to the documented canonical subset |
| Reconnect and failure behavior | Stale or missing cookies and missing required request headers return parseable MAPI errors; session cookies are bounded to the authenticated principal; EMSMDB `Connect` and NSPI `Bind` with a valid session cookie rotate the cookie while preserving valid session context; EMSMDB/NSPI `PING` validates the session cookie, refreshes session liveness, and returns expiration metadata; duplicate byte-identical EMSMDB `Execute` request IDs replay the cached protocol response without repeating canonical mutations; the same request ID with a different ROP payload fails instead of guessing | session expiry unit test, missing-cookie/auth-context tests, missing `X-RequestId` / `X-RequestType` tests, disconnect/unbind tests, RPC context tests, `mapi_over_http_ping_requires_and_refreshes_session_cookie`, `mapi_over_http_ping_refreshes_nspi_session_cookie`, `mapi_over_http_connect_reestablishes_session_context_with_open_sync_handle`, `mapi_over_http_bind_reestablishes_nspi_session_cookie`, `mapi_over_http_replayed_execute_request_id_does_not_resubmit_message` | Cross-process session replay is not durable; deployments must keep MAPI session affinity or accept reconnect through fresh `Connect` / `Bind` / `Logon` probes |

| Unsupported | Rule |
| --- | --- |
| client `SMTP` in core `LPE` | forbidden |
| Exchange-specific mailbox store | forbidden |
| parallel `Sent` / `Outbox` | forbidden |
| unauthenticated MAPI endpoints | forbidden |
| `NSPI ModLinkAtt` / `ModProps` over MAPI HTTP | recognized request types but disabled with parseable MAPI response code `16`; address-book mutation must go through canonical account/contact APIs, not NSPI-local state |
