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
| Outlook desktop profile creation | Authenticated `OPTIONS`, EMSMDB `Connect`, private mailbox `RopLogon`, root/IPM subtree open, hierarchy and contents table bootstrap, receive folder / receive folder table, transport folder, address types/options data, store-state, named-property, no-event `NotificationWait`, idle `PING`, mailbox URL, address-book URL, common non-chunked response envelope headers, and RPC/HTTP probe paths are implemented over canonical state | `mapi_over_http_connect_creates_emsmdb_session`, `mapi_over_http_notification_wait_refreshes_emsmdb_session_cookie`, `mapi_over_http_ping_requires_and_refreshes_session_cookie`, `mapi_over_http_ping_refreshes_nspi_session_cookie`, `mapi_over_http_execute_returns_private_mailbox_logon`, hierarchy/content table tests, `mapi_over_http_execute_returns_receive_folder_and_store_state`, `mapi_over_http_execute_returns_transport_folder_without_protocol_outbox_state`, `mapi_over_http_execute_returns_empty_transport_options_data`, named-property tests, NSPI URL tests, RPC proxy EMSMDB tests, and `tools/rca_outlook_connectivity_check.py --outlook-rca-readiness` with GUID-counter MAPI headers, full session cookies, and paired RPC/HTTP IN/OUT mailstore ping | Full Outlook desktop profile creation remains release-gated on live RCA and real-client evidence before public `mapiHttp` publication |
| Mailbox synchronization | `RopSynchronizationConfigure`, chunked `RopFastTransferSourceGetBuffer` continuation, bounded `RopFastTransferSourceCopyTo` / `CopyMessages` / `CopyFolder` / `CopyProperties` source manifests, upload state stream begin/continue/end, transfer-state readback, local replica IDs, `RopLongTermIdFromId` / `RopIdFromLongTermId` conversion for the canonical replica GUID, content-sync manifests scoped to the opened folder, hierarchy-sync manifests scoped to folder changes, stable change-key/predecessor-list, read/flag-state, visible-recipient, and attachment-aware content change facts, import message change, read-state import, delete import, move import, hierarchy import, stable source/change keys, and Bcc-safe message manifests are implemented against canonical folders/messages | `mapi_over_http_sync_configure_returns_canonical_manifest_buffer`, `mapi_over_http_fast_transfer_get_buffer_resumes_across_execute_requests`, `mapi_over_http_fast_transfer_copy_to_message_returns_canonical_manifest_without_bcc`, `mapi_over_http_fast_transfer_copy_messages_filters_to_requested_canonical_messages`, `mapi_over_http_fast_transfer_copy_folder_returns_canonical_folder_manifest`, `mapi_over_http_fast_transfer_copy_properties_message_returns_canonical_manifest_without_bcc`, `mapi_over_http_long_term_id_round_trips_canonical_replica_ids`, `mapi_over_http_sync_configure_separates_content_and_hierarchy_manifests`, `mapi_over_http_sync_manifest_includes_stable_change_key_facts_without_bcc`, `mapi_over_http_sync_manifest_includes_canonical_read_flag_state`, `mapi_over_http_sync_manifest_includes_visible_recipient_facts_without_bcc`, `mapi_over_http_sync_manifest_includes_attachment_change_facts_without_bcc`, upload-state, import message/read/delete/move/hierarchy, local replica ID, cached-mode property tests, and live RCA content-sync proof for an EWS-created canonical Sent message | Fast-transfer content remains a bounded LPE manifest, not a complete Exchange ICS encoding for every property stream and conflict case |
| NSPI / address book | `Bind`, `Unbind`, `DNToMId`, `QueryRows`, `SeekEntries`, `ResolveNames`, `GetMatches`, `GetProps`, `GetNamesFromIDs`, URL discovery, bound-session cookie validation, and tenant-bound directory/contact visibility are implemented | NSPI bind/unbind, bound-operation cookie diagnostics, DN-to-minimal-ID, query rows, seek/resolve/get matches/get props/get names, tenant visibility, hidden self-resolution tests | Distribution lists, ambiguous-name ranking parity, and full address-book template semantics remain bounded |
| Draft/send | MAPI create/open/set properties, recipients, attachments, save-draft/import, `RopSubmitMessage`, `RopTransportSend`, and opened-draft submit call canonical LPE draft/submission paths; no protocol-local `Outbox` or `Sent` state is introduced | create/set/save/import, recipients, attachment stream, submit pending, transport send, submit opened draft, end-to-end mail lifecycle tests | Complete Outlook compose edge cases such as rich body fidelity and every recipient property are still bounded to the documented canonical subset |
| Reconnect and failure behavior | Stale or missing cookies and missing, malformed, or invalid required request headers return parseable MAPI errors; session and request-sequence cookies are bounded to the authenticated principal and same session context for established-session operations; EMSMDB `Connect` and NSPI `Bind` with a valid session cookie ignore the old sequence cookie, rotate cookies, and preserve valid session context; established session operations reject overlapping same-session requests with `Invalid Sequence`; EMSMDB/NSPI `PING` validates the session cookie, requires zero-length idle probes, refreshes session liveness, and returns expiration metadata; duplicate byte-identical EMSMDB `Execute` request IDs replay the cached protocol response without repeating canonical mutations; the same request ID with a different ROP payload fails instead of guessing | session expiry unit test, missing-cookie/auth-context tests, missing `Host` / `Content-Length` / `X-RequestId` / `X-RequestType` / `X-ClientInfo` tests, malformed `Content-Length` / `X-RequestId` / `X-ClientInfo` tests, invalid `X-RequestType`, nonzero PING `Content-Length`, ignored reconnect `MapiSequence`, mismatched established-session `MapiSequence`, and concurrent same-session `Invalid Sequence` tests, disconnect/unbind tests, RPC context tests, `mapi_over_http_ping_requires_and_refreshes_session_cookie`, `mapi_over_http_ping_refreshes_nspi_session_cookie`, `mapi_over_http_connect_reestablishes_session_context_with_open_sync_handle`, `mapi_over_http_bind_reestablishes_nspi_session_cookie`, `mapi_over_http_replayed_execute_request_id_does_not_resubmit_message` | Cross-process session replay is not durable; deployments must keep MAPI session affinity or accept reconnect through fresh `Connect` / `Bind` / `Logon` probes |

| Unsupported | Rule |
| --- | --- |
| client `SMTP` in core `LPE` | forbidden |
| Exchange-specific mailbox store | forbidden |
| parallel `Sent` / `Outbox` | forbidden |
| unauthenticated MAPI endpoints | forbidden |
| `NSPI ModLinkAtt` / `ModProps` over MAPI HTTP | recognized request types but disabled with parseable MAPI response code `16`; address-book mutation must go through canonical account/contact APIs, not NSPI-local state |
| FastTransfer upload streams | parsed but unsupported with ROP-specific protocol errors until upload-side streams are mapped to canonical import paths |
| public-folder per-user read/unread sync ROPs | `RopGetPerUserLongTermIds`, `RopGetPerUserGuid`, `RopReadPerUserInformation`, and `RopWritePerUserInformation` are parsed but unsupported with ROP-specific protocol errors until public-folder/per-user read state is mapped to canonical `LPE` state |
| folder rule ROPs | `RopGetRulesTable` and zero-rule `RopModifyRules` requests are parsed but unsupported with ROP-specific protocol errors until MAPI rules are mapped to canonical `LPE` filtering/rule state |
| folder permission ROPs | `RopGetPermissionsTable` and zero-permission `RopModifyPermissions` requests are parsed but unsupported with ROP-specific protocol errors until MAPI permissions are mapped to canonical `LPE` rights state |
| search-folder criteria ROPs | `RopSetSearchCriteria` and `RopGetSearchCriteria` requests are parsed but unsupported with ROP-specific protocol errors until search folders are mapped to canonical `LPE` search state |
| notification registration ROPs | `RopRegisterNotification` requests are parsed but unsupported with ROP-specific protocol errors until registered MAPI notifications are mapped to canonical `LPE` mailbox change state |
| async table/progress control ROPs | `RopAbort` and `RopProgress` cancellation probes are parsed but unsupported with ROP-specific protocol errors; `RopGetStatus` continues to report synchronous completion for implemented table operations |
| whole-folder purge ROPs | `RopEmptyFolder` and `RopHardDeleteMessagesAndSubfolders` requests are parsed but unsupported with ROP-specific protocol errors until whole-folder purge behavior is mapped to canonical `LPE` delete semantics |
| public-folder replica ROPs | `RopGetOwningServers` and `RopPublicFolderIsGhosted` requests are parsed but unsupported with ROP-specific protocol errors until public-folder replica topology has a canonical `LPE` model |
