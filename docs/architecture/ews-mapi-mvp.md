# EWS and MAPI

## Current State/Functionality Overview

`lpe-exchange` exposes bounded EWS compatibility and guarded MAPI over HTTP endpoints over canonical `LPE` mailbox, contacts, calendar, task, address-book, and submission state. It is not a complete Exchange server and must not introduce Exchange-specific storage.

The detailed Microsoft specification-to-`LPE` implementation matrix for MAPI over HTTP is maintained in `docs/architecture/mapi-over-http-implementation-plan.md`.

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
- MAPI implementation modules under `crates/lpe-exchange/src/mapi/`:
  - `transport.rs` owns MAPI/HTTP request validation, response envelopes, cookies, diagnostics, and endpoint routing.
  - `session.rs` owns authenticated session state, handle tables, request replay caches, and RPC/HTTP EMSMDB context execution.
  - `dispatch.rs` owns Execute request decoding and ROP dispatch against canonical `LPE` state.
  - `rop.rs` owns ROP buffer parsing, ROP response encoding, and low-level cursor helpers.
  - `tables.rs` owns hierarchy, contents, attachment, contact, and calendar table projections.
  - `properties.rs` owns property tags, named properties, value conversion, streams, and canonical property application.
  - `sync.rs` owns replica identifiers, FastTransfer/ICS buffers, manifests, and sync object mapping.
  - `nspi.rs` owns address-book request handling and tenant-visible NSPI projections.
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
  - `mapiHttp` publication requires the MAPI profile/sync/reconnect release gate, live RCA evidence, real Outlook desktop profile-creation evidence, `LPE_AUTOCONFIG_MAPI_ENABLED`, and `LPE_AUTOCONFIG_OUTLOOK_INTEROP_GATE_PASSED`; an Outlook `X-MapiHttpCapability` probe does not publish MAPI without the explicit deployment flag and final gate
  - legacy `EXCH` publication requires `LPE_AUTOCONFIG_EXCH_AUTODISCOVER_ENABLED`
  - legacy `EXPR` publication requires `LPE_AUTOCONFIG_EXPR_AUTODISCOVER_ENABLED`, `LPE_AUTOCONFIG_RPC_PROXY_ENABLED`, and `LPE_AUTOCONFIG_OUTLOOK_INTEROP_GATE_PASSED`

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
| MAPI identity | store-backed projection from canonical UUIDs to replica GUID, FID, MID, LongTermID, source key, change key, and instance key values |
| Folder permissions | `RopGetPermissionsTable` projects canonical mailbox delegation grants into a bounded folder permission table with `PidTagMemberId`, `PidTagMemberName`, and `PidTagMemberRights`; mailbox owners map to full owner rights, delegates map from canonical `may_read`, `may_write`, `may_delete`, and `may_share`, while `Default` and `Anonymous` rows are exposed with no rights |
| Notifications | `RopRegisterNotification` registers session-scoped content or hierarchy watches for the supported notification bitmask, and `NotificationWait` returns protocol-shaped no-event or event-pending responses from registered session events and the canonical mail change cursor |

| MAPI ROP parser / dispatch boundary | Current support |
| --- | --- |
| ROP ID handling | `rop.rs` maps parsed ROP buffers to typed request views for the currently supported dispatch surface and keeps unknown or reserved `RopId` values on the unsupported path described by [MS-OXCROPS 2.2.2](https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxcrops/6c623489-576d-45ef-9288-5b62b73c6961). Unsupported or reserved ROPs return a single common ROP error response instead of letting trailing bytes be interpreted as more ROPs. |
| Request / response buffers | `rop.rs` owns request parsing, selected typed request serialization for golden fixtures, and common ROP error serialization. `dispatch.rs` consumes parsed request boundaries and must not walk raw request bytes for supported ROPs. |
| Handle table contract | Execute request handle tables are parsed as 32-bit slots. Malformed handle-table byte counts fail the ROP batch predictably, and hierarchy/content table opens now return ROP handle-index errors when the input handle slot is missing or invalid. |
| Rows and restrictions | Table rows continue to use the MS-OXCDATA property-row and restriction codecs, including `StandardPropertyRow` and bounded restriction parsing for table filters. Unsupported restriction forms fail through the ROP property/table error path rather than being coerced. |
| Deferred / unsupported parsed ROPs | Deferred ROPs listed in the unsupported matrix remain parseable only to the documented bounded request length and return ROP-specific protocol errors without canonical mailbox side effects. |

| MAPI property area | Current support |
| --- | --- |
| Property tags | `properties.rs` parses a property tag into a 16-bit property ID and a 16-bit MS-OXCDATA property type. IDs `0x8001..0xFFFE` are treated as named-property IDs. |
| Scalar values | Inline ROP property values support `PtypInteger16`, `PtypInteger32`, `PtypBoolean`, `PtypInteger64`, `PtypTime`, `PtypString8`, `PtypString`, `PtypGuid`, `PtypBinary`, and `PtypErrorCode`. Unicode strings are UTF-16LE with a terminating null. |
| Multivalued values | The bounded initial codec supports `PtypMultipleInteger16`, `PtypMultipleInteger32`, `PtypMultipleInteger64`, `PtypMultipleString8`, `PtypMultipleString`, `PtypMultipleGuid`, and `PtypMultipleBinary` using ROP-buffer counts. |
| Binary and large values | Inline `PtypBinary` uses the ROP 16-bit byte count. Large body, HTML, and attachment data use the existing stream ROP path instead of inventing a protocol-local blob store. |
| Named properties | `RopGetPropertyIdsFromNames`, `RopGetNamesFromPropertyIds`, and `RopQueryNamedProperties` use the session-local registry. Durable named-property mapping is deferred until a canonical persistent custom-property surface is documented. |
| Unsupported property types | Unsupported inline property types fail at decode time and surface through the existing ROP property error path instead of being silently coerced. |

| MAPI gate | Required behavior |
| --- | --- |
| profile creation | `OPTIONS`, `Connect`, private mailbox `Logon`, hierarchy table, receive-folder table, and store-state probes succeed after authentication |
| address book | `NSPI Bind`, row lookup, seek/query rows, resolve names, mailbox URL, and address-book URL behavior use canonical account/contact visibility |
| mailbox sync | `RopSynchronizationConfigure`, fast-transfer buffer, upload transfer state, import message/read/delete/move/hierarchy changes, and local replica IDs remain bounded to canonical mailbox state |
| folder and message tables | open folder, hierarchy table, contents table, query all available columns, set columns, sort/restrict/seek/query rows, and query position return canonical folder/message data |
| draft/send | create/open message, set properties, recipients, save changes, submit, and canonical `Sent` visibility use core submission |
| cross-protocol mutation gate | MAPI send, draft, move, copy, delete, flag, attachment, and protected `Bcc` paths must leave one canonical state that IMAP and JMAP projections agree on; no MAPI-only `Sent`, `Outbox`, search, or sync state is allowed |
| reconnect | session cookies, idle `PING`, and request IDs maintain bounded authenticated state; reconnect can reissue `Connect`, `Logon`, and sync probes |
| RPC proxy | `/rpc/rpcproxy.dll` authenticates and maps Outlook Anywhere mailbox transport probes to the same canonical MAPI path |

| Exchange / MAPI behavior | Current LPE implementation | Test coverage | Remaining gap |
| --- | --- | --- | --- |
| Outlook desktop profile creation | Authenticated `OPTIONS`, EMSMDB `Connect`, private mailbox `RopLogon`, root/IPM subtree open, hierarchy and contents table bootstrap, table set/query/status/reset probes that validate table handles, available-column probes, receive folder / receive folder table with message-class validation and longest-prefix fallback for canonical Inbox delivery, transport folder, address types/options data, store-state, named-property, table cursor positioning including fractional seek, no-event and event-pending `NotificationWait`, session-scoped `RopRegisterNotification`, idle `PING`, mailbox URL, address-book URL, common non-chunked response envelope headers, and RPC/HTTP probe paths are implemented over canonical state | `mapi_over_http_connect_creates_emsmdb_session`, `mapi_over_http_notification_wait_refreshes_emsmdb_session_cookie`, `mapi_over_http_notification_wait_reports_content_event_after_registered_delete`, `mapi_over_http_notification_wait_reports_hierarchy_event_after_registered_create_folder`, `mapi_over_http_register_notification_returns_protocol_success_handles`, `mapi_over_http_ping_requires_and_refreshes_session_cookie`, `mapi_over_http_ping_refreshes_nspi_session_cookie`, `mapi_over_http_execute_returns_private_mailbox_logon`, hierarchy/content table tests, `mapi_over_http_query_columns_all_reports_canonical_table_columns`, `mapi_over_http_table_control_rops_require_table_handles`, `mapi_over_http_seek_row_fractional_moves_table_cursor`, `mapi_over_http_execute_returns_receive_folder_and_store_state`, `mapi_over_http_get_receive_folder_uses_message_class_matching`, `mapi_over_http_execute_returns_transport_folder_without_protocol_outbox_state`, `mapi_over_http_execute_returns_empty_transport_options_data`, named-property tests, NSPI URL tests, RPC proxy EMSMDB tests, and `tools/rca_outlook_connectivity_check.py --outlook-rca-readiness` with GUID-counter MAPI headers, full session cookies, and paired RPC/HTTP IN/OUT mailstore ping | Full Outlook desktop profile creation remains release-gated on live RCA and real-client evidence before public `mapiHttp` publication; until then public MAPI autodiscover remains opt-in only and is never inferred from a client capability header |
| Mailbox synchronization | `RopSynchronizationConfigure`, chunked `RopFastTransferSourceGetBuffer` continuation, `RopTellVersion` on established FastTransfer/ICS contexts, bounded `RopFastTransferSourceCopyTo` / `CopyMessages` / `CopyFolder` / `CopyProperties` source manifests, upload state stream begin/continue/end, transfer-state readback, local replica IDs, deleted-local-replica midset checkpointing, `RopLongTermIdFromId` / `RopIdFromLongTermId` conversion for the canonical replica GUID, MS-OXCFXICS-marker-based content and hierarchy download streams over canonical changes, durable `mapi_sync_checkpoints`, tombstone export for delete/move-out changes, stable change-key/predecessor-list, read/flag-state, visible-recipient, and attachment-aware content change facts, import message change with `SaveChangesMessage` persistence, read-state import, delete import, move import, hierarchy import, stable source/change keys, and Bcc-safe message manifests are implemented against canonical folders/messages | `mapi_over_http_sync_configure_returns_canonical_manifest_buffer`, `mapi_over_http_tell_version_accepts_fast_transfer_sync_context`, `mapi_over_http_set_local_replica_midset_deleted_round_trips_in_transfer_state`, `mapi_over_http_sync_import_new_message_saves_canonical_email`, `mapi_over_http_fast_transfer_get_buffer_resumes_across_execute_requests`, `mapi_over_http_fast_transfer_copy_to_message_returns_canonical_manifest_without_bcc`, `mapi_over_http_fast_transfer_copy_messages_filters_to_requested_canonical_messages`, `mapi_over_http_fast_transfer_copy_folder_returns_canonical_folder_manifest`, `mapi_over_http_fast_transfer_copy_properties_message_returns_canonical_manifest_without_bcc`, `mapi_over_http_long_term_id_round_trips_canonical_replica_ids`, `mapi_over_http_sync_configure_separates_content_and_hierarchy_manifests`, `mapi_over_http_sync_checkpoint_resumes_incremental_content_with_tombstone`, `mapi_over_http_sync_manifest_includes_stable_change_key_facts_without_bcc`, `mapi_over_http_sync_manifest_includes_canonical_read_flag_state`, `mapi_over_http_sync_manifest_includes_visible_recipient_facts_without_bcc`, `mapi_over_http_sync_manifest_includes_attachment_change_facts_without_bcc`, upload-state, import message/read/delete/move/hierarchy, local replica ID, cached-mode property tests, and live RCA content-sync proof for an EWS-created canonical Sent message | Fast-transfer content remains bounded to the documented canonical property subset, not a complete Exchange ICS encoding for every property stream and conflict case |
| NSPI / address book | `Bind`, `Unbind`, `DNToMId`, `QueryRows`, `SeekEntries`, `ResolveNames`, `GetMatches`, `GetProps`, `GetNamesFromIDs`, URL discovery, bound-session cookie validation, and tenant-bound directory/contact visibility are implemented | NSPI bind/unbind, bound-operation cookie diagnostics, DN-to-minimal-ID, query rows, seek/resolve/get matches/get props/get names, tenant visibility, hidden self-resolution tests | Distribution lists, ambiguous-name ranking parity, and full address-book template semantics remain bounded |
| Draft/send | MAPI create/open/set properties, recipients, attachments, save-draft/import, `RopSubmitMessage`, `RopTransportSend`, and opened-draft submit call canonical LPE draft/submission paths; no protocol-local `Outbox` or `Sent` state is introduced; transport/spooler advisory probes are parsed to their full request length but remain unsupported until LPE has a canonical abort/spooler event model | create/set/save/import, recipients, attachment stream read/seek/write/copy, submit pending, transport send, submit opened draft, transport/spooler unsupported-batch parsing, end-to-end mail lifecycle tests, and the storage runtime cross-protocol gate for MAPI-sourced canonical `Sent`, Bcc-safe search/AI projections, attachments, and IMAP UID/modseq invariants | Complete Outlook compose edge cases such as rich body fidelity, submission cancellation, spooler advisory events, and every recipient property are still bounded to the documented canonical subset |
| Reconnect and failure behavior | Stale or missing cookies and missing, malformed, or invalid required request headers return parseable MAPI errors; session and request-sequence cookies are bounded to the authenticated principal and same session context for established-session operations; EMSMDB `Connect` and NSPI `Bind` with a valid session cookie ignore the old sequence cookie, rotate cookies, and preserve valid session context; established session operations reject overlapping same-session requests with `Invalid Sequence`; EMSMDB/NSPI `PING` validates the session cookie, requires zero-length idle probes, refreshes session liveness, and returns expiration metadata; duplicate byte-identical EMSMDB `Execute` request IDs replay the cached protocol response without repeating canonical mutations; the same request ID with a different ROP payload fails instead of guessing | session expiry unit test, missing-cookie/auth-context tests, missing `Host` / `Content-Length` / `X-RequestId` / `X-RequestType` / `X-ClientInfo` tests, malformed `Content-Length` / `X-RequestId` / `X-ClientInfo` tests, invalid `X-RequestType`, nonzero PING `Content-Length`, ignored reconnect `MapiSequence`, mismatched established-session `MapiSequence`, and concurrent same-session `Invalid Sequence` tests, disconnect/unbind tests, RPC context tests, `mapi_over_http_ping_requires_and_refreshes_session_cookie`, `mapi_over_http_ping_refreshes_nspi_session_cookie`, `mapi_over_http_connect_reestablishes_session_context_with_open_sync_handle`, `mapi_over_http_bind_reestablishes_nspi_session_cookie`, `mapi_over_http_replayed_execute_request_id_does_not_resubmit_message` | Cross-process session replay is not durable; deployments must keep MAPI session affinity or accept reconnect through fresh `Connect` / `Bind` / `Logon` probes |

## Delete, Move, Tombstone, And Recovery Matrix

| Surface | LPE behavior | Cross-protocol outcome |
| --- | --- | --- |
| `RopDeleteMessages` | Soft delete moves the addressed source mailbox membership to canonical `Trash` when it exists. If the source is already `Trash`, the operation hard-deletes that addressed membership. | JMAP and IMAP observe the same mailbox membership move or removal through canonical `mailbox_messages`, `mail_change_log`, and `tombstones`. |
| `RopHardDeleteMessages` | Hard delete expunges only the addressed source mailbox membership and writes a canonical `mailbox_message` tombstone with the source IMAP UID. | The message disappears from the addressed IMAP folder and from JMAP when no visible memberships remain; MAPI ICS exports the tombstone as a delete. |
| `RopSynchronizationImportDeletes` | `ImportDeleteFlags` with `HardDelete` (`0x02`) maps to canonical hard delete. Without `HardDelete`, LPE maps the imported delete to the same canonical soft-delete-to-`Trash` behavior used by `RopDeleteMessages`. | MAPI-uploaded deletes do not create protocol-local deleted state; IMAP and JMAP see the resulting canonical move or expunge. |
| `RopSynchronizationImportMessageMove` | Move import expunges the source mailbox membership, creates a target membership with a target mailbox UID, and writes a source-membership move tombstone. | IMAP QRESYNC-style replay, JMAP changes, and MAPI ICS all derive from the same canonical move row and tombstone. |
| JMAP `Email/set destroy` and IMAP `EXPUNGE` | Both hard-delete canonical visible memberships and write tombstones. | MAPI content sync exports those canonical tombstones as delete facts; no adapter owns a separate deleted-item list. |
| Folder delete | Custom folder delete destroys the canonical mailbox and writes canonical mailbox tombstones. System folders are not deletable through MAPI. | JMAP and IMAP mailbox lists converge on the same canonical folder removal. |
| Recovery / dumpster | Exchange-style recoverable item views, `OpenSoftDeleted` recovery browsing, and deleted-item retention restore are not currently exposed. Tombstones are retained for sync/replay and audit boundaries only. | Clients can rely on consistent disappearance and sync replay, not on Exchange deleted-item recovery semantics. |

| Unsupported | Rule |
| --- | --- |
| client `SMTP` in core `LPE` | forbidden |
| Exchange-specific mailbox store | forbidden |
| parallel `Sent` / `Outbox` | forbidden |
| unauthenticated MAPI endpoints | forbidden |
| receive-folder mutation | `RopSetReceiveFolder` is parsed but unsupported with a ROP-specific protocol error until receive-folder routing has a canonical `LPE` model |
| folder move/copy ROPs | `RopMoveFolder` and `RopCopyFolder` are parsed but unsupported with ROP-specific protocol errors until MAPI folder hierarchy moves and recursive copies are mapped to canonical `LPE` mailbox state |
| transport/spooler advisory ROPs | `RopAbortSubmit`, `RopSetSpooler`, `RopSpoolerLockMessage`, and `RopTransportNewMail` are parsed but unsupported with ROP-specific protocol errors until submission cancellation and spooler event semantics are mapped to canonical `LPE` submission state |
| `NSPI ModLinkAtt` / `ModProps` over MAPI HTTP | recognized request types but disabled with parseable MAPI response code `16`; address-book mutation must go through canonical account/contact APIs, not NSPI-local state |
| FastTransfer destination upload streams | `RopFastTransferDestinationConfigure`, `RopFastTransferDestinationPutBuffer`, and `RopFastTransferDestinationPutBufferExtended` are parsed but unsupported with ROP-specific protocol errors until raw destination streams are mapped to canonical import paths; this does not include the implemented ICS upload-state and synchronization import ROPs listed in the mailbox synchronization row |
| public-folder per-user read/unread sync ROPs | `RopGetPerUserLongTermIds`, `RopGetPerUserGuid`, `RopReadPerUserInformation`, and `RopWritePerUserInformation` are parsed but unsupported with ROP-specific protocol errors until public-folder/per-user read state is mapped to canonical `LPE` state |
| folder rule and deferred-action ROPs | `RopGetRulesTable`, zero-rule `RopModifyRules`, and `RopUpdateDeferredActionMessages` requests are parsed but unsupported with ROP-specific protocol errors until MAPI rules and deferred action messages are mapped to canonical `LPE` filtering/rule state |
| folder permission mutations | `RopGetPermissionsTable` is implemented as a bounded projection over canonical mailbox delegation grants. `RopModifyPermissions` with no rows is accepted as a no-op compatibility probe; non-empty permission mutation requests are parsed and rejected with a ROP-specific unsupported response until canonical grant mutation semantics, audit, and Outlook ACL parity are explicitly documented. |
| search-folder criteria ROPs | `RopSetSearchCriteria` and `RopGetSearchCriteria` requests are parsed but unsupported with ROP-specific protocol errors until search folders are mapped to canonical `LPE` search state |
| notification delivery scope | `RopRegisterNotification` and `NotificationWait` support bounded content and hierarchy event-pending signaling for registered session events and canonical mail change cursor replay. Full Exchange notification payload parity, cross-process session replay, and spooler advisory event semantics remain deferred. |
| async table/progress control ROPs | `RopAbort` and `RopProgress` cancellation probes are parsed but unsupported with ROP-specific protocol errors; `RopSetColumns`, `RopQueryRows`, `RopGetStatus`, `RopQueryPosition`, and `RopResetTable` operate only on implemented hierarchy/content/attachment table handles and return ROP-specific errors for non-table handles |
| categorized-table row and collapse-state ROPs | `RopExpandRow`, `RopCollapseRow`, `RopGetCollapseState`, and `RopSetCollapseState` requests are parsed but unsupported with ROP-specific protocol errors until categorized table state is implemented over canonical table projections |
| auxiliary stream ROPs | `RopCloneStream` is implemented for read-only canonical attachment streams with independent seek pointers; `RopLockRegionStream` and `RopUnlockRegionStream` requests are parsed but unsupported with ROP-specific protocol errors; normal attachment open/read including the `0xBABE` extended byte-count form, seek/write/extended-write/copy/commit, message body read/write/copy streams for pending messages, and stream-size paths remain mapped to canonical attachments/messages; `RopOpenStream` returns the protocol `StreamSize` as a 32-bit value, honors read-only/read-write/create modes for pending attachment and pending-message body streams, and returns stream access denied for writes attempted through read-only streams |
| whole-folder purge ROPs | `RopEmptyFolder` and `RopHardDeleteMessagesAndSubfolders` requests are parsed but unsupported with ROP-specific protocol errors until whole-folder purge behavior is mapped to canonical `LPE` delete semantics |
| public-folder replica ROPs | `RopPublicFolderIsGhosted` returns bounded private-mailbox `IsGhosted = false` without replica data; `RopGetOwningServers` is parsed but unsupported with a ROP-specific protocol error until public-folder replica topology has a canonical `LPE` model |
