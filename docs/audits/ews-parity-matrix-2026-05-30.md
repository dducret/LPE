# EWS Parity Matrix - 2026-05-30

## Scope

This matrix expands `docs/audits/ews-audit-2026-05-30.md` into one row for every operation listed in Microsoft's EWS operation catalog:

- <https://learn.microsoft.com/en-us/exchange/client-developer/web-service-reference/ews-operations-in-exchange>
- <https://learn.microsoft.com/en-us/exchange/client-developer/web-service-reference/ews-xml-elements-in-exchange>

Microsoft's operation catalog page was last updated on 2023-03-29 and was reviewed for this matrix on 2026-05-30. This is a documentation parity matrix only. It does not add, remove, or change runtime behavior.

## Status And Priority Legend

| Value | Meaning |
| --- | --- |
| Implemented | LPE has concrete EWS behavior and no known material Exchange-visible gap for the documented LPE scope. |
| Partially implemented | LPE dispatches the operation to concrete behavior, but the behavior is bounded and differs from full Exchange semantics. |
| Unsupported | LPE explicitly recognizes the operation and returns a parseable unsupported EWS response. |
| Missing | The operation has no dedicated dispatcher branch and falls through to generic unsupported handling. |

| Priority | Outlook/native-client compatibility meaning |
| --- | --- |
| P0 | Required for the core Outlook/native-client mail, folder, sync, compose, identity, or calendar path. |
| P1 | Important for common Outlook/native-client workflows, recovery, delegation, rules, room scheduling, reminders, or durable sync quality. |
| P2 | Useful compatibility surface, but not required for the first bounded Outlook/native-client gate. |
| P3 | Administrative, compliance, add-in, telephony, or organization feature that is not central to mailbox interoperability. |
| P4 | Out of scope unless the product explicitly adds the corresponding Exchange feature family. |

## Cross-Cutting LPE Constraints

- EWS must remain an adapter over canonical `LPE` state, not an Exchange-specific mailbox store.
- Mail sending must use canonical submission and authoritative `Sent`; EWS must not introduce client `SMTP`, protocol-local `Outbox`, or parallel `Sent` behavior.
- `Bcc` remains protected metadata and must not enter normal user search, AI, sync manifests, or user-visible projections.
- SQL references below name the durable data required for parity. When a row says "new SQL required", that means the current schema described by `docs/architecture/sql-schema-v2.md` does not yet model the Exchange feature.
- Canonical API/storage references identify the LPE subsystem that should own the behavior if the operation is implemented later.

## eDiscovery Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `GetDiscoverySearchConfiguration` | Missing | New eDiscovery search configuration, compliance scopes, legal-hold policy rows | Compliance/admin search API over canonical mailbox/search data | EWS clients receive unsupported instead of discovery configuration | P4 |
| `GetHoldOnMailboxes` | Missing | New legal hold policy/mailbox assignment state; existing `recoverable_items.legal_hold` is item lifecycle only | Compliance hold management API | No Exchange hold query behavior through EWS | P4 |
| `GetNonIndexableItemDetails` | Missing | New non-indexable item diagnostics; existing attachment extraction jobs are not Exchange discovery reports | Search/index diagnostics API | No Exchange non-indexable report payloads | P4 |
| `GetNonIndexableItemStatistics` | Missing | New aggregate non-indexable statistics derived from indexing/extraction state | Search/index diagnostics API | Unsupported instead of statistics | P4 |
| `GetSearchableMailboxes` | Missing | Tenant account/mailbox directory plus compliance search scope grants | Admin/compliance mailbox discovery API | Unsupported instead of searchable mailbox list | P4 |
| `SearchMailboxes` | Missing | Existing mailbox/search tables plus new compliance query/audit/result-set state | Compliance search API with Bcc-safe defaults and explicit protected-metadata access | No EWS discovery search results | P4 |
| `SetHoldOnMailboxes` | Missing | New legal hold policy assignment and audit rows | Compliance hold mutation API | Cannot create/update Exchange-style holds through EWS | P4 |

## Mailbox Item Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `ArchiveItem` | Missing | Existing mailboxes/messages can model a canonical Archive only if configured; no Exchange archive-mailbox model | Mail move API to canonical Archive plus archive policy rules | Unsupported; no Exchange archive mailbox or retention archive semantics | P2 |
| `CreateItem` | Partially implemented | `messages`, `mailbox_messages`, recipients, protected Bcc, blobs/MIME, contacts, calendars, tasks, submission tables | Draft/import/create APIs for mail, contacts, events, tasks; canonical submission for send dispositions | Bounded item classes; no `AcceptSharingInvitation` special handling; no full Exchange property bag | P0 |
| `CopyItem` | Partially implemented | `messages`, `mailbox_messages`, change log/tombstones | Canonical message copy API | Supports canonical message ids only; not full item-class copy parity | P1 |
| `DeleteItem` | Partially implemented | `mailbox_messages`, contacts/events/tasks, `recoverable_items`, change log/tombstones | Canonical delete, Trash move, collaboration delete APIs | Exchange delete types are mapped to LPE hard delete or Trash behavior; no full dumpster parity through EWS | P0 |
| `FindItem` | Partially implemented | Mail, contacts, calendar, task tables plus search projections | Canonical item list/query APIs | Bounded to mail/contacts/calendar/tasks; Exchange views, property sets, folders, and public/archive stores are incomplete | P0 |
| `GetItem` | Partially implemented | Mail, MIME/body/attachment, contacts, calendar, task tables | Canonical item fetch/export APIs | LPE-prefixed ids and bounded property projection; no full Exchange opaque IDs or property bag | P0 |
| `MarkAllItemsAsRead` | Missing | `mailbox_messages` read state and change log | Canonical mailbox scoped flag mutation API | Unsupported; clients cannot bulk mark read through EWS | P1 |
| `MoveItem` | Partially implemented | `mailbox_messages`, target `mailboxes`, change log/tombstones | Canonical message move API | Supports canonical message ids only; not full item-class move parity | P1 |
| `SendItem` | Missing | Draft messages, submission tables, sender rights, authoritative `Sent` membership | Canonical submit-existing-draft API | Major Exchange-visible gap for clients that save then call `SendItem`; `CreateItem` send dispositions work instead | P0 |
| `UpdateItem` | Partially implemented | Message flags/keywords, contacts, calendar, task rows, change log | Canonical update APIs for flags/read state and collaboration objects | Mail updates are limited mainly to read/flag state; no full Exchange item property mutation | P0 |

## Folder Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `CreateFolder` | Partially implemented | `mailboxes`, subscriptions, change log | Canonical mailbox create API | Custom mail folders only; no full folder class, public folder, archive, or managed folder behavior | P0 |
| `CreateFolderPath` | Missing | `mailboxes` hierarchy rows | Canonical recursive mailbox create API | Unsupported; clients must create one folder at a time through supported paths | P2 |
| `CreateManagedFolder` | Missing | New managed-folder/retention policy state | Retention policy API if this deprecated Exchange feature becomes scoped | Unsupported deprecated Exchange behavior | P4 |
| `CopyFolder` | Missing | `mailboxes`, mailbox contents for recursive copy if allowed | Canonical folder copy API with bounded user-folder scope | Unsupported through EWS | P2 |
| `DeleteFolder` | Partially implemented | `mailboxes`, change log/tombstones | Canonical mailbox destroy API | Canonical custom folders only; protected/system behavior follows LPE storage rules | P0 |
| `EmptyFolder` | Missing | `mailbox_messages`, `recoverable_items`, change log/tombstones | Canonical folder purge API | Unsupported; clients cannot purge folder contents through EWS | P1 |
| `FindFolder` | Partially implemented | `mailboxes`, contact books, calendars, task lists and grants | Canonical folder/collection projection API | No full Exchange hierarchy including public/archive/voice/search folders | P0 |
| `GetFolder` | Partially implemented | Same as `FindFolder` | Canonical folder/collection fetch API | Unsupported distinguished ids return folder errors; bounded properties | P0 |
| `MoveFolder` | Missing | `mailboxes` parent/name fields, change log | Canonical mailbox update/move API | Unsupported through EWS | P2 |
| `UpdateFolder` | Missing | `mailboxes` mutable display/parent/subscription fields, change log | Canonical mailbox update API | Unsupported; clients cannot rename/update folders through EWS | P1 |

## Attachment Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `CreateAttachment` | Partially implemented | `blobs`, `blob_placements`, `mime_parts`, `attachments`, `calendar_event_attachments`, Magika validation fields | Canonical attachment create API with Magika validation | File attachments on one canonical message parent; `ItemAttachment` unsupported | P0 |
| `GetAttachment` | Partially implemented | Attachment/blob/MIME/calendar attachment rows | Canonical attachment read/export API | Supports LPE attachment references; bounded file content projection | P0 |
| `DeleteAttachment` | Partially implemented | Attachment rows and change log | Canonical attachment delete API | Supports LPE attachment references; no full Exchange item attachment model | P0 |

## Reminder Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `GetReminders` | Missing | Existing reminder fields on `calendar_events` and `tasks`; no separate reminder table | Computed reminders API over calendar/task reminders | Unsupported through EWS; reminders exist only as computed LPE views | P1 |
| `PerformReminderAction` | Missing | Reminder dismiss/snooze state is needed; current schema has reminder fields but no full action history | Canonical reminder action API | EWS clients cannot dismiss/snooze reminders through this operation | P1 |

## Conversation Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `ApplyConversationAction` | Missing | Existing lightweight `thread_id` plus `conversation_actions` for bounded actions | Canonical conversation-action API | Unsupported through EWS; no Exchange conversation action semantics | P2 |
| `FindConversation` | Missing | `mailbox_messages.thread_id`, message summaries, search/query state | Canonical thread/conversation query API | Unsupported; clients must use item listing instead | P1 |
| `GetConversationItems` | Missing | Same as `FindConversation` plus item fetch data | Canonical conversation item fetch API | Unsupported; no Exchange conversation item payloads | P1 |

## Utility Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `ConvertId` | Missing | MAPI/EWS identity projection rows if Exchange-compatible alternate ids are supported | Canonical id conversion service over LPE/EWS/MAPI ids | Unsupported; LPE exposes prefixed canonical ids instead of Exchange opaque id formats | P1 |
| `ExpandDL` | Unsupported | Existing aliases/groups may help; full distribution-list membership model may need more SQL | Canonical directory/group expansion API | Explicit unsupported EWS response; no DL expansion through EWS | P2 |
| `GetUserPhoto` | Missing | New account/contact photo blob metadata | Directory/profile photo API | Unsupported; no EWS SOAP/REST photo payload | P3 |
| `MarkAsJunk` | Missing | New junk classification or mailbox rule state; possible LPE-CT spam feedback integration | Canonical junk-report/move API plus LPE-CT feedback boundary | Unsupported; no EWS junk reporting or block/safe sender behavior | P2 |
| `ResolveNames` | Partially implemented | Accounts, tenant directory rows, contact books/contacts and grants | Canonical address-book/contact lookup API | No full GAL templates, ambiguous name behavior, or distribution-list expansion parity | P0 |
| `GetPasswordExpirationDate` | Missing | Credential expiry policy/state if supported | Account credential policy API | Unsupported; clients cannot query Exchange-style password expiry | P3 |

## Availability Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `GetUserAvailability` | Partially implemented | `calendars`, `calendar_events`, calendar grants | Canonical free/busy API | Authenticated mailbox free/busy only; no full organization availability service | P0 |
| `GetRoomLists` | Unsupported | Room/equipment accounts exist by `directory_kind`; room-list grouping SQL is missing | Directory room-list API | Explicit unsupported response | P1 |
| `GetRooms` | Missing | Room/equipment accounts plus room-list membership/grouping | Directory rooms API | Unsupported; room picker behavior is incomplete | P1 |
| `GetUserOofSettings` | Partially implemented | `sieve_scripts`, `sieve_vacation_responses` | Canonical Sieve vacation projection API | OOF is projected from vacation Sieve, not Exchange OOF state | P1 |
| `SetUserOofSettings` | Partially implemented | Same as `GetUserOofSettings` | Canonical Sieve vacation mutation API | Disabling clears active script; bounded scheduled/external-audience behavior | P1 |

## Bulk Transfer Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `UploadItems` | Missing | Mail/MIME/blob/folder data plus import job/audit state | Canonical bulk import API with Magika/blob validation and mailbox membership writes | Unsupported; no EWS streaming import | P2 |
| `ExportItems` | Missing | Mail/MIME/blob/contact/calendar/task data | Canonical export API reconstructing messages from blobs and metadata | Unsupported; no EWS streaming export | P2 |

## Delegate Management Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `AddDelegate` | Missing | `mailbox_delegation_grants`, `calendar_grants`, `sender_rights`, audit/change rows | Canonical delegation/free-busy/send-on-behalf API | Unsupported; clients cannot create Exchange delegates through EWS | P1 |
| `GetDelegate` | Unsupported | Same as `AddDelegate` | Canonical delegate read API | Explicit unsupported response | P1 |
| `UpdateDelegate` | Missing | Same as `AddDelegate` | Canonical delegate mutation API | Unsupported | P1 |
| `RemoveDelegate` | Missing | Same as `AddDelegate` plus tombstones/change rows | Canonical delegate removal API | Unsupported | P1 |

## Inbox Rules Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `GetInboxRules` | Missing | `sieve_scripts` and bounded rule projection state | Canonical Sieve-backed mailbox rule read API | Unsupported through EWS; MAPI has bounded rule table projection but EWS does not | P0 |
| `UpdateInboxRules` | Missing | Same as `GetInboxRules` plus rule mutation/change rows | Canonical generated-Sieve mutation API | Unsupported; Outlook rule editing through EWS cannot work | P0 |

## Mail App Management Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `DisableApp` | Missing | New Outlook add-in installation/disable state | Add-in management API if Outlook add-ins become product scope | Unsupported | P3 |
| `GetAppManifests` | Missing | New add-in manifest catalog and assignment state | Add-in catalog API | Unsupported | P3 |
| `GetAppMarketplaceUrl` | Missing | Optional tenant/server add-in marketplace configuration | Add-in marketplace configuration API | Unsupported | P4 |
| `GetClientAccessToken` | Missing | New OAuth/add-in token delegation state | Token issuance API with bounded scopes | Unsupported | P3 |
| `InstallApp` | Missing | New add-in installation state | Add-in install API | Unsupported | P3 |
| `UninstallApp` | Missing | New add-in installation state and audit rows | Add-in uninstall API | Unsupported | P3 |

## Mail Tips Operation

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `GetMailTips` | Missing | Directory/accounts, OOF/vacation, mailbox quota, recipient policy data; new custom mail-tip state if supported | Compose-recipient advisory API | Unsupported; clients get no oversized-recipient, OOF, external, or policy tips through EWS | P1 |

## Message Tracking Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `FindMessageTrackingReport` | Missing | `submission_queue`, `submission_events`, LPE-CT delivery receipts plus new report projections | Trace/report API across LPE and LPE-CT boundary | Unsupported; no Exchange tracking reports through EWS | P3 |
| `GetMessageTrackingReport` | Missing | Same as `FindMessageTrackingReport` | Trace/report detail API | Unsupported | P3 |

## Notification Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `GetEvents` | Partially implemented | Current implementation uses in-process event queue plus canonical mailbox projections; durable parity needs `mail_change_log` backed subscription cursors | Canonical notification replay API | Not durable Exchange notification queue; fallback events may be synthetic | P0 |
| `GetStreamingEvents` | Missing | Durable or session-backed notification subscription state and replay cursors | Streaming notification API over canonical changes | Unsupported; streaming subscriptions unavailable | P1 |
| `Subscribe` | Partially implemented | In-process subscription registry; durable parity would need subscription/cursor rows | Canonical subscription API for pull notifications | Pull only; push and streaming subscriptions out of scope; state not SQL-durable | P0 |
| `Unsubscribe` | Partially implemented | Current implementation does not persist subscription state | Canonical subscription cleanup API if durable subscriptions are added | Returns success without durable subscription mutation | P0 |

## Persona Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `FindPeople` | Unsupported | Contacts/accounts plus new persona aggregation/link state | Canonical people/persona query API | Explicit unsupported response; no linked-contact persona aggregation | P2 |
| `GetPersona` | Missing | Same as `FindPeople` | Canonical persona fetch API | Unsupported | P2 |

## Retention Policy Operation

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `GetUserRetentionPolicyTags` | Missing | Existing mailbox retention days are insufficient; Exchange-like retention tag/policy rows required | Retention policy projection API | Unsupported; clients cannot retrieve retention tags through EWS | P2 |

## Service Configuration Operation

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `GetServiceConfiguration` | Missing | Server/tenant settings plus optional mail tips, policy tips, protection rules, UM settings | Service configuration read API | Unsupported; clients cannot discover Exchange service feature configuration | P2 |

## Sharing Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `CreateItem` with `AcceptSharingInvitation` | Missing | Contact/calendar grants plus invitation token/share metadata | Canonical sharing invitation acceptance API | Generic `CreateItem` exists, but sharing invitations are not handled as Exchange sharing objects | P2 |
| `GetSharingFolder` | Unsupported | Sharing invitation/folder binding metadata plus grants | Canonical shared folder binding API | Explicit unsupported response | P2 |
| `GetSharingMetadata` | Unsupported | Sharing metadata/token state | Canonical sharing metadata API | Explicit unsupported response | P2 |
| `RefreshSharingFolder` | Missing | Shared folder binding metadata and remote sync state if federated sharing is supported | Canonical sharing refresh API | Unsupported | P3 |

## Synchronization Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `SyncFolderHierarchy` | Partially implemented | `mailboxes`, collaboration collections/grants, change log | Canonical hierarchy sync projection API | Projects canonical folders/collections; no full Exchange public/archive/voice/search hierarchy | P0 |
| `SyncFolderItems` | Partially implemented | Mail/collaboration rows, `account_sync_state`, `mail_change_log`, tombstones; current EWS tokens are synthetic | Canonical item sync API over current state and change logs | One-way current-state sync with synthetic state tokens, not full Exchange durable sync cursor semantics | P0 |

## Time Zone Operation

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `GetServerTimeZones` | Partially implemented | None for current static response; full parity might need versioned time-zone catalog data | Time-zone catalog projection | Minimal static compatibility set, not full Exchange time-zone corpus | P0 |

## Unified Messaging Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `DisconnectPhoneCall` | Missing | New UM call/session state | Unified Messaging/telephony API if product scope changes | Unsupported | P4 |
| `GetPhoneCallInformation` | Missing | New UM call/session state | Unified Messaging/telephony API | Unsupported | P4 |
| `PlayOnPhone` | Missing | New UM mailbox/phone call state | Unified Messaging/telephony API | Unsupported | P4 |

## Unified Contact Store Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `AddNewImContactToGroup` | Missing | New IM contact and group membership tables | IM/contact-store API if product scope changes | Unsupported | P4 |
| `AddImContactToGroup` | Missing | New IM contact and group membership tables | IM/contact-store API | Unsupported | P4 |
| `AddImGroup` | Missing | New IM group table | IM/contact-store API | Unsupported | P4 |
| `AddNewTelUriContactToGroup` | Missing | New tel URI contact/group tables | IM/contact-store API | Unsupported | P4 |
| `AddDistributionGroupToImList` | Missing | New IM list membership for distribution groups | IM/contact-store API | Unsupported | P4 |
| `GetImItemList` | Missing | New IM list/group/contact tables | IM/contact-store API | Unsupported | P4 |
| `GetImItems` | Missing | New IM contact/group tables | IM/contact-store API | Unsupported | P4 |
| `RemoveContactFromImList` | Missing | New IM list membership tables | IM/contact-store API | Unsupported | P4 |
| `RemoveImContactFromGroup` | Missing | New IM group membership tables | IM/contact-store API | Unsupported | P4 |
| `RemoveDistributionGroupFromImList` | Missing | New IM list membership for distribution groups | IM/contact-store API | Unsupported | P4 |
| `RemoveImGroup` | Missing | New IM group table and tombstones | IM/contact-store API | Unsupported | P4 |
| `SetImGroup` | Missing | New IM group table | IM/contact-store API | Unsupported | P4 |

## User Configuration Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `CreateUserConfiguration` | Missing | New bounded user-configuration blob/key-value table if allowed | Canonical user/client configuration API with strict scope limits | Unsupported; Outlook clients cannot persist Exchange user configuration blobs through EWS | P1 |
| `DeleteUserConfiguration` | Missing | Same as `CreateUserConfiguration` plus tombstones/change rows | Canonical user/client configuration API | Unsupported | P1 |
| `GetUserConfiguration` | Unsupported | Same as `CreateUserConfiguration` | Canonical user/client configuration API | Explicit unsupported response | P1 |
| `UpdateUserConfiguration` | Missing | Same as `CreateUserConfiguration` | Canonical user/client configuration API | Unsupported | P1 |

## Priority Summary

| Priority | Operations |
| --- | --- |
| P0 | `CreateItem`, `DeleteItem`, `FindItem`, `GetItem`, `SendItem`, `UpdateItem`, `CreateFolder`, `DeleteFolder`, `FindFolder`, `GetFolder`, `CreateAttachment`, `GetAttachment`, `DeleteAttachment`, `ResolveNames`, `GetUserAvailability`, `GetInboxRules`, `UpdateInboxRules`, `GetEvents`, `Subscribe`, `Unsubscribe`, `SyncFolderHierarchy`, `SyncFolderItems`, `GetServerTimeZones` |
| P1 | `CopyItem`, `MarkAllItemsAsRead`, `MoveItem`, `EmptyFolder`, `UpdateFolder`, `GetReminders`, `PerformReminderAction`, `FindConversation`, `GetConversationItems`, `ConvertId`, `GetRoomLists`, `GetRooms`, `GetUserOofSettings`, `SetUserOofSettings`, `AddDelegate`, `GetDelegate`, `UpdateDelegate`, `RemoveDelegate`, `GetMailTips`, `GetStreamingEvents`, `CreateUserConfiguration`, `DeleteUserConfiguration`, `GetUserConfiguration`, `UpdateUserConfiguration` |
| P2 | `ArchiveItem`, `CreateFolderPath`, `CopyFolder`, `MoveFolder`, `ApplyConversationAction`, `ExpandDL`, `MarkAsJunk`, `UploadItems`, `ExportItems`, `FindPeople`, `GetPersona`, `GetUserRetentionPolicyTags`, `GetServiceConfiguration`, `CreateItem` with `AcceptSharingInvitation`, `GetSharingFolder`, `GetSharingMetadata` |
| P3 | `GetUserPhoto`, `GetPasswordExpirationDate`, `DisableApp`, `GetAppManifests`, `GetClientAccessToken`, `InstallApp`, `UninstallApp`, `FindMessageTrackingReport`, `GetMessageTrackingReport`, `RefreshSharingFolder` |
| P4 | eDiscovery operations, `CreateManagedFolder`, `GetAppMarketplaceUrl`, Unified Messaging operations, Unified Contact Store operations |

## Main Parity Gaps For Outlook And Native Clients

1. `SendItem` is the largest P0 item gap because clients that save a draft and then send it expect the existing draft to enter canonical submission with authoritative `Sent` visibility.
2. `GetInboxRules` and `UpdateInboxRules` are P0 because Outlook rule visibility/editing should project the existing canonical Sieve-backed rule model instead of being unavailable through EWS.
3. EWS sync and notifications are only partial because current sync-state and pull-subscription behavior is not a durable Exchange-equivalent cursor/subscription model.
4. Reminder actions, room/resource discovery, mail tips, delegate management, and user configuration are the main P1 gaps for a more natural Outlook/native-client experience.
5. Most P3/P4 operations require feature families that LPE intentionally does not model today, such as Exchange eDiscovery, mail apps, Unified Messaging, and Unified Contact Store IM groups.
