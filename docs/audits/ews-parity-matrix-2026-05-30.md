# EWS Parity Matrix - 2026-06-02

## Scope

This matrix expands `docs/audits/ews-audit-2026-05-30.md` into one row for every operation listed in Microsoft's EWS operation catalog:

- <https://learn.microsoft.com/en-us/exchange/client-developer/web-service-reference/ews-operations-in-exchange>
- <https://learn.microsoft.com/en-us/exchange/client-developer/web-service-reference/ews-xml-elements-in-exchange>

Microsoft's operation catalog page was last updated on 2023-03-29 and was reviewed again for this matrix on 2026-06-02. The current `LPE` dispatcher surface was checked against `crates/lpe-exchange/src/service.rs` on the same date. This is a documentation parity matrix only. It does not add, remove, or change runtime behavior.

`crates/lpe-exchange/src/tests/ews.rs::ews_catalog_gate_covers_documented_operations_and_unsupported_gaps` consumes this matrix as the local EWS operation catalog for automated compatibility gating. Every operation name listed in the matrix must have exactly one gate entry: either a named SOAP behavior test for implemented/partial operations or an explicit unsupported SOAP assertion for unsupported gaps.

## Status And Priority Legend

| Value | Meaning |
| --- | --- |
| Implemented | LPE has concrete EWS behavior and no known material Exchange-visible gap for the documented LPE scope. |
| Partial | LPE dispatches the operation to concrete behavior, but the behavior is bounded and differs from full Exchange semantics. |
| Explicitly unsupported | LPE explicitly recognizes the operation and returns a parseable unsupported EWS response. |
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
| `CreateItem` | Partial | `messages`, `mailbox_messages`, recipients, protected Bcc, blobs/MIME, contacts, calendars, tasks, submission tables | Draft/import/create APIs for mail, contacts, events, tasks, public-folder posts; canonical submission for send dispositions | Bounded item classes; no `AcceptSharingInvitation` special handling; no full Exchange property bag | P0 |
| `CopyItem` | Partial | `messages`, `mailbox_messages`, `public_folder_items`, change log/tombstones | Canonical message copy API; canonical public-folder item clone API | Supports canonical message and public-folder item ids only; not full item-class copy parity | P1 |
| `DeleteItem` | Partial | `mailbox_messages`, contacts/events/tasks, `recoverable_items`, `public_folder_items`, change log/tombstones | Canonical delete, Trash move, collaboration delete, and public-folder item delete APIs | Exchange delete types are mapped to LPE hard delete or Trash behavior; no full dumpster parity through EWS | P0 |
| `FindItem` | Partial | Mail, contacts, calendar, task tables, public-folder item tables plus search projections | Canonical item list/query APIs | Bounded to mail/contacts/calendar/tasks/public-folder posts; Exchange views, property sets, folders, and archive stores are incomplete | P0 |
| `GetItem` | Partial | Mail, MIME/body/attachment, contacts, calendar, task, public-folder item tables | Canonical item fetch/export APIs | LPE-prefixed ids and bounded property projection; no full Exchange opaque IDs or property bag | P0 |
| `MarkAllItemsAsRead` | Partial | `mailbox_messages` read state and change log | Canonical mailbox scoped query plus message flag update API | Supports bounded mailbox folders only; public-folder per-user read state remains unsupported through this operation | P1 |
| `MoveItem` | Partial | `mailbox_messages`, target `mailboxes`, `public_folder_items`, change log/tombstones | Canonical message move API; canonical public-folder item clone/delete API | Supports canonical message and public-folder item ids only; not full item-class move parity | P1 |
| `SendItem` | Partial | Draft messages, submission tables, sender rights, authoritative `Sent` membership | Canonical submit-existing-draft API | Sends supported canonical draft ids through LPE submission; no EWS-local `Outbox` or parallel `Sent`; full Exchange saved-item options remain bounded | P0 |
| `UpdateItem` | Partial | Message flags/keywords, contacts, calendar, task rows, public-folder items, change log | Canonical update APIs for flags/read state, collaboration objects, and public-folder posts | Mail updates are limited mainly to read/flag state; no full Exchange item property mutation | P0 |

## Folder Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `CreateFolder` | Partial | `mailboxes`, `public_folders`, subscriptions, change log | Canonical mailbox create API; canonical public-folder child create API | Custom mail folders and child public folders only; no full folder class, archive, or managed folder behavior | P0 |
| `CreateFolderPath` | Partial | `mailboxes`, `public_folders`, change log | Canonical mailbox create API; canonical public-folder child create API | Creates/reuses bounded path segments only; no full Exchange rollback or folder-class parity | P2 |
| `CreateManagedFolder` | Missing | New managed-folder/retention policy state | Retention policy API if this deprecated Exchange feature becomes scoped | Unsupported deprecated Exchange behavior | P4 |
| `CopyFolder` | Partial | `mailboxes`, mailbox contents, `public_folders`, `public_folder_items`, change log | Canonical mailbox create/message copy APIs; canonical public-folder child/item APIs | Copies bounded custom mailbox and public-folder trees; system folders are rejected | P2 |
| `DeleteFolder` | Partial | `mailboxes`, `public_folders`, change log/tombstones | Canonical mailbox destroy API; canonical public-folder delete API | Canonical deletable mailbox and public folders only; protected/system behavior follows LPE storage rules | P0 |
| `EmptyFolder` | Partial | `mailbox_messages`, `recoverable_items`, `public_folder_items`, change log/tombstones | Canonical mailbox scoped delete API; canonical public-folder item delete API | Empties bounded mailbox/public-folder contents; optional subfolder deletion is limited to canonical deletable subfolders | P1 |
| `FindFolder` | Partial | `mailboxes`, contact books, calendars, task lists, public folders, search folders, and grants | Canonical folder/collection projection API | No full Exchange hierarchy including archive, voice, and complete search-folder behavior | P0 |
| `GetFolder` | Partial | Same as `FindFolder` | Canonical folder/collection fetch API | Unsupported distinguished ids return folder errors; bounded properties | P0 |
| `MoveFolder` | Partial | `mailboxes` parent fields, change log | Canonical mailbox update/move API | Moves bounded custom mailbox folders; public-folder move is explicitly rejected until canonical reparenting exists | P2 |
| `UpdateFolder` | Partial | `mailboxes`, `public_folders`, change log | Canonical mailbox update API; canonical public-folder update API | Updates bounded `DisplayName` only; protected/system folders are rejected | P1 |

## Attachment Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `CreateAttachment` | Partial | `blobs`, `blob_placements`, `mime_parts`, `attachments`, `calendar_event_attachments`, Magika validation fields | Canonical attachment create API with Magika validation | File attachments on one canonical message parent; `ItemAttachment` unsupported | P0 |
| `GetAttachment` | Partial | Attachment/blob/MIME/calendar attachment rows | Canonical attachment read/export API | Supports LPE attachment references; bounded file content projection | P0 |
| `DeleteAttachment` | Partial | Attachment rows and change log | Canonical attachment delete API | Supports LPE attachment references; no full Exchange item attachment model | P0 |

## Reminder Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `GetReminders` | Partial | Existing reminder fields on `calendar_events`, `tasks`, and message follow-up metadata; `reminder_occurrence_dismissals` for dismissed occurrences | Computed reminders API over calendar/task/message reminders | Bounded computed LPE view; no Exchange reminder table or full response-shape parity | P1 |
| `PerformReminderAction` | Partial | `reminder_occurrence_dismissals`, task/event reminder fields, message follow-up reminder dismissal state | Canonical reminder action API | Supports bounded dismiss and snooze behavior; unsupported Exchange action shapes return EWS errors | P1 |

## Conversation Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `ApplyConversationAction` | Missing | Existing lightweight `thread_id` plus `conversation_actions` for bounded actions | Canonical conversation-action API | Unsupported through EWS; no Exchange conversation action semantics | P2 |
| `FindConversation` | Missing | `mailbox_messages.thread_id`, message summaries, search/query state | Canonical thread/conversation query API | Unsupported; clients must use item listing instead | P1 |
| `GetConversationItems` | Missing | Same as `FindConversation` plus item fetch data | Canonical conversation item fetch API | Unsupported; no Exchange conversation item payloads | P1 |

## Utility Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `ConvertId` | Partial | No SQL; stateless opaque ids encode canonical LPE EWS family/id payloads | Canonical EWS id codec over supported LPE object families | Supports deterministic opaque alternate ids for canonical message, folder, contact, event, task, attachment, and public-folder ids; no Exchange identity table or full MAPI EntryId parity | P1 |
| `ExpandDL` | Explicitly unsupported | Existing aliases/groups may help; full distribution-list membership model may need more SQL | Canonical directory/group expansion API | Explicit unsupported EWS response; no DL expansion through EWS | P2 |
| `GetUserPhoto` | Missing | New account/contact photo blob metadata | Directory/profile photo API | Unsupported; no EWS SOAP/REST photo payload | P3 |
| `MarkAsJunk` | Missing | New junk classification or mailbox rule state; possible LPE-CT spam feedback integration | Canonical junk-report/move API plus LPE-CT feedback boundary | Unsupported; no EWS junk reporting or block/safe sender behavior | P2 |
| `ResolveNames` | Partial | Accounts, tenant directory rows, contact books/contacts and grants | Canonical address-book/contact lookup API | No full GAL templates, ambiguous name behavior, or distribution-list expansion parity | P0 |
| `GetPasswordExpirationDate` | Missing | Credential expiry policy/state if supported | Account credential policy API | Unsupported; clients cannot query Exchange-style password expiry | P3 |

## Availability Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `GetUserAvailability` | Partial | `calendars`, `calendar_events`, calendar grants | Canonical free/busy API | Authenticated mailbox free/busy only; no full organization availability service | P0 |
| `GetRoomLists` | Partial | Room/equipment accounts by `directory_kind`; explicit room-list grouping SQL is still absent | Directory room-list API over computed tenant room/resource projection | Returns a computed tenant room/resource list, not arbitrary Exchange room-list membership | P1 |
| `GetRooms` | Partial | Room/equipment accounts plus tenant scoping and GAL visibility | Directory rooms API | Lists visible room/equipment accounts; explicit room-list membership filtering is rejected unless it matches the computed LPE list | P1 |
| `GetUserOofSettings` | Partial | `sieve_scripts`, `sieve_vacation_responses` | Canonical Sieve vacation projection API | OOF is projected from vacation Sieve, not Exchange OOF state | P1 |
| `SetUserOofSettings` | Partial | Same as `GetUserOofSettings` | Canonical Sieve vacation mutation API | Disabling clears active script; bounded scheduled/external-audience behavior | P1 |

## Bulk Transfer Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `UploadItems` | Missing | Mail/MIME/blob/folder data plus import job/audit state | Canonical bulk import API with Magika/blob validation and mailbox membership writes | Unsupported; no EWS streaming import | P2 |
| `ExportItems` | Missing | Mail/MIME/blob/contact/calendar/task data | Canonical export API reconstructing messages from blobs and metadata | Unsupported; no EWS streaming export | P2 |

## Delegate Management Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `AddDelegate` | Partial | `mailbox_delegation_grants`, `calendar_grants`, `sender_rights`, `delegate_preferences`, audit/change rows | Bounded canonical delegation/free-busy/send-on-behalf API | Supports same-tenant Inbox/Calendar delegate grants, send-on-behalf, meeting delivery copy/private preferences; Exchange-only folder permission shapes are rejected | P1 |
| `GetDelegate` | Partial | Same as `AddDelegate` | Bounded canonical delegate read API | Returns bounded canonical delegate projection only; no Exchange-only delegate folders | P1 |
| `UpdateDelegate` | Partial | Same as `AddDelegate` | Bounded canonical delegate mutation API | Updates only canonical Inbox/Calendar grants, send-on-behalf, and preferences | P1 |
| `RemoveDelegate` | Partial | Same as `AddDelegate` plus tombstones/change rows | Bounded canonical delegate removal API | Removes canonical delegate grants, sender rights, and preferences; no protocol-local delegate state | P1 |

## Inbox Rules Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `GetInboxRules` | Partial | `sieve_scripts` and bounded rule projection state | Canonical Sieve-backed mailbox rule read API | Projects bounded server-side rules only; Exchange rule blobs and client-only rules are not exposed as canonical rules | P0 |
| `UpdateInboxRules` | Partial | Same as `GetInboxRules` plus rule mutation/change rows | Canonical generated-Sieve mutation API | Creates, updates, and deletes only rule shapes that map safely to canonical Sieve behavior | P0 |

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
| `GetMailTips` | Partial | Directory/accounts/contacts/groups plus canonical Sieve vacation state | Compose-recipient advisory API over canonical directory and OOF state | Supports invalid-recipient and OOF tips only; no quota, moderation, policy, or custom Exchange mail-tip state | P1 |

## Message Tracking Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `FindMessageTrackingReport` | Missing | `submission_queue`, `submission_events`, LPE-CT delivery receipts plus new report projections | Trace/report API across LPE and LPE-CT boundary | Unsupported; no Exchange tracking reports through EWS | P3 |
| `GetMessageTrackingReport` | Missing | Same as `FindMessageTrackingReport` | Trace/report detail API | Unsupported | P3 |

## Notification Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `GetEvents` | Partial | `mail_change_log` and deterministic cursor/watermark projection; no Exchange subscription table | Canonical notification replay API | Durable canonical replay only; no full Exchange push/affinity semantics | P0 |
| `GetStreamingEvents` | Partial | `mail_change_log` and deterministic cursor/watermark projection; no Exchange subscription table | Streaming notification response over canonical changes | Bounded immediate streaming-shaped response; no full long-held Exchange streaming affinity | P1 |
| `Subscribe` | Partial | `mail_change_log` cursor; deterministic EWS subscription id/watermark only | Canonical subscription API for pull notifications | Pull subscriptions only; push remains unsupported; no protocol-local subscription truth | P0 |
| `Unsubscribe` | Partial | No protocol-local subscription state | Compatibility cleanup response over cursor-based subscription truth | Returns success without durable subscription mutation | P0 |

## Persona Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `FindPeople` | Explicitly unsupported | Contacts/accounts plus new persona aggregation/link state | Canonical people/persona query API | Explicit unsupported response; no linked-contact persona aggregation | P2 |
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
| `GetSharingFolder` | Explicitly unsupported | Sharing invitation/folder binding metadata plus grants | Canonical shared folder binding API | Explicit unsupported response | P2 |
| `GetSharingMetadata` | Explicitly unsupported | Sharing metadata/token state | Canonical sharing metadata API | Explicit unsupported response | P2 |
| `RefreshSharingFolder` | Missing | Shared folder binding metadata and remote sync state if federated sharing is supported | Canonical sharing refresh API | Unsupported | P3 |

## Synchronization Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `SyncFolderHierarchy` | Partial | `mailboxes`, collaboration collections/grants, public folders, change log | Canonical hierarchy sync projection API | Projects canonical folders/collections/public folders; no full Exchange archive/voice/search hierarchy | P0 |
| `SyncFolderItems` | Partial | Mail/collaboration/public-folder rows, `account_sync_state`, `mail_change_log`, tombstones; current EWS tokens are bounded | Canonical item sync API over current state and change logs | One-way bounded current-state sync, not full Exchange durable sync cursor semantics | P0 |

## Time Zone Operation

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `GetServerTimeZones` | Partial | None for current static response; full parity might need versioned time-zone catalog data | Time-zone catalog projection | Minimal static compatibility set, not full Exchange time-zone corpus | P0 |

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
| `CreateUserConfiguration` | Partial | `account_client_configurations` keyed by account, optional mailbox/public-folder scope, config class, and config name | Canonical user/client configuration API with strict scope limits | Stores bounded dictionary, XML, and binary payloads; no Exchange arbitrary user-configuration store is introduced | P1 |
| `DeleteUserConfiguration` | Partial | Same as `CreateUserConfiguration` | Canonical user/client configuration API | Deletes canonical user configuration blobs; missing rows return EWS item-not-found errors | P1 |
| `GetUserConfiguration` | Partial | Same as `CreateUserConfiguration` | Canonical user/client configuration API | Returns bounded dictionary, XML, and binary payloads from canonical storage | P1 |
| `UpdateUserConfiguration` | Partial | Same as `CreateUserConfiguration` plus audit/modseq update | Canonical user/client configuration API | Replaces bounded canonical payloads and advances canonical modseq | P1 |

## Priority Summary

| Priority | Operations |
| --- | --- |
| P0 | `CreateItem`, `DeleteItem`, `FindItem`, `GetItem`, `SendItem`, `UpdateItem`, `CreateFolder`, `DeleteFolder`, `FindFolder`, `GetFolder`, `CreateAttachment`, `GetAttachment`, `DeleteAttachment`, `ResolveNames`, `GetUserAvailability`, `GetInboxRules`, `UpdateInboxRules`, `GetEvents`, `Subscribe`, `Unsubscribe`, `SyncFolderHierarchy`, `SyncFolderItems`, `GetServerTimeZones` |
| P1 | `CopyItem`, `MarkAllItemsAsRead`, `MoveItem`, `EmptyFolder`, `UpdateFolder`, `GetReminders`, `PerformReminderAction`, `FindConversation`, `GetConversationItems`, `ConvertId`, `GetRoomLists`, `GetRooms`, `GetUserOofSettings`, `SetUserOofSettings`, `GetMailTips`, `GetStreamingEvents`, `CreateUserConfiguration`, `DeleteUserConfiguration`, `GetUserConfiguration`, `UpdateUserConfiguration` |
| P2 | `ArchiveItem`, `CreateFolderPath`, `CopyFolder`, `MoveFolder`, `ApplyConversationAction`, `ExpandDL`, `MarkAsJunk`, `UploadItems`, `ExportItems`, `FindPeople`, `GetPersona`, `GetUserRetentionPolicyTags`, `GetServiceConfiguration`, `CreateItem` with `AcceptSharingInvitation`, `GetSharingFolder`, `GetSharingMetadata` |
| P3 | `GetUserPhoto`, `GetPasswordExpirationDate`, `DisableApp`, `GetAppManifests`, `GetClientAccessToken`, `InstallApp`, `UninstallApp`, `FindMessageTrackingReport`, `GetMessageTrackingReport`, `RefreshSharingFolder` |
| P4 | eDiscovery operations, `CreateManagedFolder`, `GetAppMarketplaceUrl`, Unified Messaging operations, Unified Contact Store operations |

## Main Parity Gaps For Outlook And Native Clients

1. The highest-value remaining P0/P1 gaps are conversation listing/expansion and any real-client mail-tip fields beyond invalid-recipient and OOF that prove necessary in Outlook testing.
2. `SendItem`, inbox rules, reminders, room/resource discovery, bounded streaming notifications, and user configuration are now wired, but remain partial because they expose canonical LPE behavior rather than full Exchange storage, rule, room-list, reminder, notification, or user-configuration semantics.
3. EWS sync and notifications remain partial because current sync-state and subscription behavior is not a full Exchange-equivalent cursor, affinity, or push/streaming model.
4. Most P3/P4 operations require feature families that LPE intentionally does not model as Exchange-compatible runtime behavior today, such as Exchange eDiscovery, mail apps, Unified Messaging, and Unified Contact Store IM groups.
