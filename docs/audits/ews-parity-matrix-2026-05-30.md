# EWS Parity Matrix - 2026-06-02

## Scope

This matrix expands `docs/audits/ews-audit-2026-05-30.md` into one row for every operation listed in Microsoft's EWS operation catalog:

- <https://learn.microsoft.com/en-us/exchange/client-developer/web-service-reference/ews-operations-in-exchange>
- <https://learn.microsoft.com/en-us/exchange/client-developer/web-service-reference/ews-xml-elements-in-exchange>

Microsoft's operation catalog page was last updated on 2023-03-29 and was reviewed again for this matrix on 2026-06-02. The current `LPE` dispatcher surface was checked against `crates/lpe-exchange/src/service.rs` on the same date. This is a documentation parity matrix only. It does not add, remove, or change runtime behavior.

`crates/lpe-exchange/src/tests/ews.rs::ews_catalog_gate_covers_documented_operations_and_unsupported_gaps` owns a local snapshot of Microsoft's operation catalog from that page and checks this matrix against it. Every documented operation name must have exactly one gate entry: either a named SOAP behavior test for implemented/partial operations or an explicit unsupported SOAP assertion with a tracked reason for unsupported gaps.

Current automated gate coverage:

- Accounted catalog coverage: 96/96 documented operation names, 100.0%.
- Behavioral EWS SOAP coverage: 78/96 operation names, 81.2%.
- Explicit unsupported EWS SOAP coverage with tracked reasons: 18/96 operation names, 18.8%.

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
| `GetDiscoverySearchConfiguration` | Partial | `discovery_searches` | Compliance/admin search API over canonical mailbox/search data | Projects bounded same-tenant discovery search definitions; no Exchange compliance role/scope policy model through EWS | P4 |
| `GetHoldOnMailboxes` | Partial | `compliance_holds`, `compliance_hold_mailboxes`, account litigation-hold fields | Compliance hold management API | Returns canonical same-tenant hold rows only; no Exchange In-Place Hold distribution state | P4 |
| `GetNonIndexableItemDetails` | Partial | `non_indexable_item_reports` | Search/index diagnostics API | Projects bounded diagnostics without protected metadata; no Exchange crawl/report payload parity | P4 |
| `GetNonIndexableItemStatistics` | Partial | `non_indexable_item_reports` | Search/index diagnostics API | Aggregates bounded report counts per mailbox; no Exchange crawl mailbox statistics | P4 |
| `GetSearchableMailboxes` | Partial | Same-tenant `accounts` and litigation-hold fields | Admin/compliance mailbox discovery API | Lists same-tenant account mailboxes; no Exchange discovery-scope grants, external mailboxes, or federation | P4 |
| `SearchMailboxes` | Partial | `discovery_searches`, `discovery_search_jobs`, `discovery_result_items`, `mail_search_documents` | Compliance search API with Bcc-safe defaults | Creates canonical search/job/result rows from Bcc-safe search documents; no Exchange preview/estimate/de-dup/refiner parity | P4 |
| `SetHoldOnMailboxes` | Partial | `compliance_holds`, `compliance_hold_mailboxes`, account litigation-hold fields, audit rows | Compliance hold mutation API | Creates/releases bounded canonical hold rows; no Exchange hold policy distribution semantics | P4 |

## Mailbox Item Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `ArchiveItem` | Missing | Existing mailboxes/messages can model a canonical Archive only if configured; no Exchange archive-mailbox model | Mail move API to canonical Archive plus archive policy rules | Unsupported; no Exchange archive mailbox or retention archive semantics | P2 |
| `CreateItem` | Partial | `messages`, `mailbox_messages`, recipients, protected Bcc, blobs/MIME, contacts, calendars, tasks, submission tables, contact/calendar grants | Draft/import/create APIs for mail, contacts, events, tasks, public-folder posts; canonical submission for send dispositions; bounded sharing invitation acceptance | Bounded item classes; `AcceptSharingInvitation` is supported only for same-tenant contact/calendar grants; no full Exchange property bag or sharing token store | P0 |
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
| `ApplyConversationAction` | Partial | Existing lightweight `thread_id` and canonical message state | Canonical message move/delete/read-state APIs over current thread messages | Supports one-shot `Move`, `Delete`, and `SetReadState`; persistent future-message `Always*` actions return parseable gaps because no first-class thread lifecycle exists | P2 |
| `FindConversation` | Partial | `mailbox_messages.thread_id`, message summaries, search/query state | Canonical message grouping by lightweight thread id | Lists current folder-scoped conversations only; no Exchange conversation index or lifecycle identity | P1 |
| `GetConversationItems` | Partial | Same as `FindConversation` plus item fetch data | Canonical message fetch grouped by lightweight thread id | Returns current message nodes only; folder ignore is bounded to canonical folder membership | P1 |

## Utility Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `ConvertId` | Partial | No SQL; stateless opaque ids encode canonical LPE EWS family/id payloads | Canonical EWS id codec over supported LPE object families | Supports deterministic opaque alternate ids for canonical message, folder, contact, event, task, attachment, and public-folder ids; no Exchange identity table or full MAPI EntryId parity | P1 |
| `ExpandDL` | Partial | Existing canonical group aliases/members and visible directory entries | Canonical directory/group expansion API | Expands visible same-tenant public DL membership only; no recursive expansion or private Exchange DL item expansion | P2 |
| `GetUserPhoto` | Partial parseable gap | New account/contact photo blob metadata if photo support is later introduced | Directory/profile photo API | Validates same-tenant directory visibility, then returns parseable no-photo because no canonical photo blob state exists | P3 |
| `MarkAsJunk` | Partial | Existing canonical mailbox/message state; no protocol-local junk list state | Canonical message move to Junk; future spam feedback must cross LPE-CT boundary explicitly | Supports `IsJunk=true` plus `MoveItem=true`; Exchange blocked/safe sender list and unblock-only behavior return parseable gaps | P2 |
| `ResolveNames` | Partial | Accounts, tenant directory rows, contact books/contacts and grants | Canonical address-book/contact lookup API | No full GAL templates, ambiguous name behavior, or distribution-list expansion parity | P0 |
| `GetPasswordExpirationDate` | Partial parseable gap | Credential expiry policy/state if supported later | Account credential policy API | Authenticated account query returns parseable gap because no canonical password-expiration field exists; other-account query is denied | P3 |

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
| `UploadItems` | Partial | `mailbox_item_transfer_jobs`, `mailbox_item_transfer_entries` | Canonical bulk import job API with later Magika/blob validation and mailbox membership writes | Records bounded EWS import jobs and entries; no full Exchange streaming item import or MIME conversion | P2 |
| `ExportItems` | Partial | `mailbox_item_transfer_jobs`, `mailbox_item_transfer_entries`, canonical item ids | Canonical export job API reconstructing messages from blobs and metadata | Records bounded EWS export jobs and entries; no full Exchange streaming item export payload | P2 |

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
| `DisableApp` | Partial | `mail_app_installations` | Canonical account app-installation mutation API | Disables only the authenticated account's canonical install row; no Exchange org-wide add-in deployment surface | P3 |
| `GetAppManifests` | Partial | `mail_app_catalog`, `mail_app_tenant_policies`, `mail_app_installations` | Canonical add-in catalog projection API | Returns stored same-tenant catalog manifests visible through install state or tenant default-install policy; no remote marketplace manifest discovery | P3 |
| `GetAppMarketplaceUrl` | Partial | `mail_app_tenant_policies` | Canonical tenant marketplace policy lookup | Returns configured canonical tenant URL only; disabled or missing policy returns a parseable EWS gap and no Exchange marketplace federation | P4 |
| `GetClientAccessToken` | Partial | `mail_app_catalog`, `mail_app_installations`, `mail_app_consents`, `mail_app_token_events` | Bounded token-event issuance API | Issues opaque EWS app tokens and stores only hashes/scope/expiry; no Exchange OAuth delegation or remote callback-token service | P3 |
| `InstallApp` | Partial | `mail_app_catalog`, `mail_app_tenant_policies`, `mail_app_installations`, `mail_app_consents` | Canonical account add-in install API | Installs active same-tenant catalog apps allowed by tenant policy and grants bounded `ews` consent; arbitrary client manifests are unsupported | P3 |
| `UninstallApp` | Partial | `mail_app_installations`, `mail_app_token_events` | Canonical account add-in uninstall API | Marks the authenticated account install uninstalled and revokes token events; no Exchange deployment package cleanup | P3 |

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
| `GetUserRetentionPolicyTags` | Partial | `retention_policy_tags`, `account_retention_policy_assignments` | Bounded retention policy tag projection API | Returns active same-tenant visible tags plus the authenticated account's assigned default tag, including hidden assigned tags; no Exchange managed-folder policy engine or cross-tenant tag visibility | P2 |

## Service Configuration Operation

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `GetServiceConfiguration` | Partial | Existing bounded MailTips capability; no Exchange-only UM, Protection Rules, or Policy Tips service settings | Bounded service configuration read path | Returns MailTips configuration for the implemented MailTips surface; requested Unified Messaging, Protection Rules, Policy Tips, or unknown service configurations return parseable EWS gaps | P2 |

## Sharing Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `CreateItem` with `AcceptSharingInvitation` | Partial | `contact_book_grants`, `calendar_grants`, same-tenant account directory | Canonical sharing invitation acceptance API | Creates/updates same-tenant contact/calendar grants only; no Exchange invitation token, federation, mailbox-folder, or external sharing state | P2 |
| `GetSharingFolder` | Partial | Contact/calendar collections and grants plus same-tenant account directory | Canonical shared folder binding API | Returns only accessible same-tenant contact/calendar folders; ungranted, cross-tenant, mailbox, and federated shares return parseable EWS errors | P2 |
| `GetSharingMetadata` | Partial | Owned contact/calendar collections | Canonical sharing metadata projection | Emits bounded metadata for owned contact/calendar collections only; no Exchange tokens, mailbox sharing metadata, or federation discovery | P2 |
| `RefreshSharingFolder` | Partial | Accessible contact/calendar collections and grants | Canonical shared folder visibility check | Verifies the shared contact/calendar folder is still accessible; no remote/federated refresh state | P3 |

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
| `DisconnectPhoneCall` | Partial | `unified_messaging_calls` | Canonical Unified Messaging call-state mutation API | Cancels active same-account canonical calls only; no PBX, dial-plan, voicemail transport, or Exchange UM policy integration | P4 |
| `GetPhoneCallInformation` | Partial | `unified_messaging_calls` | Canonical Unified Messaging call-state read API | Returns same-account canonical call state only; no Exchange UM diagnostics or telephony details | P4 |
| `PlayOnPhone` | Partial | `unified_messaging_calls`, optional canonical message ids | Canonical Unified Messaging play request API | Records a `play_on_phone` request only; real outbound call control and voicemail playback are external to EWS | P4 |

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
| P1 | `CopyItem`, `MarkAllItemsAsRead`, `MoveItem`, `EmptyFolder`, `UpdateFolder`, `GetReminders`, `PerformReminderAction`, `ConvertId`, `GetRoomLists`, `GetRooms`, `GetUserOofSettings`, `SetUserOofSettings`, `GetMailTips`, `GetStreamingEvents`, `CreateUserConfiguration`, `DeleteUserConfiguration`, `GetUserConfiguration`, `UpdateUserConfiguration` |
| P2 | `ArchiveItem`, `CreateFolderPath`, `CopyFolder`, `MoveFolder`, `FindPeople`, `GetPersona` |
| P3 | `FindMessageTrackingReport`, `GetMessageTrackingReport` |
| P4 | `CreateManagedFolder`, Unified Contact Store operations |

## Main Parity Gaps For Outlook And Native Clients

1. The remaining operation-name blockers to 100% behavioral coverage are the 18 explicit unsupported operations listed as `Missing` or `Explicitly unsupported`: `AddDistributionGroupToImList`, `AddImContactToGroup`, `AddImGroup`, `AddNewImContactToGroup`, `AddNewTelUriContactToGroup`, `ArchiveItem`, `CreateManagedFolder`, `FindMessageTrackingReport`, `FindPeople`, `GetImItemList`, `GetImItems`, `GetMessageTrackingReport`, `GetPersona`, `RemoveContactFromImList`, `RemoveDistributionGroupFromImList`, `RemoveImContactFromGroup`, `RemoveImGroup`, and `SetImGroup`.
2. The highest-value unsupported blockers are `ArchiveItem`, persona operations, and message tracking reports. UCS/IM operations and deprecated managed folders remain lower priority unless LPE explicitly adds those Exchange feature families.
3. Many implemented operations remain partial because they expose canonical LPE behavior rather than full Exchange storage, rule, room-list, reminder, notification, mail app, UM, user-configuration, sync, or identity semantics.
4. Full Exchange parity still requires replacing bounded compatibility behavior with first-class canonical models where justified: archive mailbox semantics, linked persona aggregation, LPE/LPE-CT tracking reports, UCS IM groups, durable Exchange-equivalent sync/notification semantics, Exchange identity compatibility, and any Outlook-proven mail-tip or policy-tip fields beyond the bounded current surface.
