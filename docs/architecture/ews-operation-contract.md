# EWS Operation Contract

## Scope

This matrix records one row for every operation listed in Microsoft's EWS operation catalog:

- <https://learn.microsoft.com/en-us/exchange/client-developer/web-service-reference/ews-operations-in-exchange>
- <https://learn.microsoft.com/en-us/exchange/client-developer/web-service-reference/ews-xml-elements-in-exchange>

Microsoft's operation catalog page was last updated on 2023-03-29. This is the current documentation contract for the `LPE` dispatcher surface. It does not add, remove, or change runtime behavior.

`crates/lpe-exchange/src/tests/ews.rs::ews_catalog_gate_covers_documented_operations_and_unsupported_gaps` owns a local snapshot of Microsoft's operation catalog from that page and checks this matrix's operation names and statuses against its explicit coverage manifest. Every documented operation name must have exactly one manifest entry. The gate sends a parseable SOAP probe to every behavioral entry and requires a tracked reason plus an explicit unsupported SOAP assertion for every unsupported entry.

Current automated gate coverage:

- Accounted catalog coverage: 96/96 documented operation names, 100.0%.
- Behavioral EWS SOAP coverage: 96/96 operation names, 100.0%.
- Explicit unsupported EWS SOAP coverage with tracked reasons: 0/96 operation names, 0.0%.

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
| `ArchiveItem` | Partial | Existing mailboxes/messages with the configured `archive` role; no Exchange archive-mailbox model | Canonical atomic mailbox message-batch move API | Requires one visible canonical source mailbox id, preflights every source membership, then moves at most 100 canonical messages to an existing configured Archive mailbox in one canonical transaction without exposing Bcc. It never provisions an Archive mailbox and does not implement Exchange online-archive or retention semantics | P2 |
| `CreateItem` | Partial | `messages`, `mailbox_messages`, recipients, protected Bcc, blobs/MIME, contacts, calendars, tasks, submission tables, contact/calendar grants | Draft/import/create APIs for mail, contacts, events, tasks, public-folder posts; canonical submission for send dispositions; bounded sharing invitation acceptance | Bounded item classes; an explicit `SavedItemFolderId` must contain exactly one supported, well-formed canonical target and is validated before any write. Embedded attachment payloads are rejected before mutation; callers use the bounded `CreateAttachment` operation after item creation. `AcceptSharingInvitation` is supported only for same-tenant contact/calendar grants; no full Exchange property bag or sharing token store | P0 |
| `CopyItem` | Partial | `messages`, `mailbox_messages`, `public_folder_items`, change log/tombstones | Canonical atomic message-copy API; canonical atomic public-folder item clone API | Supports at most 100 canonical message or public-folder item ids from exactly one direct `ItemIds` collection after complete target/source/access preflight; the supported batch commits atomically, retains source membership, and creates target membership | P1 |
| `DeleteItem` | Partial | `mailbox_messages`, contacts/events/tasks, `recoverable_items`, `public_folder_items`, change log/tombstones | Canonical delete, Trash move, collaboration delete, and public-folder item delete APIs | Supports exactly one canonical target until a cross-family atomic delete transaction exists. Calendar `MoveToDeletedItems` uses the canonical deleted-event lifecycle and `HardDelete` permanently deletes; contact deletion supports canonical `HardDelete` only. `SoftDelete` is rejected before mutation until EWS projects canonical recoverable items; no full dumpster parity through EWS | P0 |
| `FindItem` | Partial | Mail, contacts, calendar, task tables, public-folder item tables plus search projections | Canonical item list/query APIs | Bounded to mail/contacts/calendar/tasks/public-folder posts; malformed explicit canonical folder ids return an EWS folder error before a query. If a mail item is no longer canonically visible after its ID query, the operation fails closed before projecting a partial page or its count. Exchange views, property sets, folders, and archive stores are incomplete | P0 |
| `GetItem` | Partial | Mail, MIME/body/attachment, contacts, calendar, task, public-folder item tables | Canonical item fetch/export APIs | LPE-prefixed ids and bounded property projection; no full Exchange opaque IDs or property bag | P0 |
| `MarkAllItemsAsRead` | Partial | `mailbox_messages` read state and change log | Canonical mailbox-scoped bulk read-state mutation API | Supports one visible canonical mailbox folder with a 10,000 changed-item limit; public-folder per-user read state remains unsupported through this operation | P1 |
| `MoveItem` | Partial | `mailbox_messages`, target `mailboxes`, `public_folder_items`, change log/tombstones | Canonical atomic message move API; canonical atomic public-folder item clone/delete API | Supports at most 100 canonical message or public-folder item ids after complete target/source/access preflight; the supported batch commits target membership, source tombstone/change evidence, and public-folder clone/delete effects atomically | P1 |
| `SendItem` | Partial | Draft messages, submission tables, sender rights, authoritative `Sent` membership | Canonical submit-existing-draft API | Sends exactly one supported canonical draft through LPE submission until atomic batch submission exists; no EWS-local `Outbox` or parallel `Sent`; full Exchange saved-item options remain bounded | P0 |
| `UpdateItem` | Partial | Message flags/keywords, contacts, calendar, task rows, public-folder items, change log | Canonical update APIs for flags/read state, collaboration objects, and public-folder posts | Validates and applies exactly one item mutation before writing until a canonical cross-family atomic batch transaction exists. Contact/calendar mutation requires a current EWS ChangeKey; mail updates are limited mainly to read/flag state | P0 |

## Folder Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `CreateFolder` | Partial | `mailboxes`, `public_folders`, subscriptions, change log | Canonical mailbox create API; canonical public-folder child create API | Exactly one `ParentFolderId` and one `Folders` collection containing one `IPF.Note` custom mailbox folder under `msgfolderroot` or a custom mailbox, or one permitted child public folder; duplicate wrapper, system/protected, inaccessible, archive, managed, voice, and other folder-class shapes are rejected before mutation | P0 |
| `CreateFolderPath` | Partial | `mailboxes`, subscriptions, change log | Canonical atomic mailbox-path create API | Creates/reuses nonempty segments under `msgfolderroot` or a custom mailbox in one canonical transaction. Public-folder paths are rejected until canonical public-folder path transactions exist | P2 |
| `CreateManagedFolder` | Partial | `mailboxes.retention_policy_tag_id`, `retention_policy_tags`, `account_retention_policy_assignments`, `mail_change_log` | Canonical managed-retention folder API over mailbox creation and retention tags | Creates or reuses a canonical root custom mailbox folder for an active visible or assigned same-tenant custom/personal retention tag; no Exchange managed-folder policy blob is created | P4 |
| `CopyFolder` | Partial | `mailboxes`, subscriptions, change log | Canonical custom mailbox create API | Copies one empty custom mailbox leaf under root or a custom mailbox. Nonempty or recursive trees and public-folder shapes are rejected before mutation until canonical whole-tree transactions exist | P2 |
| `DeleteFolder` | Partial | `mailboxes`, `public_folders`, change log/tombstones | Canonical mailbox destroy API; canonical public-folder delete API | Exactly one `FolderIds` collection with one canonical deletable custom mailbox or permitted public folder; duplicate wrapper, protected/system, inaccessible, unsupported, and batch shapes are rejected before mutation | P0 |
| `EmptyFolder` | Partial | `mailbox_messages`, `recoverable_items`, `public_folder_items`, change log/tombstones | Canonical atomic mailbox scoped delete API; canonical atomic public-folder item delete API | Empties at most 10,000 items in one bounded custom mailbox or accessible public-folder scope atomically; protected/system folders, mixed targets, over-limit requests, and undeletable descendants are rejected before mutation; optional subfolder deletion is limited to canonical deletable subfolders | P1 |
| `FindFolder` | Partial | `mailboxes`, contact books, calendars, task lists, public folders, search folders, and grants | Canonical folder/collection projection API | Lists the bounded canonical hierarchy or immediate supported children of one requested parent; exactly one `ParentFolderIds` collection is accepted when supplied, and collaboration/public projections remain access-scoped; no archive, voice, or complete search-folder behavior | P0 |
| `GetFolder` | Partial | Same as `FindFolder` | Canonical folder/collection fetch API | Requires exactly one `FolderIds` collection. Returns each supported mailbox/public-folder id, root, distinguished mailbox role, or accessible collaboration collection in request order with bounded properties; malformed, inaccessible, or unsupported targets return a folder error without an unscoped or partial projection | P0 |
| `MoveFolder` | Partial | `mailboxes` parent fields, change log | Canonical mailbox update/move API | Moves one custom mailbox folder under root or a custom mailbox through one canonical transaction. Public-folder reparenting and batches are rejected before mutation | P2 |
| `UpdateFolder` | Partial | `mailboxes`, `public_folders`, change log | Canonical mailbox update API; canonical public-folder update API | Updates one bounded `folder:DisplayName` only after complete request validation; protected/system folders and unsupported/batch shapes are rejected before mutation | P1 |

## Attachment Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `CreateAttachment` | Partial | `blobs`, `blob_placements`, `mime_parts`, `attachments`, `calendar_event_attachments`, Magika validation fields | Canonical attachment create API with Magika validation | One validated `FileAttachment` on one canonical message parent per request; `ItemAttachment`, reference attachments, and attachment batches are unsupported | P0 |
| `GetAttachment` | Partial | Attachment/blob/MIME/calendar attachment rows | Canonical attachment read/export API | One canonical message or calendar attachment reference per request; calendar attachment reads require current effective read rights on the referenced event and use its canonical owner content; bounded file content and metadata projection | P0 |
| `DeleteAttachment` | Partial | Attachment rows and change log | Canonical attachment delete API | One canonical message or calendar attachment reference per request; calendar attachment deletion requires current effective delete rights on the referenced event and mutates its canonical owner content; no full Exchange item attachment model | P0 |

## Reminder Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `GetReminders` | Partial | Existing reminder fields on `calendar_events`, `tasks`, and message follow-up metadata; `reminder_occurrence_dismissals` for per-occurrence dismissal/snooze state | Computed reminders API over calendar/task/message reminders | Bounded computed LPE view; no Exchange reminder table or full response-shape parity | P1 |
| `PerformReminderAction` | Partial | `reminder_occurrence_dismissals`, task/event reminder fields, message follow-up reminder dismissal state | Canonical reminder action API | Supports one bounded dismiss or per-occurrence calendar/task snooze action per request; multi-action or multi-item requests are rejected before mutation until a canonical atomic reminder-batch API exists | P1 |

## Conversation Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `ApplyConversationAction` | Partial | Existing lightweight `thread_id` and canonical message state | Canonical message move/delete/read-state APIs over current thread messages | Supports exactly one one-shot `Move`, `Delete`, or `SetReadState` action per request; multi-action and persistent future-message `Always*` shapes return parseable gaps before mutation because no canonical multi-action transaction or first-class thread lifecycle exists | P2 |
| `FindConversation` | Partial | `mailbox_messages.thread_id`, message summaries, search/query state | Canonical message grouping by lightweight thread id | Lists current conversations in one visible mailbox folder, using every current canonical membership; no Exchange conversation index or lifecycle identity | P1 |
| `GetConversationItems` | Partial | Same as `FindConversation` plus item fetch data | Canonical message fetch grouped by lightweight thread id | Returns current message nodes only; folder ignore is limited to visible canonical folder memberships | P1 |

## Utility Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `ConvertId` | Partial | No SQL; stateless opaque ids encode canonical LPE EWS family/id payloads | Canonical EWS id codec over supported LPE object families | Supports deterministic opaque alternate ids for canonical message, folder, contact, event, task, attachment, and public-folder ids; no Exchange identity table or full MAPI EntryId parity | P1 |
| `ExpandDL` | Partial | Existing canonical group aliases/members and visible directory entries | Canonical directory/group expansion API | Expands visible same-tenant public DL membership only; no recursive expansion or private Exchange DL item expansion | P2 |
| `GetUserPhoto` | Partial | New account/contact photo blob metadata if photo support is later introduced | Directory/profile photo API | Validates same-tenant directory visibility, then returns parseable no-photo because no canonical photo blob state exists | P3 |
| `MarkAsJunk` | Partial | Existing canonical mailbox/message state; no protocol-local junk list state | Canonical preflighted atomic message move to Junk; any future spam feedback must cross the LPE-CT boundary explicitly | Supports `IsJunk=true` plus `MoveItem=true` for visible canonical messages. Retries after a completed Junk move succeed without another mutation. It does not emit spam/not-junk feedback today. Exchange blocked/safe sender list and unblock-only behavior return parseable gaps | P2 |
| `ResolveNames` | Partial | Accounts, tenant directory rows, canonical group aliases, contact books/contacts and grants | Canonical address-book/contact lookup API | `ActiveDirectory` resolves one visible canonical account, distribution-list, or contact match; `Contacts` resolves only accessible contacts. Invalid, unsupported-scope, and ambiguous lookups are parseable and expose no candidates. No full GAL templates or Exchange-only distribution-list state | P0 |
| `GetPasswordExpirationDate` | Partial | Credential expiry policy/state if supported later | Account credential policy API | Authenticated account query returns parseable gap because no canonical password-expiration field exists; other-account query is denied | P3 |

## Availability Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `GetUserAvailability` | Partial | `calendars`, `calendar_events`, calendar grants | Canonical free/busy API | Returns ordered busy-interval responses for up to 20 requested mailboxes with readable canonical calendars; unreadable mailboxes receive a redacted per-mailbox error. Simple daily/weekly recurrence is expanded within a 42-day request window. No event detail or full organization availability service | P0 |
| `GetRoomLists` | Partial | Room/equipment accounts by `directory_kind`; explicit room-list grouping SQL is still absent | Directory room-list API over computed tenant room/resource projection | Returns a computed tenant room/resource list, not arbitrary Exchange room-list membership | P1 |
| `GetRooms` | Partial | Room/equipment accounts plus tenant scoping and GAL visibility | Directory rooms API | Lists visible room/equipment accounts when no selector or exactly one computed LPE room-list selector is supplied; arbitrary or multiple selectors are rejected before directory projection | P1 |
| `GetUserOofSettings` | Partial | `sieve_scripts`, `sieve_vacation_responses` | Canonical Sieve vacation projection API | OOF is projected from vacation Sieve, not Exchange OOF state | P1 |
| `SetUserOofSettings` | Partial | Same as `GetUserOofSettings` | Canonical locked compare-and-replace Sieve vacation mutation API | Updates or disables only the marker-bearing EWS-owned active vacation script after the expected active name/content is rechecked in the same transaction; an active Inbox-rule or other canonical Sieve script is preserved and returns a parseable EWS error. Scheduled/external-audience behavior remains bounded | P1 |

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
| `GetInboxRules` | Partial | `sieve_scripts` and bounded generated-rule comments | Canonical Sieve-backed mailbox rule read API | Projects only the exact authenticated-mailbox `lpe-ews-inbox-rules-v1` generated script; arbitrary Sieve, OOF, Exchange rule blobs, and client-only rules are not exposed as canonical rules | P0 |
| `UpdateInboxRules` | Partial | Same as `GetInboxRules` plus rule mutation/change rows | Canonical generated-Sieve mutation API | Atomically creates, updates, and deletes ordered subject-contains plus custom-folder move or discard rules in one generated active Sieve script; rejects an active non-generated Sieve/OOF script rather than replacing it | P0 |

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
| `FindMessageTrackingReport` | Partial | `submission_queue`, `submission_events`, `lpe_ct_transport_trace_events` | Canonical traceability API bridged to LPE-CT | Finds tracking reports without making LPE core own SMTP perimeter state | P3 |
| `GetMessageTrackingReport` | Partial | `submission_events`, `lpe_ct_transport_trace_events` | Canonical traceability API bridged to LPE-CT | Shows delivery trace from canonical submission and LPE-CT relay state | P3 |

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
| `FindPeople` | Partial | Same-tenant accounts, accessible contacts, and contact-book grants; no persona table | Stateless canonical address-book/contact projection API | Returns bounded visible account and contact personas with `persona:account:{uuid}` and `persona:contact:{uuid}` ids. No linked-contact aggregation, social sources, or Exchange persona state | P2 |
| `GetPersona` | Partial | Same as `FindPeople` | Stateless canonical address-book/contact projection API | Resolves only visible `persona:account:{uuid}` and `persona:contact:{uuid}` ids; malformed, stale, inaccessible, linked-person, and group ids return a parseable item-not-found response | P2 |

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
| `GetSharingFolder` | Partial | Contact/calendar collections and grants plus same-tenant account directory | Canonical shared folder binding API | Returns only accessible same-tenant contact/calendar folders for exactly one supported `DataType` selector; Exchange `SharedFolderId`, ungranted, cross-tenant, mailbox, and federated shapes return parseable EWS errors before a shared-folder projection | P2 |
| `GetSharingMetadata` | Partial | Owned contact/calendar collections | Canonical sharing metadata projection | Emits bounded metadata for owned contact/calendar collections only; no Exchange tokens, mailbox sharing metadata, or federation discovery | P2 |
| `RefreshSharingFolder` | Partial | Accessible contact/calendar collections and grants | Canonical shared folder visibility check | Verifies the shared contact/calendar folder is still accessible; no remote/federated refresh state | P3 |

## Synchronization Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `SyncFolderHierarchy` | Partial | `mailboxes`, collaboration collections/grants, public folders, change log, `ews_sync_cursors` | Canonical hierarchy sync projection API | Replays current canonical mailbox, accessible collaboration, and accessible public-folder hierarchy state through opaque, account- and scope-bound, 30-day durable cursor snapshots. Bounded paged continuations survive an adapter restart; the cursor table holds only derived protocol state. No full Exchange archive/voice/search hierarchy or adapter-local hierarchy store. | P0 |
| `SyncFolderItems` | Partial | Mail/collaboration/public-folder rows, `account_sync_state`, `mail_change_log`, tombstones, `ews_sync_cursors` | Canonical item sync API over current state and retained change logs | All newly issued states are opaque, account- and scope-bound, 30-day durable cursor snapshots. Only legacy empty-state reset markers remain accepted for collaboration/public folders; nonempty legacy inventories are rejected. The first mailbox page remains a bounded current-state projection; later mailbox pages and resumes replay retained canonical changes with account/folder-bound cursors. This is not full Exchange durable sync cursor semantics. | P0 |

## Time Zone Operation

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `GetServerTimeZones` | Partial | None | Explicit EWS time-zone catalog projection | `lpe-ews-time-zones-v1` exposes compact `UTC` and `Europe/Berlin` definitions only when `ReturnFullTimeZoneData=false`; no filter returns both definitions, while a supplied filter contains exactly one known case-sensitive ID. Full transition definitions and the Exchange corpus remain unsupported | P0 |

## Unified Messaging Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `DisconnectPhoneCall` | Partial | `unified_messaging_calls` | Canonical Unified Messaging call-state mutation API | Cancels active same-account canonical calls only; no PBX, dial-plan, voicemail transport, or Exchange UM policy integration | P4 |
| `GetPhoneCallInformation` | Partial | `unified_messaging_calls` | Canonical Unified Messaging call-state read API | Returns same-account canonical call state only; no Exchange UM diagnostics or telephony details | P4 |
| `PlayOnPhone` | Partial | `unified_messaging_calls`, optional canonical message ids | Canonical Unified Messaging play request API | Records a `play_on_phone` request only; real outbound call control and voicemail playback are external to EWS | P4 |

## Unified Contact Store Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `AddNewImContactToGroup` | Partial | `contact_books` with `im_contact_list` role, `contacts`, `contact_groups`, `contact_group_members` | Canonical contact create and IM group membership API | Creates a canonical contact in the IM list and links it to a canonical IM group | P4 |
| `AddImContactToGroup` | Partial | `contacts`, `accounts`, `contact_groups`, `contact_group_members` | Canonical IM group membership API over visible contacts/accounts | Links visible canonical contacts/accounts to canonical IM groups | P4 |
| `AddImGroup` | Partial | `contact_books` with `im_contact_list` role, `contact_groups` | Canonical IM group API | Creates canonical IM groups without Exchange UCS-only state | P4 |
| `AddNewTelUriContactToGroup` | Partial | `contact_groups`, `contact_group_members` | Canonical IM group membership API for tel URI external members | Adds tel URI members to canonical IM groups | P4 |
| `AddDistributionGroupToImList` | Partial | Aliases/directory projection, `contact_groups`, `contact_group_members` | Canonical address-book distribution-list projection and IM group membership API | Adds only visible same-tenant distribution-list addresses to canonical IM groups | P4 |
| `GetImItemList` | Partial | `contact_books` with `im_contact_list` role, `contact_groups`, `contact_group_members` | Canonical IM list projection API | Lists canonical IM groups and members | P4 |
| `GetImItems` | Partial | `contacts`, `accounts`, `contact_groups`, `contact_group_members` | Canonical IM member projection API | Reads canonical IM contacts, accounts, tel URIs, and distribution-list members | P4 |
| `RemoveContactFromImList` | Partial | `contact_group_members` | Canonical IM membership delete API | Removes contact/account membership from canonical IM groups | P4 |
| `RemoveImContactFromGroup` | Partial | `contact_group_members` | Canonical IM membership delete API | Removes contact/account membership from one canonical IM group | P4 |
| `RemoveDistributionGroupFromImList` | Partial | `contact_group_members` | Canonical IM membership delete API | Removes canonical distribution-list membership rows | P4 |
| `RemoveImGroup` | Partial | `contact_groups`, `contact_group_members` | Canonical IM group delete API | Deletes canonical IM groups and memberships | P4 |
| `SetImGroup` | Partial | `contact_groups` | Canonical IM group update API | Updates canonical IM group display names | P4 |

## User Configuration Operations

| Operation | LPE status | Required SQL data | Required canonical LPE API/storage integration | Client-visible differences from Exchange | Priority |
| --- | --- | --- | --- | --- | --- |
| `CreateUserConfiguration` | Partial | `account_client_configurations` keyed by account, optional mailbox/public-folder scope, config class, and config name | Canonical user/client configuration API with strict scope limits | Stores bounded dictionary, XML, and binary payloads only after the selected mailbox is visible or public folder has current read access; denied scopes return a parseable EWS access error before configuration or audit writes. No Exchange arbitrary user-configuration store is introduced | P1 |
| `DeleteUserConfiguration` | Partial | Same as `CreateUserConfiguration` | Canonical user/client configuration API | Revalidates the selected mailbox/public-folder scope before deleting canonical user configuration blobs; missing rows return EWS item-not-found errors | P1 |
| `GetUserConfiguration` | Partial | Same as `CreateUserConfiguration` | Canonical user/client configuration API | Revalidates the selected mailbox/public-folder scope before returning bounded dictionary, XML, and binary payloads from canonical storage | P1 |
| `UpdateUserConfiguration` | Partial | Same as `CreateUserConfiguration` plus audit/modseq update | Canonical user/client configuration API | Revalidates the selected mailbox/public-folder scope before replacing bounded canonical payloads and advancing canonical modseq | P1 |

## Priority Summary

| Priority | Operations |
| --- | --- |
| P0 | `CreateItem`, `DeleteItem`, `FindItem`, `GetItem`, `SendItem`, `UpdateItem`, `CreateFolder`, `DeleteFolder`, `FindFolder`, `GetFolder`, `CreateAttachment`, `GetAttachment`, `DeleteAttachment`, `ResolveNames`, `GetUserAvailability`, `GetInboxRules`, `UpdateInboxRules`, `GetEvents`, `Subscribe`, `Unsubscribe`, `SyncFolderHierarchy`, `SyncFolderItems`, `GetServerTimeZones` |
| P1 | `CopyItem`, `MarkAllItemsAsRead`, `MoveItem`, `EmptyFolder`, `UpdateFolder`, `GetReminders`, `PerformReminderAction`, `FindConversation`, `GetConversationItems`, `ConvertId`, `GetRoomLists`, `GetRooms`, `GetUserOofSettings`, `SetUserOofSettings`, `AddDelegate`, `GetDelegate`, `UpdateDelegate`, `RemoveDelegate`, `GetMailTips`, `GetStreamingEvents`, `CreateUserConfiguration`, `DeleteUserConfiguration`, `GetUserConfiguration`, `UpdateUserConfiguration` |
| P2 | `ArchiveItem`, `CreateFolderPath`, `CopyFolder`, `MoveFolder`, `ApplyConversationAction`, `ExpandDL`, `MarkAsJunk`, `UploadItems`, `ExportItems`, `FindPeople`, `GetPersona`, `GetUserRetentionPolicyTags`, `GetServiceConfiguration`, `GetSharingFolder`, `GetSharingMetadata` |
| P3 | `GetUserPhoto`, `GetPasswordExpirationDate`, `DisableApp`, `GetAppManifests`, `GetClientAccessToken`, `InstallApp`, `UninstallApp`, `FindMessageTrackingReport`, `GetMessageTrackingReport`, `RefreshSharingFolder` |
| P4 | `GetDiscoverySearchConfiguration`, `GetHoldOnMailboxes`, `GetNonIndexableItemDetails`, `GetNonIndexableItemStatistics`, `GetSearchableMailboxes`, `SearchMailboxes`, `SetHoldOnMailboxes`, `CreateManagedFolder`, `GetAppMarketplaceUrl`, `DisconnectPhoneCall`, `GetPhoneCallInformation`, `PlayOnPhone`, Unified Contact Store operations |

## Main Parity Gaps For Outlook And Native Clients

1. All 96 catalog operation names have bounded behavioral SOAP handlers. There are no operation-level explicit unsupported entries.
2. Every operation remains `Partial` because it exposes bounded canonical LPE behavior rather than full Exchange storage, rule, room-list, reminder, notification, mail app, Unified Messaging, user-configuration, sync, or identity semantics.
3. The highest-value parity work is to deepen real Outlook/native-client paths: canonical submission, rules, reminders, rooms, notification replay, and durable user configuration.
4. Full Exchange parity would require first-class canonical models where justified: archive mailbox semantics, linked persona aggregation, LPE/LPE-CT tracking reports, UCS IM groups, durable Exchange-equivalent sync/notification semantics, Exchange identity compatibility, and any Outlook-proven mail-tip or policy-tip fields beyond the bounded current surface.
