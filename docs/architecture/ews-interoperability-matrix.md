# EWS Interoperability Matrix

## Current State/Functionality Overview

This matrix compares `LPE` against Microsoft's EWS operation catalog:

- <https://learn.microsoft.com/en-us/exchange/client-developer/web-service-reference/ews-operations-in-exchange>
- <https://learn.microsoft.com/en-us/exchange/client-developer/web-service-reference/ews-xml-elements-in-exchange>

`LPE` does not implement complete Microsoft Exchange Web Services. The current `lpe-exchange` EWS adapter is bounded compatibility over canonical `LPE` mailbox, contacts, calendar, task, public-folder, attachment, submission, OOF, availability, and pull-notification state.

Status meanings:

- `implemented`: complete enough for the Microsoft operation contract, with no known LPE-specific behavioral gap.
- `partial`: dispatched and useful, but bounded compared with Exchange.
- `unsupported`: explicitly recognized by the dispatcher and returned as a parseable EWS unsupported response.
- `missing`: listed by Microsoft but not implemented as an LPE EWS operation.

No current EWS operation is marked `implemented` against full Microsoft parity. Supported LPE operations are marked `partial` because they intentionally expose bounded canonical behavior rather than Exchange's full storage, identity, policy, and sync model.

## Implementation/Usage

- EWS routes are `POST /EWS/Exchange.asmx` and `POST /ews/exchange.asmx`.
- Authentication uses canonical mailbox account authentication.
- Supported writes must mutate canonical LPE state only.
- Message submission must use canonical submission and authoritative `Sent`; EWS must not introduce client `SMTP`, an EWS `Outbox`, or an EWS-only `Sent` copy.
- Unsupported or missing operations must return parseable SOAP/EWS errors rather than generic transport failures.
- EWS ids are currently LPE-prefixed canonical ids such as `message:{uuid}`, `contact:{uuid}`, `event:{uuid}`, `task:{uuid}`, `public-folder:{uuid}`, and `public-folder-item:{uuid}`.

## Full Microsoft EWS Operation Matrix

For `partial` and `missing` rows, the planning columns identify the canonical LPE API, SQL state, tests, docs, and client-visible behavior required to close the parity gap. `N/A` means the operation is intentionally unsupported and should remain a parseable unsupported response unless product scope changes.

| Area | Operation | Status | Required canonical LPE API | Required SQL state | Required tests | Required docs | Client-visible behavior |
| --- | --- | --- | --- | --- | --- | --- | --- |
| eDiscovery | `GetDiscoverySearchConfiguration` | missing | Compliance discovery API for saved searches and hold-scoped sources | eDiscovery search config, legal hold scope, audit rows | EWS config read, tenant scoping, permission denial | eDiscovery architecture and admin docs | Authorized compliance clients see discovery searches; normal users get EWS access errors |
| eDiscovery | `GetHoldOnMailboxes` | missing | Compliance hold API | mailbox hold assignments, hold policy metadata, audit rows | get hold state, cross-tenant denial | data lifecycle and eDiscovery docs | Reports mailbox hold state only to authorized roles |
| eDiscovery | `GetNonIndexableItemDetails` | missing | Search diagnostics API | non-indexable item reports linked to messages and attachments | non-indexable detail pagination, Bcc protection | search and compliance docs | Returns diagnostic item details without exposing protected `Bcc` |
| eDiscovery | `GetNonIndexableItemStatistics` | missing | Search diagnostics API | non-indexable counters per mailbox/search | statistics aggregation tests | search and compliance docs | Returns mailbox statistics for discovery tooling |
| eDiscovery | `GetSearchableMailboxes` | missing | Compliance mailbox discovery API | searchable mailbox projection over accounts/domains/grants | role filter, tenant isolation | eDiscovery admin docs | Lists only authorized searchable mailboxes |
| eDiscovery | `SearchMailboxes` | missing | Compliance search API over canonical mail search | discovery search jobs, result sets, hold-safe audit rows | query execution, export safety, protected metadata exclusion | search and compliance docs | Produces scoped discovery results without user-facing AI or `Bcc` leakage |
| eDiscovery | `SetHoldOnMailboxes` | missing | Compliance hold mutation API | mailbox hold assignments, retention/hold audit rows | add/remove hold, idempotency, permission denial | data lifecycle and install/update docs if schema changes | Applies legal hold through canonical lifecycle state |
| Mailbox item | `ArchiveItem` | missing | Archive mailbox move API | archive mailbox role/state, mailbox memberships, change log | archive move, sync replay, permission tests | mailbox lifecycle docs | Moves items to canonical archive when archive support exists |
| Mailbox item | `CreateItem` | partial | Existing contact/event/task/mail/public-folder create plus full item-class mapper | messages, contacts, calendar_events, tasks, public_folder_items, attachments, submission_queue | item-class matrix, send dispositions, recurrence, attachment, Bcc-safe submission | EWS item-class mapping docs | Creates only canonical supported items today; unsupported item classes fail |
| Mailbox item | `CopyItem` | partial | Existing copy API plus full item class and folder coverage | mailbox_messages, messages, public_folder_items, change log | cross-folder copy, public/private guards, attachment preservation | EWS mailbox operation docs | Copies supported messages and public-folder posts only |
| Mailbox item | `DeleteItem` | partial | Existing delete API plus complete delete-type/recoverable behavior | mailbox_messages, tombstones, recoverable_items, public_folder_items, change log | soft, hard, move-to-trash, recoverable and public-folder delete tests | delete/recovery docs | Maps deletion to canonical Trash, hard delete, or public-folder deletion |
| Mailbox item | `FindItem` | partial | Existing item query API plus Exchange views, restrictions, sorts, all item classes | messages/search, contacts, calendar_events, tasks, public_folder_items | restriction/sort/view tests, Bcc exclusion | EWS query docs | Finds bounded canonical mail, contacts, events, tasks, and public-folder posts |
| Mailbox item | `GetItem` | partial | Existing item read API plus full property-shape and item-class mapper | canonical item tables, MIME, attachments, custom properties if added | property shape, body, MIME, attachment, unsupported id tests | EWS property mapping docs | Reads LPE-prefixed canonical ids only |
| Mailbox item | `MarkAllItemsAsRead` | missing | Mailbox bulk read-state API | mailbox_messages read state, mail_change_log | bulk read/unread, modseq, IMAP/JMAP convergence | mailbox read-state docs | Marks all visible items in a folder read or unread |
| Mailbox item | `MoveItem` | partial | Existing move API plus full item-class and recoverable/public-folder semantics | mailbox_messages, tombstones, public_folder_items, change log | move, copy-delete public-folder behavior, sync replay | EWS mailbox operation docs | Moves canonical messages and bounded public-folder posts only |
| Mailbox item | `SendItem` | missing | Draft submit API over canonical draft ids | messages, mailbox_messages, submission_queue, submission_recipients, Sent copy | send saved draft, Bcc protection, Sent visibility, retry/idempotency | submission docs and EWS docs | Sends an existing canonical draft through LPE submission |
| Mailbox item | `UpdateItem` | partial | Existing update API plus full property and item-class mutation | canonical item tables, custom property table if widened, change log | property mutation matrix, recurrence, categories, read/flag tests | EWS property/update docs | Updates bounded contact, event, task, message read/flag, and public-folder post fields |
| Folder | `CreateFolder` | partial | Existing mailbox/public-folder create plus full folder-class support | mailboxes, public_folders, change log | protected folder, public-folder ACL, duplicate name tests | folder mapping docs | Creates custom mail folders and child public folders only |
| Folder | `CreateFolderPath` | missing | Atomic folder-path creation API | mailboxes/public_folders with parent chain, change log | nested creation, rollback, duplicate segments | folder hierarchy docs | Creates a path of canonical folders or fails without partial client surprise |
| Folder | `CreateManagedFolder` | missing | Managed-folder or retention-policy API | managed/retention folder metadata if in scope | managed-folder creation and policy tests | retention/managed-folder docs | Currently should remain unavailable unless managed folders are scoped |
| Folder | `CopyFolder` | missing | Folder copy API for custom folders | mailboxes, messages memberships, public folder tree if in scope, change log | recursive copy, system-folder rejection, sync replay | folder operation docs | Copies user-created folder trees when canonical semantics exist |
| Folder | `DeleteFolder` | partial | Existing delete API plus complete recoverable/system semantics | mailboxes, public_folders, tombstones, recoverable_items, change log | protected folder, child handling, public-folder permissions | delete/recovery docs | Deletes only canonical deletable mailbox or public folders |
| Folder | `EmptyFolder` | missing | Folder purge API | mailbox_messages, tombstones, recoverable_items, public_folder_items if in scope | soft/hard purge, retention/hold guards, partial completion | recovery lifecycle docs | Empties a folder through canonical retention rules |
| Folder | `FindFolder` | partial | Existing folder discovery plus full distinguished/archive/search/public folder projection | mailboxes, search_folders, public_folders | shape, paging, search-folder, archive tests | folder hierarchy docs | Projects canonical folders and bounded public-folder roots |
| Folder | `GetFolder` | partial | Existing folder read plus full property-shape mapping | mailboxes, public_folders, search_folders | distinguished id, shape, permission tests | folder property docs | Reads selected distinguished, mailbox, and public-folder ids |
| Folder | `MoveFolder` | missing | Folder move API for user-created folders | mailboxes/public_folders parent fields, change log | cycle rejection, system-folder rejection, sync replay | folder hierarchy docs | Moves canonical user-created folders only |
| Folder | `UpdateFolder` | missing | Folder rename/settings API | mailboxes, public_folders, search_folders if scoped, change log | rename, protected property rejection, sync replay | folder property docs | Updates supported canonical folder properties |
| Attachment | `CreateAttachment` | partial | Existing attachment create plus item attachments if scoped | blobs, blob_placements, attachments, mime_parts, calendar_event_attachments | Magika validation, file attachment, item attachment rejection/coverage | attachment docs and EWS docs | Adds validated file attachments to supported message parents |
| Attachment | `GetAttachment` | partial | Existing attachment fetch plus full attachment-shape support | blobs, blob_placements, attachments, mime_parts | content, MIME, permission, missing blob tests | attachment docs | Fetches bounded canonical attachment content |
| Attachment | `DeleteAttachment` | partial | Existing attachment delete plus all supported parent kinds | attachments, blob retention metadata, change log | delete, retention, parent kind tests | attachment docs | Deletes supported canonical attachments |
| Reminder | `GetReminders` | missing | Computed reminders API | reminder_occurrence_dismissals plus calendar_events and tasks reminder fields | due window, dismissed, cancelled/completed exclusion | reminders docs | Returns active reminders for canonical events and tasks |
| Reminder | `PerformReminderAction` | missing | Reminder dismissal/snooze API | reminder_occurrence_dismissals, task/event reminder metadata | dismiss, snooze if scoped, recurrence instance tests | reminders docs | Dismisses or snoozes canonical reminders |
| Conversation | `ApplyConversationAction` | missing | Conversation action API | lightweight thread ids or future threads table, messages, mail_change_log | move/delete/read/category action tests | conversation/thread docs | Applies supported conversation-wide actions consistently |
| Conversation | `FindConversation` | missing | Conversation query API | message thread identifiers or threads table, search summaries | grouping, paging, folder scope tests | conversation docs | Lists conversations derived from canonical messages |
| Conversation | `GetConversationItems` | missing | Conversation item expansion API | messages, mailbox_messages, thread state | expansion, Bcc-safe participants, permissions | conversation docs | Returns items in a canonical conversation |
| Utilities | `ConvertId` | missing | EWS id conversion API | durable EWS/MAPI id mapping if opaque ids are introduced | id round-trip, invalid id, cross-tenant denial | identity mapping docs | Converts between supported id formats without exposing foreign objects |
| Utilities | `ExpandDL` | unsupported | N/A | N/A | Unsupported-response test should remain | EWS unsupported list | Returns parseable unsupported response |
| Utilities | `GetUserPhoto` | missing | Profile photo API | account profile photo/blob metadata | size, permission, absent photo tests | identity/profile docs | Returns mailbox/contact photos when canonical profile photos exist |
| Utilities | `MarkAsJunk` | missing | User junk classification API routed to LPE-CT policy where appropriate | mailbox move state plus perimeter feedback/audit if scoped | junk/not-junk, LPE-CT boundary tests | mail security and EWS docs | Moves mail and records feedback without moving SMTP filtering into LPE core |
| Utilities | `ResolveNames` | partial | Existing address-book/contact resolution plus full GAL/persona templates | accounts, contacts, groups, grants | ambiguous ranking, DL/contact/account, hidden entries | address book docs | Resolves authenticated account, tenant directory, and accessible contacts |
| Utilities | `GetPasswordExpirationDate` | missing | Account credential policy API | password policy and credential expiry metadata | local password, external auth, no-expiry tests | auth/admin docs | Returns password expiry only for credential types with canonical expiry |
| Availability | `GetUserAvailability` | partial | Existing free/busy API plus attendee scope and richer suggestions | calendar_events, calendar_grants, delegation/free-busy state | free/busy visibility, cross-tenant denial, suggestion tests | calendar availability docs | Returns bounded free/busy for accessible calendar state |
| Availability | `GetRoomLists` | unsupported | N/A | N/A | Unsupported-response test should remain until room-list API exists | EWS unsupported list | Returns parseable unsupported response |
| Availability | `GetRooms` | missing | Room directory API | account directory kind, room-list membership if added | room list filtering, hidden room tests | room/equipment docs | Lists room mailboxes from canonical directory state |
| Availability | `GetUserOofSettings` | partial | Existing vacation settings projection | sieve_scripts, sieve_vacation_responses | disabled/enabled/scheduled round-trip tests | OOF/Sieve docs | Projects canonical Sieve vacation as EWS OOF |
| Availability | `SetUserOofSettings` | partial | Existing vacation settings mutation | sieve_scripts, sieve_vacation_responses, audit | scheduled/external audience/idempotency tests | OOF/Sieve docs | Updates canonical vacation state, not an EWS-local OOF table |
| Bulk transfer | `UploadItems` | missing | Bulk import API over canonical MIME/items | messages, MIME, attachments, contacts/events/tasks if scoped, import audit | MIME import, malformed payload, idempotency tests | import/export docs | Imports streamed items only after canonical validation |
| Bulk transfer | `ExportItems` | missing | Bulk export API over canonical items | messages, MIME/blob reconstruction, contacts/events/tasks if scoped | export fidelity, attachment reconstruction, Bcc protection | import/export docs | Exports supported canonical items without leaking protected metadata |
| Delegate | `AddDelegate` | missing | Delegation mutation API | mailbox_delegation_grants, calendar_grants, sender_rights, audit | add delegate, send-on-behalf, permission denial | delegation docs | Adds canonical delegate rights only |
| Delegate | `GetDelegate` | unsupported | N/A | N/A | Unsupported-response test should remain until delegate EWS model exists | EWS unsupported list | Returns parseable unsupported response |
| Delegate | `UpdateDelegate` | missing | Delegation mutation API | mailbox_delegation_grants, calendar_grants, sender_rights, audit | update delegate rights, convergence with MAPI | delegation docs | Updates canonical delegate rights |
| Delegate | `RemoveDelegate` | missing | Delegation mutation API | mailbox_delegation_grants, calendar_grants, sender_rights, audit | remove delegate, stale access denial | delegation docs | Removes canonical delegate rights |
| Inbox rules | `GetInboxRules` | missing | Rules API over Sieve-backed mailbox rules | sieve_scripts plus rule projection metadata | get bounded rules, unsupported blobs | Sieve/rules docs | Returns Outlook-compatible projection of canonical rules |
| Inbox rules | `UpdateInboxRules` | missing | Rules mutation API over Sieve-backed mailbox rules | sieve_scripts, change log, audit | create/update/delete bounded rules, unsupported action rejection | Sieve/rules docs | Mutates only rules that map to canonical Sieve behavior |
| Mail app | `DisableApp` | missing | Outlook add-in management API if scoped | mail app catalog/install state | disable app, tenant/user scope tests | mail app docs | Disables an installed app if app management is in scope |
| Mail app | `GetAppManifests` | missing | Outlook add-in catalog API if scoped | app manifests, tenant policy, user install state | manifest listing and policy tests | mail app docs | Returns available add-in manifests |
| Mail app | `GetAppMarketplaceUrl` | missing | Add-in marketplace policy API if scoped | tenant app marketplace policy | configured URL and disabled tests | mail app docs | Returns configured marketplace URL or unsupported |
| Mail app | `GetClientAccessToken` | missing | Add-in token issuance API if scoped | app registrations, consent, token audit | token scope, expiry, denial tests | mail app/security docs | Issues scoped app tokens only for trusted add-ins |
| Mail app | `InstallApp` | missing | Add-in install API if scoped | user/tenant app installs | install, duplicate, policy denial tests | mail app docs | Installs an approved app |
| Mail app | `UninstallApp` | missing | Add-in uninstall API if scoped | user/tenant app installs | uninstall and stale token tests | mail app docs | Removes an installed app |
| Mail tips | `GetMailTips` | missing | Recipient policy/tips API | accounts, groups, OOF, moderation/quota policy if scoped | OOF, invalid recipient, large audience tests | mail tips docs | Returns recipient compose warnings from canonical policy |
| Message tracking | `FindMessageTrackingReport` | missing | Traceability API bridged to LPE-CT | submission_queue, submission_events, LPE-CT trace/bounce data | submitted message lookup, authz, CT boundary tests | LPE-CT traceability docs | Finds tracking reports without making LPE core own SMTP perimeter state |
| Message tracking | `GetMessageTrackingReport` | missing | Traceability API bridged to LPE-CT | submission_events plus LPE-CT delivery trace | delivery detail, DSN, permission tests | LPE-CT traceability docs | Shows delivery trace from canonical submission and CT relay state |
| Notification | `GetEvents` | partial | Existing pull event API plus durable subscription replay | current in-process registry; needs durable subscriptions and mail_change_log cursor linkage | replay, expiry, cross-process, folder event tests | notification docs | Returns queued/fallback pull events today |
| Notification | `GetStreamingEvents` | missing | Streaming notification API | durable/session notification cursors, mail_change_log replay | long-poll/stream, reconnect, timeout tests | notification docs | Streams item/folder events for supported subscriptions |
| Notification | `Subscribe` | partial | Existing pull subscribe plus push/streaming and durable state | current in-process registry; needs durable subscriptions | pull/push/stream request tests, watermark tests | notification docs | Accepts bounded pull subscriptions only |
| Notification | `Unsubscribe` | partial | Existing unsubscribe plus durable lifecycle cleanup | current in-process registry; needs durable subscription rows | unsubscribe, missing id, expiry tests | notification docs | Removes bounded in-process subscriptions |
| Persona | `FindPeople` | unsupported | N/A | N/A | Unsupported-response test should remain until persona API exists | EWS unsupported list | Returns parseable unsupported response |
| Persona | `GetPersona` | missing | Persona/contact aggregation API | contacts, accounts, possible linked persona rows | aggregation, privacy, tenant scope tests | persona/address-book docs | Returns linked-person details if canonical persona support exists |
| Retention | `GetUserRetentionPolicyTags` | missing | Retention policy API | retention policy tags, mailbox policy assignments | tag listing and no-policy tests | retention docs | Returns tags only after canonical retention policy tags exist |
| Service configuration | `GetServiceConfiguration` | missing | Service configuration API | tenant feature flags and policy state | mail tips/protection/UM config tests | service configuration docs | Returns only actually implemented service config |
| Sharing | `CreateItem` with `AcceptSharingInvitation` | missing | Sharing invitation accept API | sharing invitations, contact/calendar grants, audit | accept, decline, expired token, permission tests | sharing docs | Accepts supported calendar/contact shares through canonical grants |
| Sharing | `GetSharingFolder` | unsupported | N/A | N/A | Unsupported-response test should remain until sharing API exists | EWS unsupported list | Returns parseable unsupported response |
| Sharing | `GetSharingMetadata` | unsupported | N/A | N/A | Unsupported-response test should remain until sharing API exists | EWS unsupported list | Returns parseable unsupported response |
| Sharing | `RefreshSharingFolder` | missing | Sharing sync refresh API | sharing grants, remote share metadata if scoped | refresh, revoked share, stale data tests | sharing docs | Refreshes canonical shared folder metadata |
| Synchronization | `SyncFolderHierarchy` | partial | Existing hierarchy projection plus durable Exchange-like sync cursors | mailboxes, contact_books, calendars, task_lists, public_folders, change log | incremental hierarchy, deletes, cursor retention tests | sync docs | Emits bounded current hierarchy changes with synthetic state |
| Synchronization | `SyncFolderItems` | partial | Existing item projection plus durable item sync cursors | canonical item tables, mail_change_log, tombstones | incremental creates/updates/deletes, cursor expiry tests | sync docs | Emits bounded current item projections with synthetic state |
| Time zone | `GetServerTimeZones` | partial | Static response today; full timezone catalog API if parity required | timezone catalog table or generated data source if adopted | full catalog and version tests | timezone docs | Returns a small static compatibility set today |
| Unified Messaging | `DisconnectPhoneCall` | missing | Unified Messaging API if scoped | UM call/session state | disconnect call tests | UM docs | Unsupported until voicemail/UM is in scope |
| Unified Messaging | `GetPhoneCallInformation` | missing | Unified Messaging API if scoped | UM call/session state | call info tests | UM docs | Unsupported until voicemail/UM is in scope |
| Unified Messaging | `PlayOnPhone` | missing | Unified Messaging API if scoped | voicemail and phone-call state | play request and auth tests | UM docs | Unsupported until voicemail/UM is in scope |
| Unified Contact Store | `AddNewImContactToGroup` | missing | IM contact API if scoped | IM contacts/groups, contact links | add new IM contact tests | UCS docs | Unsupported until IM contact store is in scope |
| Unified Contact Store | `AddImContactToGroup` | missing | IM contact API if scoped | IM contacts/groups | add existing contact tests | UCS docs | Unsupported until IM contact store is in scope |
| Unified Contact Store | `AddImGroup` | missing | IM group API if scoped | IM groups | add group tests | UCS docs | Unsupported until IM contact store is in scope |
| Unified Contact Store | `AddNewTelUriContactToGroup` | missing | IM contact API if scoped | tel URI contacts/groups | add tel URI tests | UCS docs | Unsupported until IM contact store is in scope |
| Unified Contact Store | `AddDistributionGroupToImList` | missing | IM list and group directory API if scoped | directory groups, IM list links | add distribution group tests | UCS docs | Unsupported until IM contact store is in scope |
| Unified Contact Store | `GetImItemList` | missing | IM list API if scoped | IM contacts/groups/list state | list retrieval tests | UCS docs | Unsupported until IM contact store is in scope |
| Unified Contact Store | `GetImItems` | missing | IM contact API if scoped | IM contacts/groups | item retrieval tests | UCS docs | Unsupported until IM contact store is in scope |
| Unified Contact Store | `RemoveContactFromImList` | missing | IM contact API if scoped | IM contact list links | remove contact tests | UCS docs | Unsupported until IM contact store is in scope |
| Unified Contact Store | `RemoveImContactFromGroup` | missing | IM contact API if scoped | IM contact group links | remove from group tests | UCS docs | Unsupported until IM contact store is in scope |
| Unified Contact Store | `RemoveDistributionGroupFromImList` | missing | IM list API if scoped | directory group IM links | remove distribution group tests | UCS docs | Unsupported until IM contact store is in scope |
| Unified Contact Store | `RemoveImGroup` | missing | IM group API if scoped | IM groups and memberships | remove group tests | UCS docs | Unsupported until IM contact store is in scope |
| Unified Contact Store | `SetImGroup` | missing | IM group API if scoped | IM groups | rename/update group tests | UCS docs | Unsupported until IM contact store is in scope |
| User configuration | `CreateUserConfiguration` | missing | User configuration blob API if Outlook requires it | user configuration blobs keyed by account/folder/name | create blob, quota, folder scope tests | user configuration docs | Stores bounded user config only if canonical state is approved |
| User configuration | `DeleteUserConfiguration` | missing | User configuration blob API if Outlook requires it | user configuration blobs | delete and missing-row tests | user configuration docs | Deletes canonical user config blobs |
| User configuration | `GetUserConfiguration` | unsupported | N/A | N/A | Unsupported-response test should remain until user config API exists | EWS unsupported list | Returns parseable unsupported response |
| User configuration | `UpdateUserConfiguration` | missing | User configuration blob API if Outlook requires it | user configuration blobs, change/audit if scoped | update, concurrency, quota tests | user configuration docs | Updates canonical user config blobs |

## Current Partial Dispatcher Surface

The current EWS dispatcher in `crates/lpe-exchange/src/service.rs` routes these operations to concrete handlers:

`SyncFolderHierarchy`, `FindFolder`, `GetFolder`, `FindItem`, `GetItem`, `SyncFolderItems`, `GetServerTimeZones`, `ResolveNames`, `GetUserAvailability`, `CreateItem`, `UpdateItem`, `DeleteItem`, `MoveItem`, `CopyItem`, `CreateFolder`, `DeleteFolder`, `GetAttachment`, `CreateAttachment`, `DeleteAttachment`, `GetUserOofSettings`, `SetUserOofSettings`, `Subscribe`, `GetEvents`, and `Unsubscribe`.

These are all `partial` against Microsoft parity because LPE intentionally maps only bounded Exchange-compatible behavior to canonical state.

## Explicit Unsupported Dispatcher Surface

The dispatcher explicitly returns EWS-shaped unsupported responses for:

`GetRoomLists`, `FindPeople`, `ExpandDL`, `GetDelegate`, `GetUserConfiguration`, `GetSharingMetadata`, and `GetSharingFolder`.

All other Microsoft catalog operations currently fall through to the generic unsupported response and are marked `missing` in the matrix.

## Highest-Value EWS Parity Work

Priority should favor practical Outlook and native-client behavior before broad Exchange feature emulation:

1. `SendItem` over canonical drafts and submission.
2. `GetInboxRules` and `UpdateInboxRules` over Sieve-backed rules.
3. `GetReminders` and `PerformReminderAction` over computed reminder state.
4. `GetRooms` and then `GetRoomLists` from room/equipment directory state.
5. Durable notification subscriptions and `GetStreamingEvents`.
6. Durable user-configuration blobs only if real Outlook/EWS clients require them.

## Verification

This is a planning matrix only. No code was implemented for this matrix.
