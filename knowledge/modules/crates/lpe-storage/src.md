---
type: Rust Module
title: src
resource: crates/lpe-storage/src/lib.rs#L1-L156
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/pub-use-crate-activesync-activesyncattachment-activesyncattachmentcontent-activesyncdevicestate-activesyncitemstate-activesyncsyncstate
  - external/pub-use-crate-attachments-calendar-attachment-file-reference-parse-calendar-attachment-file-reference-calendareventattachment-clientattachment-mapieventattachmentchanges-mapieventattachmentupsert
  - external/pub-use-crate-auth-accountapppassword-accountauthfactor-accountcredentialinput-accountlogin-accountoidcclaims-adminauthfactor-admincredentialinput-adminlogin-adminoidcclaims-authenticatedaccount-authenticatedadmin-newaccountauthfactor-newadminauthfactor-storedaccountapppassword
  - external/pub-use-crate-calendar-calendar-attendee-labels-calendar-participant-label-normalize-calendar-email-normalize-calendar-participation-status-parse-calendar-participants-metadata-serialize-calendar-participants-metadata-calendarorganizermetadata-calendarparticipantmetadata-calendarparticipantsmetadata
  - external/pub-use-crate-change-canonicalchangecategory-canonicalchangelistener-canonicalchangereplay-canonicalpushchangeset
  - external/pub-use-crate-collaboration-accessiblecontact-accessibleevent-collaborationcollection-collaborationgrant-collaborationgrantinput-collaborationresourcekind-collaborationrights-contactnamefields-contactsourcefields-delegateaccessobject-delegatefreebusymessageobject-freebusyblock-mapieventidentitymove-mapieventimportedmoveidentity-moveaccessibleeventtodeleteditemsresult
  - external/pub-use-crate-conversation-actions-conversationaction-upsertconversationactioninput-conversation-action-version
  - external/pub-use-crate-core-storage
  - external/pub-use-crate-imap-imapemail-imapmailboxstate-imapmimepart
  - external/pub-use-crate-jmap-blobs-jmapquota-jmapuploadblob
  - external/pub-use-crate-jmap-queries-jmapemailquery-jmapstoredquerystate-jmapthreadquery
  - external/pub-use-crate-mailboxes-jmapmailbox-jmapmailboxcreateinput-jmapmailboxupdateinput-managedretentionfoldercreateinput
  - external/pub-use-crate-mapi-contacts-mapicontactcommitinput-mapicontactcommitoutcome-mapicontactcommitresult-mapicontactcreateinput-mapicontactcreateresult-mapicontactcustompropertyvalue-mapicontactimportconflict-mapicontactimportdisposition-mapicontactimportobjectdeleted-mapicontactimportedidentity-mapicontactversion
  - external/pub-use-crate-mapi-events-mapieventcommitinput-mapieventcommitoutcome-mapieventcommitsuccess-mapieventcreateinput-mapieventcreateresult-mapieventcustompropertyvalue-mapieventimportedidentity-mapieventreminderpatch-mapieventreminderstate-mapieventversion
  - external/pub-use-crate-mapi-store-identity-mapimessageidentitymove-mapimessageimportedmoveidentity-mapimessagemoveresult-mapistoreidentity
  - external/pub-use-crate-notes-journal-clientnote-clientreminder-journalentry-reminderquery-upsertclientnoteinput-upsertjournalentryinput
  - external/pub-use-crate-protocols-jmapemail-jmapemailaddress-jmapemailfollowupupdate-jmapemailmailboxstate-jmapemailsubmission-jmapimportedemailinput-jmapmailobjectchange-jmapstringobjectchange
  - external/pub-use-crate-pst-newpsttransferjob-pstjobexecutionsummary-psttransferjobrecord
  - external/pub-use-crate-public-folders-createpublicfolderinput-createpublicfoldertreeinput-publicfolder-publicfolderitem-publicfolderperuserstate-publicfolderperuserstatepatch-publicfolderpermission-publicfolderpermissioninput-publicfolderreplica-publicfolderreplicainput-publicfolderrights-publicfoldertree-updatepublicfolderinput-upsertpublicfolderiteminput
  - external/pub-use-crate-recoverable-items-recoverableitem
  - external/pub-use-crate-search-folders-searchfolderdefinition-upsertsearchfolderinput
  - external/pub-use-crate-submission-attachmentuploadinput-cancelsubmissionresult-mailboxaccountaccess-mailboxdelegationgrant-mailboxdelegationgrantinput-mailboxdelegationoverview-mailboxfolderdelegationgrantinput-saveddraftmessage-senderauthorizationkind-senderdelegationgrant-senderdelegationgrantinput-senderdelegationright-senderidentity-submissionaccountidentity-submitmessageinput-submittedmessage-submittedrecipientinput
  - external/pub-use-crate-tasks-clienttask-clienttasklist-createtasklistinput-davtask-tasklistgrant-tasklistgrantinput-updatetasklistinput-upsertclienttaskinput
  - external/pub-use-crate-types-accountrecord-admindashboard-aliasrecord-antispamsettings-auditentryinput-auditevent-dashboardupdate-domainrecord-emailtraceresult-emailtracesearchinput-filterrule-healthresponse-localaisettings-mailflowentry-mailboxrecord-mailboxrule-newaccount-newalias-newdomain-newfilterrule-newmailbox-newserveradministrator-newstoragepool-outboundqueuestatusupdate-outlookprofilestate-overviewstats-protocolstatus-quarantineitem-securitysettings-serveradministrator-serversettings-sievescriptdocument-sievescriptsummary-storagecleanupcounts-storagecleanupplacementsummary-storagecleanupvisibilityresponse-storagehealthresponse-storagemetadatadiagnostics-storagemigrationcounts-storagemigrationjobsummary-storagemigrationvisibilityresponse-storageoverview-storageplacementcounts-storagepolicyoverview-storagepolicyscope-storagepolicysummary-storagepolicyupdate-storagepoolconfigsummary-storagepoolhealth-storagepoolreference-storagepoolsummary-updateaccount-updatedomain-updatestoragepool
  - external/pub-use-crate-util-normalize-mailbox-domain-normalize-mailbox-email
  - external/pub-use-crate-workspace-clientcontact-clientevent-clientmessage-clientworkspace-recipientsuggestion-upsertclientcontactinput-upsertclienteventinput
  - external/pub-crate-use-crate-models
  - external/pub-crate-use-crate-pst-psttransferjobrow
  - external/pub-crate-use-crate-shared-canonical-change-channel-default-collection-id-default-contact-book-role-default-task-list-name-default-task-list-role-expected-schema-version-im-contact-list-collection-id-im-contact-list-role-max-sieve-scripts-per-account-max-sieve-script-bytes-platform-tenant-id-quick-contacts-collection-id-quick-contacts-role-suggested-contacts-collection-id-suggested-contacts-role
  - external/pub-crate-use-crate-tasks-map-dav-task-map-task-map-task-list-map-task-list-grant
  - external/pub-crate-use-crate-util
  member_of:
  - packages/crates/lpe-storage
---

# Imports

- `pub use crate::activesync::{
    ActiveSyncAttachment, ActiveSyncAttachmentContent, ActiveSyncDeviceState, ActiveSyncItemState,
    ActiveSyncSyncState,
}`
- `pub use crate::attachments::{
    calendar_attachment_file_reference, parse_calendar_attachment_file_reference,
    CalendarEventAttachment, ClientAttachment, MapiEventAttachmentChanges,
    MapiEventAttachmentUpsert,
}`
- `pub use crate::auth::{
    AccountAppPassword, AccountAuthFactor, AccountCredentialInput, AccountLogin, AccountOidcClaims,
    AdminAuthFactor, AdminCredentialInput, AdminLogin, AdminOidcClaims, AuthenticatedAccount,
    AuthenticatedAdmin, NewAccountAuthFactor, NewAdminAuthFactor, StoredAccountAppPassword,
}`
- `pub use crate::calendar::{
    calendar_attendee_labels, calendar_participant_label, normalize_calendar_email,
    normalize_calendar_participation_status, parse_calendar_participants_metadata,
    serialize_calendar_participants_metadata, CalendarOrganizerMetadata,
    CalendarParticipantMetadata, CalendarParticipantsMetadata,
}`
- `pub use crate::change::{
    CanonicalChangeCategory, CanonicalChangeListener, CanonicalChangeReplay, CanonicalPushChangeSet,
}`
- `pub use crate::collaboration::{
    AccessibleContact, AccessibleEvent, CollaborationCollection, CollaborationGrant,
    CollaborationGrantInput, CollaborationResourceKind, CollaborationRights, ContactNameFields,
    ContactSourceFields, DelegateAccessObject, DelegateFreeBusyMessageObject, FreeBusyBlock,
    MapiEventIdentityMove, MapiEventImportedMoveIdentity, MoveAccessibleEventToDeletedItemsResult,
}`
- `pub use crate::conversation_actions::{
    ConversationAction, UpsertConversationActionInput, CONVERSATION_ACTION_VERSION,
}`
- `pub use crate::core::Storage`
- `pub use crate::imap::{ImapEmail, ImapMailboxState, ImapMimePart}`
- `pub use crate::jmap_blobs::{JmapQuota, JmapUploadBlob}`
- `pub use crate::jmap_queries::{JmapEmailQuery, JmapStoredQueryState, JmapThreadQuery}`
- `pub use crate::mailboxes::{
    JmapMailbox, JmapMailboxCreateInput, JmapMailboxUpdateInput, ManagedRetentionFolderCreateInput,
}`
- `pub use crate::mapi_contacts::{
    MapiContactCommitInput, MapiContactCommitOutcome, MapiContactCommitResult,
    MapiContactCreateInput, MapiContactCreateResult, MapiContactCustomPropertyValue,
    MapiContactImportConflict, MapiContactImportDisposition, MapiContactImportObjectDeleted,
    MapiContactImportedIdentity, MapiContactVersion,
}`
- `pub use crate::mapi_events::{
    MapiEventCommitInput, MapiEventCommitOutcome, MapiEventCommitSuccess, MapiEventCreateInput,
    MapiEventCreateResult, MapiEventCustomPropertyValue, MapiEventImportedIdentity,
    MapiEventReminderPatch, MapiEventReminderState, MapiEventVersion,
}`
- `pub use crate::mapi_store_identity::{
    MapiMessageIdentityMove, MapiMessageImportedMoveIdentity, MapiMessageMoveResult,
    MapiStoreIdentity,
}`
- `pub use crate::notes_journal::{
    ClientNote, ClientReminder, JournalEntry, ReminderQuery, UpsertClientNoteInput,
    UpsertJournalEntryInput,
}`
- `pub use crate::protocols::{
    JmapEmail, JmapEmailAddress, JmapEmailFollowupUpdate, JmapEmailMailboxState,
    JmapEmailSubmission, JmapImportedEmailInput, JmapMailObjectChange, JmapStringObjectChange,
}`
- `pub use crate::pst::{NewPstTransferJob, PstJobExecutionSummary, PstTransferJobRecord}`
- `pub use crate::public_folders::{
    CreatePublicFolderInput, CreatePublicFolderTreeInput, PublicFolder, PublicFolderItem,
    PublicFolderPerUserState, PublicFolderPerUserStatePatch, PublicFolderPermission,
    PublicFolderPermissionInput, PublicFolderReplica, PublicFolderReplicaInput, PublicFolderRights,
    PublicFolderTree, UpdatePublicFolderInput, UpsertPublicFolderItemInput,
}`
- `pub use crate::recoverable_items::RecoverableItem`
- `pub use crate::search_folders::{SearchFolderDefinition, UpsertSearchFolderInput}`
- `pub use crate::submission::{
    AttachmentUploadInput, CancelSubmissionResult, MailboxAccountAccess, MailboxDelegationGrant,
    MailboxDelegationGrantInput, MailboxDelegationOverview, MailboxFolderDelegationGrantInput,
    SavedDraftMessage, SenderAuthorizationKind, SenderDelegationGrant, SenderDelegationGrantInput,
    SenderDelegationRight, SenderIdentity, SubmissionAccountIdentity, SubmitMessageInput,
    SubmittedMessage, SubmittedRecipientInput,
}`
- `pub use crate::tasks::{
    ClientTask, ClientTaskList, CreateTaskListInput, DavTask, TaskListGrant, TaskListGrantInput,
    UpdateTaskListInput, UpsertClientTaskInput,
}`
- `pub use crate::types::{
    AccountRecord, AdminDashboard, AliasRecord, AntispamSettings, AuditEntryInput, AuditEvent,
    DashboardUpdate, DomainRecord, EmailTraceResult, EmailTraceSearchInput, FilterRule,
    HealthResponse, LocalAiSettings, MailFlowEntry, MailboxRecord, MailboxRule, NewAccount,
    NewAlias, NewDomain, NewFilterRule, NewMailbox, NewServerAdministrator, NewStoragePool,
    OutboundQueueStatusUpdate, OutlookProfileState, OverviewStats, ProtocolStatus, QuarantineItem,
    SecuritySettings, ServerAdministrator, ServerSettings, SieveScriptDocument, SieveScriptSummary,
    StorageCleanupCounts, StorageCleanupPlacementSummary, StorageCleanupVisibilityResponse,
    StorageHealthResponse, StorageMetadataDiagnostics, StorageMigrationCounts,
    StorageMigrationJobSummary, StorageMigrationVisibilityResponse, StorageOverview,
    StoragePlacementCounts, StoragePolicyOverview, StoragePolicyScope, StoragePolicySummary,
    StoragePolicyUpdate, StoragePoolConfigSummary, StoragePoolHealth, StoragePoolReference,
    StoragePoolSummary, UpdateAccount, UpdateDomain, UpdateStoragePool,
}`
- `pub use crate::util::{normalize_mailbox_domain, normalize_mailbox_email}`
- `pub use crate::workspace::{
    ClientContact, ClientEvent, ClientMessage, ClientWorkspace, RecipientSuggestion,
    UpsertClientContactInput, UpsertClientEventInput,
}`
- `pub(crate) use crate::models::*`
- `pub(crate) use crate::pst::PstTransferJobRow`
- `pub(crate) use crate::shared::{
    CANONICAL_CHANGE_CHANNEL, DEFAULT_COLLECTION_ID, DEFAULT_CONTACT_BOOK_ROLE,
    DEFAULT_TASK_LIST_NAME, DEFAULT_TASK_LIST_ROLE, EXPECTED_SCHEMA_VERSION,
    IM_CONTACT_LIST_COLLECTION_ID, IM_CONTACT_LIST_ROLE, MAX_SIEVE_SCRIPTS_PER_ACCOUNT,
    MAX_SIEVE_SCRIPT_BYTES, PLATFORM_TENANT_ID, QUICK_CONTACTS_COLLECTION_ID, QUICK_CONTACTS_ROLE,
    SUGGESTED_CONTACTS_COLLECTION_ID, SUGGESTED_CONTACTS_ROLE,
}`
- `pub(crate) use crate::tasks::{map_dav_task, map_task, map_task_list, map_task_list_grant}`
- `pub(crate) use crate::util::*`

# Member of

- [lpe-storage](../../../packages/crates/lpe-storage.md)