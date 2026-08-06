---
type: Rust Module
title: store
resource: crates/lpe-exchange/src/store.rs#L1-L1304
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-result
  - external/lpe-mail-auth-accountauthstore-accountprincipal-storefuture
  - external/lpe-storage-accessiblecontact-accessibleevent-activesyncattachment-activesyncattachmentcontent-attachmentuploadinput-auditentryinput-calendareventattachment-cancelsubmissionresult-clientnote-clientreminder-clienttask-collaborationcollection-collaborationgrant-collaborationgrantinput-collaborationresourcekind-collaborationrights-conversationaction-createpublicfolderinput-delegatefreebusymessageobject-jmapemail-jmapemailfollowupupdate-jmapemailquery-jmapimportedemailinput-jmapmailbox-jmapmailboxcreateinput-jmapmailboxupdateinput-journalentry-mailboxdelegationgrantinput-mailboxfolderdelegationgrantinput-mailboxrule-managedretentionfoldercreateinput-mapicontactcommitinput-mapicontactcommitoutcome-mapicontactcreateinput-mapieventcommitinput-mapieventcommitoutcome-mapieventcreateinput-mapieventimportedmoveidentity-mapieventversion-mapimessageimportedmoveidentity-mapimessagemoveresult-mapistoreidentity-moveaccessibleeventtodeleteditemsresult-publicfolder-publicfolderitem-publicfolderperuserstate-publicfolderperuserstatepatch-publicfolderpermission-publicfolderpermissioninput-publicfolderreplica-publicfoldertree-recoverableitem-reminderquery-saveddraftmessage-searchfolderdefinition-senderdelegationgrantinput-senderdelegationright-sievescriptdocument-storage-submitmessageinput-submittedmessage-submittedrecipientinput-updatepublicfolderinput-upsertclientcontactinput-upsertclienteventinput-upsertclientnoteinput-upsertclienttaskinput-upsertconversationactioninput-upsertjournalentryinput-upsertpublicfolderiteminput-upsertsearchfolderinput
  - external/sqlx-row
  - external/uuid-uuid
  - external/crate-mapi-notifications-mapinotificationevent-mapinotificationkind
  - external/crate-mapi-permissions-owner-permission-rights-from-grant-mapifolderpermission
  - external/crate-mapi-properties-is-reserved-named-property-id-well-known-named-property-id-mapinamedproperty-mapinamedpropertykind
  - external/pub-crate-use-types
  member_of:
  - packages/crates/lpe-exchange
---

# Contains

- [MapiFolderVersion](../../../../classes/crates/lpe-exchange/src/store/MapiFolderVersion.md)
- [MapiFolderHierarchyCommitOutcome](../../../../classes/crates/lpe-exchange/src/store/MapiFolderHierarchyCommitOutcome.md)
- [ExchangeStore](../../../../interfaces/crates/lpe-exchange/src/store/ExchangeStore.md)
- [fetch_mapi_store_identity](../../../../functions/crates/lpe-exchange/src/store/ExchangeStore/fetch_mapi_store_identity.md)
- [fetch_account_category_modseq](../../../../functions/crates/lpe-exchange/src/store/ExchangeStore/fetch_account_category_modseq.md)
- [fetch_mapi_notes](../../../../functions/crates/lpe-exchange/src/store/ExchangeStore/fetch_mapi_notes.md)
- [fetch_mapi_notes_by_ids](../../../../functions/crates/lpe-exchange/src/store/ExchangeStore/fetch_mapi_notes_by_ids.md)
- [fetch_mapi_journal_entries](../../../../functions/crates/lpe-exchange/src/store/ExchangeStore/fetch_mapi_journal_entries.md)
- [fetch_mapi_journal_entries_by_ids](../../../../functions/crates/lpe-exchange/src/store/ExchangeStore/fetch_mapi_journal_entries_by_ids.md)
- [upsert_mapi_note](../../../../functions/crates/lpe-exchange/src/store/ExchangeStore/upsert_mapi_note.md)
- [upsert_mapi_journal_entry](../../../../functions/crates/lpe-exchange/src/store/ExchangeStore/upsert_mapi_journal_entry.md)
- [delete_mapi_note](../../../../functions/crates/lpe-exchange/src/store/ExchangeStore/delete_mapi_note.md)
- [delete_mapi_journal_entry](../../../../functions/crates/lpe-exchange/src/store/ExchangeStore/delete_mapi_journal_entry.md)

# Imports

- `anyhow::Result`
- `lpe_mail_auth::{AccountAuthStore, AccountPrincipal, StoreFuture}`
- `lpe_storage::{
    AccessibleContact, AccessibleEvent, ActiveSyncAttachment, ActiveSyncAttachmentContent,
    AttachmentUploadInput, AuditEntryInput, CalendarEventAttachment, CancelSubmissionResult,
    ClientNote, ClientReminder, ClientTask, CollaborationCollection, CollaborationGrant,
    CollaborationGrantInput, CollaborationResourceKind, CollaborationRights, ConversationAction,
    CreatePublicFolderInput, DelegateFreeBusyMessageObject, JmapEmail, JmapEmailFollowupUpdate,
    JmapEmailQuery, JmapImportedEmailInput, JmapMailbox, JmapMailboxCreateInput,
    JmapMailboxUpdateInput, JournalEntry, MailboxDelegationGrantInput,
    MailboxFolderDelegationGrantInput, MailboxRule, ManagedRetentionFolderCreateInput,
    MapiContactCommitInput, MapiContactCommitOutcome, MapiContactCreateInput, MapiEventCommitInput,
    MapiEventCommitOutcome, MapiEventCreateInput, MapiEventImportedMoveIdentity, MapiEventVersion,
    MapiMessageImportedMoveIdentity, MapiMessageMoveResult, MapiStoreIdentity,
    MoveAccessibleEventToDeletedItemsResult, PublicFolder, PublicFolderItem,
    PublicFolderPerUserState, PublicFolderPerUserStatePatch, PublicFolderPermission,
    PublicFolderPermissionInput, PublicFolderReplica, PublicFolderTree, RecoverableItem,
    ReminderQuery, SavedDraftMessage, SearchFolderDefinition, SenderDelegationGrantInput,
    SenderDelegationRight, SieveScriptDocument, Storage, SubmitMessageInput, SubmittedMessage,
    SubmittedRecipientInput, UpdatePublicFolderInput, UpsertClientContactInput,
    UpsertClientEventInput, UpsertClientNoteInput, UpsertClientTaskInput,
    UpsertConversationActionInput, UpsertJournalEntryInput, UpsertPublicFolderItemInput,
    UpsertSearchFolderInput,
}`
- `sqlx::Row`
- `uuid::Uuid`
- `crate::mapi::notifications::{MapiNotificationEvent, MapiNotificationKind}`
- `crate::mapi::permissions::{owner_permission, rights_from_grant, MapiFolderPermission}`
- `crate::mapi::properties::{
    is_reserved_named_property_id, well_known_named_property_id, MapiNamedProperty,
    MapiNamedPropertyKind,
}`
- `pub(crate) use types::*`

# Member of

- [lpe-exchange](../../../../packages/crates/lpe-exchange.md)