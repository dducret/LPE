use anyhow::Result;
use lpe_mail_auth::{AccountAuthStore, AccountPrincipal, StoreFuture};
use lpe_storage::{
    AccessibleContact, AccessibleEvent, ActiveSyncAttachment, ActiveSyncAttachmentContent,
    AttachmentUploadInput, AuditEntryInput, CalendarEventAttachment, CancelSubmissionResult,
    ClientNote, ClientReminder, ClientTask, CollaborationCollection, CollaborationGrant,
    CollaborationGrantInput, CollaborationResourceKind, CollaborationRights, ConversationAction,
    CreatePublicFolderInput, DelegateFreeBusyMessageObject, JmapEmail, JmapEmailFollowupUpdate,
    JmapEmailQuery, JmapImportedEmailInput, JmapMailbox, JmapMailboxCreateInput,
    JmapMailboxUpdateInput, JournalEntry, MailboxDelegationGrantInput,
    MailboxFolderDelegationGrantInput, MailboxRule, ManagedRetentionFolderCreateInput,
    PublicFolder, PublicFolderItem, PublicFolderPerUserState, PublicFolderPerUserStatePatch,
    PublicFolderPermission, PublicFolderPermissionInput, PublicFolderReplica, PublicFolderTree,
    RecoverableItem, ReminderQuery, SavedDraftMessage, SearchFolderDefinition,
    SenderDelegationGrantInput, SenderDelegationRight, SieveScriptDocument, Storage,
    SubmitMessageInput, SubmittedMessage, SubmittedRecipientInput, UpdatePublicFolderInput,
    UpsertClientContactInput, UpsertClientEventInput, UpsertClientNoteInput, UpsertClientTaskInput,
    UpsertConversationActionInput, UpsertJournalEntryInput, UpsertPublicFolderItemInput,
    UpsertSearchFolderInput,
};
use sqlx::Row;
use std::collections::HashSet;
use uuid::Uuid;

use crate::mapi::notifications::{MapiNotificationEvent, MapiNotificationKind};
use crate::mapi::permissions::{owner_permission, rights_from_grant, MapiFolderPermission};
use crate::mapi::properties::{
    is_reserved_named_property_id, MapiNamedProperty, MapiNamedPropertyKind,
};

const MAPI_ASSOCIATED_CONFIG_VIRTUAL_PARENT_FOLDER_IDS: [i64; 6] = [
    crate::mapi::identity::CONTACTS_FOLDER_ID as i64,
    crate::mapi::identity::SUGGESTED_CONTACTS_FOLDER_ID as i64,
    crate::mapi::identity::QUICK_CONTACTS_FOLDER_ID as i64,
    crate::mapi::identity::IM_CONTACT_LIST_FOLDER_ID as i64,
    crate::mapi::identity::QUICK_STEP_SETTINGS_FOLDER_ID as i64,
    crate::mapi::identity::FREEBUSY_DATA_FOLDER_ID as i64,
];

mod implementation;
mod types;

pub(crate) use types::*;

pub trait ExchangeStore: AccountAuthStore {
    fn fetch_or_allocate_mapi_identities<'a>(
        &'a self,
        account_id: Uuid,
        requests: &'a [MapiIdentityRequest],
    ) -> StoreFuture<'a, Vec<MapiIdentityRecord>>;

    fn fetch_mapi_identities_by_object_ids<'a>(
        &'a self,
        account_id: Uuid,
        object_ids: &'a [u64],
    ) -> StoreFuture<'a, Vec<MapiIdentityLookupRecord>>;

    fn fetch_mapi_object_ids_for_deleted_changes<'a>(
        &'a self,
        account_id: Uuid,
        object_kind: MapiIdentityObjectKind,
        canonical_ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<u64>>;

    #[allow(dead_code)]
    fn fetch_mapi_identities_by_source_keys<'a>(
        &'a self,
        account_id: Uuid,
        source_keys: &'a [Vec<u8>],
    ) -> StoreFuture<'a, Vec<MapiIdentityLookupRecord>>;

    fn fetch_or_allocate_mapi_named_property_ids<'a>(
        &'a self,
        account_id: Uuid,
        properties: &'a [MapiNamedProperty],
        create: bool,
    ) -> StoreFuture<'a, Vec<Option<MapiNamedPropertyMapping>>>;

    fn fetch_mapi_named_properties_by_ids<'a>(
        &'a self,
        account_id: Uuid,
        property_ids: &'a [u16],
    ) -> StoreFuture<'a, Vec<MapiNamedPropertyMapping>>;

    fn fetch_mapi_named_properties<'a>(
        &'a self,
        account_id: Uuid,
        guid: Option<[u8; 16]>,
    ) -> StoreFuture<'a, Vec<MapiNamedPropertyMapping>>;

    #[allow(dead_code)]
    fn upsert_mapi_custom_property_values<'a>(
        &'a self,
        account_id: Uuid,
        object_kind: MapiCustomPropertyObjectKind,
        canonical_id: Uuid,
        values: &'a [MapiCustomPropertyValue],
    ) -> StoreFuture<'a, ()>;

    #[allow(dead_code)]
    fn fetch_mapi_custom_property_values<'a>(
        &'a self,
        account_id: Uuid,
        object_kind: MapiCustomPropertyObjectKind,
        canonical_id: Uuid,
        property_tags: &'a [u32],
    ) -> StoreFuture<'a, Vec<MapiCustomPropertyValue>>;

    #[allow(dead_code)]
    fn fetch_all_mapi_custom_property_values<'a>(
        &'a self,
        account_id: Uuid,
        object_kind: MapiCustomPropertyObjectKind,
        canonical_id: Uuid,
    ) -> StoreFuture<'a, Vec<MapiCustomPropertyValue>>;

    #[allow(dead_code)]
    fn delete_mapi_custom_property_values<'a>(
        &'a self,
        account_id: Uuid,
        object_kind: MapiCustomPropertyObjectKind,
        canonical_id: Uuid,
        property_tags: &'a [u32],
    ) -> StoreFuture<'a, ()>;

    #[allow(dead_code)]
    fn fetch_mapi_sync_checkpoint<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        checkpoint_kind: MapiCheckpointKind,
    ) -> StoreFuture<'a, Option<MapiSyncCheckpoint>>;

    #[allow(dead_code)]
    fn store_mapi_sync_checkpoint<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        checkpoint_kind: MapiCheckpointKind,
        last_change_sequence: u64,
        last_modseq: u64,
        cursor_json: serde_json::Value,
    ) -> StoreFuture<'a, MapiSyncCheckpoint>;

    fn fetch_mapi_ipm_subtree_ost_id<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Option<Vec<u8>>>;

    fn store_mapi_ipm_subtree_ost_id<'a>(
        &'a self,
        account_id: Uuid,
        ost_id: &'a [u8],
    ) -> StoreFuture<'a, ()>;

    fn fetch_mapi_folder_profile_property_values<'a>(
        &'a self,
        account_id: Uuid,
        folder_id: u64,
        property_tags: &'a [u32],
    ) -> StoreFuture<'a, Vec<MapiFolderProfilePropertyValue>>;

    fn upsert_mapi_folder_profile_property_values<'a>(
        &'a self,
        account_id: Uuid,
        values: &'a [MapiFolderProfilePropertyValue],
    ) -> StoreFuture<'a, ()>;

    fn fetch_mapi_sync_changes<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        checkpoint_kind: MapiCheckpointKind,
        after_change_sequence: u64,
    ) -> StoreFuture<'a, MapiSyncChangeSet>;

    fn fetch_mapi_folder_permissions<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<MapiFolderPermission>>;

    fn set_mapi_folder_permission<'a>(
        &'a self,
        owner_account_id: Uuid,
        mailbox_id: Uuid,
        grantee_account_id: Uuid,
        may_read: bool,
        may_write: bool,
        may_delete: bool,
        may_share: bool,
        _audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()>;

    fn set_mapi_calendar_permission<'a>(
        &'a self,
        owner_account_id: Uuid,
        grantee_account_id: Uuid,
        may_read: bool,
        may_write: bool,
        may_delete: bool,
        may_share: bool,
        _audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()>;

    fn set_mapi_calendar_collection_permission<'a>(
        &'a self,
        owner_account_id: Uuid,
        calendar_collection_id: &'a str,
        grantee_account_id: Uuid,
        may_read: bool,
        may_write: bool,
        may_delete: bool,
        may_share: bool,
        _audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()>;

    fn fetch_mapi_notification_cursor<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Option<i64>>;

    fn poll_mapi_notifications<'a>(
        &'a self,
        account_id: Uuid,
        after_cursor: i64,
    ) -> StoreFuture<'a, MapiNotificationPoll>;

    fn fetch_ews_user_configuration<'a>(
        &'a self,
        account_id: Uuid,
        key: &'a EwsUserConfigurationKey,
    ) -> StoreFuture<'a, Option<EwsUserConfiguration>>;

    fn upsert_ews_user_configuration<'a>(
        &'a self,
        input: UpsertEwsUserConfigurationInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, EwsUserConfiguration>;

    fn delete_ews_user_configuration<'a>(
        &'a self,
        account_id: Uuid,
        key: &'a EwsUserConfigurationKey,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, bool>;

    fn fetch_ews_retention_policy_tags<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
    ) -> StoreFuture<'a, Vec<EwsRetentionPolicyTag>>;

    fn create_managed_retention_folder<'a>(
        &'a self,
        input: ManagedRetentionFolderCreateInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapMailbox>;

    fn fetch_ews_searchable_mailboxes<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
    ) -> StoreFuture<'a, Vec<EwsSearchableMailbox>>;

    fn fetch_ews_discovery_search_configurations<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
    ) -> StoreFuture<'a, Vec<EwsDiscoverySearchConfig>>;

    fn search_ews_mailboxes<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        query_text: &'a str,
        mailbox_emails: &'a [String],
        limit: usize,
    ) -> StoreFuture<'a, EwsDiscoverySearchResult>;

    fn fetch_ews_message_tracking_reports<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        query_text: &'a str,
        limit: usize,
    ) -> StoreFuture<'a, Vec<EwsMessageTrackingReport>>;

    fn fetch_ews_message_tracking_report_detail<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        report_id: &'a str,
    ) -> StoreFuture<'a, Option<EwsMessageTrackingReportDetail>>;

    fn fetch_ews_hold_mailboxes<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        mailbox_emails: &'a [String],
    ) -> StoreFuture<'a, Vec<EwsHoldMailbox>>;

    fn set_ews_hold_mailboxes<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        hold_name: &'a str,
        query_text: &'a str,
        mailbox_emails: &'a [String],
        enable: bool,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, Vec<EwsHoldMailbox>>;

    fn fetch_ews_non_indexable_reports<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
    ) -> StoreFuture<'a, Vec<EwsNonIndexableReport>>;

    fn create_ews_transfer_job<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        direction: &'a str,
        item_ids: &'a [String],
        request_json: serde_json::Value,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, EwsTransferJob>;

    fn fetch_ews_mail_app_manifests<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
    ) -> StoreFuture<'a, Vec<EwsMailAppManifest>>;

    fn fetch_ews_app_marketplace_policy<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
    ) -> StoreFuture<'a, EwsAppMarketplacePolicy>;

    fn install_ews_mail_app<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        app_id: &'a str,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, EwsMailAppInstall>;

    fn disable_ews_mail_app<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        app_id: &'a str,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, EwsMailAppInstall>;

    fn uninstall_ews_mail_app<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        app_id: &'a str,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, EwsMailAppInstall>;

    fn issue_ews_mail_app_token<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        app_id: &'a str,
        token_hash: &'a str,
        scopes: &'a [String],
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, EwsMailAppTokenEvent>;

    fn create_ews_unified_messaging_call<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        phone_number: Option<&'a str>,
        message_id: Option<Uuid>,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, EwsUnifiedMessagingCall>;

    fn fetch_ews_unified_messaging_call<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        call_id: &'a str,
    ) -> StoreFuture<'a, Option<EwsUnifiedMessagingCall>>;

    fn disconnect_ews_unified_messaging_call<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        call_id: &'a str,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, Option<EwsUnifiedMessagingCall>>;

    fn upsert_ews_sharing_grant<'a>(
        &'a self,
        owner_account_id: Uuid,
        grantee_email: &'a str,
        kind: CollaborationResourceKind,
        rights: CollaborationRights,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, CollaborationGrant>;

    fn fetch_ews_delegates<'a>(
        &'a self,
        owner_account_id: Uuid,
    ) -> StoreFuture<'a, Vec<EwsDelegate>>;

    fn upsert_ews_delegate<'a>(
        &'a self,
        input: UpsertEwsDelegateInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, EwsDelegate>;

    fn remove_ews_delegate<'a>(
        &'a self,
        owner_account_id: Uuid,
        grantee_account_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, bool>;

    fn fetch_address_book_entries<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
    ) -> StoreFuture<'a, Vec<ExchangeAddressBookEntry>>;

    fn fetch_ews_im_list<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
    ) -> StoreFuture<'a, EwsImList>;

    fn upsert_ews_im_group<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        group_id: Option<Uuid>,
        display_name: &'a str,
        _audit: AuditEntryInput,
    ) -> StoreFuture<'a, EwsImGroup>;

    fn remove_ews_im_group<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        group_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, bool>;

    fn add_ews_im_group_member<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        group_id: Uuid,
        member: EwsImMemberInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, EwsImGroupMember>;

    fn remove_ews_im_group_member<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
        group_id: Option<Uuid>,
        member_kind: &'a str,
        member_value: &'a str,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, bool>;

    fn fetch_accessible_contact_collections<'a>(
        &'a self,
        principal_account_id: Uuid,
    ) -> StoreFuture<'a, Vec<CollaborationCollection>>;

    fn fetch_accessible_calendar_collections<'a>(
        &'a self,
        principal_account_id: Uuid,
    ) -> StoreFuture<'a, Vec<CollaborationCollection>>;

    fn fetch_accessible_task_collections<'a>(
        &'a self,
        principal_account_id: Uuid,
    ) -> StoreFuture<'a, Vec<CollaborationCollection>>;

    fn fetch_delegate_freebusy_messages<'a>(
        &'a self,
        principal_account_id: Uuid,
    ) -> StoreFuture<'a, Vec<DelegateFreeBusyMessageObject>>;

    fn fetch_public_folder_trees<'a>(
        &'a self,
        principal_account_id: Uuid,
    ) -> StoreFuture<'a, Vec<PublicFolderTree>>;

    fn fetch_public_folder<'a>(
        &'a self,
        principal_account_id: Uuid,
        folder_id: Uuid,
    ) -> StoreFuture<'a, PublicFolder>;

    fn fetch_public_folder_children<'a>(
        &'a self,
        principal_account_id: Uuid,
        folder_id: Uuid,
    ) -> StoreFuture<'a, Vec<PublicFolder>>;

    fn create_public_folder_child<'a>(
        &'a self,
        input: CreatePublicFolderInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, PublicFolder>;

    fn update_public_folder<'a>(
        &'a self,
        input: UpdatePublicFolderInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, PublicFolder>;

    fn delete_public_folder<'a>(
        &'a self,
        principal_account_id: Uuid,
        folder_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()>;

    fn fetch_public_folder_items<'a>(
        &'a self,
        principal_account_id: Uuid,
        folder_id: Uuid,
    ) -> StoreFuture<'a, Vec<PublicFolderItem>>;

    fn fetch_public_folder_items_by_ids<'a>(
        &'a self,
        principal_account_id: Uuid,
        item_ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<PublicFolderItem>>;

    fn fetch_public_folder_permissions<'a>(
        &'a self,
        principal_account_id: Uuid,
        folder_id: Uuid,
    ) -> StoreFuture<'a, Vec<PublicFolderPermission>>;

    fn fetch_public_folder_replicas<'a>(
        &'a self,
        principal_account_id: Uuid,
        folder_id: Uuid,
    ) -> StoreFuture<'a, Vec<PublicFolderReplica>>;

    fn upsert_public_folder_permission<'a>(
        &'a self,
        input: PublicFolderPermissionInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, PublicFolderPermission>;

    fn delete_public_folder_permission<'a>(
        &'a self,
        principal_account_id: Uuid,
        folder_id: Uuid,
        grantee_account_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()>;

    fn upsert_public_folder_item<'a>(
        &'a self,
        input: UpsertPublicFolderItemInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, PublicFolderItem>;

    fn delete_public_folder_item<'a>(
        &'a self,
        principal_account_id: Uuid,
        folder_id: Uuid,
        item_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()>;

    fn fetch_public_folder_per_user_state<'a>(
        &'a self,
        principal_account_id: Uuid,
        folder_id: Uuid,
    ) -> StoreFuture<'a, Vec<PublicFolderPerUserState>>;

    fn patch_public_folder_per_user_state<'a>(
        &'a self,
        principal_account_id: Uuid,
        folder_id: Uuid,
        patches: &'a [PublicFolderPerUserStatePatch],
    ) -> StoreFuture<'a, Vec<PublicFolderPerUserState>>;

    fn fetch_accessible_contacts_in_collection<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: &'a str,
    ) -> StoreFuture<'a, Vec<AccessibleContact>>;

    fn fetch_contact_sync_versions<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: &'a str,
    ) -> StoreFuture<'a, Vec<(Uuid, String)>>;

    fn fetch_accessible_events_in_collection<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: &'a str,
    ) -> StoreFuture<'a, Vec<AccessibleEvent>>;

    fn fetch_event_sync_versions<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: &'a str,
    ) -> StoreFuture<'a, Vec<(Uuid, String)>>;

    fn fetch_accessible_tasks_in_collection<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: &'a str,
    ) -> StoreFuture<'a, Vec<ClientTask>>;

    fn fetch_task_sync_versions<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: &'a str,
    ) -> StoreFuture<'a, Vec<(Uuid, String)>>;

    fn fetch_accessible_contacts_by_ids<'a>(
        &'a self,
        principal_account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<AccessibleContact>>;

    fn create_accessible_contact<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: Option<&'a str>,
        input: UpsertClientContactInput,
    ) -> StoreFuture<'a, AccessibleContact>;

    fn update_accessible_contact<'a>(
        &'a self,
        principal_account_id: Uuid,
        contact_id: Uuid,
        input: UpsertClientContactInput,
    ) -> StoreFuture<'a, AccessibleContact>;

    fn delete_accessible_contact<'a>(
        &'a self,
        principal_account_id: Uuid,
        contact_id: Uuid,
    ) -> StoreFuture<'a, ()>;

    fn fetch_accessible_events_by_ids<'a>(
        &'a self,
        principal_account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<AccessibleEvent>>;

    fn create_accessible_event<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: Option<&'a str>,
        input: UpsertClientEventInput,
    ) -> StoreFuture<'a, AccessibleEvent>;

    fn update_accessible_event<'a>(
        &'a self,
        principal_account_id: Uuid,
        event_id: Uuid,
        input: UpsertClientEventInput,
    ) -> StoreFuture<'a, AccessibleEvent>;

    fn update_accessible_event_reminder<'a>(
        &'a self,
        principal_account_id: Uuid,
        event_id: Uuid,
        reminder_set: Option<bool>,
        reminder_at: Option<String>,
        reminder_dismissed_at: Option<String>,
    ) -> StoreFuture<'a, ()>;

    fn delete_accessible_event<'a>(
        &'a self,
        principal_account_id: Uuid,
        event_id: Uuid,
    ) -> StoreFuture<'a, ()>;

    fn fetch_accessible_tasks_by_ids<'a>(
        &'a self,
        principal_account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<ClientTask>>;

    fn fetch_mapi_notes<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<ClientNote>> {
        Box::pin(async move {
            let _ = account_id;
            Ok(Vec::new())
        })
    }

    fn fetch_mapi_notes_by_ids<'a>(
        &'a self,
        account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<ClientNote>> {
        Box::pin(async move {
            let _ = (account_id, ids);
            Ok(Vec::new())
        })
    }

    fn fetch_mapi_journal_entries<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Vec<JournalEntry>> {
        Box::pin(async move {
            let _ = account_id;
            Ok(Vec::new())
        })
    }

    fn fetch_mapi_journal_entries_by_ids<'a>(
        &'a self,
        account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<JournalEntry>> {
        Box::pin(async move {
            let _ = (account_id, ids);
            Ok(Vec::new())
        })
    }

    fn upsert_mapi_note<'a>(&'a self, input: UpsertClientNoteInput) -> StoreFuture<'a, ClientNote> {
        Box::pin(async move {
            let _ = input;
            Err(anyhow::anyhow!(
                "MAPI note writes are not supported by this store"
            ))
        })
    }

    fn upsert_mapi_journal_entry<'a>(
        &'a self,
        input: UpsertJournalEntryInput,
    ) -> StoreFuture<'a, JournalEntry> {
        Box::pin(async move {
            let _ = input;
            Err(anyhow::anyhow!(
                "MAPI journal entry writes are not supported by this store"
            ))
        })
    }

    fn delete_mapi_note<'a>(&'a self, account_id: Uuid, note_id: Uuid) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            let _ = (account_id, note_id);
            Err(anyhow::anyhow!(
                "MAPI note deletes are not supported by this store"
            ))
        })
    }

    fn delete_mapi_journal_entry<'a>(
        &'a self,
        account_id: Uuid,
        entry_id: Uuid,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            let _ = (account_id, entry_id);
            Err(anyhow::anyhow!(
                "MAPI journal entry deletes are not supported by this store"
            ))
        })
    }

    fn fetch_active_sieve_script<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Option<SieveScriptDocument>>;

    fn list_mailbox_rules<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<MailboxRule>>;

    fn put_sieve_script<'a>(
        &'a self,
        account_id: Uuid,
        name: &'a str,
        content: &'a str,
        activate: bool,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, SieveScriptDocument>;

    fn set_active_sieve_script<'a>(
        &'a self,
        account_id: Uuid,
        name: Option<&'a str>,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, Option<String>>;

    fn delete_sieve_script<'a>(
        &'a self,
        account_id: Uuid,
        name: &'a str,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()>;

    fn create_accessible_task<'a>(
        &'a self,
        principal_account_id: Uuid,
        input: UpsertClientTaskInput,
    ) -> StoreFuture<'a, ClientTask>;

    fn update_accessible_task<'a>(
        &'a self,
        principal_account_id: Uuid,
        task_id: Uuid,
        input: UpsertClientTaskInput,
    ) -> StoreFuture<'a, ClientTask>;

    fn update_accessible_task_reminder<'a>(
        &'a self,
        principal_account_id: Uuid,
        task_id: Uuid,
        reminder_set: Option<bool>,
        reminder_at: Option<String>,
        reminder_dismissed_at: Option<String>,
        reminder_reset: Option<bool>,
    ) -> StoreFuture<'a, ()>;

    fn delete_accessible_task<'a>(
        &'a self,
        principal_account_id: Uuid,
        task_id: Uuid,
    ) -> StoreFuture<'a, ()>;

    fn fetch_jmap_mailboxes<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<JmapMailbox>>;

    fn ensure_jmap_system_mailboxes<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Vec<JmapMailbox>>;

    fn fetch_search_folders<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Vec<SearchFolderDefinition>>;

    fn upsert_search_folder<'a>(
        &'a self,
        input: UpsertSearchFolderInput,
    ) -> StoreFuture<'a, SearchFolderDefinition>;

    fn delete_search_folder<'a>(
        &'a self,
        account_id: Uuid,
        search_folder_id: Uuid,
    ) -> StoreFuture<'a, ()>;

    fn fetch_conversation_actions<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Vec<ConversationAction>>;

    fn fetch_mapi_navigation_shortcuts<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Vec<MapiNavigationShortcutRecord>>;

    fn upsert_mapi_navigation_shortcut<'a>(
        &'a self,
        input: UpsertMapiNavigationShortcutInput,
    ) -> StoreFuture<'a, MapiNavigationShortcutRecord>;

    fn delete_mapi_navigation_shortcut<'a>(
        &'a self,
        account_id: Uuid,
        shortcut_id: Uuid,
    ) -> StoreFuture<'a, ()>;

    fn fetch_mapi_associated_configs<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Vec<MapiAssociatedConfigRecord>>;

    fn upsert_mapi_associated_config<'a>(
        &'a self,
        input: UpsertMapiAssociatedConfigInput,
    ) -> StoreFuture<'a, MapiAssociatedConfigRecord>;

    fn delete_mapi_associated_config<'a>(
        &'a self,
        account_id: Uuid,
        config_id: Uuid,
    ) -> StoreFuture<'a, ()>;

    fn upsert_conversation_action<'a>(
        &'a self,
        input: UpsertConversationActionInput,
    ) -> StoreFuture<'a, ConversationAction>;

    fn delete_conversation_action<'a>(
        &'a self,
        account_id: Uuid,
        conversation_action_id: Uuid,
    ) -> StoreFuture<'a, ()>;

    fn query_client_reminders<'a>(
        &'a self,
        account_id: Uuid,
        query: ReminderQuery,
    ) -> StoreFuture<'a, Vec<ClientReminder>>;

    fn dismiss_reminder_occurrence<'a>(
        &'a self,
        account_id: Uuid,
        source_type: &'a str,
        source_id: Uuid,
        occurrence_start_at: Option<&'a str>,
        dismissed_at: &'a str,
    ) -> StoreFuture<'a, ()>;

    fn create_jmap_mailbox<'a>(
        &'a self,
        input: JmapMailboxCreateInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapMailbox>;

    fn update_jmap_mailbox<'a>(
        &'a self,
        input: JmapMailboxUpdateInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapMailbox>;

    fn destroy_jmap_mailbox<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()>;

    fn query_jmap_email_ids<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        search_text: Option<&'a str>,
        position: u64,
        limit: u64,
    ) -> StoreFuture<'a, JmapEmailQuery>;

    fn fetch_all_jmap_email_ids<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<Uuid>>;

    fn query_mapi_content_table_ids<'a>(
        &'a self,
        account_id: Uuid,
        query: MapiContentTableQuery,
    ) -> StoreFuture<'a, MapiContentTableQueryResult>;

    fn list_recoverable_items<'a>(
        &'a self,
        account_id: Uuid,
        recoverable_folder: Option<&'a str>,
    ) -> StoreFuture<'a, Vec<RecoverableItem>>;

    fn restore_recoverable_item<'a>(
        &'a self,
        account_id: Uuid,
        recoverable_item_id: Uuid,
        target_mailbox_id: Option<Uuid>,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail>;

    fn purge_recoverable_item<'a>(
        &'a self,
        account_id: Uuid,
        recoverable_item_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()>;

    fn fetch_jmap_emails<'a>(
        &'a self,
        account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<JmapEmail>>;

    fn fetch_jmap_emails_with_protected_bcc<'a>(
        &'a self,
        account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<JmapEmail>>;

    fn fetch_message_attachments<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
    ) -> StoreFuture<'a, Vec<ActiveSyncAttachment>>;

    fn fetch_calendar_attachments_for_events<'a>(
        &'a self,
        account_id: Uuid,
        event_ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<(Uuid, Vec<CalendarEventAttachment>)>>;

    fn fetch_attachment_content<'a>(
        &'a self,
        account_id: Uuid,
        file_reference: &'a str,
    ) -> StoreFuture<'a, Option<ActiveSyncAttachmentContent>>;

    fn add_message_attachment<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
        attachment: AttachmentUploadInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, Option<(JmapEmail, ActiveSyncAttachment)>>;

    fn add_calendar_event_attachment<'a>(
        &'a self,
        account_id: Uuid,
        event_id: Uuid,
        attachment: AttachmentUploadInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, Option<CalendarEventAttachment>>;

    fn delete_calendar_event_attachment<'a>(
        &'a self,
        account_id: Uuid,
        file_reference: &'a str,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, Option<Uuid>>;

    fn delete_message_attachment<'a>(
        &'a self,
        account_id: Uuid,
        file_reference: &'a str,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, Option<JmapEmail>>;

    fn import_jmap_email<'a>(
        &'a self,
        input: JmapImportedEmailInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail>;

    fn move_jmap_email<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail>;

    fn move_jmap_email_from_mailbox<'a>(
        &'a self,
        account_id: Uuid,
        source_mailbox_id: Uuid,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail>;

    fn copy_jmap_email<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail>;

    fn update_jmap_email_flags<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
        unread: Option<bool>,
        flagged: Option<bool>,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail>;

    fn update_jmap_email_followup_flags<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
        update: JmapEmailFollowupUpdate,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail>;

    fn update_jmap_email_content<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
        subject: Option<String>,
        body_text: Option<String>,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail>;

    fn delete_jmap_email<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()>;

    fn delete_jmap_email_from_mailbox<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Uuid,
        message_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()>;

    fn replace_message_recipients<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
        to: &'a [SubmittedRecipientInput],
        cc: &'a [SubmittedRecipientInput],
        bcc: &'a [SubmittedRecipientInput],
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()>;

    fn save_draft_message<'a>(
        &'a self,
        input: SubmitMessageInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, SavedDraftMessage>;

    fn submit_draft_message<'a>(
        &'a self,
        account_id: Uuid,
        draft_message_id: Uuid,
        submitted_by_account_id: Uuid,
        source: &'a str,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, SubmittedMessage>;

    fn submit_message<'a>(
        &'a self,
        input: SubmitMessageInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, SubmittedMessage>;

    fn cancel_queued_submission<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, CancelSubmissionResult>;
}
