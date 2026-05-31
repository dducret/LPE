use anyhow::Result;
use lpe_mail_auth::{AccountAuthStore, AccountPrincipal, StoreFuture};
use lpe_storage::{
    AccessibleContact, AccessibleEvent, ActiveSyncAttachment, ActiveSyncAttachmentContent,
    AttachmentUploadInput, AuditEntryInput, CalendarEventAttachment, CancelSubmissionResult,
    ClientNote, ClientReminder, ClientTask, CollaborationCollection, CollaborationGrantInput,
    CollaborationResourceKind, ConversationAction, CreatePublicFolderInput,
    DelegateFreeBusyMessageObject, JmapEmail, JmapEmailFollowupUpdate, JmapEmailQuery,
    JmapImportedEmailInput, JmapMailbox, JmapMailboxCreateInput, JmapMailboxUpdateInput,
    JournalEntry, MailboxFolderDelegationGrantInput, MailboxRule, PublicFolder, PublicFolderItem,
    PublicFolderPerUserState, PublicFolderPerUserStatePatch, PublicFolderPermission,
    PublicFolderPermissionInput, PublicFolderReplica, PublicFolderTree, RecoverableItem,
    ReminderQuery, SavedDraftMessage, SearchFolderDefinition, SieveScriptDocument, Storage,
    SubmitMessageInput, SubmittedMessage, UpsertClientContactInput, UpsertClientEventInput,
    UpsertClientNoteInput, UpsertClientTaskInput, UpsertConversationActionInput,
    UpsertJournalEntryInput, UpsertPublicFolderItemInput, UpsertSearchFolderInput,
};
use sqlx::Row;
use uuid::Uuid;

use crate::mapi::notifications::{MapiNotificationEvent, MapiNotificationKind};
use crate::mapi::permissions::{owner_permission, rights_from_grant, MapiFolderPermission};
use crate::mapi::properties::{
    is_reserved_named_property_id, MapiNamedProperty, MapiNamedPropertyKind,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MapiIdentityObjectKind {
    Account,
    Mailbox,
    Message,
    Contact,
    CalendarEvent,
    Task,
    Note,
    JournalEntry,
    SearchFolderDefinition,
    ConversationAction,
    NavigationShortcut,
    DelegateFreeBusyMessage,
    PublicFolder,
    PublicFolderItem,
    Rule,
}

impl MapiIdentityObjectKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Account => "account",
            Self::Mailbox => "mailbox",
            Self::Message => "message",
            Self::Contact => "contact",
            Self::CalendarEvent => "calendar_event",
            Self::Task => "task",
            Self::Note => "note",
            Self::JournalEntry => "journal_entry",
            Self::SearchFolderDefinition => "search_folder_definition",
            Self::ConversationAction => "conversation_action",
            Self::NavigationShortcut => "navigation_shortcut",
            Self::DelegateFreeBusyMessage => "delegate_freebusy_message",
            Self::PublicFolder => "public_folder",
            Self::PublicFolderItem => "public_folder_item",
            Self::Rule => "sieve_script",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MapiNavigationShortcutRecord {
    pub(crate) id: Uuid,
    pub(crate) account_id: Uuid,
    pub(crate) subject: String,
    pub(crate) target_folder_id: Option<u64>,
    pub(crate) shortcut_type: u32,
    pub(crate) flags: u32,
    pub(crate) section: u32,
    pub(crate) ordinal: u32,
    pub(crate) group_header_id: Option<Uuid>,
    pub(crate) group_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UpsertMapiNavigationShortcutInput {
    pub(crate) id: Option<Uuid>,
    pub(crate) account_id: Uuid,
    pub(crate) subject: String,
    pub(crate) target_folder_id: Option<u64>,
    pub(crate) shortcut_type: u32,
    pub(crate) flags: u32,
    pub(crate) section: u32,
    pub(crate) ordinal: u32,
    pub(crate) group_header_id: Option<Uuid>,
    pub(crate) group_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MapiIdentityRequest {
    pub(crate) object_kind: MapiIdentityObjectKind,
    pub(crate) canonical_id: Uuid,
    pub(crate) reserved_global_counter: Option<u64>,
    pub(crate) source_key: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MapiIdentityRecord {
    pub(crate) canonical_id: Uuid,
    pub(crate) object_id: u64,
    pub(crate) source_key: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MapiIdentityLookupRecord {
    pub(crate) object_kind: MapiIdentityObjectKind,
    pub(crate) canonical_id: Uuid,
    pub(crate) object_id: u64,
    pub(crate) source_key: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MapiNamedPropertyMapping {
    pub(crate) property_id: u16,
    pub(crate) property: MapiNamedProperty,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum MapiCustomPropertyObjectKind {
    Message,
    Contact,
    CalendarEvent,
    Task,
    Note,
    JournalEntry,
    Attachment,
}

impl MapiCustomPropertyObjectKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Message => "message",
            Self::Contact => "contact",
            Self::CalendarEvent => "calendar_event",
            Self::Task => "task",
            Self::Note => "note",
            Self::JournalEntry => "journal_entry",
            Self::Attachment => "attachment",
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MapiCustomPropertyValue {
    pub(crate) property_tag: u32,
    pub(crate) property_type: u16,
    pub(crate) property_value: Vec<u8>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum MapiCheckpointKind {
    Hierarchy,
    Content,
    ReadState,
}

impl MapiCheckpointKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Hierarchy => "hierarchy",
            Self::Content => "content",
            Self::ReadState => "read_state",
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MapiSyncCheckpoint {
    pub(crate) mailbox_id: Option<Uuid>,
    pub(crate) checkpoint_kind: MapiCheckpointKind,
    pub(crate) last_change_sequence: u64,
    pub(crate) last_modseq: u64,
    pub(crate) cursor_json: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MapiNotificationPoll {
    pub(crate) event_pending: bool,
    pub(crate) cursor: Option<i64>,
    pub(crate) events: Vec<MapiNotificationEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MapiSyncChangeSet {
    pub(crate) current_change_sequence: u64,
    pub(crate) current_modseq: u64,
    pub(crate) changed_mailbox_ids: Vec<Uuid>,
    pub(crate) changed_message_ids: Vec<Uuid>,
    pub(crate) changed_contact_ids: Vec<Uuid>,
    pub(crate) changed_calendar_event_ids: Vec<Uuid>,
    pub(crate) changed_task_ids: Vec<Uuid>,
    pub(crate) changed_note_ids: Vec<Uuid>,
    pub(crate) changed_journal_entry_ids: Vec<Uuid>,
    pub(crate) changed_conversation_action_ids: Vec<Uuid>,
    pub(crate) deleted_message_ids: Vec<Uuid>,
    pub(crate) deleted_contact_ids: Vec<Uuid>,
    pub(crate) deleted_calendar_event_ids: Vec<Uuid>,
    pub(crate) deleted_task_ids: Vec<Uuid>,
    pub(crate) deleted_note_ids: Vec<Uuid>,
    pub(crate) deleted_journal_entry_ids: Vec<Uuid>,
    pub(crate) deleted_conversation_action_ids: Vec<Uuid>,
}

impl Default for MapiSyncChangeSet {
    fn default() -> Self {
        Self {
            current_change_sequence: 0,
            current_modseq: 1,
            changed_mailbox_ids: Vec::new(),
            changed_message_ids: Vec::new(),
            changed_contact_ids: Vec::new(),
            changed_calendar_event_ids: Vec::new(),
            changed_task_ids: Vec::new(),
            changed_note_ids: Vec::new(),
            changed_journal_entry_ids: Vec::new(),
            changed_conversation_action_ids: Vec::new(),
            deleted_message_ids: Vec::new(),
            deleted_contact_ids: Vec::new(),
            deleted_calendar_event_ids: Vec::new(),
            deleted_task_ids: Vec::new(),
            deleted_note_ids: Vec::new(),
            deleted_journal_entry_ids: Vec::new(),
            deleted_conversation_action_ids: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExchangeAddressBookEntry {
    pub(crate) id: Uuid,
    pub(crate) display_name: String,
    pub(crate) email: String,
    pub(crate) entry_kind: ExchangeAddressBookEntryKind,
    pub(crate) directory_kind: ExchangeAddressBookDirectoryKind,
    pub(crate) member_emails: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MapiContentTableQuery {
    pub(crate) mailbox_id: Uuid,
    pub(crate) position: u64,
    pub(crate) limit: u64,
    pub(crate) sort_orders: Vec<MapiContentTableSort>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MapiContentTableQueryResult {
    pub(crate) ids: Vec<Uuid>,
    pub(crate) total: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MapiContentTableSort {
    pub(crate) field: MapiContentTableSortField,
    pub(crate) descending: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MapiContentTableSortField {
    ReceivedAt,
    Subject,
    SenderName,
    SenderEmail,
    DisplayTo,
    MessageSize,
    HasAttachments,
    MessageFlags,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExchangeAddressBookEntryKind {
    Account,
    Contact,
    DistributionList,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExchangeAddressBookDirectoryKind {
    Person,
    Room,
    Equipment,
}

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
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()>;

    fn set_mapi_calendar_permission<'a>(
        &'a self,
        owner_account_id: Uuid,
        grantee_account_id: Uuid,
        may_read: bool,
        may_write: bool,
        may_delete: bool,
        may_share: bool,
        audit: AuditEntryInput,
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

    fn fetch_address_book_entries<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
    ) -> StoreFuture<'a, Vec<ExchangeAddressBookEntry>>;

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

    fn save_draft_message<'a>(
        &'a self,
        input: SubmitMessageInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, SavedDraftMessage>;

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

impl ExchangeStore for Storage {
    fn fetch_or_allocate_mapi_identities<'a>(
        &'a self,
        account_id: Uuid,
        requests: &'a [MapiIdentityRequest],
    ) -> StoreFuture<'a, Vec<MapiIdentityRecord>> {
        Box::pin(async move {
            let tenant_id = sqlx::query_scalar::<_, Uuid>(
                r#"
                SELECT tenant_id
                FROM accounts
                WHERE id = $1
                LIMIT 1
                "#,
            )
            .bind(account_id)
            .fetch_optional(self.pool())
            .await?
            .ok_or_else(|| anyhow::anyhow!("account not found"))?;

            let mut tx = self.pool().begin().await?;
            sqlx::query(
                r#"
                INSERT INTO mapi_mailbox_replicas (
                    tenant_id,
                    account_id,
                    replica_guid,
                    next_global_counter
                )
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (tenant_id, account_id)
                DO UPDATE SET
                    next_global_counter = GREATEST(
                        mapi_mailbox_replicas.next_global_counter,
                        $4
                    ),
                    updated_at = CASE
                        WHEN mapi_mailbox_replicas.next_global_counter < $4 THEN NOW()
                        ELSE mapi_mailbox_replicas.updated_at
                    END
                "#,
            )
            .bind(tenant_id)
            .bind(account_id)
            .bind(Uuid::from_bytes(crate::mapi::identity::STORE_REPLICA_GUID))
            .bind(crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER as i64)
            .execute(&mut *tx)
            .await?;
            advance_mapi_replica_counter_past_allocated(&mut tx, tenant_id, account_id).await?;
            repair_reserved_mapi_identity_counter_collisions(&mut tx, tenant_id, account_id)
                .await?;
            repair_reserved_mapi_mailbox_identities(&mut tx, tenant_id, account_id).await?;
            repair_invalid_mapi_identity_change_keys(&mut tx, tenant_id, account_id).await?;
            repair_stale_mapi_collaboration_identities(&mut tx, tenant_id, account_id).await?;

            let mut records = Vec::with_capacity(requests.len());
            for request in requests {
                let kind = request.object_kind.as_str();
                let existing = sqlx::query(
                    r#"
                    SELECT mapi_object_id, source_key
                    FROM mapi_object_identities
                    WHERE tenant_id = $1
                      AND account_id = $2
                      AND object_kind = $3
                      AND canonical_id = $4
                      AND deleted_at IS NULL
                    LIMIT 1
                    "#,
                )
                .bind(tenant_id)
                .bind(account_id)
                .bind(kind)
                .bind(request.canonical_id)
                .fetch_optional(&mut *tx)
                .await?;

                let (object_id, source_key) = if let Some(row) = existing {
                    (
                        row.get::<i64, _>("mapi_object_id") as u64,
                        row.get("source_key"),
                    )
                } else {
                    let global_counter = if let Some(counter) = request.reserved_global_counter {
                        counter
                    } else {
                        allocate_next_mapi_global_counter(&mut tx, tenant_id, account_id).await?
                    };
                    let (object_id, default_source_key, change_key, instance_key) =
                        crate::mapi::identity::persisted_identity_material(global_counter);
                    let source_key = request.source_key.clone().unwrap_or(default_source_key);
                    let row = sqlx::query(
                        r#"
                        INSERT INTO mapi_object_identities (
                            tenant_id,
                            account_id,
                            object_kind,
                            canonical_id,
                            mapi_global_counter,
                            mapi_object_id,
                            source_key,
                            change_key,
                            instance_key
                        )
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                        ON CONFLICT (tenant_id, account_id, object_kind, canonical_id)
                        DO UPDATE SET
                            deleted_at = NULL,
                            updated_at = CASE
                                WHEN mapi_object_identities.deleted_at IS NULL
                                THEN mapi_object_identities.updated_at
                                ELSE NOW()
                            END
                        RETURNING mapi_object_id, source_key
                        "#,
                    )
                    .bind(tenant_id)
                    .bind(account_id)
                    .bind(kind)
                    .bind(request.canonical_id)
                    .bind(global_counter as i64)
                    .bind(object_id as i64)
                    .bind(source_key)
                    .bind(change_key)
                    .bind(instance_key)
                    .fetch_one(&mut *tx)
                    .await?;
                    (
                        row.get::<i64, _>("mapi_object_id") as u64,
                        row.get("source_key"),
                    )
                };
                records.push(MapiIdentityRecord {
                    canonical_id: request.canonical_id,
                    object_id,
                    source_key,
                });
            }
            tx.commit().await?;
            Ok(records)
        })
    }

    fn fetch_mapi_identities_by_object_ids<'a>(
        &'a self,
        account_id: Uuid,
        object_ids: &'a [u64],
    ) -> StoreFuture<'a, Vec<MapiIdentityLookupRecord>> {
        Box::pin(async move {
            if object_ids.is_empty() {
                return Ok(Vec::new());
            }
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            let object_ids = object_ids
                .iter()
                .map(|value| *value as i64)
                .collect::<Vec<_>>();
            let rows = sqlx::query(
                r#"
                SELECT object_kind, canonical_id, mapi_object_id, source_key
                FROM mapi_object_identities
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND mapi_object_id = ANY($3)
                  AND deleted_at IS NULL
                "#,
            )
            .bind(&tenant_id)
            .bind(account_id)
            .bind(&object_ids)
            .fetch_all(self.pool())
            .await?;

            rows.into_iter()
                .map(mapi_identity_lookup_from_row)
                .collect()
        })
    }

    fn fetch_mapi_identities_by_source_keys<'a>(
        &'a self,
        account_id: Uuid,
        source_keys: &'a [Vec<u8>],
    ) -> StoreFuture<'a, Vec<MapiIdentityLookupRecord>> {
        Box::pin(async move {
            if source_keys.is_empty() {
                return Ok(Vec::new());
            }
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            let rows = sqlx::query(
                r#"
                SELECT object_kind, canonical_id, mapi_object_id, source_key
                FROM mapi_object_identities
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND source_key = ANY($3)
                  AND deleted_at IS NULL
                "#,
            )
            .bind(&tenant_id)
            .bind(account_id)
            .bind(source_keys)
            .fetch_all(self.pool())
            .await?;

            rows.into_iter()
                .map(mapi_identity_lookup_from_row)
                .collect()
        })
    }

    fn fetch_or_allocate_mapi_named_property_ids<'a>(
        &'a self,
        account_id: Uuid,
        properties: &'a [MapiNamedProperty],
        create: bool,
    ) -> StoreFuture<'a, Vec<Option<MapiNamedPropertyMapping>>> {
        Box::pin(async move {
            if properties.is_empty() {
                return Ok(Vec::new());
            }
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            for _attempt in 0..8 {
                let mut tx = self.pool().begin().await?;
                let mut mappings = Vec::with_capacity(properties.len());
                let mut retry = false;

                for property in properties {
                    let property = normalize_mapi_named_property(property.clone());
                    if let Some(mapping) =
                        fetch_mapi_named_property_in_tx(&mut tx, tenant_id, account_id, &property)
                            .await?
                    {
                        mappings.push(Some(mapping));
                        continue;
                    }
                    if !create {
                        mappings.push(None);
                        continue;
                    }

                    let property_id =
                        allocate_next_mapi_named_property_id(&mut tx, tenant_id, account_id)
                            .await?;
                    match insert_mapi_named_property_in_tx(
                        &mut tx,
                        tenant_id,
                        account_id,
                        property_id,
                        &property,
                    )
                    .await
                    {
                        Ok(()) => mappings.push(Some(MapiNamedPropertyMapping {
                            property_id,
                            property,
                        })),
                        Err(error) if is_unique_violation(&error) => {
                            retry = true;
                            break;
                        }
                        Err(error) => return Err(error),
                    }
                }

                if retry {
                    tx.rollback().await?;
                    continue;
                }

                tx.commit().await?;
                return Ok(mappings);
            }
            Err(anyhow::anyhow!(
                "MAPI named property allocation conflicted repeatedly"
            ))
        })
    }

    fn fetch_mapi_named_properties_by_ids<'a>(
        &'a self,
        account_id: Uuid,
        property_ids: &'a [u16],
    ) -> StoreFuture<'a, Vec<MapiNamedPropertyMapping>> {
        Box::pin(async move {
            if property_ids.is_empty() {
                return Ok(Vec::new());
            }
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            let ids = property_ids
                .iter()
                .map(|id| i32::from(*id))
                .collect::<Vec<_>>();
            let rows = sqlx::query(
                r#"
                SELECT property_id, property_guid, property_kind, property_lid, property_name
                FROM mapi_named_properties
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND property_id = ANY($3)
                "#,
            )
            .bind(tenant_id)
            .bind(account_id)
            .bind(&ids)
            .fetch_all(self.pool())
            .await?;

            rows.into_iter()
                .map(mapi_named_property_mapping_from_row)
                .collect()
        })
    }

    fn fetch_mapi_named_properties<'a>(
        &'a self,
        account_id: Uuid,
        guid: Option<[u8; 16]>,
    ) -> StoreFuture<'a, Vec<MapiNamedPropertyMapping>> {
        Box::pin(async move {
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            let guid = guid.map(Vec::from);
            let rows = sqlx::query(
                r#"
                SELECT property_id, property_guid, property_kind, property_lid, property_name
                FROM mapi_named_properties
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND ($3::bytea IS NULL OR property_guid = $3)
                ORDER BY property_id
                "#,
            )
            .bind(tenant_id)
            .bind(account_id)
            .bind(guid)
            .fetch_all(self.pool())
            .await?;

            rows.into_iter()
                .map(mapi_named_property_mapping_from_row)
                .collect()
        })
    }

    fn upsert_mapi_custom_property_values<'a>(
        &'a self,
        account_id: Uuid,
        object_kind: MapiCustomPropertyObjectKind,
        canonical_id: Uuid,
        values: &'a [MapiCustomPropertyValue],
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            if values.is_empty() {
                return Ok(());
            }
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            let mut tx = self.pool().begin().await?;
            for value in values {
                sqlx::query(
                    r#"
                    INSERT INTO mapi_custom_property_values (
                        tenant_id,
                        account_id,
                        object_kind,
                        canonical_id,
                        property_tag,
                        property_type,
                        property_value
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (
                        tenant_id,
                        account_id,
                        object_kind,
                        canonical_id,
                        property_tag,
                        property_type
                    )
                    DO UPDATE SET
                        property_value = EXCLUDED.property_value,
                        updated_at = NOW()
                    "#,
                )
                .bind(tenant_id)
                .bind(account_id)
                .bind(object_kind.as_str())
                .bind(canonical_id)
                .bind(i64::from(value.property_tag))
                .bind(i32::from(value.property_type))
                .bind(&value.property_value)
                .execute(&mut *tx)
                .await?;
            }
            tx.commit().await?;
            Ok(())
        })
    }

    fn fetch_mapi_custom_property_values<'a>(
        &'a self,
        account_id: Uuid,
        object_kind: MapiCustomPropertyObjectKind,
        canonical_id: Uuid,
        property_tags: &'a [u32],
    ) -> StoreFuture<'a, Vec<MapiCustomPropertyValue>> {
        Box::pin(async move {
            if property_tags.is_empty() {
                return Ok(Vec::new());
            }
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            let tags = property_tags
                .iter()
                .map(|tag| i64::from(*tag))
                .collect::<Vec<_>>();
            let rows = sqlx::query(
                r#"
                SELECT property_tag, property_type, property_value
                FROM mapi_custom_property_values
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND object_kind = $3
                  AND canonical_id = $4
                  AND property_tag = ANY($5)
                ORDER BY property_tag, property_type
                "#,
            )
            .bind(tenant_id)
            .bind(account_id)
            .bind(object_kind.as_str())
            .bind(canonical_id)
            .bind(&tags)
            .fetch_all(self.pool())
            .await?;

            rows.into_iter()
                .map(mapi_custom_property_value_from_row)
                .collect()
        })
    }

    fn delete_mapi_custom_property_values<'a>(
        &'a self,
        account_id: Uuid,
        object_kind: MapiCustomPropertyObjectKind,
        canonical_id: Uuid,
        property_tags: &'a [u32],
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            if property_tags.is_empty() {
                return Ok(());
            }
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            let tags = property_tags
                .iter()
                .map(|tag| i64::from(*tag))
                .collect::<Vec<_>>();
            sqlx::query(
                r#"
                DELETE FROM mapi_custom_property_values
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND object_kind = $3
                  AND canonical_id = $4
                  AND property_tag = ANY($5)
                "#,
            )
            .bind(tenant_id)
            .bind(account_id)
            .bind(object_kind.as_str())
            .bind(canonical_id)
            .bind(&tags)
            .execute(self.pool())
            .await?;
            Ok(())
        })
    }

    fn fetch_mapi_sync_checkpoint<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        checkpoint_kind: MapiCheckpointKind,
    ) -> StoreFuture<'a, Option<MapiSyncCheckpoint>> {
        Box::pin(async move {
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            let row = sqlx::query(
                r#"
                SELECT mailbox_id, checkpoint_kind, last_change_sequence, last_modseq, cursor_json
                FROM mapi_sync_checkpoints
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND checkpoint_kind = $3
                  AND mapi_replica_guid = $4
                  AND (
                      ($5::uuid IS NULL AND mailbox_id IS NULL)
                      OR mailbox_id = $5
                  )
                LIMIT 1
                "#,
            )
            .bind(&tenant_id)
            .bind(account_id)
            .bind(checkpoint_kind.as_str())
            .bind(Uuid::from_bytes(crate::mapi::identity::STORE_REPLICA_GUID))
            .bind(mailbox_id)
            .fetch_optional(self.pool())
            .await?;

            row.map(mapi_sync_checkpoint_from_row).transpose()
        })
    }

    fn store_mapi_sync_checkpoint<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        checkpoint_kind: MapiCheckpointKind,
        last_change_sequence: u64,
        last_modseq: u64,
        cursor_json: serde_json::Value,
    ) -> StoreFuture<'a, MapiSyncCheckpoint> {
        Box::pin(async move {
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            let mut tx = self.pool().begin().await?;
            let existing = sqlx::query(
                r#"
                SELECT id, mailbox_id, checkpoint_kind, last_change_sequence, last_modseq, cursor_json
                FROM mapi_sync_checkpoints
                WHERE tenant_id = $1
                  AND account_id = $2
                  AND checkpoint_kind = $3
                  AND mapi_replica_guid = $4
                  AND (
                      ($5::uuid IS NULL AND mailbox_id IS NULL)
                      OR mailbox_id = $5
                  )
                LIMIT 1
                "#,
            )
            .bind(&tenant_id)
            .bind(account_id)
            .bind(checkpoint_kind.as_str())
            .bind(Uuid::from_bytes(crate::mapi::identity::STORE_REPLICA_GUID))
            .bind(mailbox_id)
            .fetch_optional(&mut *tx)
            .await?;
            if let Some(existing) = existing.as_ref() {
                let existing_change_sequence =
                    existing.get::<i64, _>("last_change_sequence").max(0) as u64;
                let existing_modseq = existing.get::<i64, _>("last_modseq").max(0) as u64;
                if existing_change_sequence > last_change_sequence
                    || (existing_change_sequence == last_change_sequence
                        && existing_modseq > last_modseq)
                {
                    let checkpoint = MapiSyncCheckpoint {
                        mailbox_id: existing.get::<Option<Uuid>, _>("mailbox_id"),
                        checkpoint_kind,
                        last_change_sequence: existing_change_sequence,
                        last_modseq: existing_modseq,
                        cursor_json: existing.get("cursor_json"),
                    };
                    tx.commit().await?;
                    return Ok(checkpoint);
                }
            }
            let existing_id = existing.as_ref().map(|row| row.get::<Uuid, _>("id"));
            let row = sqlx::query(
                if existing_id.is_some() {
                    r#"
                    UPDATE mapi_sync_checkpoints
                    SET
                        last_change_sequence = $7,
                        last_modseq = $8,
                        cursor_json = $9,
                        updated_at = NOW(),
                        expires_at = NOW() + INTERVAL '30 days'
                    WHERE id = $1
                    RETURNING mailbox_id, checkpoint_kind, last_change_sequence, last_modseq, cursor_json
                    "#
                } else {
                    r#"
                    INSERT INTO mapi_sync_checkpoints (
                        id, tenant_id, account_id, mailbox_id, checkpoint_kind,
                        mapi_replica_guid, last_change_sequence, last_modseq,
                        cursor_json, expires_at
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW() + INTERVAL '30 days')
                    RETURNING mailbox_id, checkpoint_kind, last_change_sequence, last_modseq, cursor_json
                    "#
                },
            )
            .bind(existing_id.unwrap_or_else(Uuid::new_v4))
            .bind(&tenant_id)
            .bind(account_id)
            .bind(mailbox_id)
            .bind(checkpoint_kind.as_str())
            .bind(Uuid::from_bytes(crate::mapi::identity::STORE_REPLICA_GUID))
            .bind(last_change_sequence as i64)
            .bind(last_modseq as i64)
            .bind(cursor_json)
            .fetch_one(&mut *tx)
            .await?;
            tx.commit().await?;

            mapi_sync_checkpoint_from_row(row)
        })
    }

    fn fetch_mapi_ipm_subtree_ost_id<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move { Storage::fetch_mapi_ipm_subtree_ost_id(self, account_id).await })
    }

    fn store_mapi_ipm_subtree_ost_id<'a>(
        &'a self,
        account_id: Uuid,
        ost_id: &'a [u8],
    ) -> StoreFuture<'a, ()> {
        Box::pin(
            async move { Storage::store_mapi_ipm_subtree_ost_id(self, account_id, ost_id).await },
        )
    }

    fn fetch_mapi_sync_changes<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        checkpoint_kind: MapiCheckpointKind,
        after_change_sequence: u64,
    ) -> StoreFuture<'a, MapiSyncChangeSet> {
        Box::pin(async move {
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            let cursor = sqlx::query(
                r#"
                SELECT
                    COALESCE(MAX(cursor), 0) AS current_change_sequence,
                    COALESCE(MAX(modseq), 1) AS current_modseq
                FROM mail_change_log
                WHERE tenant_id = $1
                  AND (account_id = $2 OR affected_principal_ids @> ARRAY[$2]::uuid[])
                  AND (retained_until IS NULL OR retained_until > NOW())
                "#,
            )
            .bind(&tenant_id)
            .bind(account_id)
            .fetch_one(self.pool())
            .await?;
            let mut changes = MapiSyncChangeSet {
                current_change_sequence: cursor.get::<i64, _>("current_change_sequence") as u64,
                current_modseq: cursor.get::<i64, _>("current_modseq") as u64,
                ..Default::default()
            };
            let special_object_kind =
                mapi_special_object_kind_for_checkpoint_mailbox(checkpoint_kind, mailbox_id);

            let rows = sqlx::query(
                r#"
                SELECT object_kind, object_id, mailbox_id, change_kind, summary_json
                FROM mail_change_log
                WHERE tenant_id = $1
                  AND cursor > $2
                  AND (account_id = $3 OR affected_principal_ids @> ARRAY[$3]::uuid[])
                  AND (retained_until IS NULL OR retained_until > NOW())
                  AND (
                    ($4 = 'hierarchy' AND object_kind = 'mailbox')
                    OR (
                        $4 IN ('content', 'read_state')
                        AND (
                            (
                                object_kind IN ('mailbox_message', 'attachment')
                                AND ($5::uuid IS NULL OR mailbox_id = $5 OR mailbox_id IS NULL)
                            )
                            OR ($5::uuid IS NULL AND object_kind IN (
                                'contact',
                                'calendar_event',
                                'task',
                                'note',
                                'journal_entry',
                                'conversation_action'
                            ))
                            OR ($6::text IS NOT NULL AND object_kind = $6)
                        )
                    )
                  )
                ORDER BY cursor ASC
                LIMIT 1000
                "#,
            )
            .bind(&tenant_id)
            .bind(after_change_sequence as i64)
            .bind(account_id)
            .bind(checkpoint_kind.as_str())
            .bind(mailbox_id)
            .bind(special_object_kind)
            .fetch_all(self.pool())
            .await?;

            for row in rows {
                let object_kind = row.get::<String, _>("object_kind");
                let change_kind = row.get::<String, _>("change_kind");
                let summary_json = row.get::<serde_json::Value, _>("summary_json");
                match object_kind.as_str() {
                    "mailbox" => {
                        let object_id = row.get::<Uuid, _>("object_id");
                        if change_kind == "destroyed" || change_kind == "expunged" {
                            continue;
                        }
                        push_unique_uuid(&mut changes.changed_mailbox_ids, object_id);
                    }
                    "mailbox_message" | "attachment" => {
                        let Some(message_id) = summary_json
                            .get("messageId")
                            .and_then(serde_json::Value::as_str)
                            .and_then(|value| Uuid::parse_str(value).ok())
                        else {
                            continue;
                        };
                        if change_kind == "destroyed" || change_kind == "expunged" {
                            push_unique_uuid(&mut changes.deleted_message_ids, message_id);
                        } else {
                            push_unique_uuid(&mut changes.changed_message_ids, message_id);
                        }
                    }
                    "contact" => {
                        let object_id = row.get::<Uuid, _>("object_id");
                        if change_kind == "destroyed" || change_kind == "expunged" {
                            push_unique_uuid(&mut changes.deleted_contact_ids, object_id);
                        } else {
                            push_unique_uuid(&mut changes.changed_contact_ids, object_id);
                        }
                    }
                    "calendar_event" => {
                        let object_id = row.get::<Uuid, _>("object_id");
                        if change_kind == "destroyed" || change_kind == "expunged" {
                            push_unique_uuid(&mut changes.deleted_calendar_event_ids, object_id);
                        } else {
                            push_unique_uuid(&mut changes.changed_calendar_event_ids, object_id);
                        }
                    }
                    "task" => {
                        let object_id = row.get::<Uuid, _>("object_id");
                        if change_kind == "destroyed" || change_kind == "expunged" {
                            push_unique_uuid(&mut changes.deleted_task_ids, object_id);
                        } else {
                            push_unique_uuid(&mut changes.changed_task_ids, object_id);
                        }
                    }
                    "note" => {
                        let object_id = row.get::<Uuid, _>("object_id");
                        if change_kind == "destroyed" || change_kind == "expunged" {
                            push_unique_uuid(&mut changes.deleted_note_ids, object_id);
                        } else {
                            push_unique_uuid(&mut changes.changed_note_ids, object_id);
                        }
                    }
                    "journal_entry" => {
                        let object_id = row.get::<Uuid, _>("object_id");
                        if change_kind == "destroyed" || change_kind == "expunged" {
                            push_unique_uuid(&mut changes.deleted_journal_entry_ids, object_id);
                        } else {
                            push_unique_uuid(&mut changes.changed_journal_entry_ids, object_id);
                        }
                    }
                    "conversation_action" => {
                        let object_id = row.get::<Uuid, _>("object_id");
                        if change_kind == "destroyed" || change_kind == "expunged" {
                            push_unique_uuid(
                                &mut changes.deleted_conversation_action_ids,
                                object_id,
                            );
                        } else {
                            push_unique_uuid(
                                &mut changes.changed_conversation_action_ids,
                                object_id,
                            );
                        }
                    }
                    _ => {}
                }
            }

            if checkpoint_kind != MapiCheckpointKind::Hierarchy {
                let tombstones = sqlx::query(
                    r#"
                    SELECT message_id
                    FROM tombstones
                    WHERE tenant_id = $1
                      AND account_id = $2
                      AND object_kind = 'mailbox_message'
                      AND change_cursor > $3
                      AND ($4::uuid IS NULL OR mailbox_id = $4)
                      AND message_id IS NOT NULL
                      AND (retained_until IS NULL OR retained_until > NOW())
                    ORDER BY change_cursor ASC
                    LIMIT 1000
                    "#,
                )
                .bind(&tenant_id)
                .bind(account_id)
                .bind(after_change_sequence as i64)
                .bind(mailbox_id)
                .fetch_all(self.pool())
                .await?;
                for row in tombstones {
                    push_unique_uuid(&mut changes.deleted_message_ids, row.get("message_id"));
                }
                let collaboration_tombstones = sqlx::query(
                    r#"
                    SELECT object_kind, object_id
                    FROM tombstones
                    WHERE tenant_id = $1
                      AND account_id = $2
                      AND object_kind IN (
                          'contact',
                          'calendar_event',
                          'task',
                          'note',
                          'journal_entry',
                          'conversation_action'
                      )
                      AND change_cursor > $3
                      AND ($4::uuid IS NULL OR object_kind = $5)
                      AND (retained_until IS NULL OR retained_until > NOW())
                    ORDER BY change_cursor ASC
                    LIMIT 1000
                    "#,
                )
                .bind(&tenant_id)
                .bind(account_id)
                .bind(after_change_sequence as i64)
                .bind(mailbox_id)
                .bind(special_object_kind)
                .fetch_all(self.pool())
                .await?;
                for row in collaboration_tombstones {
                    match row.get::<String, _>("object_kind").as_str() {
                        "contact" => {
                            push_unique_uuid(&mut changes.deleted_contact_ids, row.get("object_id"))
                        }
                        "calendar_event" => push_unique_uuid(
                            &mut changes.deleted_calendar_event_ids,
                            row.get("object_id"),
                        ),
                        "task" => {
                            push_unique_uuid(&mut changes.deleted_task_ids, row.get("object_id"))
                        }
                        "note" => {
                            push_unique_uuid(&mut changes.deleted_note_ids, row.get("object_id"))
                        }
                        "journal_entry" => push_unique_uuid(
                            &mut changes.deleted_journal_entry_ids,
                            row.get("object_id"),
                        ),
                        "conversation_action" => push_unique_uuid(
                            &mut changes.deleted_conversation_action_ids,
                            row.get("object_id"),
                        ),
                        _ => {}
                    }
                }
            }

            Ok(changes)
        })
    }

    fn fetch_mapi_folder_permissions<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<MapiFolderPermission>> {
        Box::pin(async move {
            let row = sqlx::query(
                r#"
                SELECT tenant_id, primary_email, display_name
                FROM accounts
                WHERE id = $1
                LIMIT 1
                "#,
            )
            .bind(account_id)
            .fetch_optional(self.pool())
            .await?
            .ok_or_else(|| anyhow::anyhow!("account not found"))?;
            let principal = lpe_mail_auth::AccountPrincipal {
                tenant_id: row.get("tenant_id"),
                account_id,
                email: row.get("primary_email"),
                display_name: row.get("display_name"),
            };
            let mut permissions = mailbox_ids
                .iter()
                .copied()
                .map(|mailbox_id| owner_permission(mailbox_id, &principal))
                .collect::<Vec<_>>();
            if mailbox_ids.is_empty() {
                return Ok(permissions);
            }

            let rows = sqlx::query(
                r#"
                SELECT
                    g.mailbox_id,
                    g.grantee_account_id,
                    grantee.display_name,
                    g.may_read,
                    g.may_write,
                    g.may_delete,
                    g.may_share
                FROM mailbox_delegation_grants g
                JOIN accounts grantee
                  ON grantee.tenant_id = g.tenant_id
                 AND grantee.id = g.grantee_account_id
                WHERE g.tenant_id = $1
                  AND g.mailbox_id = ANY($2)
                ORDER BY lower(grantee.primary_email) ASC
                "#,
            )
            .bind(principal.tenant_id)
            .bind(mailbox_ids)
            .fetch_all(self.pool())
            .await?;

            permissions.extend(rows.into_iter().map(|row| MapiFolderPermission {
                mailbox_id: row.get("mailbox_id"),
                member_account_id: Some(row.get("grantee_account_id")),
                member_name: row.get("display_name"),
                rights: rights_from_grant(
                    row.get("may_read"),
                    row.get("may_write"),
                    row.get("may_delete"),
                    row.get("may_share"),
                ),
            }));
            Ok(permissions)
        })
    }

    fn set_mapi_folder_permission<'a>(
        &'a self,
        owner_account_id: Uuid,
        mailbox_id: Uuid,
        grantee_account_id: Uuid,
        may_read: bool,
        may_write: bool,
        may_delete: bool,
        may_share: bool,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            self.set_mailbox_folder_delegation_grant(
                MailboxFolderDelegationGrantInput {
                    owner_account_id,
                    mailbox_id,
                    grantee_account_id,
                    may_read,
                    may_write,
                    may_delete,
                    may_share,
                },
                audit,
            )
            .await
        })
    }

    fn set_mapi_calendar_permission<'a>(
        &'a self,
        owner_account_id: Uuid,
        grantee_account_id: Uuid,
        may_read: bool,
        may_write: bool,
        may_delete: bool,
        may_share: bool,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            if !may_read {
                return self
                    .delete_collaboration_grant(
                        owner_account_id,
                        CollaborationResourceKind::Calendar,
                        grantee_account_id,
                        audit,
                    )
                    .await;
            }
            let tenant_id = mapi_tenant_id_for_account(self, owner_account_id).await?;
            let grantee_email = sqlx::query_scalar::<_, String>(
                r#"
                SELECT primary_email
                FROM accounts
                WHERE tenant_id = $1
                  AND id = $2
                LIMIT 1
                "#,
            )
            .bind(tenant_id)
            .bind(grantee_account_id)
            .fetch_optional(self.pool())
            .await?
            .ok_or_else(|| anyhow::anyhow!("calendar permission grantee account not found"))?;
            self.upsert_collaboration_grant(
                CollaborationGrantInput {
                    kind: CollaborationResourceKind::Calendar,
                    owner_account_id,
                    grantee_email,
                    may_read,
                    may_write,
                    may_delete,
                    may_share,
                },
                audit,
            )
            .await
            .map(|_| ())
        })
    }

    fn fetch_mapi_notification_cursor<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Option<i64>> {
        Box::pin(async move {
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            sqlx::query_scalar::<_, Option<i64>>(
                r#"
                SELECT MAX(cursor)
                FROM mail_change_log
                WHERE tenant_id = $1
                  AND (account_id = $2 OR affected_principal_ids @> ARRAY[$2]::uuid[])
                  AND (retained_until IS NULL OR retained_until > NOW())
                "#,
            )
            .bind(tenant_id)
            .bind(account_id)
            .fetch_one(self.pool())
            .await
            .map_err(Into::into)
        })
    }

    fn fetch_public_folder_trees<'a>(
        &'a self,
        principal_account_id: Uuid,
    ) -> StoreFuture<'a, Vec<PublicFolderTree>> {
        Box::pin(
            async move { Storage::fetch_public_folder_trees(self, principal_account_id).await },
        )
    }

    fn fetch_public_folder<'a>(
        &'a self,
        principal_account_id: Uuid,
        folder_id: Uuid,
    ) -> StoreFuture<'a, PublicFolder> {
        Box::pin(async move {
            Storage::fetch_public_folder(self, principal_account_id, folder_id).await
        })
    }

    fn fetch_public_folder_children<'a>(
        &'a self,
        principal_account_id: Uuid,
        folder_id: Uuid,
    ) -> StoreFuture<'a, Vec<PublicFolder>> {
        Box::pin(async move {
            Storage::fetch_public_folder_children(self, principal_account_id, folder_id).await
        })
    }

    fn create_public_folder_child<'a>(
        &'a self,
        input: CreatePublicFolderInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, PublicFolder> {
        Box::pin(async move { Storage::create_public_folder_child(self, input, audit).await })
    }

    fn delete_public_folder<'a>(
        &'a self,
        principal_account_id: Uuid,
        folder_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            Storage::delete_public_folder(self, principal_account_id, folder_id, audit).await
        })
    }

    fn fetch_public_folder_items<'a>(
        &'a self,
        principal_account_id: Uuid,
        folder_id: Uuid,
    ) -> StoreFuture<'a, Vec<PublicFolderItem>> {
        Box::pin(async move {
            Storage::fetch_public_folder_items(self, principal_account_id, folder_id).await
        })
    }

    fn fetch_public_folder_items_by_ids<'a>(
        &'a self,
        principal_account_id: Uuid,
        item_ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<PublicFolderItem>> {
        Box::pin(async move {
            Storage::fetch_public_folder_items_by_ids(self, principal_account_id, item_ids).await
        })
    }

    fn fetch_public_folder_permissions<'a>(
        &'a self,
        principal_account_id: Uuid,
        folder_id: Uuid,
    ) -> StoreFuture<'a, Vec<PublicFolderPermission>> {
        Box::pin(async move {
            Storage::fetch_public_folder_permissions(self, principal_account_id, folder_id).await
        })
    }

    fn fetch_public_folder_replicas<'a>(
        &'a self,
        principal_account_id: Uuid,
        folder_id: Uuid,
    ) -> StoreFuture<'a, Vec<PublicFolderReplica>> {
        Box::pin(async move {
            Storage::fetch_public_folder_replicas(self, principal_account_id, folder_id).await
        })
    }

    fn upsert_public_folder_permission<'a>(
        &'a self,
        input: PublicFolderPermissionInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, PublicFolderPermission> {
        Box::pin(async move { Storage::upsert_public_folder_permission(self, input, audit).await })
    }

    fn delete_public_folder_permission<'a>(
        &'a self,
        principal_account_id: Uuid,
        folder_id: Uuid,
        grantee_account_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            Storage::delete_public_folder_permission(
                self,
                principal_account_id,
                folder_id,
                grantee_account_id,
                audit,
            )
            .await
        })
    }

    fn upsert_public_folder_item<'a>(
        &'a self,
        input: UpsertPublicFolderItemInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, PublicFolderItem> {
        Box::pin(async move { Storage::upsert_public_folder_item(self, input, audit).await })
    }

    fn delete_public_folder_item<'a>(
        &'a self,
        principal_account_id: Uuid,
        folder_id: Uuid,
        item_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            Storage::delete_public_folder_item(
                self,
                principal_account_id,
                folder_id,
                item_id,
                audit,
            )
            .await
        })
    }

    fn fetch_public_folder_per_user_state<'a>(
        &'a self,
        principal_account_id: Uuid,
        folder_id: Uuid,
    ) -> StoreFuture<'a, Vec<PublicFolderPerUserState>> {
        Box::pin(async move {
            Storage::fetch_public_folder_per_user_state(self, principal_account_id, folder_id).await
        })
    }

    fn patch_public_folder_per_user_state<'a>(
        &'a self,
        principal_account_id: Uuid,
        folder_id: Uuid,
        patches: &'a [PublicFolderPerUserStatePatch],
    ) -> StoreFuture<'a, Vec<PublicFolderPerUserState>> {
        Box::pin(async move {
            Storage::patch_public_folder_per_user_state(
                self,
                principal_account_id,
                folder_id,
                patches,
            )
            .await
        })
    }

    fn poll_mapi_notifications<'a>(
        &'a self,
        account_id: Uuid,
        after_cursor: i64,
    ) -> StoreFuture<'a, MapiNotificationPoll> {
        Box::pin(async move {
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            let current_cursor = sqlx::query_scalar::<_, Option<i64>>(
                r#"
                SELECT MAX(cursor)
                FROM mail_change_log
                WHERE tenant_id = $1
                  AND (account_id = $2 OR affected_principal_ids @> ARRAY[$2]::uuid[])
                  AND (retained_until IS NULL OR retained_until > NOW())
                "#,
            )
            .bind(tenant_id)
            .bind(account_id)
            .fetch_one(self.pool())
            .await?;
            let rows = sqlx::query(
                r#"
                SELECT
                    log.cursor,
                    log.object_kind,
                    log.object_id,
                    log.mailbox_id,
                    log.change_kind,
                    log.modseq,
                    log.summary_json,
                    scope_box.display_name AS scope_display_name,
                    scope_box.role AS scope_role,
                    scope_box.total_messages AS scope_total_messages,
                    scope_box.unread_messages AS scope_unread_messages,
                    object_box.display_name AS object_display_name,
                    object_box.role AS object_role,
                    object_box.parent_mailbox_id AS object_parent_id,
                    object_box.total_messages AS object_total_messages,
                    object_box.unread_messages AS object_unread_messages,
                    parent_box.display_name AS parent_display_name,
                    parent_box.role AS parent_role,
                    source_box.display_name AS source_display_name,
                    message.normalized_subject AS message_subject,
                    scope_identity.mapi_object_id AS scope_mapi_object_id,
                    object_identity.mapi_object_id AS object_mapi_object_id,
                    parent_identity.mapi_object_id AS parent_mapi_object_id,
                    message_identity.mapi_object_id AS message_mapi_object_id,
                    source_identity.mapi_object_id AS source_mapi_object_id
                FROM mail_change_log log
                LEFT JOIN mailboxes scope_box
                  ON scope_box.tenant_id = log.tenant_id
                 AND scope_box.account_id = log.account_id
                 AND scope_box.id = log.mailbox_id
                LEFT JOIN mailboxes object_box
                  ON object_box.tenant_id = log.tenant_id
                 AND object_box.account_id = log.account_id
                 AND object_box.id = log.object_id
                 AND log.object_kind = 'mailbox'
                LEFT JOIN mailboxes parent_box
                  ON parent_box.tenant_id = object_box.tenant_id
                 AND parent_box.account_id = object_box.account_id
                 AND parent_box.id = object_box.parent_mailbox_id
                LEFT JOIN mailboxes source_box
                  ON source_box.tenant_id = log.tenant_id
                 AND source_box.account_id = log.account_id
                 AND source_box.id = (log.summary_json->>'sourceMailboxId')::uuid
                LEFT JOIN messages message
                  ON message.tenant_id = log.tenant_id
                 AND message.id = (log.summary_json->>'messageId')::uuid
                LEFT JOIN mapi_object_identities scope_identity
                  ON scope_identity.tenant_id = log.tenant_id
                 AND scope_identity.account_id = log.account_id
                 AND scope_identity.object_kind = 'mailbox'
                 AND scope_identity.canonical_id = log.mailbox_id
                 AND scope_identity.deleted_at IS NULL
                LEFT JOIN mapi_object_identities object_identity
                  ON object_identity.tenant_id = log.tenant_id
                 AND object_identity.account_id = log.account_id
                 AND object_identity.object_kind = 'mailbox'
                 AND object_identity.canonical_id = log.object_id
                 AND object_identity.deleted_at IS NULL
                LEFT JOIN mapi_object_identities parent_identity
                  ON parent_identity.tenant_id = log.tenant_id
                 AND parent_identity.account_id = log.account_id
                 AND parent_identity.object_kind = 'mailbox'
                 AND parent_identity.canonical_id = object_box.parent_mailbox_id
                 AND parent_identity.deleted_at IS NULL
                LEFT JOIN mapi_object_identities message_identity
                  ON message_identity.tenant_id = log.tenant_id
                 AND message_identity.account_id = log.account_id
                 AND message_identity.object_kind = 'message'
                 AND message_identity.canonical_id = (log.summary_json->>'messageId')::uuid
                 AND message_identity.deleted_at IS NULL
                LEFT JOIN mapi_object_identities source_identity
                  ON source_identity.tenant_id = log.tenant_id
                 AND source_identity.account_id = log.account_id
                 AND source_identity.object_kind = 'mailbox'
                 AND source_identity.canonical_id = (log.summary_json->>'sourceMailboxId')::uuid
                 AND source_identity.deleted_at IS NULL
                WHERE log.tenant_id = $1
                  AND log.cursor > $2
                  AND (log.account_id = $3 OR log.affected_principal_ids @> ARRAY[$3]::uuid[])
                  AND (log.retained_until IS NULL OR log.retained_until > NOW())
                  AND log.object_kind IN ('mailbox', 'mailbox_message', 'attachment')
                ORDER BY log.cursor ASC
                LIMIT 101
                "#,
            )
            .bind(tenant_id)
            .bind(after_cursor)
            .bind(account_id)
            .fetch_all(self.pool())
            .await?;
            let truncated = rows.len() > 100;
            let mut cursor = None;
            let mut events = Vec::new();
            for row in rows.into_iter().take(100) {
                cursor = Some(row.get("cursor"));
                if let Some(event) = mapi_notification_event_from_change_row(row) {
                    events.push(event);
                }
            }
            let cursor = cursor.or(current_cursor);
            Ok(MapiNotificationPoll {
                event_pending: truncated || !events.is_empty(),
                cursor,
                events,
            })
        })
    }

    fn fetch_address_book_entries<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
    ) -> StoreFuture<'a, Vec<ExchangeAddressBookEntry>> {
        Box::pin(async move {
            let tenant_id = sqlx::query_scalar::<_, Uuid>(
                r#"
                SELECT tenant_id
                FROM accounts
                WHERE tenant_id = $1
                  AND id = $2
                LIMIT 1
                "#,
            )
            .bind(principal.tenant_id)
            .bind(principal.account_id)
            .fetch_optional(self.pool())
            .await?
            .ok_or_else(|| anyhow::anyhow!("account not found"))?;
            let account_rows = sqlx::query(
                r#"
                SELECT id, primary_email, display_name, directory_kind
                FROM accounts
                WHERE tenant_id = $1
                  AND status = 'active'
                  AND gal_visibility = 'tenant'
                ORDER BY lower(display_name) ASC, lower(primary_email) ASC, id ASC
            "#,
            )
            .bind(tenant_id)
            .fetch_all(self.pool())
            .await?;

            let mut entries = account_rows
                .into_iter()
                .map(|row| ExchangeAddressBookEntry {
                    id: row.get("id"),
                    display_name: row.get("display_name"),
                    email: row.get("primary_email"),
                    entry_kind: ExchangeAddressBookEntryKind::Account,
                    directory_kind: directory_kind_from_storage(row.get("directory_kind")),
                    member_emails: Vec::new(),
                })
                .collect::<Vec<_>>();

            entries.extend(
                self.fetch_accessible_contacts(principal.account_id)
                    .await?
                    .into_iter()
                    .filter(|contact| {
                        !contact.email.trim().is_empty() || !contact.name.trim().is_empty()
                    })
                    .map(|contact| ExchangeAddressBookEntry {
                        id: contact.id,
                        display_name: contact.name,
                        email: contact.email,
                        entry_kind: ExchangeAddressBookEntryKind::Contact,
                        directory_kind: ExchangeAddressBookDirectoryKind::Person,
                        member_emails: Vec::new(),
                    }),
            );
            let group_rows = sqlx::query(
                r#"
                SELECT id, source, target
                FROM aliases
                WHERE tenant_id = $1
                  AND kind = 'group'
                  AND status = 'active'
                ORDER BY lower(source) ASC, id ASC
                "#,
            )
            .bind(tenant_id)
            .fetch_all(self.pool())
            .await?;
            entries.extend(group_rows.into_iter().map(|row| {
                let source: String = row.get("source");
                let target: String = row.get("target");
                ExchangeAddressBookEntry {
                    id: row.get("id"),
                    display_name: address_book_group_display_name(&source, &target),
                    email: source,
                    entry_kind: ExchangeAddressBookEntryKind::DistributionList,
                    directory_kind: ExchangeAddressBookDirectoryKind::Person,
                    member_emails: vec![target],
                }
            }));
            entries.sort_by(|left, right| {
                left.display_name
                    .to_ascii_lowercase()
                    .cmp(&right.display_name.to_ascii_lowercase())
                    .then_with(|| {
                        left.email
                            .to_ascii_lowercase()
                            .cmp(&right.email.to_ascii_lowercase())
                    })
                    .then_with(|| (left.entry_kind as u8).cmp(&(right.entry_kind as u8)))
                    .then_with(|| left.id.cmp(&right.id))
            });
            Ok(entries)
        })
    }

    fn fetch_accessible_contact_collections<'a>(
        &'a self,
        principal_account_id: Uuid,
    ) -> StoreFuture<'a, Vec<CollaborationCollection>> {
        Box::pin(async move {
            self.fetch_accessible_contact_collections(principal_account_id)
                .await
        })
    }

    fn fetch_accessible_calendar_collections<'a>(
        &'a self,
        principal_account_id: Uuid,
    ) -> StoreFuture<'a, Vec<CollaborationCollection>> {
        Box::pin(async move {
            self.fetch_accessible_calendar_collections(principal_account_id)
                .await
        })
    }

    fn fetch_accessible_task_collections<'a>(
        &'a self,
        principal_account_id: Uuid,
    ) -> StoreFuture<'a, Vec<CollaborationCollection>> {
        Box::pin(async move {
            self.fetch_accessible_task_collections(principal_account_id)
                .await
        })
    }

    fn fetch_delegate_freebusy_messages<'a>(
        &'a self,
        principal_account_id: Uuid,
    ) -> StoreFuture<'a, Vec<DelegateFreeBusyMessageObject>> {
        Box::pin(async move {
            self.fetch_delegate_freebusy_messages(principal_account_id, None)
                .await
        })
    }

    fn fetch_accessible_contacts_in_collection<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: &'a str,
    ) -> StoreFuture<'a, Vec<AccessibleContact>> {
        Box::pin(async move {
            self.fetch_accessible_contacts_in_collection(principal_account_id, collection_id)
                .await
        })
    }

    fn fetch_contact_sync_versions<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: &'a str,
    ) -> StoreFuture<'a, Vec<(Uuid, String)>> {
        Box::pin(async move {
            let contacts = self
                .fetch_accessible_contacts_in_collection(principal_account_id, collection_id)
                .await?;
            let ids = contacts
                .iter()
                .map(|contact| contact.id)
                .collect::<Vec<_>>();
            if ids.is_empty() {
                return Ok(Vec::new());
            }
            let rows = sqlx::query(
                r#"
                SELECT
                    id,
                    to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS updated_at
                FROM contacts
                WHERE id = ANY($1)
                "#,
            )
            .bind(&ids)
            .fetch_all(self.pool())
            .await?;
            Ok(rows
                .into_iter()
                .map(|row| (row.get("id"), row.get("updated_at")))
                .collect())
        })
    }

    fn fetch_accessible_events_in_collection<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: &'a str,
    ) -> StoreFuture<'a, Vec<AccessibleEvent>> {
        Box::pin(async move {
            self.fetch_accessible_events_in_collection(principal_account_id, collection_id)
                .await
        })
    }

    fn fetch_event_sync_versions<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: &'a str,
    ) -> StoreFuture<'a, Vec<(Uuid, String)>> {
        Box::pin(async move {
            let events = self
                .fetch_accessible_events_in_collection(principal_account_id, collection_id)
                .await?;
            let ids = events.iter().map(|event| event.id).collect::<Vec<_>>();
            if ids.is_empty() {
                return Ok(Vec::new());
            }
            let rows = sqlx::query(
                r#"
                SELECT
                    id,
                    to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS updated_at
                FROM calendar_events
                WHERE id = ANY($1)
                "#,
            )
            .bind(&ids)
            .fetch_all(self.pool())
            .await?;
            Ok(rows
                .into_iter()
                .map(|row| (row.get("id"), row.get("updated_at")))
                .collect())
        })
    }

    fn fetch_accessible_tasks_in_collection<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: &'a str,
    ) -> StoreFuture<'a, Vec<ClientTask>> {
        Box::pin(async move {
            let tasks = self.fetch_client_tasks(principal_account_id).await?;
            Ok(tasks
                .into_iter()
                .filter(|task| task_matches_collection(task, collection_id))
                .collect())
        })
    }

    fn fetch_task_sync_versions<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: &'a str,
    ) -> StoreFuture<'a, Vec<(Uuid, String)>> {
        Box::pin(async move {
            let tasks = self.fetch_client_tasks(principal_account_id).await?;
            Ok(tasks
                .into_iter()
                .filter(|task| task_matches_collection(task, collection_id))
                .map(|task| (task.id, task.updated_at))
                .collect())
        })
    }

    fn fetch_accessible_contacts_by_ids<'a>(
        &'a self,
        principal_account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<AccessibleContact>> {
        Box::pin(async move {
            self.fetch_accessible_contacts_by_ids(principal_account_id, ids)
                .await
        })
    }

    fn create_accessible_contact<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: Option<&'a str>,
        input: UpsertClientContactInput,
    ) -> StoreFuture<'a, AccessibleContact> {
        Box::pin(async move {
            self.create_accessible_contact(principal_account_id, collection_id, input)
                .await
        })
    }

    fn update_accessible_contact<'a>(
        &'a self,
        principal_account_id: Uuid,
        contact_id: Uuid,
        input: UpsertClientContactInput,
    ) -> StoreFuture<'a, AccessibleContact> {
        Box::pin(async move {
            self.update_accessible_contact(principal_account_id, contact_id, input)
                .await
        })
    }

    fn delete_accessible_contact<'a>(
        &'a self,
        principal_account_id: Uuid,
        contact_id: Uuid,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            self.delete_accessible_contact(principal_account_id, contact_id)
                .await
        })
    }

    fn fetch_accessible_events_by_ids<'a>(
        &'a self,
        principal_account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<AccessibleEvent>> {
        Box::pin(async move {
            self.fetch_accessible_events_by_ids(principal_account_id, ids)
                .await
        })
    }

    fn create_accessible_event<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: Option<&'a str>,
        input: UpsertClientEventInput,
    ) -> StoreFuture<'a, AccessibleEvent> {
        Box::pin(async move {
            self.create_accessible_event(principal_account_id, collection_id, input)
                .await
        })
    }

    fn update_accessible_event<'a>(
        &'a self,
        principal_account_id: Uuid,
        event_id: Uuid,
        input: UpsertClientEventInput,
    ) -> StoreFuture<'a, AccessibleEvent> {
        Box::pin(async move {
            self.update_accessible_event(principal_account_id, event_id, input)
                .await
        })
    }

    fn update_accessible_event_reminder<'a>(
        &'a self,
        principal_account_id: Uuid,
        event_id: Uuid,
        reminder_set: Option<bool>,
        reminder_at: Option<String>,
        reminder_dismissed_at: Option<String>,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            self.update_accessible_event_reminder(
                principal_account_id,
                event_id,
                reminder_set,
                reminder_at,
                reminder_dismissed_at,
            )
            .await
        })
    }

    fn delete_accessible_event<'a>(
        &'a self,
        principal_account_id: Uuid,
        event_id: Uuid,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            self.delete_accessible_event(principal_account_id, event_id)
                .await
        })
    }

    fn fetch_accessible_tasks_by_ids<'a>(
        &'a self,
        principal_account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<ClientTask>> {
        Box::pin(async move {
            self.fetch_client_tasks_by_ids(principal_account_id, ids)
                .await
        })
    }

    fn fetch_mapi_notes<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<ClientNote>> {
        Box::pin(async move { self.fetch_client_notes(account_id).await })
    }

    fn fetch_mapi_notes_by_ids<'a>(
        &'a self,
        account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<ClientNote>> {
        Box::pin(async move { self.fetch_client_notes_by_ids(account_id, ids).await })
    }

    fn fetch_mapi_journal_entries<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Vec<JournalEntry>> {
        Box::pin(async move { self.fetch_journal_entries(account_id).await })
    }

    fn fetch_mapi_journal_entries_by_ids<'a>(
        &'a self,
        account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<JournalEntry>> {
        Box::pin(async move { self.fetch_journal_entries_by_ids(account_id, ids).await })
    }

    fn upsert_mapi_note<'a>(&'a self, input: UpsertClientNoteInput) -> StoreFuture<'a, ClientNote> {
        Box::pin(async move { self.upsert_client_note(input).await })
    }

    fn upsert_mapi_journal_entry<'a>(
        &'a self,
        input: UpsertJournalEntryInput,
    ) -> StoreFuture<'a, JournalEntry> {
        Box::pin(async move { self.upsert_journal_entry(input).await })
    }

    fn delete_mapi_note<'a>(&'a self, account_id: Uuid, note_id: Uuid) -> StoreFuture<'a, ()> {
        Box::pin(async move { self.delete_client_note(account_id, note_id).await })
    }

    fn delete_mapi_journal_entry<'a>(
        &'a self,
        account_id: Uuid,
        entry_id: Uuid,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move { self.delete_journal_entry(account_id, entry_id).await })
    }

    fn fetch_active_sieve_script<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Option<SieveScriptDocument>> {
        Box::pin(async move { self.fetch_active_sieve_script(account_id).await })
    }

    fn list_mailbox_rules<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<MailboxRule>> {
        Box::pin(async move { self.list_mailbox_rules(account_id).await })
    }

    fn put_sieve_script<'a>(
        &'a self,
        account_id: Uuid,
        name: &'a str,
        content: &'a str,
        activate: bool,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, SieveScriptDocument> {
        Box::pin(async move {
            self.put_sieve_script(account_id, name, content, activate, audit)
                .await
        })
    }

    fn set_active_sieve_script<'a>(
        &'a self,
        account_id: Uuid,
        name: Option<&'a str>,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, Option<String>> {
        Box::pin(async move { self.set_active_sieve_script(account_id, name, audit).await })
    }

    fn delete_sieve_script<'a>(
        &'a self,
        account_id: Uuid,
        name: &'a str,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move { self.delete_sieve_script(account_id, name, audit).await })
    }

    fn create_accessible_task<'a>(
        &'a self,
        _principal_account_id: Uuid,
        input: UpsertClientTaskInput,
    ) -> StoreFuture<'a, ClientTask> {
        Box::pin(async move { self.upsert_client_task(input).await })
    }

    fn update_accessible_task<'a>(
        &'a self,
        _principal_account_id: Uuid,
        task_id: Uuid,
        mut input: UpsertClientTaskInput,
    ) -> StoreFuture<'a, ClientTask> {
        Box::pin(async move {
            input.id = Some(task_id);
            self.upsert_client_task(input).await
        })
    }

    fn update_accessible_task_reminder<'a>(
        &'a self,
        principal_account_id: Uuid,
        task_id: Uuid,
        reminder_set: Option<bool>,
        reminder_at: Option<String>,
        reminder_dismissed_at: Option<String>,
        reminder_reset: Option<bool>,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            self.update_accessible_task_reminder(
                principal_account_id,
                task_id,
                reminder_set,
                reminder_at,
                reminder_dismissed_at,
                reminder_reset,
            )
            .await
        })
    }

    fn delete_accessible_task<'a>(
        &'a self,
        principal_account_id: Uuid,
        task_id: Uuid,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move { self.delete_client_task(principal_account_id, task_id).await })
    }

    fn fetch_jmap_mailboxes<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<JmapMailbox>> {
        Box::pin(async move { self.fetch_jmap_mailboxes(account_id).await })
    }

    fn ensure_jmap_system_mailboxes<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Vec<JmapMailbox>> {
        Box::pin(async move { self.ensure_imap_mailboxes(account_id).await })
    }

    fn fetch_search_folders<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Vec<SearchFolderDefinition>> {
        Box::pin(async move { self.fetch_search_folders(account_id).await })
    }

    fn upsert_search_folder<'a>(
        &'a self,
        input: UpsertSearchFolderInput,
    ) -> StoreFuture<'a, SearchFolderDefinition> {
        Box::pin(async move { self.upsert_search_folder(input).await })
    }

    fn fetch_conversation_actions<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Vec<ConversationAction>> {
        Box::pin(async move { self.fetch_conversation_actions(account_id).await })
    }

    fn fetch_mapi_navigation_shortcuts<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Vec<MapiNavigationShortcutRecord>> {
        Box::pin(async move {
            let tenant_id = mapi_tenant_id_for_account(self, account_id).await?;
            let rows = sqlx::query(
                r#"
                SELECT id, account_id, subject, target_folder_id, shortcut_type,
                       flags, section, ordinal, group_header_id, group_name
                FROM mapi_navigation_shortcuts
                WHERE tenant_id = $1 AND account_id = $2
                ORDER BY section, ordinal, subject, id
                "#,
            )
            .bind(tenant_id)
            .bind(account_id)
            .fetch_all(self.pool())
            .await?;

            rows.into_iter()
                .map(mapi_navigation_shortcut_from_row)
                .collect()
        })
    }

    fn upsert_mapi_navigation_shortcut<'a>(
        &'a self,
        input: UpsertMapiNavigationShortcutInput,
    ) -> StoreFuture<'a, MapiNavigationShortcutRecord> {
        Box::pin(async move {
            let tenant_id = mapi_tenant_id_for_account(self, input.account_id).await?;
            let id = input.id.unwrap_or_else(Uuid::new_v4);
            let row = sqlx::query(
                r#"
                INSERT INTO mapi_navigation_shortcuts (
                    tenant_id, id, account_id, subject, target_folder_id,
                    shortcut_type, flags, section, ordinal, group_header_id, group_name
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (tenant_id, id)
                DO UPDATE SET
                    subject = EXCLUDED.subject,
                    target_folder_id = EXCLUDED.target_folder_id,
                    shortcut_type = EXCLUDED.shortcut_type,
                    flags = EXCLUDED.flags,
                    section = EXCLUDED.section,
                    ordinal = EXCLUDED.ordinal,
                    group_header_id = EXCLUDED.group_header_id,
                    group_name = EXCLUDED.group_name,
                    updated_at = NOW()
                RETURNING id, account_id, subject, target_folder_id, shortcut_type,
                          flags, section, ordinal, group_header_id, group_name
                "#,
            )
            .bind(tenant_id)
            .bind(id)
            .bind(input.account_id)
            .bind(input.subject)
            .bind(input.target_folder_id.map(|value| value as i64))
            .bind(input.shortcut_type as i64)
            .bind(input.flags as i64)
            .bind(input.section as i64)
            .bind(input.ordinal as i64)
            .bind(input.group_header_id)
            .bind(input.group_name)
            .fetch_one(self.pool())
            .await?;

            mapi_navigation_shortcut_from_row(row)
        })
    }

    fn upsert_conversation_action<'a>(
        &'a self,
        input: UpsertConversationActionInput,
    ) -> StoreFuture<'a, ConversationAction> {
        Box::pin(async move { self.upsert_conversation_action(input).await })
    }

    fn delete_conversation_action<'a>(
        &'a self,
        account_id: Uuid,
        conversation_action_id: Uuid,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            self.delete_conversation_action(account_id, conversation_action_id)
                .await
        })
    }

    fn query_client_reminders<'a>(
        &'a self,
        account_id: Uuid,
        query: ReminderQuery,
    ) -> StoreFuture<'a, Vec<ClientReminder>> {
        Box::pin(async move { self.query_client_reminders(account_id, query).await })
    }

    fn create_jmap_mailbox<'a>(
        &'a self,
        input: JmapMailboxCreateInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapMailbox> {
        Box::pin(async move { self.create_jmap_mailbox(input, audit).await })
    }

    fn update_jmap_mailbox<'a>(
        &'a self,
        input: JmapMailboxUpdateInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapMailbox> {
        Box::pin(async move { self.update_jmap_mailbox(input, audit).await })
    }

    fn destroy_jmap_mailbox<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            self.destroy_jmap_mailbox(account_id, mailbox_id, audit)
                .await
        })
    }

    fn query_jmap_email_ids<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        search_text: Option<&'a str>,
        position: u64,
        limit: u64,
    ) -> StoreFuture<'a, JmapEmailQuery> {
        Box::pin(async move {
            self.query_jmap_email_ids(account_id, mailbox_id, search_text, position, limit)
                .await
        })
    }

    fn query_mapi_content_table_ids<'a>(
        &'a self,
        account_id: Uuid,
        query: MapiContentTableQuery,
    ) -> StoreFuture<'a, MapiContentTableQueryResult> {
        Box::pin(async move {
            let tenant_id = sqlx::query_scalar::<_, Uuid>(
                r#"
                SELECT tenant_id
                FROM accounts
                WHERE id = $1
                LIMIT 1
                "#,
            )
            .bind(account_id)
            .fetch_optional(self.pool())
            .await?
            .ok_or_else(|| anyhow::anyhow!("account not found"))?;

            let total = sqlx::query_scalar::<_, i64>(
                r#"
                SELECT COUNT(DISTINCT mm.message_id)
                FROM mailbox_messages mm
                WHERE mm.tenant_id = $1
                  AND mm.account_id = $2
                  AND mm.mailbox_id = $3
                  AND mm.visibility = 'visible'
                "#,
            )
            .bind(tenant_id)
            .bind(account_id)
            .bind(query.mailbox_id)
            .fetch_one(self.pool())
            .await?;

            let order_by = mapi_content_table_order_by(&query.sort_orders);
            let sql = format!(
                r#"
                WITH row_source AS (
                    SELECT
                        m.id,
                        m.received_at,
                        lower(COALESCE(m.normalized_subject, '')) AS subject_key,
                        lower(COALESCE(fr.display_name, fr.address, '')) AS sender_name_key,
                        lower(COALESCE(fr.address, '')) AS sender_email_key,
                        lower(COALESCE(to_rollup.display_to, '')) AS display_to_key,
                        m.size_octets,
                        m.has_attachments,
                        ((CASE WHEN mm.is_seen THEN 1 ELSE 0 END)
                            + (CASE WHEN m.has_attachments THEN 16 ELSE 0 END)) AS message_flags
                    FROM mailbox_messages mm
                    JOIN messages m
                      ON m.tenant_id = mm.tenant_id
                     AND m.id = mm.message_id
                    LEFT JOIN message_recipients fr
                      ON fr.tenant_id = m.tenant_id
                     AND fr.message_id = m.id
                     AND fr.role = 'from'
                    LEFT JOIN LATERAL (
                        SELECT string_agg(COALESCE(NULLIF(r.display_name, ''), r.address), '; ' ORDER BY r.ordinal) AS display_to
                        FROM message_recipients r
                        WHERE r.tenant_id = m.tenant_id
                          AND r.message_id = m.id
                          AND r.role = 'to'
                    ) to_rollup ON TRUE
                    WHERE mm.tenant_id = $1
                      AND mm.account_id = $2
                      AND mm.mailbox_id = $3
                      AND mm.visibility = 'visible'
                )
                SELECT id
                FROM row_source
                ORDER BY {order_by}
                OFFSET $4
                LIMIT $5
                "#
            );
            let ids = sqlx::query(&sql)
                .bind(tenant_id)
                .bind(account_id)
                .bind(query.mailbox_id)
                .bind(query.position as i64)
                .bind(query.limit as i64)
                .fetch_all(self.pool())
                .await?
                .into_iter()
                .map(|row| row.try_get("id"))
                .collect::<std::result::Result<Vec<Uuid>, sqlx::Error>>()?;

            Ok(MapiContentTableQueryResult {
                ids,
                total: total.max(0) as u64,
            })
        })
    }

    fn list_recoverable_items<'a>(
        &'a self,
        account_id: Uuid,
        recoverable_folder: Option<&'a str>,
    ) -> StoreFuture<'a, Vec<RecoverableItem>> {
        Box::pin(async move {
            self.list_recoverable_items(account_id, recoverable_folder)
                .await
        })
    }

    fn restore_recoverable_item<'a>(
        &'a self,
        account_id: Uuid,
        recoverable_item_id: Uuid,
        target_mailbox_id: Option<Uuid>,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail> {
        Box::pin(async move {
            self.restore_recoverable_item(account_id, recoverable_item_id, target_mailbox_id, audit)
                .await
        })
    }

    fn purge_recoverable_item<'a>(
        &'a self,
        account_id: Uuid,
        recoverable_item_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            self.purge_recoverable_item(account_id, recoverable_item_id, audit)
                .await
        })
    }

    fn fetch_all_jmap_email_ids<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<Uuid>> {
        Box::pin(async move { self.fetch_all_jmap_email_ids(account_id).await })
    }

    fn fetch_jmap_emails<'a>(
        &'a self,
        account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<JmapEmail>> {
        Box::pin(async move { self.fetch_jmap_emails(account_id, ids).await })
    }

    fn fetch_jmap_emails_with_protected_bcc<'a>(
        &'a self,
        account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<JmapEmail>> {
        Box::pin(async move {
            self.fetch_jmap_emails_with_protected_bcc(account_id, ids)
                .await
        })
    }

    fn fetch_message_attachments<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
    ) -> StoreFuture<'a, Vec<ActiveSyncAttachment>> {
        Box::pin(async move {
            self.fetch_activesync_message_attachments(account_id, message_id)
                .await
        })
    }

    fn fetch_calendar_attachments_for_events<'a>(
        &'a self,
        account_id: Uuid,
        event_ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<(Uuid, Vec<CalendarEventAttachment>)>> {
        Box::pin(async move {
            self.fetch_calendar_attachments_for_events(account_id, event_ids)
                .await
        })
    }

    fn fetch_attachment_content<'a>(
        &'a self,
        account_id: Uuid,
        file_reference: &'a str,
    ) -> StoreFuture<'a, Option<ActiveSyncAttachmentContent>> {
        Box::pin(async move {
            if let Some(content) = self
                .fetch_activesync_attachment_content(account_id, file_reference)
                .await?
            {
                return Ok(Some(content));
            }
            self.fetch_calendar_attachment_content(account_id, file_reference)
                .await
        })
    }

    fn add_message_attachment<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
        attachment: AttachmentUploadInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, Option<(JmapEmail, ActiveSyncAttachment)>> {
        Box::pin(async move {
            self.add_message_attachment(account_id, message_id, attachment, audit)
                .await
        })
    }

    fn add_calendar_event_attachment<'a>(
        &'a self,
        account_id: Uuid,
        event_id: Uuid,
        attachment: AttachmentUploadInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, Option<CalendarEventAttachment>> {
        Box::pin(async move {
            self.add_calendar_event_attachment(account_id, event_id, attachment, audit)
                .await
        })
    }

    fn delete_message_attachment<'a>(
        &'a self,
        account_id: Uuid,
        file_reference: &'a str,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, Option<JmapEmail>> {
        Box::pin(async move {
            self.delete_message_attachment(account_id, file_reference, audit)
                .await
        })
    }

    fn import_jmap_email<'a>(
        &'a self,
        input: JmapImportedEmailInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail> {
        Box::pin(async move { self.import_jmap_email(input, audit).await })
    }

    fn move_jmap_email<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail> {
        Box::pin(async move {
            self.move_jmap_email(account_id, message_id, target_mailbox_id, audit)
                .await
        })
    }

    fn move_jmap_email_from_mailbox<'a>(
        &'a self,
        account_id: Uuid,
        source_mailbox_id: Uuid,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail> {
        Box::pin(async move {
            self.move_jmap_email_from_mailbox(
                account_id,
                source_mailbox_id,
                message_id,
                target_mailbox_id,
                audit,
            )
            .await
        })
    }

    fn copy_jmap_email<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail> {
        Box::pin(async move {
            self.copy_jmap_email(account_id, message_id, target_mailbox_id, audit)
                .await
        })
    }

    fn update_jmap_email_flags<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
        unread: Option<bool>,
        flagged: Option<bool>,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail> {
        Box::pin(async move {
            self.update_jmap_email_flags(account_id, message_id, unread, flagged, audit)
                .await
        })
    }

    fn update_jmap_email_followup_flags<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
        update: JmapEmailFollowupUpdate,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail> {
        Box::pin(async move {
            self.update_jmap_email_followup_flags(account_id, message_id, update, audit)
                .await
        })
    }

    fn delete_jmap_email<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move { self.delete_jmap_email(account_id, message_id, audit).await })
    }

    fn delete_jmap_email_from_mailbox<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Uuid,
        message_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            self.delete_jmap_email_from_mailbox(account_id, mailbox_id, message_id, audit)
                .await
        })
    }

    fn save_draft_message<'a>(
        &'a self,
        input: SubmitMessageInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, SavedDraftMessage> {
        Box::pin(async move { self.save_draft_message(input, audit).await })
    }

    fn submit_message<'a>(
        &'a self,
        input: SubmitMessageInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, SubmittedMessage> {
        Box::pin(async move { self.submit_message(input, audit).await })
    }

    fn cancel_queued_submission<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, CancelSubmissionResult> {
        Box::pin(async move {
            self.cancel_queued_submission(account_id, message_id, audit)
                .await
        })
    }
}

fn mapi_special_object_kind_for_checkpoint_mailbox(
    checkpoint_kind: MapiCheckpointKind,
    mailbox_id: Option<Uuid>,
) -> Option<&'static str> {
    if checkpoint_kind == MapiCheckpointKind::Hierarchy {
        return None;
    }
    let mailbox_id = mailbox_id?;
    let matches_virtual_folder = |folder_id| {
        crate::mapi_mailstore::virtual_special_mailbox(folder_id)
            .map(|mailbox| mailbox.id == mailbox_id)
            .unwrap_or(false)
    };
    if [
        crate::mapi::identity::CONTACTS_FOLDER_ID,
        crate::mapi::identity::SUGGESTED_CONTACTS_FOLDER_ID,
        crate::mapi::identity::QUICK_CONTACTS_FOLDER_ID,
        crate::mapi::identity::IM_CONTACT_LIST_FOLDER_ID,
        crate::mapi::identity::CONTACTS_SEARCH_FOLDER_ID,
    ]
    .into_iter()
    .any(matches_virtual_folder)
    {
        return Some("contact");
    }
    if matches_virtual_folder(crate::mapi::identity::CALENDAR_FOLDER_ID) {
        return Some("calendar_event");
    }
    if [
        crate::mapi::identity::TASKS_FOLDER_ID,
        crate::mapi::identity::TODO_SEARCH_FOLDER_ID,
        crate::mapi::identity::REMINDERS_FOLDER_ID,
    ]
    .into_iter()
    .any(matches_virtual_folder)
    {
        return Some("task");
    }
    if matches_virtual_folder(crate::mapi::identity::NOTES_FOLDER_ID) {
        return Some("note");
    }
    if matches_virtual_folder(crate::mapi::identity::JOURNAL_FOLDER_ID) {
        return Some("journal_entry");
    }
    if matches_virtual_folder(crate::mapi::identity::CONVERSATION_ACTION_SETTINGS_FOLDER_ID) {
        return Some("conversation_action");
    }
    None
}

async fn advance_mapi_replica_counter_past_allocated(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    account_id: Uuid,
) -> Result<()> {
    let next_counter = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT GREATEST(
            COALESCE(MAX(mapi_global_counter), $3 - 1) + 1,
            $3
        )
        FROM mapi_object_identities
        WHERE tenant_id = $1
          AND account_id = $2
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER as i64)
    .fetch_one(&mut **tx)
    .await?;

    sqlx::query(
        r#"
        UPDATE mapi_mailbox_replicas
        SET next_global_counter = GREATEST(next_global_counter, $3),
            updated_at = CASE
                WHEN next_global_counter < $3 THEN NOW()
                ELSE updated_at
            END
        WHERE tenant_id = $1
          AND account_id = $2
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(next_counter)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn repair_reserved_mapi_identity_counter_collisions(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    account_id: Uuid,
) -> Result<()> {
    let rows = sqlx::query(
        r#"
        SELECT identities.object_kind, identities.canonical_id, identities.mapi_global_counter,
               mailboxes.role
        FROM mapi_object_identities identities
        LEFT JOIN mailboxes
          ON mailboxes.tenant_id = identities.tenant_id
         AND mailboxes.account_id = identities.account_id
         AND mailboxes.id = identities.canonical_id
         AND identities.object_kind = 'mailbox'
        WHERE identities.tenant_id = $1
          AND identities.account_id = $2
          AND identities.mapi_global_counter >= $3
          AND identities.mapi_global_counter < $4
        ORDER BY identities.mapi_global_counter, identities.created_at, identities.canonical_id
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(crate::mapi::identity::JOURNAL_FOLDER_COUNTER as i64)
    .bind(crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER as i64)
    .fetch_all(&mut **tx)
    .await?;

    for row in rows {
        let object_kind = row.get::<String, _>("object_kind");
        let role = row.try_get::<String, _>("role").ok();
        let current_counter = row.get::<i64, _>("mapi_global_counter") as u64;
        if object_kind == "mailbox"
            && role
                .as_deref()
                .and_then(crate::mapi_store::reserved_folder_counter_for_role)
                == Some(current_counter)
        {
            continue;
        }

        let global_counter = allocate_next_mapi_global_counter(tx, tenant_id, account_id).await?;
        let (object_id, source_key, change_key, instance_key) =
            crate::mapi::identity::persisted_identity_material(global_counter);

        sqlx::query(
            r#"
            UPDATE mapi_object_identities
            SET mapi_global_counter = $5,
                mapi_object_id = $6,
                source_key = $7,
                change_key = $8,
                instance_key = $9,
                updated_at = NOW()
            WHERE tenant_id = $1
              AND account_id = $2
              AND object_kind = $3
              AND canonical_id = $4
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(object_kind)
        .bind(row.get::<Uuid, _>("canonical_id"))
        .bind(global_counter as i64)
        .bind(object_id as i64)
        .bind(source_key)
        .bind(change_key)
        .bind(instance_key)
        .execute(&mut **tx)
        .await?;
    }

    Ok(())
}

async fn repair_reserved_mapi_mailbox_identities(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    account_id: Uuid,
) -> Result<()> {
    let rows = sqlx::query(
        r#"
        SELECT identities.canonical_id, identities.mapi_global_counter, mailboxes.role
        FROM mapi_object_identities identities
        JOIN mailboxes
          ON mailboxes.tenant_id = identities.tenant_id
         AND mailboxes.account_id = identities.account_id
         AND mailboxes.id = identities.canonical_id
        WHERE identities.tenant_id = $1
          AND identities.account_id = $2
          AND identities.object_kind = 'mailbox'
          AND identities.deleted_at IS NULL
        ORDER BY identities.created_at, identities.canonical_id
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .fetch_all(&mut **tx)
    .await?;

    for row in rows {
        let role = row.get::<String, _>("role");
        let Some(reserved_counter) = crate::mapi_store::reserved_folder_counter_for_role(&role)
        else {
            continue;
        };
        let current_counter = row.get::<i64, _>("mapi_global_counter") as u64;
        if current_counter == reserved_counter {
            continue;
        }

        let canonical_id = row.get::<Uuid, _>("canonical_id");
        let (object_id, source_key, change_key, instance_key) =
            crate::mapi::identity::persisted_identity_material(reserved_counter);

        sqlx::query(
            r#"
            UPDATE mapi_object_identities
            SET mapi_global_counter = $5,
                mapi_object_id = $6,
                source_key = $7,
                change_key = $8,
                instance_key = $9,
                updated_at = NOW()
            WHERE tenant_id = $1
              AND account_id = $2
              AND object_kind = 'mailbox'
              AND canonical_id = $3
              AND mapi_global_counter = $4
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(canonical_id)
        .bind(current_counter as i64)
        .bind(reserved_counter as i64)
        .bind(object_id as i64)
        .bind(source_key)
        .bind(change_key)
        .bind(instance_key)
        .execute(&mut **tx)
        .await?;
    }

    Ok(())
}

async fn allocate_next_mapi_global_counter(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    account_id: Uuid,
) -> Result<u64> {
    let next = sqlx::query_scalar::<_, i64>(
        r#"
        UPDATE mapi_mailbox_replicas
        SET next_global_counter = next_global_counter + 1,
            updated_at = NOW()
        WHERE tenant_id = $1
          AND account_id = $2
        RETURNING next_global_counter - 1
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .fetch_one(&mut **tx)
    .await?;

    Ok(next as u64)
}

async fn fetch_mapi_named_property_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    account_id: Uuid,
    property: &MapiNamedProperty,
) -> Result<Option<MapiNamedPropertyMapping>> {
    let (property_kind, property_lid, property_name) = mapi_named_property_parts(property);
    let row = sqlx::query(
        r#"
        SELECT property_id, property_guid, property_kind, property_lid, property_name
        FROM mapi_named_properties
        WHERE tenant_id = $1
          AND account_id = $2
          AND property_guid = $3
          AND property_kind = $4
          AND (
              ($4 = 'lid' AND property_lid = $5)
              OR ($4 = 'name' AND property_name = $6)
          )
        LIMIT 1
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(property.guid.to_vec())
    .bind(property_kind)
    .bind(property_lid)
    .bind(property_name)
    .fetch_optional(&mut **tx)
    .await?;

    row.map(mapi_named_property_mapping_from_row).transpose()
}

async fn insert_mapi_named_property_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    account_id: Uuid,
    property_id: u16,
    property: &MapiNamedProperty,
) -> Result<()> {
    let (property_kind, property_lid, property_name) = mapi_named_property_parts(property);
    sqlx::query(
        r#"
        INSERT INTO mapi_named_properties (
            tenant_id,
            account_id,
            property_id,
            property_guid,
            property_kind,
            property_lid,
            property_name
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(i32::from(property_id))
    .bind(property.guid.to_vec())
    .bind(property_kind)
    .bind(property_lid)
    .bind(property_name)
    .execute(&mut **tx)
    .await?;

    Ok(())
}

async fn allocate_next_mapi_named_property_id(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    account_id: Uuid,
) -> Result<u16> {
    let existing = sqlx::query_scalar::<_, i32>(
        r#"
        SELECT property_id
        FROM mapi_named_properties
        WHERE tenant_id = $1
          AND account_id = $2
        ORDER BY property_id
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .fetch_all(&mut **tx)
    .await?;
    let existing = existing
        .into_iter()
        .filter_map(|id| u16::try_from(id).ok())
        .collect::<std::collections::HashSet<_>>();
    for property_id in crate::mapi::properties::FIRST_NAMED_PROPERTY_ID
        ..=crate::mapi::properties::MAX_NAMED_PROPERTY_ID
    {
        if existing.contains(&property_id) || is_reserved_named_property_id(property_id) {
            continue;
        }
        return Ok(property_id);
    }
    anyhow::bail!("MAPI named property id space exhausted");
}

fn mapi_named_property_parts(
    property: &MapiNamedProperty,
) -> (&'static str, Option<i32>, Option<&str>) {
    match &property.kind {
        MapiNamedPropertyKind::Lid(lid) => ("lid", Some(*lid as i32), None),
        MapiNamedPropertyKind::Name(name) => ("name", None, Some(name.as_str())),
    }
}

const MAPI_PS_INTERNET_HEADERS_GUID: [u8; 16] = [
    0x86, 0x03, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x46,
];

fn normalize_mapi_named_property(mut property: MapiNamedProperty) -> MapiNamedProperty {
    if property.guid == MAPI_PS_INTERNET_HEADERS_GUID {
        if let MapiNamedPropertyKind::Name(name) = property.kind {
            property.kind = MapiNamedPropertyKind::Name(name.to_ascii_lowercase());
        }
    }
    property
}

fn is_unique_violation(error: &anyhow::Error) -> bool {
    error
        .downcast_ref::<sqlx::Error>()
        .and_then(|error| match error {
            sqlx::Error::Database(database_error) => database_error.code(),
            _ => None,
        })
        .as_deref()
        == Some("23505")
}

fn mapi_named_property_mapping_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<MapiNamedPropertyMapping> {
    let guid: Vec<u8> = row.get("property_guid");
    let guid: [u8; 16] = guid
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid MAPI named property GUID length"))?;
    let property_kind: String = row.get("property_kind");
    let kind = match property_kind.as_str() {
        "lid" => MapiNamedPropertyKind::Lid(row.get::<i32, _>("property_lid") as u32),
        "name" => MapiNamedPropertyKind::Name(row.get::<String, _>("property_name")),
        value => anyhow::bail!("unsupported MAPI named property kind: {value}"),
    };
    Ok(MapiNamedPropertyMapping {
        property_id: row.get::<i32, _>("property_id") as u16,
        property: MapiNamedProperty { guid, kind },
    })
}

#[allow(dead_code)]
fn mapi_custom_property_value_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<MapiCustomPropertyValue> {
    Ok(MapiCustomPropertyValue {
        property_tag: row.get::<i64, _>("property_tag") as u32,
        property_type: row.get::<i32, _>("property_type") as u16,
        property_value: row.get("property_value"),
    })
}

fn mapi_navigation_shortcut_from_row(
    row: sqlx::postgres::PgRow,
) -> Result<MapiNavigationShortcutRecord> {
    Ok(MapiNavigationShortcutRecord {
        id: row.try_get("id")?,
        account_id: row.try_get("account_id")?,
        subject: row.try_get("subject")?,
        target_folder_id: row
            .try_get::<Option<i64>, _>("target_folder_id")?
            .map(|value| value as u64),
        shortcut_type: row.try_get::<i64, _>("shortcut_type")? as u32,
        flags: row.try_get::<i64, _>("flags")? as u32,
        section: row.try_get::<i64, _>("section")? as u32,
        ordinal: row.try_get::<i64, _>("ordinal")? as u32,
        group_header_id: row.try_get("group_header_id")?,
        group_name: row.try_get("group_name")?,
    })
}

async fn repair_invalid_mapi_identity_change_keys(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    account_id: Uuid,
) -> Result<()> {
    let rows = sqlx::query(
        r#"
        SELECT object_kind, canonical_id, mapi_global_counter
        FROM mapi_object_identities
        WHERE tenant_id = $1
          AND account_id = $2
          AND octet_length(change_key) <> 22
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .fetch_all(&mut **tx)
    .await?;

    for row in rows {
        let global_counter = row.get::<i64, _>("mapi_global_counter") as u64;
        let (_, _, change_key, _) =
            crate::mapi::identity::persisted_identity_material(global_counter);
        sqlx::query(
            r#"
            UPDATE mapi_object_identities
            SET change_key = $5,
                updated_at = NOW()
            WHERE tenant_id = $1
              AND account_id = $2
              AND object_kind = $3
              AND canonical_id = $4
            "#,
        )
        .bind(tenant_id)
        .bind(account_id)
        .bind(row.get::<String, _>("object_kind"))
        .bind(row.get::<Uuid, _>("canonical_id"))
        .bind(change_key)
        .execute(&mut **tx)
        .await?;
    }

    Ok(())
}

async fn repair_stale_mapi_collaboration_identities(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    tenant_id: Uuid,
    account_id: Uuid,
) -> Result<()> {
    let contact_count = sqlx::query(
        r#"
        UPDATE mapi_object_identities identity
        SET deleted_at = NOW(),
            updated_at = NOW()
        WHERE identity.tenant_id = $1
          AND identity.account_id = $2
          AND identity.object_kind = 'contact'
          AND identity.deleted_at IS NULL
          AND NOT EXISTS (
              SELECT 1
              FROM contacts contact
              WHERE contact.tenant_id = identity.tenant_id
                AND contact.owner_account_id = identity.account_id
                AND contact.id = identity.canonical_id
          )
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .execute(&mut **tx)
    .await?
    .rows_affected();
    let calendar_event_count = sqlx::query(
        r#"
        UPDATE mapi_object_identities identity
        SET deleted_at = NOW(),
            updated_at = NOW()
        WHERE identity.tenant_id = $1
          AND identity.account_id = $2
          AND identity.object_kind = 'calendar_event'
          AND identity.deleted_at IS NULL
          AND NOT EXISTS (
              SELECT 1
              FROM calendar_events event
              WHERE event.tenant_id = identity.tenant_id
                AND event.owner_account_id = identity.account_id
                AND event.id = identity.canonical_id
          )
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .execute(&mut **tx)
    .await?
    .rows_affected();
    let task_count = sqlx::query(
        r#"
        UPDATE mapi_object_identities identity
        SET deleted_at = NOW(),
            updated_at = NOW()
        WHERE identity.tenant_id = $1
          AND identity.account_id = $2
          AND identity.object_kind = 'task'
          AND identity.deleted_at IS NULL
          AND NOT EXISTS (
              SELECT 1
              FROM tasks task
              WHERE task.tenant_id = identity.tenant_id
                AND task.owner_account_id = identity.account_id
                AND task.id = identity.canonical_id
          )
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .execute(&mut **tx)
    .await?
    .rows_affected();

    let total_count = contact_count + calendar_event_count + task_count;
    if total_count > 0 {
        tracing::info!(
            rca_debug = true,
            adapter = "mapi",
            account_id = %account_id,
            repaired_stale_contact_identity_count = contact_count,
            repaired_stale_calendar_event_identity_count = calendar_event_count,
            repaired_stale_task_identity_count = task_count,
            repaired_stale_collaboration_identity_count = total_count,
            message = "rca debug mapi repaired stale collaboration identities",
        );
    }

    Ok(())
}

fn mapi_content_table_order_by(sort_orders: &[MapiContentTableSort]) -> String {
    if sort_orders.is_empty() {
        return "received_at DESC, id DESC".to_string();
    }

    let mut clauses = sort_orders
        .iter()
        .map(|sort| {
            let column = match sort.field {
                MapiContentTableSortField::ReceivedAt => "received_at",
                MapiContentTableSortField::Subject => "subject_key",
                MapiContentTableSortField::SenderName => "sender_name_key",
                MapiContentTableSortField::SenderEmail => "sender_email_key",
                MapiContentTableSortField::DisplayTo => "display_to_key",
                MapiContentTableSortField::MessageSize => "size_octets",
                MapiContentTableSortField::HasAttachments => "has_attachments",
                MapiContentTableSortField::MessageFlags => "message_flags",
            };
            let direction = if sort.descending { "DESC" } else { "ASC" };
            format!("{column} {direction}")
        })
        .collect::<Vec<_>>();
    clauses.push("id DESC".to_string());
    clauses.join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mapi_content_table_order_by_uses_projected_columns() {
        let sort_fields = [
            MapiContentTableSortField::ReceivedAt,
            MapiContentTableSortField::Subject,
            MapiContentTableSortField::SenderName,
            MapiContentTableSortField::SenderEmail,
            MapiContentTableSortField::DisplayTo,
            MapiContentTableSortField::MessageSize,
            MapiContentTableSortField::HasAttachments,
            MapiContentTableSortField::MessageFlags,
        ];
        let sort_orders = sort_fields
            .into_iter()
            .map(|field| MapiContentTableSort {
                field,
                descending: false,
            })
            .collect::<Vec<_>>();

        let order_by = mapi_content_table_order_by(&sort_orders);

        assert!(!order_by.contains("mm."));
        assert!(!order_by.contains("m."));
        assert!(order_by.contains("message_flags ASC"));
        assert!(order_by.ends_with("id DESC"));
    }
}

fn task_matches_collection(task: &ClientTask, collection_id: &str) -> bool {
    matches!(collection_id, "tasks" | "default") || task.task_list_id.to_string() == collection_id
}

fn directory_kind_from_storage(value: String) -> ExchangeAddressBookDirectoryKind {
    match value.as_str() {
        "room" => ExchangeAddressBookDirectoryKind::Room,
        "equipment" => ExchangeAddressBookDirectoryKind::Equipment,
        _ => ExchangeAddressBookDirectoryKind::Person,
    }
}

fn address_book_group_display_name(source: &str, target: &str) -> String {
    let target = target.trim();
    if !target.is_empty() && !target.eq_ignore_ascii_case(source.trim()) {
        return target.to_string();
    }
    source
        .split_once('@')
        .map(|(local_part, _)| local_part)
        .filter(|local_part| !local_part.trim().is_empty())
        .unwrap_or(source)
        .to_string()
}

async fn mapi_tenant_id_for_account(storage: &Storage, account_id: Uuid) -> Result<Uuid> {
    sqlx::query_scalar::<_, Uuid>(
        r#"
        SELECT tenant_id
        FROM accounts
        WHERE id = $1
        LIMIT 1
        "#,
    )
    .bind(account_id)
    .fetch_optional(storage.pool())
    .await?
    .ok_or_else(|| anyhow::anyhow!("account not found"))
}

fn mapi_identity_lookup_from_row(row: sqlx::postgres::PgRow) -> Result<MapiIdentityLookupRecord> {
    let object_kind = match row.get::<String, _>("object_kind").as_str() {
        "account" => MapiIdentityObjectKind::Account,
        "mailbox" => MapiIdentityObjectKind::Mailbox,
        "message" => MapiIdentityObjectKind::Message,
        "contact" => MapiIdentityObjectKind::Contact,
        "calendar_event" => MapiIdentityObjectKind::CalendarEvent,
        "task" => MapiIdentityObjectKind::Task,
        "note" => MapiIdentityObjectKind::Note,
        "journal_entry" => MapiIdentityObjectKind::JournalEntry,
        "search_folder_definition" => MapiIdentityObjectKind::SearchFolderDefinition,
        "conversation_action" => MapiIdentityObjectKind::ConversationAction,
        "navigation_shortcut" => MapiIdentityObjectKind::NavigationShortcut,
        "delegate_freebusy_message" => MapiIdentityObjectKind::DelegateFreeBusyMessage,
        value => anyhow::bail!("unsupported MAPI object kind: {value}"),
    };
    Ok(MapiIdentityLookupRecord {
        object_kind,
        canonical_id: row.get("canonical_id"),
        object_id: row.get::<i64, _>("mapi_object_id") as u64,
        source_key: row.get("source_key"),
    })
}

fn mapi_notification_event_from_change_row(
    row: sqlx::postgres::PgRow,
) -> Option<MapiNotificationEvent> {
    let object_kind = row.get::<String, _>("object_kind");
    let change_kind = row.get::<String, _>("change_kind");
    let event_mask = mapi_notification_event_mask_for_change(&change_kind);
    let cursor = row.get::<i64, _>("cursor");
    let modseq = row.get::<i64, _>("modseq").max(0) as u64;
    match object_kind.as_str() {
        "mailbox" => {
            let changed_folder_id = mapi_folder_id_from_role_or_identity(
                row.try_get::<String, _>("object_role").ok().as_deref(),
                row.try_get::<i64, _>("object_mapi_object_id").ok(),
            )?;
            let parent_folder_id = row
                .try_get::<String, _>("parent_role")
                .ok()
                .as_deref()
                .and_then(crate::mapi_store::reserved_folder_counter_for_role)
                .map(crate::mapi::identity::mapi_store_id)
                .or_else(|| {
                    row.try_get::<i64, _>("parent_mapi_object_id")
                        .ok()
                        .map(|value| value as u64)
                })
                .or(Some(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID));
            Some(MapiNotificationEvent::canonical(
                MapiNotificationKind::Hierarchy,
                event_mask,
                parent_folder_id?,
                Some(changed_folder_id),
                None,
                cursor,
                modseq,
                row.try_get("object_total_messages").ok(),
                row.try_get("object_unread_messages").ok(),
                change_kind,
                row.try_get("object_display_name").ok(),
                row.try_get("parent_display_name").ok(),
                None,
            ))
        }
        "mailbox_message" | "attachment" => {
            let folder_id = mapi_folder_id_from_role_or_identity(
                row.try_get::<String, _>("scope_role").ok().as_deref(),
                row.try_get::<i64, _>("scope_mapi_object_id").ok(),
            )?;
            Some(MapiNotificationEvent::canonical(
                MapiNotificationKind::Content,
                event_mask,
                folder_id,
                row.try_get::<i64, _>("message_mapi_object_id")
                    .ok()
                    .map(|value| value as u64),
                row.try_get::<i64, _>("source_mapi_object_id")
                    .ok()
                    .map(|value| value as u64),
                cursor,
                modseq,
                row.try_get("scope_total_messages").ok(),
                row.try_get("scope_unread_messages").ok(),
                change_kind,
                row.try_get("scope_display_name").ok(),
                row.try_get("source_display_name").ok(),
                row.try_get("message_subject").ok(),
            ))
        }
        _ => None,
    }
}

fn mapi_folder_id_from_role_or_identity(role: Option<&str>, identity: Option<i64>) -> Option<u64> {
    role.and_then(crate::mapi_store::reserved_folder_counter_for_role)
        .map(crate::mapi::identity::mapi_store_id)
        .or_else(|| identity.map(|value| value as u64))
}

fn mapi_notification_event_mask_for_change(change_kind: &str) -> u16 {
    match change_kind {
        "created" => 0x0004,
        "destroyed" | "deleted" | "expunged" => 0x0008,
        "moved" => 0x0020,
        _ => 0x0010,
    }
}

#[allow(dead_code)]
fn mapi_sync_checkpoint_from_row(row: sqlx::postgres::PgRow) -> Result<MapiSyncCheckpoint> {
    let checkpoint_kind = match row.get::<String, _>("checkpoint_kind").as_str() {
        "hierarchy" => MapiCheckpointKind::Hierarchy,
        "content" => MapiCheckpointKind::Content,
        "read_state" => MapiCheckpointKind::ReadState,
        value => anyhow::bail!("unsupported MAPI checkpoint kind: {value}"),
    };
    Ok(MapiSyncCheckpoint {
        mailbox_id: row.get("mailbox_id"),
        checkpoint_kind,
        last_change_sequence: row.get::<i64, _>("last_change_sequence") as u64,
        last_modseq: row.get::<i64, _>("last_modseq") as u64,
        cursor_json: row.get("cursor_json"),
    })
}

fn push_unique_uuid(values: &mut Vec<Uuid>, value: Uuid) {
    if !values.contains(&value) {
        values.push(value);
    }
}
