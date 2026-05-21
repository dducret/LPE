use anyhow::Result;
use lpe_mail_auth::{AccountAuthStore, AccountPrincipal, StoreFuture};
use lpe_storage::{
    AccessibleContact, AccessibleEvent, ActiveSyncAttachment, ActiveSyncAttachmentContent,
    AttachmentUploadInput, AuditEntryInput, CanonicalChangeCategory, ClientNote, ClientReminder,
    ClientTask, CollaborationCollection, JmapEmail, JmapEmailFollowupUpdate, JmapEmailQuery,
    JmapImportedEmailInput, JmapMailbox, JmapMailboxCreateInput, JournalEntry, ReminderQuery,
    SavedDraftMessage, SearchFolderDefinition, SieveScriptDocument, Storage, SubmitMessageInput,
    SubmittedMessage, UpsertClientContactInput, UpsertClientEventInput, UpsertClientNoteInput,
    UpsertClientTaskInput, UpsertJournalEntryInput,
};
use sqlx::Row;
use uuid::Uuid;

use crate::mapi::permissions::{owner_permission, rights_from_grant, MapiFolderPermission};

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
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MapiIdentityRequest {
    pub(crate) object_kind: MapiIdentityObjectKind,
    pub(crate) canonical_id: Uuid,
    pub(crate) reserved_global_counter: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MapiIdentityRecord {
    pub(crate) canonical_id: Uuid,
    pub(crate) object_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MapiIdentityLookupRecord {
    pub(crate) object_kind: MapiIdentityObjectKind,
    pub(crate) canonical_id: Uuid,
    pub(crate) object_id: u64,
    pub(crate) source_key: Vec<u8>,
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MapiSyncChangeSet {
    pub(crate) current_change_sequence: u64,
    pub(crate) current_modseq: u64,
    pub(crate) changed_mailbox_ids: Vec<Uuid>,
    pub(crate) changed_message_ids: Vec<Uuid>,
    pub(crate) changed_note_ids: Vec<Uuid>,
    pub(crate) changed_journal_entry_ids: Vec<Uuid>,
    pub(crate) deleted_message_ids: Vec<Uuid>,
    pub(crate) deleted_note_ids: Vec<Uuid>,
    pub(crate) deleted_journal_entry_ids: Vec<Uuid>,
}

impl Default for MapiSyncChangeSet {
    fn default() -> Self {
        Self {
            current_change_sequence: 0,
            current_modseq: 1,
            changed_mailbox_ids: Vec::new(),
            changed_message_ids: Vec::new(),
            changed_note_ids: Vec::new(),
            changed_journal_entry_ids: Vec::new(),
            deleted_message_ids: Vec::new(),
            deleted_note_ids: Vec::new(),
            deleted_journal_entry_ids: Vec::new(),
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

    fn query_mapi_content_table_ids<'a>(
        &'a self,
        account_id: Uuid,
        query: MapiContentTableQuery,
    ) -> StoreFuture<'a, MapiContentTableQueryResult>;

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
            repair_invalid_mapi_identity_change_keys(&mut tx, tenant_id, account_id).await?;

            let mut records = Vec::with_capacity(requests.len());
            for request in requests {
                let kind = request.object_kind.as_str();
                let existing = sqlx::query(
                    r#"
                    SELECT mapi_object_id
                    FROM mapi_object_identities
                    WHERE tenant_id = $1
                      AND account_id = $2
                      AND object_kind = $3
                      AND canonical_id = $4
                    LIMIT 1
                    "#,
                )
                .bind(tenant_id)
                .bind(account_id)
                .bind(kind)
                .bind(request.canonical_id)
                .fetch_optional(&mut *tx)
                .await?;

                let object_id = if let Some(row) = existing {
                    row.get::<i64, _>("mapi_object_id") as u64
                } else {
                    let global_counter = if let Some(counter) = request.reserved_global_counter {
                        counter
                    } else {
                        allocate_next_mapi_global_counter(&mut tx, tenant_id, account_id).await?
                    };
                    let (object_id, source_key, change_key, instance_key) =
                        crate::mapi::identity::persisted_identity_material(global_counter);
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
                        DO UPDATE SET updated_at = mapi_object_identities.updated_at
                        RETURNING mapi_object_id
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
                    row.get::<i64, _>("mapi_object_id") as u64
                };
                records.push(MapiIdentityRecord {
                    canonical_id: request.canonical_id,
                    object_id,
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
            let existing_id = sqlx::query_scalar::<_, Uuid>(
                r#"
                SELECT id
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
                            OR ($5::uuid IS NULL AND object_kind IN ('note', 'journal_entry'))
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
                      AND object_kind IN ('note', 'journal_entry')
                      AND change_cursor > $3
                      AND $4::uuid IS NULL
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
                for row in collaboration_tombstones {
                    match row.get::<String, _>("object_kind").as_str() {
                        "note" => {
                            push_unique_uuid(&mut changes.deleted_note_ids, row.get("object_id"))
                        }
                        "journal_entry" => push_unique_uuid(
                            &mut changes.deleted_journal_entry_ids,
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

    fn fetch_mapi_notification_cursor<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Option<i64>> {
        Box::pin(async move { self.fetch_canonical_change_cursor(account_id).await })
    }

    fn poll_mapi_notifications<'a>(
        &'a self,
        account_id: Uuid,
        after_cursor: i64,
    ) -> StoreFuture<'a, MapiNotificationPoll> {
        Box::pin(async move {
            let replay = self
                .replay_canonical_changes(
                    account_id,
                    after_cursor,
                    &[CanonicalChangeCategory::Mail],
                    100,
                )
                .await?;
            let cursor = replay.change_set.journal_cursor().or(replay.current_cursor);
            Ok(MapiNotificationPoll {
                event_pending: replay.truncated || !replay.change_set.is_empty(),
                cursor,
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
                    }),
            );
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

    fn fetch_attachment_content<'a>(
        &'a self,
        account_id: Uuid,
        file_reference: &'a str,
    ) -> StoreFuture<'a, Option<ActiveSyncAttachmentContent>> {
        Box::pin(async move {
            self.fetch_activesync_attachment_content(account_id, file_reference)
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
        SELECT object_kind, canonical_id
        FROM mapi_object_identities
        WHERE tenant_id = $1
          AND account_id = $2
          AND mapi_global_counter >= $3
          AND mapi_global_counter < $4
        ORDER BY mapi_global_counter, created_at, canonical_id
        "#,
    )
    .bind(tenant_id)
    .bind(account_id)
    .bind(crate::mapi::identity::JOURNAL_FOLDER_COUNTER as i64)
    .bind(crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER as i64)
    .fetch_all(&mut **tx)
    .await?;

    for row in rows {
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
        .bind(row.get::<String, _>("object_kind"))
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
        value => anyhow::bail!("unsupported MAPI object kind: {value}"),
    };
    Ok(MapiIdentityLookupRecord {
        object_kind,
        canonical_id: row.get("canonical_id"),
        object_id: row.get::<i64, _>("mapi_object_id") as u64,
        source_key: row.get("source_key"),
    })
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
