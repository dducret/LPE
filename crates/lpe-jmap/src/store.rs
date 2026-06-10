use anyhow::Result;
use lpe_storage::{
    AccessibleContact, AccessibleEvent, AttachmentUploadInput, AuditEntryInput,
    AuthenticatedAccount, CalendarEventAttachment, CanonicalChangeCategory,
    CanonicalChangeListener, CanonicalChangeReplay, CanonicalPushChangeSet, ClientNote,
    ClientReminder, ClientTask, ClientTaskList, CollaborationCollection, CollaborationGrantInput,
    CollaborationResourceKind, CreateTaskListInput, JmapEmail, JmapEmailFollowupUpdate,
    JmapEmailQuery, JmapEmailSubmission, JmapImportedEmailInput, JmapMailObjectChange, JmapMailbox,
    JmapMailboxCreateInput, JmapMailboxUpdateInput, JmapQuota, JmapStoredQueryState,
    JmapStringObjectChange, JmapThreadQuery, JmapUploadBlob, JournalEntry, MailboxAccountAccess,
    MailboxDelegationGrantInput, MailboxRule, OutlookProfileState, RecipientSuggestion,
    ReminderQuery, SavedDraftMessage, SearchFolderDefinition, SenderDelegationGrantInput,
    SenderDelegationRight, SenderIdentity, SieveScriptDocument, Storage, SubmitMessageInput,
    SubmittedMessage, TaskListGrantInput, UpdateTaskListInput, UpsertClientContactInput,
    UpsertClientEventInput, UpsertClientNoteInput, UpsertClientTaskInput, UpsertJournalEntryInput,
    UpsertSearchFolderInput,
};
use serde_json::{json, Map, Value};
use uuid::Uuid;

pub(crate) const MAX_JMAP_MAIL_OBJECT_REPLAY_ROWS: u64 = 4096;

#[derive(Debug, Clone)]
pub struct JmapShareInput {
    pub owner_account_id: Uuid,
    pub share_type: String,
    pub grantee_email: String,
    pub calendar_id: Option<Uuid>,
    pub task_list_id: Option<Uuid>,
    pub sender_right: Option<String>,
    pub may_read: bool,
    pub may_write: bool,
    pub may_delete: bool,
    pub may_share: bool,
}

#[allow(async_fn_in_trait)]
pub trait JmapPushListener: Send {
    async fn wait_for_change(
        &mut self,
        categories: &[CanonicalChangeCategory],
    ) -> Result<CanonicalPushChangeSet>;
}

#[allow(async_fn_in_trait)]
pub trait JmapStore: Clone + Send + Sync + 'static {
    type PushListener: JmapPushListener;

    async fn fetch_account_session(&self, token: &str) -> Result<Option<AuthenticatedAccount>>;
    async fn create_push_listener(&self, principal_account_id: Uuid) -> Result<Self::PushListener>;
    async fn fetch_canonical_change_cursor(
        &self,
        principal_account_id: Uuid,
    ) -> Result<Option<i64>>;
    async fn fetch_jmap_mail_change_cursor(&self, account_id: Uuid) -> Result<Option<i64>> {
        let _ = account_id;
        Ok(None)
    }
    async fn fetch_jmap_object_change_cursor(
        &self,
        account_id: Uuid,
        data_type: &str,
    ) -> Result<Option<i64>> {
        let _ = (account_id, data_type);
        Ok(None)
    }
    async fn replay_jmap_mail_object_changes(
        &self,
        account_id: Uuid,
        data_type: &str,
        after_cursor: i64,
        max_rows: u64,
    ) -> Result<Option<Vec<JmapMailObjectChange>>> {
        let _ = (account_id, data_type, after_cursor, max_rows);
        Ok(None)
    }
    async fn replay_jmap_object_changes(
        &self,
        account_id: Uuid,
        data_type: &str,
        after_cursor: i64,
        max_rows: u64,
    ) -> Result<Option<Vec<JmapMailObjectChange>>> {
        let _ = (account_id, data_type, after_cursor, max_rows);
        Ok(None)
    }
    async fn replay_jmap_string_object_changes(
        &self,
        account_id: Uuid,
        data_type: &str,
        after_cursor: i64,
        max_rows: u64,
    ) -> Result<Option<Vec<JmapStringObjectChange>>> {
        let _ = (account_id, data_type, after_cursor, max_rows);
        Ok(None)
    }
    async fn save_jmap_query_state(
        &self,
        account_id: Uuid,
        method_name: &str,
        filter: Option<Value>,
        sort: Option<Vec<Value>>,
        last_change_sequence: i64,
        snapshot_ids: &[String],
    ) -> Result<Option<Uuid>> {
        let _ = (
            account_id,
            method_name,
            filter,
            sort,
            last_change_sequence,
            snapshot_ids,
        );
        Ok(None)
    }
    async fn fetch_jmap_query_state(
        &self,
        account_id: Uuid,
        method_name: &str,
        state_id: Uuid,
        filter: Option<Value>,
        sort: Option<Vec<Value>>,
    ) -> Result<Option<JmapStoredQueryState>> {
        let _ = (account_id, method_name, state_id, filter, sort);
        Ok(None)
    }
    async fn replay_canonical_changes(
        &self,
        principal_account_id: Uuid,
        after_cursor: i64,
        categories: &[CanonicalChangeCategory],
        max_rows: u64,
    ) -> Result<CanonicalChangeReplay>;
    async fn fetch_jmap_mailboxes(&self, account_id: Uuid) -> Result<Vec<JmapMailbox>>;
    async fn fetch_accessible_mailbox_accounts(
        &self,
        principal_account_id: Uuid,
    ) -> Result<Vec<MailboxAccountAccess>>;
    async fn fetch_sender_identities(
        &self,
        principal_account_id: Uuid,
        target_account_id: Uuid,
    ) -> Result<Vec<SenderIdentity>>;
    async fn fetch_jmap_mailbox_ids(&self, account_id: Uuid) -> Result<Vec<Uuid>>;
    async fn create_jmap_mailbox(
        &self,
        input: JmapMailboxCreateInput,
        audit: AuditEntryInput,
    ) -> Result<JmapMailbox>;
    async fn update_jmap_mailbox(
        &self,
        input: JmapMailboxUpdateInput,
        audit: AuditEntryInput,
    ) -> Result<JmapMailbox>;
    async fn destroy_jmap_mailbox(
        &self,
        account_id: Uuid,
        mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<()>;
    async fn query_jmap_email_ids(
        &self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        search_text: Option<&str>,
        position: u64,
        limit: u64,
    ) -> Result<JmapEmailQuery>;
    async fn fetch_all_jmap_email_ids(&self, account_id: Uuid) -> Result<Vec<Uuid>>;
    async fn fetch_all_jmap_thread_ids(&self, account_id: Uuid) -> Result<Vec<Uuid>>;
    async fn query_jmap_thread_ids(
        &self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        search_text: Option<&str>,
        position: u64,
        limit: u64,
    ) -> Result<JmapThreadQuery>;
    async fn fetch_jmap_emails(&self, account_id: Uuid, ids: &[Uuid]) -> Result<Vec<JmapEmail>>;
    async fn fetch_jmap_emails_with_protected_bcc(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<JmapEmail>>;
    async fn fetch_jmap_draft(&self, account_id: Uuid, id: Uuid) -> Result<Option<JmapEmail>>;
    async fn fetch_jmap_email_submissions(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<JmapEmailSubmission>>;
    async fn fetch_jmap_quota(&self, account_id: Uuid) -> Result<JmapQuota>;
    async fn list_mailbox_rules(&self, account_id: Uuid) -> Result<Vec<MailboxRule>>;
    async fn fetch_outlook_profile_state(&self, account_id: Uuid) -> Result<OutlookProfileState>;
    async fn fetch_search_folders(&self, account_id: Uuid) -> Result<Vec<SearchFolderDefinition>>;
    async fn fetch_search_folders_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<SearchFolderDefinition>>;
    async fn upsert_search_folder(
        &self,
        input: UpsertSearchFolderInput,
    ) -> Result<SearchFolderDefinition>;
    async fn delete_search_folder(&self, account_id: Uuid, search_folder_id: Uuid) -> Result<()>;
    async fn fetch_active_sieve_script(
        &self,
        account_id: Uuid,
    ) -> Result<Option<SieveScriptDocument>>;
    async fn put_sieve_script(
        &self,
        account_id: Uuid,
        name: &str,
        content: &str,
        activate: bool,
        audit: AuditEntryInput,
    ) -> Result<SieveScriptDocument>;
    async fn set_active_sieve_script(
        &self,
        account_id: Uuid,
        name: Option<&str>,
        audit: AuditEntryInput,
    ) -> Result<Option<String>>;
    async fn save_jmap_upload_blob(
        &self,
        account_id: Uuid,
        media_type: &str,
        blob_bytes: &[u8],
    ) -> Result<JmapUploadBlob>;
    async fn fetch_jmap_upload_blob(
        &self,
        account_id: Uuid,
        blob_id: Uuid,
    ) -> Result<Option<JmapUploadBlob>>;
    async fn fetch_jmap_message_blob(
        &self,
        account_id: Uuid,
        message_id: Uuid,
    ) -> Result<Option<JmapUploadBlob>> {
        let _ = (account_id, message_id);
        Ok(None)
    }
    async fn fetch_calendar_attachment_blob(
        &self,
        account_id: Uuid,
        file_reference: &str,
    ) -> Result<Option<JmapUploadBlob>> {
        let _ = (account_id, file_reference);
        Ok(None)
    }
    async fn save_draft_message(
        &self,
        input: SubmitMessageInput,
        audit: AuditEntryInput,
    ) -> Result<SavedDraftMessage>;
    async fn delete_draft_message(
        &self,
        account_id: Uuid,
        message_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<()>;
    async fn submit_draft_message(
        &self,
        account_id: Uuid,
        draft_message_id: Uuid,
        submitted_by_account_id: Uuid,
        source: &str,
        audit: AuditEntryInput,
    ) -> Result<SubmittedMessage>;
    async fn copy_jmap_email(
        &self,
        account_id: Uuid,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<JmapEmail>;
    async fn import_jmap_email(
        &self,
        input: JmapImportedEmailInput,
        audit: AuditEntryInput,
    ) -> Result<JmapEmail>;
    async fn fetch_accessible_contact_collections(
        &self,
        principal_account_id: Uuid,
    ) -> Result<Vec<CollaborationCollection>>;
    async fn fetch_accessible_contacts(
        &self,
        principal_account_id: Uuid,
    ) -> Result<Vec<AccessibleContact>>;
    async fn fetch_accessible_contacts_by_ids(
        &self,
        principal_account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<AccessibleContact>>;
    async fn create_accessible_contact(
        &self,
        principal_account_id: Uuid,
        collection_id: Option<&str>,
        input: UpsertClientContactInput,
    ) -> Result<AccessibleContact>;
    async fn update_accessible_contact(
        &self,
        principal_account_id: Uuid,
        contact_id: Uuid,
        input: UpsertClientContactInput,
    ) -> Result<AccessibleContact>;
    async fn delete_accessible_contact(
        &self,
        principal_account_id: Uuid,
        contact_id: Uuid,
    ) -> Result<()>;
    async fn query_recipient_suggestions(
        &self,
        account_id: Uuid,
        query: Option<&str>,
    ) -> Result<Vec<RecipientSuggestion>>;
    async fn fetch_accessible_calendar_collections(
        &self,
        principal_account_id: Uuid,
    ) -> Result<Vec<CollaborationCollection>>;
    async fn create_accessible_calendar_collection(
        &self,
        principal_account_id: Uuid,
        display_name: &str,
    ) -> Result<CollaborationCollection>;
    async fn update_accessible_calendar_collection(
        &self,
        principal_account_id: Uuid,
        collection_id: &str,
        display_name: &str,
    ) -> Result<CollaborationCollection>;
    async fn delete_accessible_calendar_collection(
        &self,
        principal_account_id: Uuid,
        collection_id: &str,
    ) -> Result<()>;
    async fn fetch_accessible_events(
        &self,
        principal_account_id: Uuid,
    ) -> Result<Vec<AccessibleEvent>>;
    async fn fetch_accessible_events_by_ids(
        &self,
        principal_account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<AccessibleEvent>>;
    async fn create_accessible_event(
        &self,
        principal_account_id: Uuid,
        collection_id: Option<&str>,
        input: UpsertClientEventInput,
    ) -> Result<AccessibleEvent>;
    async fn update_accessible_event(
        &self,
        principal_account_id: Uuid,
        event_id: Uuid,
        input: UpsertClientEventInput,
    ) -> Result<AccessibleEvent>;
    async fn delete_accessible_event(
        &self,
        principal_account_id: Uuid,
        event_id: Uuid,
    ) -> Result<()>;
    async fn fetch_calendar_attachments_for_events(
        &self,
        principal_account_id: Uuid,
        event_ids: &[Uuid],
    ) -> Result<Vec<(Uuid, Vec<CalendarEventAttachment>)>>;
    async fn add_calendar_event_attachment(
        &self,
        principal_account_id: Uuid,
        event_id: Uuid,
        attachment: AttachmentUploadInput,
        audit: AuditEntryInput,
    ) -> Result<Option<CalendarEventAttachment>>;
    async fn fetch_jmap_task_lists(&self, account_id: Uuid) -> Result<Vec<ClientTaskList>>;
    async fn fetch_jmap_task_lists_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<ClientTaskList>>;
    async fn create_jmap_task_list(&self, input: CreateTaskListInput) -> Result<ClientTaskList>;
    async fn update_jmap_task_list(&self, input: UpdateTaskListInput) -> Result<ClientTaskList>;
    async fn delete_jmap_task_list(&self, account_id: Uuid, task_list_id: Uuid) -> Result<()>;
    async fn fetch_jmap_tasks(&self, account_id: Uuid) -> Result<Vec<ClientTask>>;
    async fn fetch_jmap_tasks_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<ClientTask>>;
    async fn upsert_jmap_task(&self, input: UpsertClientTaskInput) -> Result<ClientTask>;
    async fn delete_jmap_task(&self, account_id: Uuid, task_id: Uuid) -> Result<()>;
    async fn fetch_jmap_notes(&self, account_id: Uuid) -> Result<Vec<ClientNote>>;
    async fn fetch_jmap_notes_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<ClientNote>>;
    async fn upsert_jmap_note(&self, input: UpsertClientNoteInput) -> Result<ClientNote>;
    async fn delete_jmap_note(&self, account_id: Uuid, note_id: Uuid) -> Result<()>;
    async fn fetch_jmap_journal_entries(&self, account_id: Uuid) -> Result<Vec<JournalEntry>>;
    async fn fetch_jmap_journal_entries_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<JournalEntry>>;
    async fn upsert_jmap_journal_entry(
        &self,
        input: UpsertJournalEntryInput,
    ) -> Result<JournalEntry>;
    async fn delete_jmap_journal_entry(&self, account_id: Uuid, entry_id: Uuid) -> Result<()>;
    async fn query_jmap_reminders(
        &self,
        account_id: Uuid,
        query: ReminderQuery,
    ) -> Result<Vec<ClientReminder>>;
    async fn update_jmap_task_reminder(
        &self,
        principal_account_id: Uuid,
        task_id: Uuid,
        reminder_set: Option<bool>,
        reminder_at: Option<String>,
        reminder_dismissed_at: Option<String>,
        reminder_reset: Option<bool>,
    ) -> Result<()> {
        let _ = (
            principal_account_id,
            task_id,
            reminder_set,
            reminder_at,
            reminder_dismissed_at,
            reminder_reset,
        );
        Ok(())
    }
    async fn update_jmap_event_reminder(
        &self,
        principal_account_id: Uuid,
        event_id: Uuid,
        reminder_set: Option<bool>,
        reminder_at: Option<String>,
        reminder_dismissed_at: Option<String>,
    ) -> Result<()> {
        let _ = (
            principal_account_id,
            event_id,
            reminder_set,
            reminder_at,
            reminder_dismissed_at,
        );
        Ok(())
    }
    async fn update_jmap_mail_reminder(
        &self,
        account_id: Uuid,
        message_id: Uuid,
        reminder_set: Option<bool>,
        reminder_at: Option<String>,
        reminder_dismissed_at: Option<String>,
        audit: AuditEntryInput,
    ) -> Result<()> {
        let _ = (
            account_id,
            message_id,
            reminder_set,
            reminder_at,
            reminder_dismissed_at,
            audit,
        );
        Ok(())
    }
    async fn dismiss_jmap_reminder_occurrence(
        &self,
        account_id: Uuid,
        source_type: String,
        source_id: Uuid,
        occurrence_start_at: String,
        dismissed_at: String,
    ) -> Result<()> {
        let _ = (
            account_id,
            source_type,
            source_id,
            occurrence_start_at,
            dismissed_at,
        );
        Ok(())
    }
    async fn fetch_jmap_shares(&self, account_id: Uuid) -> Result<Vec<Value>> {
        let _ = account_id;
        Ok(Vec::new())
    }
    async fn upsert_jmap_share(
        &self,
        input: JmapShareInput,
        audit: AuditEntryInput,
    ) -> Result<Value> {
        let _ = (input, audit);
        Ok(Value::Null)
    }
    async fn delete_jmap_share(&self, share: Value, audit: AuditEntryInput) -> Result<()> {
        let _ = (share, audit);
        Ok(())
    }
}

impl JmapPushListener for CanonicalChangeListener {
    async fn wait_for_change(
        &mut self,
        categories: &[CanonicalChangeCategory],
    ) -> Result<CanonicalPushChangeSet> {
        CanonicalChangeListener::wait_for_change(self, categories).await
    }
}

impl JmapStore for Storage {
    type PushListener = CanonicalChangeListener;

    async fn fetch_account_session(&self, token: &str) -> Result<Option<AuthenticatedAccount>> {
        self.fetch_account_session(token).await
    }

    async fn create_push_listener(&self, principal_account_id: Uuid) -> Result<Self::PushListener> {
        self.create_canonical_change_listener(principal_account_id)
            .await
    }

    async fn fetch_canonical_change_cursor(
        &self,
        principal_account_id: Uuid,
    ) -> Result<Option<i64>> {
        self.fetch_canonical_change_cursor(principal_account_id)
            .await
    }

    async fn fetch_jmap_mail_change_cursor(&self, account_id: Uuid) -> Result<Option<i64>> {
        self.fetch_jmap_mail_change_cursor(account_id).await
    }

    async fn fetch_jmap_object_change_cursor(
        &self,
        account_id: Uuid,
        data_type: &str,
    ) -> Result<Option<i64>> {
        self.fetch_jmap_object_change_cursor(account_id, data_type)
            .await
    }

    async fn replay_jmap_mail_object_changes(
        &self,
        account_id: Uuid,
        data_type: &str,
        after_cursor: i64,
        max_rows: u64,
    ) -> Result<Option<Vec<JmapMailObjectChange>>> {
        self.replay_jmap_mail_object_changes(account_id, data_type, after_cursor, max_rows)
            .await
    }

    async fn replay_jmap_object_changes(
        &self,
        account_id: Uuid,
        data_type: &str,
        after_cursor: i64,
        max_rows: u64,
    ) -> Result<Option<Vec<JmapMailObjectChange>>> {
        self.replay_jmap_object_changes(account_id, data_type, after_cursor, max_rows)
            .await
    }

    async fn replay_jmap_string_object_changes(
        &self,
        account_id: Uuid,
        data_type: &str,
        after_cursor: i64,
        max_rows: u64,
    ) -> Result<Option<Vec<JmapStringObjectChange>>> {
        self.replay_jmap_string_object_changes(account_id, data_type, after_cursor, max_rows)
            .await
    }

    async fn save_jmap_query_state(
        &self,
        account_id: Uuid,
        method_name: &str,
        filter: Option<Value>,
        sort: Option<Vec<Value>>,
        last_change_sequence: i64,
        snapshot_ids: &[String],
    ) -> Result<Option<Uuid>> {
        self.save_jmap_query_state(
            account_id,
            method_name,
            filter,
            sort,
            last_change_sequence,
            snapshot_ids,
        )
        .await
        .map(Some)
    }

    async fn fetch_jmap_query_state(
        &self,
        account_id: Uuid,
        method_name: &str,
        state_id: Uuid,
        filter: Option<Value>,
        sort: Option<Vec<Value>>,
    ) -> Result<Option<JmapStoredQueryState>> {
        self.fetch_jmap_query_state(account_id, method_name, state_id, filter, sort)
            .await
    }

    async fn replay_canonical_changes(
        &self,
        principal_account_id: Uuid,
        after_cursor: i64,
        categories: &[CanonicalChangeCategory],
        max_rows: u64,
    ) -> Result<CanonicalChangeReplay> {
        self.replay_canonical_changes(principal_account_id, after_cursor, categories, max_rows)
            .await
    }

    async fn fetch_jmap_mailboxes(&self, account_id: Uuid) -> Result<Vec<JmapMailbox>> {
        self.fetch_jmap_mailboxes(account_id).await
    }

    async fn fetch_accessible_mailbox_accounts(
        &self,
        principal_account_id: Uuid,
    ) -> Result<Vec<MailboxAccountAccess>> {
        self.fetch_accessible_mailbox_accounts(principal_account_id)
            .await
    }

    async fn fetch_sender_identities(
        &self,
        principal_account_id: Uuid,
        target_account_id: Uuid,
    ) -> Result<Vec<SenderIdentity>> {
        self.fetch_sender_identities(principal_account_id, target_account_id)
            .await
    }

    async fn fetch_jmap_mailbox_ids(&self, account_id: Uuid) -> Result<Vec<Uuid>> {
        self.fetch_jmap_mailbox_ids(account_id).await
    }

    async fn create_jmap_mailbox(
        &self,
        input: JmapMailboxCreateInput,
        audit: AuditEntryInput,
    ) -> Result<JmapMailbox> {
        self.create_jmap_mailbox(input, audit).await
    }

    async fn update_jmap_mailbox(
        &self,
        input: JmapMailboxUpdateInput,
        audit: AuditEntryInput,
    ) -> Result<JmapMailbox> {
        self.update_jmap_mailbox(input, audit).await
    }

    async fn destroy_jmap_mailbox(
        &self,
        account_id: Uuid,
        mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<()> {
        self.destroy_jmap_mailbox(account_id, mailbox_id, audit)
            .await
    }

    async fn query_jmap_email_ids(
        &self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        search_text: Option<&str>,
        position: u64,
        limit: u64,
    ) -> Result<JmapEmailQuery> {
        self.query_jmap_email_ids(account_id, mailbox_id, search_text, position, limit)
            .await
    }

    async fn fetch_all_jmap_email_ids(&self, account_id: Uuid) -> Result<Vec<Uuid>> {
        self.fetch_all_jmap_email_ids(account_id).await
    }

    async fn fetch_all_jmap_thread_ids(&self, account_id: Uuid) -> Result<Vec<Uuid>> {
        self.fetch_all_jmap_thread_ids(account_id).await
    }

    async fn query_jmap_thread_ids(
        &self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        search_text: Option<&str>,
        position: u64,
        limit: u64,
    ) -> Result<JmapThreadQuery> {
        self.query_jmap_thread_ids(account_id, mailbox_id, search_text, position, limit)
            .await
    }

    async fn fetch_jmap_emails(&self, account_id: Uuid, ids: &[Uuid]) -> Result<Vec<JmapEmail>> {
        self.fetch_jmap_emails(account_id, ids).await
    }

    async fn fetch_jmap_emails_with_protected_bcc(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<JmapEmail>> {
        self.fetch_jmap_emails_with_protected_bcc(account_id, ids)
            .await
    }

    async fn fetch_jmap_draft(&self, account_id: Uuid, id: Uuid) -> Result<Option<JmapEmail>> {
        self.fetch_jmap_draft(account_id, id).await
    }

    async fn fetch_jmap_email_submissions(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<JmapEmailSubmission>> {
        self.fetch_jmap_email_submissions(account_id, ids).await
    }

    async fn fetch_jmap_quota(&self, account_id: Uuid) -> Result<JmapQuota> {
        self.fetch_jmap_quota(account_id).await
    }

    async fn list_mailbox_rules(&self, account_id: Uuid) -> Result<Vec<MailboxRule>> {
        self.list_mailbox_rules(account_id).await
    }

    async fn fetch_outlook_profile_state(&self, account_id: Uuid) -> Result<OutlookProfileState> {
        Storage::fetch_outlook_profile_state(self, account_id).await
    }

    async fn fetch_search_folders(&self, account_id: Uuid) -> Result<Vec<SearchFolderDefinition>> {
        self.fetch_search_folders(account_id).await
    }

    async fn fetch_search_folders_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<SearchFolderDefinition>> {
        self.fetch_search_folders_by_ids(account_id, ids).await
    }

    async fn upsert_search_folder(
        &self,
        input: UpsertSearchFolderInput,
    ) -> Result<SearchFolderDefinition> {
        self.upsert_search_folder(input).await
    }

    async fn delete_search_folder(&self, account_id: Uuid, search_folder_id: Uuid) -> Result<()> {
        self.delete_search_folder(account_id, search_folder_id)
            .await
    }

    async fn fetch_active_sieve_script(
        &self,
        account_id: Uuid,
    ) -> Result<Option<SieveScriptDocument>> {
        self.fetch_active_sieve_script(account_id).await
    }

    async fn put_sieve_script(
        &self,
        account_id: Uuid,
        name: &str,
        content: &str,
        activate: bool,
        audit: AuditEntryInput,
    ) -> Result<SieveScriptDocument> {
        self.put_sieve_script(account_id, name, content, activate, audit)
            .await
    }

    async fn set_active_sieve_script(
        &self,
        account_id: Uuid,
        name: Option<&str>,
        audit: AuditEntryInput,
    ) -> Result<Option<String>> {
        self.set_active_sieve_script(account_id, name, audit).await
    }

    async fn save_jmap_upload_blob(
        &self,
        account_id: Uuid,
        media_type: &str,
        blob_bytes: &[u8],
    ) -> Result<JmapUploadBlob> {
        self.save_jmap_upload_blob(account_id, media_type, blob_bytes)
            .await
    }

    async fn fetch_jmap_upload_blob(
        &self,
        account_id: Uuid,
        blob_id: Uuid,
    ) -> Result<Option<JmapUploadBlob>> {
        self.fetch_jmap_upload_blob(account_id, blob_id).await
    }

    async fn fetch_jmap_message_blob(
        &self,
        account_id: Uuid,
        message_id: Uuid,
    ) -> Result<Option<JmapUploadBlob>> {
        self.fetch_jmap_message_blob(account_id, message_id).await
    }

    async fn fetch_calendar_attachment_blob(
        &self,
        account_id: Uuid,
        file_reference: &str,
    ) -> Result<Option<JmapUploadBlob>> {
        self.fetch_calendar_attachment_blob(account_id, file_reference)
            .await
    }

    async fn save_draft_message(
        &self,
        input: SubmitMessageInput,
        audit: AuditEntryInput,
    ) -> Result<SavedDraftMessage> {
        self.save_draft_message(input, audit).await
    }

    async fn delete_draft_message(
        &self,
        account_id: Uuid,
        message_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<()> {
        self.delete_draft_message(account_id, message_id, audit)
            .await
    }

    async fn submit_draft_message(
        &self,
        account_id: Uuid,
        draft_message_id: Uuid,
        submitted_by_account_id: Uuid,
        source: &str,
        audit: AuditEntryInput,
    ) -> Result<SubmittedMessage> {
        self.submit_draft_message(
            account_id,
            draft_message_id,
            submitted_by_account_id,
            source,
            audit,
        )
        .await
    }

    async fn copy_jmap_email(
        &self,
        account_id: Uuid,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> Result<JmapEmail> {
        self.copy_jmap_email(account_id, message_id, target_mailbox_id, audit)
            .await
    }

    async fn import_jmap_email(
        &self,
        input: JmapImportedEmailInput,
        audit: AuditEntryInput,
    ) -> Result<JmapEmail> {
        self.import_jmap_email(input, audit).await
    }

    async fn fetch_accessible_contact_collections(
        &self,
        principal_account_id: Uuid,
    ) -> Result<Vec<CollaborationCollection>> {
        self.fetch_accessible_contact_collections(principal_account_id)
            .await
    }

    async fn fetch_accessible_contacts(
        &self,
        principal_account_id: Uuid,
    ) -> Result<Vec<AccessibleContact>> {
        self.fetch_accessible_contacts(principal_account_id).await
    }

    async fn fetch_accessible_contacts_by_ids(
        &self,
        principal_account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<AccessibleContact>> {
        self.fetch_accessible_contacts_by_ids(principal_account_id, ids)
            .await
    }

    async fn create_accessible_contact(
        &self,
        principal_account_id: Uuid,
        collection_id: Option<&str>,
        input: UpsertClientContactInput,
    ) -> Result<AccessibleContact> {
        self.create_accessible_contact(principal_account_id, collection_id, input)
            .await
    }

    async fn update_accessible_contact(
        &self,
        principal_account_id: Uuid,
        contact_id: Uuid,
        input: UpsertClientContactInput,
    ) -> Result<AccessibleContact> {
        self.update_accessible_contact(principal_account_id, contact_id, input)
            .await
    }

    async fn delete_accessible_contact(
        &self,
        principal_account_id: Uuid,
        contact_id: Uuid,
    ) -> Result<()> {
        self.delete_accessible_contact(principal_account_id, contact_id)
            .await
    }

    async fn query_recipient_suggestions(
        &self,
        account_id: Uuid,
        query: Option<&str>,
    ) -> Result<Vec<RecipientSuggestion>> {
        self.query_recipient_suggestions(account_id, query).await
    }

    async fn fetch_accessible_calendar_collections(
        &self,
        principal_account_id: Uuid,
    ) -> Result<Vec<CollaborationCollection>> {
        self.fetch_accessible_calendar_collections(principal_account_id)
            .await
    }

    async fn create_accessible_calendar_collection(
        &self,
        principal_account_id: Uuid,
        display_name: &str,
    ) -> Result<CollaborationCollection> {
        self.create_accessible_calendar_collection(principal_account_id, display_name)
            .await
    }

    async fn update_accessible_calendar_collection(
        &self,
        principal_account_id: Uuid,
        collection_id: &str,
        display_name: &str,
    ) -> Result<CollaborationCollection> {
        self.update_accessible_calendar_collection(
            principal_account_id,
            collection_id,
            display_name,
        )
        .await
    }

    async fn delete_accessible_calendar_collection(
        &self,
        principal_account_id: Uuid,
        collection_id: &str,
    ) -> Result<()> {
        self.delete_accessible_calendar_collection(principal_account_id, collection_id)
            .await
    }

    async fn fetch_accessible_events(
        &self,
        principal_account_id: Uuid,
    ) -> Result<Vec<AccessibleEvent>> {
        self.fetch_accessible_events(principal_account_id).await
    }

    async fn fetch_accessible_events_by_ids(
        &self,
        principal_account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<AccessibleEvent>> {
        self.fetch_accessible_events_by_ids(principal_account_id, ids)
            .await
    }

    async fn create_accessible_event(
        &self,
        principal_account_id: Uuid,
        collection_id: Option<&str>,
        input: UpsertClientEventInput,
    ) -> Result<AccessibleEvent> {
        self.create_accessible_event(principal_account_id, collection_id, input)
            .await
    }

    async fn update_accessible_event(
        &self,
        principal_account_id: Uuid,
        event_id: Uuid,
        input: UpsertClientEventInput,
    ) -> Result<AccessibleEvent> {
        self.update_accessible_event(principal_account_id, event_id, input)
            .await
    }

    async fn delete_accessible_event(
        &self,
        principal_account_id: Uuid,
        event_id: Uuid,
    ) -> Result<()> {
        self.delete_accessible_event(principal_account_id, event_id)
            .await
    }

    async fn fetch_calendar_attachments_for_events(
        &self,
        principal_account_id: Uuid,
        event_ids: &[Uuid],
    ) -> Result<Vec<(Uuid, Vec<CalendarEventAttachment>)>> {
        self.fetch_calendar_attachments_for_events(principal_account_id, event_ids)
            .await
    }

    async fn add_calendar_event_attachment(
        &self,
        principal_account_id: Uuid,
        event_id: Uuid,
        attachment: AttachmentUploadInput,
        audit: AuditEntryInput,
    ) -> Result<Option<CalendarEventAttachment>> {
        self.add_calendar_event_attachment(principal_account_id, event_id, attachment, audit)
            .await
    }

    async fn fetch_jmap_task_lists(&self, account_id: Uuid) -> Result<Vec<ClientTaskList>> {
        self.fetch_task_lists(account_id).await
    }

    async fn fetch_jmap_task_lists_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<ClientTaskList>> {
        self.fetch_task_lists_by_ids(account_id, ids).await
    }

    async fn create_jmap_task_list(&self, input: CreateTaskListInput) -> Result<ClientTaskList> {
        self.create_task_list(input).await
    }

    async fn update_jmap_task_list(&self, input: UpdateTaskListInput) -> Result<ClientTaskList> {
        self.update_task_list(input).await
    }

    async fn delete_jmap_task_list(&self, account_id: Uuid, task_list_id: Uuid) -> Result<()> {
        self.delete_task_list(account_id, task_list_id).await
    }

    async fn fetch_jmap_tasks(&self, account_id: Uuid) -> Result<Vec<ClientTask>> {
        self.fetch_client_tasks(account_id).await
    }

    async fn fetch_jmap_tasks_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<ClientTask>> {
        self.fetch_client_tasks_by_ids(account_id, ids).await
    }

    async fn upsert_jmap_task(&self, input: UpsertClientTaskInput) -> Result<ClientTask> {
        self.upsert_client_task(input).await
    }

    async fn delete_jmap_task(&self, account_id: Uuid, task_id: Uuid) -> Result<()> {
        self.delete_client_task(account_id, task_id).await
    }

    async fn fetch_jmap_notes(&self, account_id: Uuid) -> Result<Vec<ClientNote>> {
        self.fetch_client_notes(account_id).await
    }

    async fn fetch_jmap_notes_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<ClientNote>> {
        self.fetch_client_notes_by_ids(account_id, ids).await
    }

    async fn upsert_jmap_note(&self, input: UpsertClientNoteInput) -> Result<ClientNote> {
        self.upsert_client_note(input).await
    }

    async fn delete_jmap_note(&self, account_id: Uuid, note_id: Uuid) -> Result<()> {
        self.delete_client_note(account_id, note_id).await
    }

    async fn fetch_jmap_journal_entries(&self, account_id: Uuid) -> Result<Vec<JournalEntry>> {
        self.fetch_journal_entries(account_id).await
    }

    async fn fetch_jmap_journal_entries_by_ids(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<JournalEntry>> {
        self.fetch_journal_entries_by_ids(account_id, ids).await
    }

    async fn upsert_jmap_journal_entry(
        &self,
        input: UpsertJournalEntryInput,
    ) -> Result<JournalEntry> {
        self.upsert_journal_entry(input).await
    }

    async fn delete_jmap_journal_entry(&self, account_id: Uuid, entry_id: Uuid) -> Result<()> {
        self.delete_journal_entry(account_id, entry_id).await
    }

    async fn query_jmap_reminders(
        &self,
        account_id: Uuid,
        query: ReminderQuery,
    ) -> Result<Vec<ClientReminder>> {
        self.query_client_reminders(account_id, query).await
    }

    async fn update_jmap_task_reminder(
        &self,
        principal_account_id: Uuid,
        task_id: Uuid,
        reminder_set: Option<bool>,
        reminder_at: Option<String>,
        reminder_dismissed_at: Option<String>,
        reminder_reset: Option<bool>,
    ) -> Result<()> {
        self.update_accessible_task_reminder(
            principal_account_id,
            task_id,
            reminder_set,
            reminder_at,
            reminder_dismissed_at,
            reminder_reset,
        )
        .await
    }

    async fn update_jmap_event_reminder(
        &self,
        principal_account_id: Uuid,
        event_id: Uuid,
        reminder_set: Option<bool>,
        reminder_at: Option<String>,
        reminder_dismissed_at: Option<String>,
    ) -> Result<()> {
        self.update_accessible_event_reminder(
            principal_account_id,
            event_id,
            reminder_set,
            reminder_at,
            reminder_dismissed_at,
        )
        .await
    }

    async fn update_jmap_mail_reminder(
        &self,
        account_id: Uuid,
        message_id: Uuid,
        reminder_set: Option<bool>,
        reminder_at: Option<String>,
        reminder_dismissed_at: Option<String>,
        audit: AuditEntryInput,
    ) -> Result<()> {
        self.update_jmap_email_followup_flags(
            account_id,
            message_id,
            JmapEmailFollowupUpdate {
                reminder_set,
                reminder_at,
                reminder_dismissed_at,
                ..Default::default()
            },
            audit,
        )
        .await?;
        Ok(())
    }

    async fn dismiss_jmap_reminder_occurrence(
        &self,
        account_id: Uuid,
        source_type: String,
        source_id: Uuid,
        occurrence_start_at: String,
        dismissed_at: String,
    ) -> Result<()> {
        self.dismiss_reminder_occurrence(
            account_id,
            &source_type,
            source_id,
            &occurrence_start_at,
            &dismissed_at,
        )
        .await
    }

    async fn fetch_jmap_shares(&self, account_id: Uuid) -> Result<Vec<Value>> {
        let mut shares = Vec::new();
        for grant in self
            .fetch_outgoing_mailbox_delegation_grants(account_id)
            .await?
        {
            shares.push(project_share("mailbox", serde_json::to_value(grant)?)?);
        }
        for grant in self
            .fetch_outgoing_sender_delegation_grants(account_id)
            .await?
        {
            shares.push(project_share("sender", serde_json::to_value(grant)?)?);
        }
        for kind in [
            lpe_storage::CollaborationResourceKind::Contacts,
            lpe_storage::CollaborationResourceKind::Calendar,
            lpe_storage::CollaborationResourceKind::Tasks,
        ] {
            let share_type = kind.as_str();
            for grant in self
                .fetch_outgoing_collaboration_grants(account_id, kind)
                .await?
            {
                shares.push(project_share(share_type, serde_json::to_value(grant)?)?);
            }
        }
        for grant in self.fetch_outgoing_task_list_grants(account_id).await? {
            shares.push(project_share("taskList", serde_json::to_value(grant)?)?);
        }
        Ok(shares)
    }

    async fn upsert_jmap_share(
        &self,
        input: JmapShareInput,
        audit: AuditEntryInput,
    ) -> Result<Value> {
        let share_type = input.share_type.as_str();
        let value = match share_type {
            "mailbox" => serde_json::to_value(
                self.upsert_mailbox_delegation_grant(
                    MailboxDelegationGrantInput {
                        owner_account_id: input.owner_account_id,
                        grantee_email: input.grantee_email,
                        may_write: input.may_write,
                    },
                    audit,
                )
                .await?,
            )?,
            "sender" => serde_json::to_value(
                self.upsert_sender_delegation_grant(
                    SenderDelegationGrantInput {
                        owner_account_id: input.owner_account_id,
                        grantee_email: input.grantee_email,
                        sender_right: parse_sender_right(input.sender_right.as_deref())?,
                    },
                    audit,
                )
                .await?,
            )?,
            "contacts" | "calendar" | "tasks" => serde_json::to_value(
                self.upsert_collaboration_grant(
                    CollaborationGrantInput {
                        kind: parse_collaboration_kind(share_type)?,
                        owner_account_id: input.owner_account_id,
                        grantee_email: input.grantee_email,
                        calendar_id: input.calendar_id,
                        may_read: input.may_read,
                        may_write: input.may_write,
                        may_delete: input.may_delete,
                        may_share: input.may_share,
                    },
                    audit,
                )
                .await?,
            )?,
            "taskList" => serde_json::to_value(
                self.upsert_task_list_grant(
                    TaskListGrantInput {
                        owner_account_id: input.owner_account_id,
                        task_list_id: input
                            .task_list_id
                            .ok_or_else(|| anyhow::anyhow!("taskListId is required"))?,
                        grantee_email: input.grantee_email,
                        may_read: input.may_read,
                        may_write: input.may_write,
                        may_delete: input.may_delete,
                        may_share: input.may_share,
                    },
                    audit,
                )
                .await?,
            )?,
            _ => anyhow::bail!("unsupported share type"),
        };
        project_share(share_type, value)
    }

    async fn delete_jmap_share(&self, share: Value, audit: AuditEntryInput) -> Result<()> {
        let share_type = share_type(&share)?;
        let owner_account_id = share_uuid(&share, "ownerAccountId")?;
        let grantee_account_id = share_uuid(&share, "granteeAccountId")?;
        match share_type {
            "mailbox" => {
                self.delete_mailbox_delegation_grant(owner_account_id, grantee_account_id, audit)
                    .await
            }
            "sender" => {
                self.delete_sender_delegation_grant(
                    owner_account_id,
                    grantee_account_id,
                    parse_sender_right(share.get("senderRight").and_then(Value::as_str))?,
                    audit,
                )
                .await
            }
            "contacts" | "calendar" | "tasks" => {
                if share_type == "calendar" {
                    if let Some(calendar_id) = share.get("calendarId").and_then(Value::as_str) {
                        return self
                            .delete_calendar_collection_grant(
                                owner_account_id,
                                calendar_id,
                                grantee_account_id,
                                audit,
                            )
                            .await;
                    }
                }
                self.delete_collaboration_grant(
                    owner_account_id,
                    parse_collaboration_kind(share_type)?,
                    grantee_account_id,
                    audit,
                )
                .await
            }
            "taskList" => {
                self.delete_task_list_grant(
                    owner_account_id,
                    share_uuid(&share, "taskListId")?,
                    grantee_account_id,
                    audit,
                )
                .await
            }
            _ => anyhow::bail!("unsupported share type"),
        }
    }
}

fn project_share(share_type: &str, value: Value) -> Result<Value> {
    let object = value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("share projection must be an object"))?;
    let grant_id = object
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("share projection is missing id"))?
        .to_string();
    let mut projected = Map::new();
    projected.insert(
        "id".to_string(),
        Value::String(format!("{share_type}:{grant_id}")),
    );
    projected.insert("@type".to_string(), Value::String("Share".to_string()));
    projected.insert("type".to_string(), Value::String(share_type.to_string()));
    projected.insert("grantId".to_string(), Value::String(grant_id));
    copy_share_field(object, &mut projected, "ownerAccountId");
    copy_share_field(object, &mut projected, "ownerEmail");
    copy_share_field(object, &mut projected, "ownerDisplayName");
    copy_share_field(object, &mut projected, "granteeAccountId");
    copy_share_field(object, &mut projected, "granteeEmail");
    copy_share_field(object, &mut projected, "granteeDisplayName");
    copy_share_field_as(object, &mut projected, "createdAt", "created");
    copy_share_field_as(object, &mut projected, "updatedAt", "updated");
    match share_type {
        "mailbox" => {
            projected.insert(
                "rights".to_string(),
                json!({
                    "mayRead": true,
                    "mayWrite": object.get("mayWrite").and_then(Value::as_bool).unwrap_or(false),
                    "mayDelete": false,
                    "mayShare": false,
                    "maySend": false
                }),
            );
        }
        "sender" => {
            let sender_right = object
                .get("senderRight")
                .and_then(Value::as_str)
                .unwrap_or("send_on_behalf");
            projected.insert(
                "senderRight".to_string(),
                Value::String(sender_right.to_string()),
            );
            projected.insert(
                "rights".to_string(),
                json!({
                    "mayRead": false,
                    "mayWrite": false,
                    "mayDelete": false,
                    "mayShare": false,
                    "maySend": true,
                    "maySendAs": sender_right == "send_as",
                    "maySendOnBehalf": sender_right == "send_on_behalf"
                }),
            );
        }
        "contacts" | "calendar" | "tasks" => {
            if share_type == "calendar" {
                copy_share_field(object, &mut projected, "calendarId");
                copy_share_field(object, &mut projected, "calendarName");
            }
            projected.insert(
                "rights".to_string(),
                share_rights(object).unwrap_or_else(default_share_rights),
            );
        }
        "taskList" => {
            copy_share_field(object, &mut projected, "taskListId");
            copy_share_field(object, &mut projected, "taskListName");
            projected.insert(
                "rights".to_string(),
                share_rights(object).unwrap_or_else(default_share_rights),
            );
        }
        _ => anyhow::bail!("unsupported share type"),
    }
    Ok(Value::Object(projected))
}

fn copy_share_field(source: &Map<String, Value>, target: &mut Map<String, Value>, field: &str) {
    copy_share_field_as(source, target, field, field);
}

fn copy_share_field_as(
    source: &Map<String, Value>,
    target: &mut Map<String, Value>,
    source_field: &str,
    target_field: &str,
) {
    if let Some(value) = source.get(source_field).filter(|value| !value.is_null()) {
        target.insert(target_field.to_string(), value.clone());
    }
}

fn share_rights(object: &Map<String, Value>) -> Option<Value> {
    object.get("rights").cloned().or_else(|| {
        Some(json!({
            "mayRead": object.get("mayRead")?.as_bool()?,
            "mayWrite": object.get("mayWrite").and_then(Value::as_bool).unwrap_or(false),
            "mayDelete": object.get("mayDelete").and_then(Value::as_bool).unwrap_or(false),
            "mayShare": object.get("mayShare").and_then(Value::as_bool).unwrap_or(false)
        }))
    })
}

fn default_share_rights() -> Value {
    json!({
        "mayRead": true,
        "mayWrite": false,
        "mayDelete": false,
        "mayShare": false
    })
}

fn share_type(share: &Value) -> Result<&str> {
    share
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("share type is required"))
}

fn share_uuid(share: &Value, field: &str) -> Result<Uuid> {
    share
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("{field} is required"))?
        .parse()
        .map_err(Into::into)
}

fn parse_collaboration_kind(value: &str) -> Result<CollaborationResourceKind> {
    match value {
        "contacts" => Ok(CollaborationResourceKind::Contacts),
        "calendar" => Ok(CollaborationResourceKind::Calendar),
        "tasks" => Ok(CollaborationResourceKind::Tasks),
        _ => anyhow::bail!("unsupported collaboration share type"),
    }
}

fn parse_sender_right(value: Option<&str>) -> Result<SenderDelegationRight> {
    match value.unwrap_or("send_on_behalf") {
        "send_as" => Ok(SenderDelegationRight::SendAs),
        "send_on_behalf" => Ok(SenderDelegationRight::SendOnBehalf),
        _ => anyhow::bail!("unsupported sender right"),
    }
}
