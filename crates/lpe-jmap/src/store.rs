use anyhow::Result;
use lpe_storage::{
    AccessibleContact, AccessibleEvent, AuditEntryInput, AuthenticatedAccount,
    CanonicalChangeCategory, CanonicalChangeListener, CanonicalChangeReplay,
    CanonicalPushChangeSet, ClientTask, ClientTaskList, CollaborationCollection,
    CreateTaskListInput, JmapEmail, JmapEmailQuery, JmapEmailSubmission, JmapImportedEmailInput,
    JmapMailbox, JmapMailboxCreateInput, JmapMailboxUpdateInput, JmapQuota, JmapThreadQuery,
    JmapUploadBlob, MailboxAccountAccess, SavedDraftMessage, SenderIdentity, Storage,
    SubmitMessageInput, SubmittedMessage, UpdateTaskListInput, UpsertClientContactInput,
    UpsertClientEventInput, UpsertClientTaskInput,
};
use uuid::Uuid;

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
    async fn fetch_jmap_draft(&self, account_id: Uuid, id: Uuid) -> Result<Option<JmapEmail>>;
    async fn fetch_jmap_email_submissions(
        &self,
        account_id: Uuid,
        ids: &[Uuid],
    ) -> Result<Vec<JmapEmailSubmission>>;
    async fn fetch_jmap_quota(&self, account_id: Uuid) -> Result<JmapQuota>;
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
    async fn fetch_accessible_calendar_collections(
        &self,
        principal_account_id: Uuid,
    ) -> Result<Vec<CollaborationCollection>>;
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

    async fn fetch_accessible_calendar_collections(
        &self,
        principal_account_id: Uuid,
    ) -> Result<Vec<CollaborationCollection>> {
        self.fetch_accessible_calendar_collections(principal_account_id)
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
}
