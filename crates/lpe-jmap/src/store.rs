use anyhow::Result;
use lpe_storage::{
    AuditEntryInput, AuthenticatedAccount, JmapEmail, JmapEmailQuery, JmapEmailSubmission,
    JmapImportedEmailInput, JmapMailbox, JmapMailboxCreateInput, JmapMailboxUpdateInput,
    JmapQuota, JmapUploadBlob, SavedDraftMessage, Storage, SubmitMessageInput, SubmittedMessage,
};
use uuid::Uuid;

#[allow(async_fn_in_trait)]
pub trait JmapStore: Clone + Send + Sync + 'static {
    async fn fetch_account_session(&self, token: &str) -> Result<Option<AuthenticatedAccount>>;
    async fn fetch_jmap_mailboxes(&self, account_id: Uuid) -> Result<Vec<JmapMailbox>>;
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
}

impl JmapStore for Storage {
    async fn fetch_account_session(&self, token: &str) -> Result<Option<AuthenticatedAccount>> {
        self.fetch_account_session(token).await
    }

    async fn fetch_jmap_mailboxes(&self, account_id: Uuid) -> Result<Vec<JmapMailbox>> {
        self.fetch_jmap_mailboxes(account_id).await
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
        self.destroy_jmap_mailbox(account_id, mailbox_id, audit).await
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
        self.delete_draft_message(account_id, message_id, audit).await
    }

    async fn submit_draft_message(
        &self,
        account_id: Uuid,
        draft_message_id: Uuid,
        source: &str,
        audit: AuditEntryInput,
    ) -> Result<SubmittedMessage> {
        self.submit_draft_message(account_id, draft_message_id, source, audit)
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
}
