use anyhow::Result;
use lpe_mail_auth::AccountAuthStore;
use lpe_storage::{
    AuditEntryInput, ImapEmail, JmapEmailQuery, JmapMailbox, SavedDraftMessage, Storage,
    SubmitMessageInput,
};
use std::{future::Future, pin::Pin};
use uuid::Uuid;

pub(crate) type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

pub trait ImapStore: AccountAuthStore {
    fn ensure_imap_mailboxes<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<JmapMailbox>>;
    fn fetch_imap_emails<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Uuid,
    ) -> StoreFuture<'a, Vec<ImapEmail>>;
    fn update_imap_flags<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Uuid,
        message_ids: &'a [Uuid],
        unread: Option<bool>,
        flagged: Option<bool>,
    ) -> StoreFuture<'a, ()>;
    fn query_jmap_email_ids<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        search_text: Option<&'a str>,
        position: u64,
        limit: u64,
    ) -> StoreFuture<'a, JmapEmailQuery>;
    fn save_draft_message<'a>(
        &'a self,
        input: SubmitMessageInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, SavedDraftMessage>;
}

impl ImapStore for Storage {
    fn ensure_imap_mailboxes<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<JmapMailbox>> {
        Box::pin(async move { self.ensure_imap_mailboxes(account_id).await })
    }

    fn fetch_imap_emails<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Uuid,
    ) -> StoreFuture<'a, Vec<ImapEmail>> {
        Box::pin(async move { self.fetch_imap_emails(account_id, mailbox_id).await })
    }

    fn update_imap_flags<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Uuid,
        message_ids: &'a [Uuid],
        unread: Option<bool>,
        flagged: Option<bool>,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            self.update_imap_flags(account_id, mailbox_id, message_ids, unread, flagged)
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

    fn save_draft_message<'a>(
        &'a self,
        input: SubmitMessageInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, SavedDraftMessage> {
        Box::pin(async move { self.save_draft_message(input, audit).await })
    }
}
