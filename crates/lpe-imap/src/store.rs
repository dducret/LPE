use anyhow::Result;
use lpe_mail_auth::AccountAuthStore;
use lpe_storage::{
    AuditEntryInput, ImapEmail, JmapEmailQuery, JmapMailbox, JmapMailboxCreateInput,
    JmapMailboxUpdateInput, SavedDraftMessage, Storage, SubmitMessageInput,
};
use std::{future::Future, pin::Pin};
use uuid::Uuid;

pub(crate) type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

pub trait ImapStore: AccountAuthStore {
    fn ensure_imap_mailboxes<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<JmapMailbox>>;
    fn fetch_imap_highest_modseq<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, u64>;
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
        unchanged_since: Option<u64>,
    ) -> StoreFuture<'a, Vec<Uuid>>;
    fn query_jmap_email_ids<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        search_text: Option<&'a str>,
        position: u64,
        limit: u64,
    ) -> StoreFuture<'a, JmapEmailQuery>;
    fn create_imap_mailbox<'a>(
        &'a self,
        account_id: Uuid,
        name: &'a str,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapMailbox>;
    fn rename_imap_mailbox<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Uuid,
        name: &'a str,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapMailbox>;
    fn delete_imap_mailbox<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()>;
    fn copy_imap_email<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ImapEmail>;
    fn move_imap_email<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ImapEmail>;
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

    fn fetch_imap_highest_modseq<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, u64> {
        Box::pin(async move { self.fetch_imap_highest_modseq(account_id).await })
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
        unchanged_since: Option<u64>,
    ) -> StoreFuture<'a, Vec<Uuid>> {
        Box::pin(async move {
            self.update_imap_flags(
                account_id,
                mailbox_id,
                message_ids,
                unread,
                flagged,
                unchanged_since,
            )
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

    fn create_imap_mailbox<'a>(
        &'a self,
        account_id: Uuid,
        name: &'a str,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapMailbox> {
        Box::pin(async move {
            self.create_jmap_mailbox(
                JmapMailboxCreateInput {
                    account_id,
                    name: name.to_string(),
                    sort_order: None,
                },
                audit,
            )
            .await
        })
    }

    fn rename_imap_mailbox<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Uuid,
        name: &'a str,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapMailbox> {
        Box::pin(async move {
            self.update_jmap_mailbox(
                JmapMailboxUpdateInput {
                    account_id,
                    mailbox_id,
                    name: Some(name.to_string()),
                    sort_order: None,
                },
                audit,
            )
            .await
        })
    }

    fn delete_imap_mailbox<'a>(
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

    fn copy_imap_email<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ImapEmail> {
        Box::pin(async move {
            let copied = self
                .copy_jmap_email(account_id, message_id, target_mailbox_id, audit)
                .await?;
            self.fetch_imap_emails(account_id, target_mailbox_id)
                .await?
                .into_iter()
                .find(|email| email.id == copied.id)
                .ok_or_else(|| anyhow::anyhow!("copied IMAP message not found"))
        })
    }

    fn move_imap_email<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ImapEmail> {
        Box::pin(async move {
            let moved = self
                .move_jmap_email(account_id, message_id, target_mailbox_id, audit)
                .await?;
            self.fetch_imap_emails(account_id, target_mailbox_id)
                .await?
                .into_iter()
                .find(|email| email.id == moved.id)
                .ok_or_else(|| anyhow::anyhow!("moved IMAP message not found"))
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
