use anyhow::Result;
use lpe_mail_auth::AccountAuthStore;
use lpe_storage::{
    ActiveSyncSyncState, AuditEntryInput, ClientContact, ClientEvent, JmapEmail, JmapMailbox,
    SavedDraftMessage, Storage, SubmitMessageInput, SubmittedMessage,
};
use serde_json::Value;
use std::{future::Future, pin::Pin};
use uuid::Uuid;

pub(crate) type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

pub trait ActiveSyncStore: AccountAuthStore {
    fn fetch_jmap_mailboxes<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<JmapMailbox>>;
    fn query_jmap_email_ids<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        position: u64,
        limit: u64,
    ) -> StoreFuture<'a, lpe_storage::JmapEmailQuery>;
    fn fetch_jmap_emails<'a>(
        &'a self,
        account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<JmapEmail>>;
    fn fetch_jmap_draft<'a>(
        &'a self,
        account_id: Uuid,
        id: Uuid,
    ) -> StoreFuture<'a, Option<JmapEmail>>;
    fn save_draft_message<'a>(
        &'a self,
        input: SubmitMessageInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, SavedDraftMessage>;
    fn delete_draft_message<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()>;
    fn submit_message<'a>(
        &'a self,
        input: SubmitMessageInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, SubmittedMessage>;
    fn fetch_client_contacts<'a>(&'a self, account_id: Uuid)
        -> StoreFuture<'a, Vec<ClientContact>>;
    fn fetch_client_events<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<ClientEvent>>;
    fn store_activesync_sync_state<'a>(
        &'a self,
        account_id: Uuid,
        device_id: &'a str,
        collection_id: &'a str,
        sync_key: &'a str,
        snapshot: Value,
    ) -> StoreFuture<'a, ()>;
    fn fetch_activesync_sync_state<'a>(
        &'a self,
        account_id: Uuid,
        device_id: &'a str,
        collection_id: &'a str,
        sync_key: &'a str,
    ) -> StoreFuture<'a, Option<ActiveSyncSyncState>>;
}

impl ActiveSyncStore for Storage {
    fn fetch_jmap_mailboxes<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<JmapMailbox>> {
        Box::pin(async move { self.fetch_jmap_mailboxes(account_id).await })
    }

    fn query_jmap_email_ids<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        position: u64,
        limit: u64,
    ) -> StoreFuture<'a, lpe_storage::JmapEmailQuery> {
        Box::pin(async move {
            self.query_jmap_email_ids(account_id, mailbox_id, position, limit)
                .await
        })
    }

    fn fetch_jmap_emails<'a>(
        &'a self,
        account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<JmapEmail>> {
        Box::pin(async move { self.fetch_jmap_emails(account_id, ids).await })
    }

    fn fetch_jmap_draft<'a>(
        &'a self,
        account_id: Uuid,
        id: Uuid,
    ) -> StoreFuture<'a, Option<JmapEmail>> {
        Box::pin(async move { self.fetch_jmap_draft(account_id, id).await })
    }

    fn save_draft_message<'a>(
        &'a self,
        input: SubmitMessageInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, SavedDraftMessage> {
        Box::pin(async move { self.save_draft_message(input, audit).await })
    }

    fn delete_draft_message<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            self.delete_draft_message(account_id, message_id, audit)
                .await
        })
    }

    fn submit_message<'a>(
        &'a self,
        input: SubmitMessageInput,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, SubmittedMessage> {
        Box::pin(async move { self.submit_message(input, audit).await })
    }

    fn fetch_client_contacts<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Vec<ClientContact>> {
        Box::pin(async move { self.fetch_client_contacts(account_id).await })
    }

    fn fetch_client_events<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<ClientEvent>> {
        Box::pin(async move { self.fetch_client_events(account_id).await })
    }

    fn store_activesync_sync_state<'a>(
        &'a self,
        account_id: Uuid,
        device_id: &'a str,
        collection_id: &'a str,
        sync_key: &'a str,
        snapshot: Value,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            self.store_activesync_sync_state(
                account_id,
                device_id,
                collection_id,
                sync_key,
                &snapshot,
            )
            .await
        })
    }

    fn fetch_activesync_sync_state<'a>(
        &'a self,
        account_id: Uuid,
        device_id: &'a str,
        collection_id: &'a str,
        sync_key: &'a str,
    ) -> StoreFuture<'a, Option<ActiveSyncSyncState>> {
        Box::pin(async move {
            self.fetch_activesync_sync_state(account_id, device_id, collection_id, sync_key)
                .await
        })
    }
}
