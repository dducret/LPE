use anyhow::Result;
use lpe_mail_auth::AccountAuthStore;
use lpe_storage::{
    ActiveSyncAttachment, ActiveSyncAttachmentContent, ActiveSyncItemState, ActiveSyncSyncState,
    AuditEntryInput, ClientContact, ClientEvent, JmapEmail, JmapMailbox, SavedDraftMessage,
    Storage, SubmitMessageInput, SubmittedMessage, UpsertClientContactInput,
    UpsertClientEventInput,
};
use std::{future::Future, pin::Pin};
use uuid::Uuid;

pub(crate) type StoreFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

pub trait ActiveSyncStore: AccountAuthStore {
    fn fetch_jmap_mailboxes<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<JmapMailbox>>;
    fn query_jmap_email_ids<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Option<Uuid>,
        search_text: Option<&'a str>,
        position: u64,
        limit: u64,
    ) -> StoreFuture<'a, lpe_storage::JmapEmailQuery>;
    fn fetch_jmap_emails<'a>(
        &'a self,
        account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<JmapEmail>>;
    fn fetch_latest_activesync_sync_state<'a>(
        &'a self,
        account_id: Uuid,
        device_id: &'a str,
        collection_id: &'a str,
    ) -> StoreFuture<'a, Option<ActiveSyncSyncState>>;
    fn fetch_activesync_email_states<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Uuid,
        position: u64,
        limit: u64,
    ) -> StoreFuture<'a, Vec<ActiveSyncItemState>>;
    fn fetch_activesync_email_states_by_ids<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<ActiveSyncItemState>>;
    fn fetch_jmap_draft<'a>(
        &'a self,
        account_id: Uuid,
        id: Uuid,
    ) -> StoreFuture<'a, Option<JmapEmail>>;
    fn fetch_activesync_message_attachments<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
    ) -> StoreFuture<'a, Vec<ActiveSyncAttachment>>;
    fn fetch_activesync_attachment_content<'a>(
        &'a self,
        account_id: Uuid,
        file_reference: &'a str,
    ) -> StoreFuture<'a, Option<ActiveSyncAttachmentContent>>;
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
    fn fetch_client_contacts_by_ids<'a>(
        &'a self,
        account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<ClientContact>>;
    fn upsert_client_contact<'a>(
        &'a self,
        input: UpsertClientContactInput,
    ) -> StoreFuture<'a, ClientContact>;
    fn delete_client_contact<'a>(
        &'a self,
        account_id: Uuid,
        contact_id: Uuid,
    ) -> StoreFuture<'a, ()>;
    fn fetch_client_events<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<ClientEvent>>;
    fn fetch_client_events_by_ids<'a>(
        &'a self,
        account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<ClientEvent>>;
    fn upsert_client_event<'a>(
        &'a self,
        input: UpsertClientEventInput,
    ) -> StoreFuture<'a, ClientEvent>;
    fn delete_client_event<'a>(&'a self, account_id: Uuid, event_id: Uuid) -> StoreFuture<'a, ()>;
    fn fetch_activesync_contact_states<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Vec<ActiveSyncItemState>>;
    fn fetch_activesync_contact_states_by_ids<'a>(
        &'a self,
        account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<ActiveSyncItemState>>;
    fn fetch_activesync_event_states<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Vec<ActiveSyncItemState>>;
    fn fetch_activesync_event_states_by_ids<'a>(
        &'a self,
        account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<ActiveSyncItemState>>;
    fn store_activesync_sync_state<'a>(
        &'a self,
        account_id: Uuid,
        device_id: &'a str,
        collection_id: &'a str,
        sync_key: &'a str,
        snapshot_json: String,
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
        search_text: Option<&'a str>,
        position: u64,
        limit: u64,
    ) -> StoreFuture<'a, lpe_storage::JmapEmailQuery> {
        Box::pin(async move {
            self.query_jmap_email_ids(account_id, mailbox_id, search_text, position, limit)
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

    fn fetch_latest_activesync_sync_state<'a>(
        &'a self,
        account_id: Uuid,
        device_id: &'a str,
        collection_id: &'a str,
    ) -> StoreFuture<'a, Option<ActiveSyncSyncState>> {
        Box::pin(async move {
            self.fetch_latest_activesync_sync_state(account_id, device_id, collection_id)
                .await
        })
    }

    fn fetch_activesync_email_states<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Uuid,
        position: u64,
        limit: u64,
    ) -> StoreFuture<'a, Vec<ActiveSyncItemState>> {
        Box::pin(async move {
            self.fetch_activesync_email_states(account_id, mailbox_id, position, limit)
                .await
        })
    }

    fn fetch_activesync_email_states_by_ids<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<ActiveSyncItemState>> {
        Box::pin(async move {
            self.fetch_activesync_email_states_by_ids(account_id, mailbox_id, ids)
                .await
        })
    }

    fn fetch_jmap_draft<'a>(
        &'a self,
        account_id: Uuid,
        id: Uuid,
    ) -> StoreFuture<'a, Option<JmapEmail>> {
        Box::pin(async move { self.fetch_jmap_draft(account_id, id).await })
    }

    fn fetch_activesync_message_attachments<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
    ) -> StoreFuture<'a, Vec<ActiveSyncAttachment>> {
        Box::pin(async move {
            self.fetch_activesync_message_attachments(account_id, message_id)
                .await
        })
    }

    fn fetch_activesync_attachment_content<'a>(
        &'a self,
        account_id: Uuid,
        file_reference: &'a str,
    ) -> StoreFuture<'a, Option<ActiveSyncAttachmentContent>> {
        Box::pin(async move {
            self.fetch_activesync_attachment_content(account_id, file_reference)
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

    fn fetch_client_contacts_by_ids<'a>(
        &'a self,
        account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<ClientContact>> {
        Box::pin(async move { self.fetch_client_contacts_by_ids(account_id, ids).await })
    }

    fn upsert_client_contact<'a>(
        &'a self,
        input: UpsertClientContactInput,
    ) -> StoreFuture<'a, ClientContact> {
        Box::pin(async move { self.upsert_client_contact(input).await })
    }

    fn delete_client_contact<'a>(
        &'a self,
        account_id: Uuid,
        contact_id: Uuid,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move { self.delete_client_contact(account_id, contact_id).await })
    }

    fn fetch_client_events<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<ClientEvent>> {
        Box::pin(async move { self.fetch_client_events(account_id).await })
    }

    fn fetch_client_events_by_ids<'a>(
        &'a self,
        account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<ClientEvent>> {
        Box::pin(async move { self.fetch_client_events_by_ids(account_id, ids).await })
    }

    fn upsert_client_event<'a>(
        &'a self,
        input: UpsertClientEventInput,
    ) -> StoreFuture<'a, ClientEvent> {
        Box::pin(async move { self.upsert_client_event(input).await })
    }

    fn delete_client_event<'a>(&'a self, account_id: Uuid, event_id: Uuid) -> StoreFuture<'a, ()> {
        Box::pin(async move { self.delete_client_event(account_id, event_id).await })
    }

    fn fetch_activesync_contact_states<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Vec<ActiveSyncItemState>> {
        Box::pin(async move { self.fetch_activesync_contact_states(account_id).await })
    }

    fn fetch_activesync_contact_states_by_ids<'a>(
        &'a self,
        account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<ActiveSyncItemState>> {
        Box::pin(async move {
            self.fetch_activesync_contact_states_by_ids(account_id, ids)
                .await
        })
    }

    fn fetch_activesync_event_states<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Vec<ActiveSyncItemState>> {
        Box::pin(async move { self.fetch_activesync_event_states(account_id).await })
    }

    fn fetch_activesync_event_states_by_ids<'a>(
        &'a self,
        account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<ActiveSyncItemState>> {
        Box::pin(async move {
            self.fetch_activesync_event_states_by_ids(account_id, ids)
                .await
        })
    }

    fn store_activesync_sync_state<'a>(
        &'a self,
        account_id: Uuid,
        device_id: &'a str,
        collection_id: &'a str,
        sync_key: &'a str,
        snapshot_json: String,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move {
            self.store_activesync_sync_state(
                account_id,
                device_id,
                collection_id,
                sync_key,
                &snapshot_json,
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
