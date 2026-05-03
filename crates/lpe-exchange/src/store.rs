use lpe_mail_auth::{AccountAuthStore, StoreFuture};
use lpe_storage::{
    AccessibleContact, AccessibleEvent, AuditEntryInput, CollaborationCollection,
    SavedDraftMessage, Storage, SubmitMessageInput, SubmittedMessage,
};
use uuid::Uuid;

pub trait ExchangeStore: AccountAuthStore {
    fn fetch_accessible_contact_collections<'a>(
        &'a self,
        principal_account_id: Uuid,
    ) -> StoreFuture<'a, Vec<CollaborationCollection>>;

    fn fetch_accessible_calendar_collections<'a>(
        &'a self,
        principal_account_id: Uuid,
    ) -> StoreFuture<'a, Vec<CollaborationCollection>>;

    fn fetch_accessible_contacts_in_collection<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: &'a str,
    ) -> StoreFuture<'a, Vec<AccessibleContact>>;

    fn fetch_accessible_events_in_collection<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: &'a str,
    ) -> StoreFuture<'a, Vec<AccessibleEvent>>;

    fn fetch_accessible_contacts_by_ids<'a>(
        &'a self,
        principal_account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<AccessibleContact>>;

    fn fetch_accessible_events_by_ids<'a>(
        &'a self,
        principal_account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<AccessibleEvent>>;

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
