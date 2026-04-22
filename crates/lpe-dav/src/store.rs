use lpe_mail_auth::AccountAuthStore;
use lpe_storage::{
    AccessibleContact, AccessibleEvent, CollaborationCollection, DavTask, Storage,
    UpsertClientContactInput, UpsertClientEventInput, UpsertClientTaskInput,
};
use uuid::Uuid;

#[allow(dead_code)]
pub trait DavStore: AccountAuthStore {
    fn fetch_accessible_contact_collections<'a>(
        &'a self,
        principal_account_id: Uuid,
    ) -> lpe_mail_auth::StoreFuture<'a, Vec<CollaborationCollection>>;
    fn fetch_accessible_calendar_collections<'a>(
        &'a self,
        principal_account_id: Uuid,
    ) -> lpe_mail_auth::StoreFuture<'a, Vec<CollaborationCollection>>;
    fn fetch_accessible_task_collections<'a>(
        &'a self,
        principal_account_id: Uuid,
    ) -> lpe_mail_auth::StoreFuture<'a, Vec<CollaborationCollection>>;
    fn fetch_accessible_contacts<'a>(
        &'a self,
        principal_account_id: Uuid,
    ) -> lpe_mail_auth::StoreFuture<'a, Vec<AccessibleContact>>;
    fn fetch_accessible_contacts_in_collection<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: &'a str,
    ) -> lpe_mail_auth::StoreFuture<'a, Vec<AccessibleContact>>;
    fn fetch_accessible_events<'a>(
        &'a self,
        principal_account_id: Uuid,
    ) -> lpe_mail_auth::StoreFuture<'a, Vec<AccessibleEvent>>;
    fn fetch_accessible_events_in_collection<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: &'a str,
    ) -> lpe_mail_auth::StoreFuture<'a, Vec<AccessibleEvent>>;
    fn fetch_dav_tasks<'a>(
        &'a self,
        principal_account_id: Uuid,
    ) -> lpe_mail_auth::StoreFuture<'a, Vec<DavTask>>;
    fn fetch_dav_tasks_by_ids<'a>(
        &'a self,
        principal_account_id: Uuid,
        ids: &'a [Uuid],
    ) -> lpe_mail_auth::StoreFuture<'a, Vec<DavTask>>;
    fn create_accessible_contact<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: Option<&'a str>,
        input: UpsertClientContactInput,
    ) -> lpe_mail_auth::StoreFuture<'a, AccessibleContact>;
    fn create_accessible_event<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: Option<&'a str>,
        input: UpsertClientEventInput,
    ) -> lpe_mail_auth::StoreFuture<'a, AccessibleEvent>;
    fn update_accessible_contact<'a>(
        &'a self,
        principal_account_id: Uuid,
        contact_id: Uuid,
        input: UpsertClientContactInput,
    ) -> lpe_mail_auth::StoreFuture<'a, AccessibleContact>;
    fn update_accessible_event<'a>(
        &'a self,
        principal_account_id: Uuid,
        event_id: Uuid,
        input: UpsertClientEventInput,
    ) -> lpe_mail_auth::StoreFuture<'a, AccessibleEvent>;
    fn upsert_dav_task<'a>(
        &'a self,
        input: UpsertClientTaskInput,
    ) -> lpe_mail_auth::StoreFuture<'a, DavTask>;
    fn delete_accessible_contact<'a>(
        &'a self,
        principal_account_id: Uuid,
        contact_id: Uuid,
    ) -> lpe_mail_auth::StoreFuture<'a, ()>;
    fn delete_accessible_event<'a>(
        &'a self,
        principal_account_id: Uuid,
        event_id: Uuid,
    ) -> lpe_mail_auth::StoreFuture<'a, ()>;
    fn delete_dav_task<'a>(
        &'a self,
        principal_account_id: Uuid,
        task_id: Uuid,
    ) -> lpe_mail_auth::StoreFuture<'a, ()>;
}

impl DavStore for Storage {
    fn fetch_accessible_contact_collections<'a>(
        &'a self,
        principal_account_id: Uuid,
    ) -> lpe_mail_auth::StoreFuture<'a, Vec<CollaborationCollection>> {
        Box::pin(async move {
            self.fetch_accessible_contact_collections(principal_account_id)
                .await
        })
    }

    fn fetch_accessible_calendar_collections<'a>(
        &'a self,
        principal_account_id: Uuid,
    ) -> lpe_mail_auth::StoreFuture<'a, Vec<CollaborationCollection>> {
        Box::pin(async move {
            self.fetch_accessible_calendar_collections(principal_account_id)
                .await
        })
    }

    fn fetch_accessible_task_collections<'a>(
        &'a self,
        principal_account_id: Uuid,
    ) -> lpe_mail_auth::StoreFuture<'a, Vec<CollaborationCollection>> {
        Box::pin(async move {
            self.fetch_accessible_task_collections(principal_account_id)
                .await
        })
    }

    fn fetch_accessible_contacts<'a>(
        &'a self,
        principal_account_id: Uuid,
    ) -> lpe_mail_auth::StoreFuture<'a, Vec<AccessibleContact>> {
        Box::pin(async move { self.fetch_accessible_contacts(principal_account_id).await })
    }

    fn fetch_accessible_contacts_in_collection<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: &'a str,
    ) -> lpe_mail_auth::StoreFuture<'a, Vec<AccessibleContact>> {
        Box::pin(async move {
            self.fetch_accessible_contacts_in_collection(principal_account_id, collection_id)
                .await
        })
    }

    fn fetch_accessible_events<'a>(
        &'a self,
        principal_account_id: Uuid,
    ) -> lpe_mail_auth::StoreFuture<'a, Vec<AccessibleEvent>> {
        Box::pin(async move { self.fetch_accessible_events(principal_account_id).await })
    }

    fn fetch_accessible_events_in_collection<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: &'a str,
    ) -> lpe_mail_auth::StoreFuture<'a, Vec<AccessibleEvent>> {
        Box::pin(async move {
            self.fetch_accessible_events_in_collection(principal_account_id, collection_id)
                .await
        })
    }

    fn fetch_dav_tasks<'a>(
        &'a self,
        principal_account_id: Uuid,
    ) -> lpe_mail_auth::StoreFuture<'a, Vec<DavTask>> {
        Box::pin(async move { self.fetch_dav_tasks(principal_account_id).await })
    }

    fn fetch_dav_tasks_by_ids<'a>(
        &'a self,
        principal_account_id: Uuid,
        ids: &'a [Uuid],
    ) -> lpe_mail_auth::StoreFuture<'a, Vec<DavTask>> {
        Box::pin(async move { self.fetch_dav_tasks_by_ids(principal_account_id, ids).await })
    }

    fn create_accessible_contact<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: Option<&'a str>,
        input: UpsertClientContactInput,
    ) -> lpe_mail_auth::StoreFuture<'a, AccessibleContact> {
        Box::pin(async move {
            self.create_accessible_contact(principal_account_id, collection_id, input)
                .await
        })
    }

    fn create_accessible_event<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: Option<&'a str>,
        input: UpsertClientEventInput,
    ) -> lpe_mail_auth::StoreFuture<'a, AccessibleEvent> {
        Box::pin(async move {
            self.create_accessible_event(principal_account_id, collection_id, input)
                .await
        })
    }

    fn update_accessible_contact<'a>(
        &'a self,
        principal_account_id: Uuid,
        contact_id: Uuid,
        input: UpsertClientContactInput,
    ) -> lpe_mail_auth::StoreFuture<'a, AccessibleContact> {
        Box::pin(async move {
            self.update_accessible_contact(principal_account_id, contact_id, input)
                .await
        })
    }

    fn update_accessible_event<'a>(
        &'a self,
        principal_account_id: Uuid,
        event_id: Uuid,
        input: UpsertClientEventInput,
    ) -> lpe_mail_auth::StoreFuture<'a, AccessibleEvent> {
        Box::pin(async move {
            self.update_accessible_event(principal_account_id, event_id, input)
                .await
        })
    }

    fn upsert_dav_task<'a>(
        &'a self,
        input: UpsertClientTaskInput,
    ) -> lpe_mail_auth::StoreFuture<'a, DavTask> {
        Box::pin(async move { self.upsert_dav_task(input).await })
    }

    fn delete_accessible_contact<'a>(
        &'a self,
        principal_account_id: Uuid,
        contact_id: Uuid,
    ) -> lpe_mail_auth::StoreFuture<'a, ()> {
        Box::pin(async move {
            self.delete_accessible_contact(principal_account_id, contact_id)
                .await
        })
    }

    fn delete_accessible_event<'a>(
        &'a self,
        principal_account_id: Uuid,
        event_id: Uuid,
    ) -> lpe_mail_auth::StoreFuture<'a, ()> {
        Box::pin(async move {
            self.delete_accessible_event(principal_account_id, event_id)
                .await
        })
    }

    fn delete_dav_task<'a>(
        &'a self,
        principal_account_id: Uuid,
        task_id: Uuid,
    ) -> lpe_mail_auth::StoreFuture<'a, ()> {
        Box::pin(async move { self.delete_dav_task(principal_account_id, task_id).await })
    }
}
