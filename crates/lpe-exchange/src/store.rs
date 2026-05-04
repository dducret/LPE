use lpe_mail_auth::{AccountAuthStore, StoreFuture};
use lpe_storage::{
    AccessibleContact, AccessibleEvent, ActiveSyncAttachment, ActiveSyncAttachmentContent,
    AuditEntryInput, CollaborationCollection, JmapEmail, JmapEmailQuery, JmapImportedEmailInput,
    JmapMailbox, JmapMailboxCreateInput, SavedDraftMessage, Storage, SubmitMessageInput,
    SubmittedMessage, UpsertClientContactInput, UpsertClientEventInput,
};
use sqlx::Row;
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

    fn fetch_jmap_mailboxes<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<JmapMailbox>>;

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

    fn fetch_jmap_emails<'a>(
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

    fn delete_jmap_email<'a>(
        &'a self,
        account_id: Uuid,
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

    fn fetch_jmap_mailboxes<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<JmapMailbox>> {
        Box::pin(async move { self.fetch_jmap_mailboxes(account_id).await })
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

    fn fetch_jmap_emails<'a>(
        &'a self,
        account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<JmapEmail>> {
        Box::pin(async move { self.fetch_jmap_emails(account_id, ids).await })
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

    fn delete_jmap_email<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
        audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move { self.delete_jmap_email(account_id, message_id, audit).await })
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
