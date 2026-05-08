use axum::body::{to_bytes, Body};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use lpe_magika::{DetectionSource, Detector, MagikaDetection, Validator};
use lpe_mail_auth::{AccountAuthStore, AccountPrincipal, StoreFuture};
use lpe_storage::{
    AccessibleContact, AccessibleEvent, AccountLogin, ActiveSyncAttachment,
    ActiveSyncAttachmentContent, AttachmentUploadInput, AuthenticatedAccount, ClientTask,
    CollaborationCollection, CollaborationRights, JmapEmail, JmapEmailAddress, JmapEmailQuery,
    JmapImportedEmailInput, JmapMailbox, JmapMailboxCreateInput, SavedDraftMessage,
    SieveScriptDocument, StoredAccountAppPassword, SubmitMessageInput, SubmittedMessage,
    SubmittedRecipientInput, UpsertClientContactInput, UpsertClientEventInput,
    UpsertClientTaskInput,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use uuid::Uuid;

use crate::{
    mapi::MapiEndpoint,
    mapi_mailstore,
    service::{
        error_response, is_rpc_proxy_in_data_channel_request, mark_rpc_proxy_out_endpoint_bind_ack,
        rpc_proxy_in_channel_response_for_buffer, rpc_proxy_in_channel_response_for_endpoint_query,
        rpc_proxy_in_channel_response_for_endpoint_query_with_store, ExchangeService,
    },
    store::{
        ExchangeAddressBookDirectoryKind, ExchangeAddressBookEntry, ExchangeAddressBookEntryKind,
        ExchangeStore,
    },
};

#[derive(Clone, Default)]
struct FakeStore {
    session: Option<AuthenticatedAccount>,
    contact_collections: Arc<Mutex<Vec<CollaborationCollection>>>,
    calendar_collections: Arc<Mutex<Vec<CollaborationCollection>>>,
    task_collections: Arc<Mutex<Vec<CollaborationCollection>>>,
    contacts: Arc<Mutex<Vec<AccessibleContact>>>,
    contact_versions: Arc<Mutex<HashMap<Uuid, u64>>>,
    deleted_contacts: Arc<Mutex<Vec<Uuid>>>,
    events: Arc<Mutex<Vec<AccessibleEvent>>>,
    event_versions: Arc<Mutex<HashMap<Uuid, u64>>>,
    deleted_events: Arc<Mutex<Vec<Uuid>>>,
    tasks: Arc<Mutex<Vec<ClientTask>>>,
    task_versions: Arc<Mutex<HashMap<Uuid, u64>>>,
    deleted_tasks: Arc<Mutex<Vec<Uuid>>>,
    active_sieve_script: Arc<Mutex<Option<String>>>,
    saved_drafts: Arc<Mutex<Vec<SubmitMessageInput>>>,
    imported_emails: Arc<Mutex<Vec<JmapImportedEmailInput>>>,
    emails: Arc<Mutex<Vec<JmapEmail>>>,
    attachments: Arc<Mutex<HashMap<Uuid, Vec<ActiveSyncAttachment>>>>,
    attachment_contents: Arc<Mutex<HashMap<String, ActiveSyncAttachmentContent>>>,
    created_attachments: Arc<Mutex<Vec<AttachmentUploadInput>>>,
    submitted_messages: Arc<Mutex<Vec<SubmitMessageInput>>>,
    deleted_emails: Arc<Mutex<Vec<Uuid>>>,
    moved_emails: Arc<Mutex<Vec<(Uuid, Uuid)>>>,
    copied_emails: Arc<Mutex<Vec<(Uuid, Uuid)>>>,
    mailboxes: Arc<Mutex<Vec<JmapMailbox>>>,
    created_mailboxes: Arc<Mutex<Vec<JmapMailboxCreateInput>>>,
    destroyed_mailboxes: Arc<Mutex<Vec<Uuid>>>,
    directory_accounts: Arc<Mutex<Vec<AuthenticatedAccount>>>,
    omit_principal_from_directory: bool,
}

#[derive(Clone)]
struct FakeDetector {
    detection: MagikaDetection,
}

impl FakeDetector {
    fn pdf() -> Self {
        Self {
            detection: MagikaDetection {
                label: "pdf".to_string(),
                mime_type: "application/pdf".to_string(),
                description: "PDF document".to_string(),
                group: "document".to_string(),
                extensions: vec!["pdf".to_string()],
                score: Some(0.99),
            },
        }
    }

    fn executable() -> Self {
        Self {
            detection: MagikaDetection {
                label: "pebin".to_string(),
                mime_type: "application/vnd.microsoft.portable-executable".to_string(),
                description: "Windows executable".to_string(),
                group: "executable".to_string(),
                extensions: vec!["exe".to_string()],
                score: Some(0.99),
            },
        }
    }
}

impl Detector for FakeDetector {
    fn detect(&self, _source: DetectionSource<'_>) -> anyhow::Result<MagikaDetection> {
        Ok(self.detection.clone())
    }
}

impl FakeStore {
    fn account() -> AuthenticatedAccount {
        AuthenticatedAccount {
            tenant_id: "tenant-a".to_string(),
            account_id: Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap(),
            email: "alice@example.test".to_string(),
            display_name: "Alice".to_string(),
            expires_at: "2099-01-01T00:00:00Z".to_string(),
        }
    }

    fn rights() -> CollaborationRights {
        CollaborationRights {
            may_read: true,
            may_write: true,
            may_delete: true,
            may_share: true,
        }
    }

    fn collection(id: &str, kind: &str, display_name: &str) -> CollaborationCollection {
        let account = Self::account();
        CollaborationCollection {
            id: id.to_string(),
            kind: kind.to_string(),
            owner_account_id: account.account_id,
            owner_email: account.email,
            owner_display_name: account.display_name,
            display_name: display_name.to_string(),
            is_owned: true,
            rights: Self::rights(),
        }
    }

    fn contact(id: &str, name: &str, email: &str) -> AccessibleContact {
        let account = Self::account();
        AccessibleContact {
            id: Uuid::parse_str(id).unwrap(),
            collection_id: "default".to_string(),
            owner_account_id: account.account_id,
            owner_email: account.email,
            owner_display_name: account.display_name,
            rights: Self::rights(),
            name: name.to_string(),
            role: String::new(),
            email: email.to_string(),
            phone: String::new(),
            team: String::new(),
            notes: String::new(),
        }
    }

    fn mailbox(id: &str, role: &str, name: &str) -> JmapMailbox {
        JmapMailbox {
            id: Uuid::parse_str(id).unwrap(),
            role: role.to_string(),
            name: name.to_string(),
            sort_order: 40,
            total_emails: 0,
            unread_emails: 0,
        }
    }

    fn email(id: &str, mailbox_id: &str, mailbox_role: &str, subject: &str) -> JmapEmail {
        let account = Self::account();
        JmapEmail {
            id: Uuid::parse_str(id).unwrap(),
            thread_id: Uuid::parse_str("12121212-1212-1212-1212-121212121212").unwrap(),
            mailbox_id: Uuid::parse_str(mailbox_id).unwrap(),
            mailbox_role: mailbox_role.to_string(),
            mailbox_name: "RCA Sync".to_string(),
            received_at: "2026-05-03T12:00:00Z".to_string(),
            sent_at: None,
            from_address: account.email,
            from_display: Some(account.display_name),
            sender_address: None,
            sender_display: None,
            sender_authorization_kind: "self".to_string(),
            submitted_by_account_id: account.account_id,
            to: vec![JmapEmailAddress {
                address: "bob@example.test".to_string(),
                display_name: Some("Bob".to_string()),
            }],
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: subject.to_string(),
            preview: "Hello".to_string(),
            body_text: "Hello".to_string(),
            body_html_sanitized: None,
            unread: false,
            flagged: false,
            has_attachments: false,
            size_octets: 128,
            internet_message_id: None,
            mime_blob_ref: Some(format!("test:{id}")),
            delivery_status: "stored".to_string(),
        }
    }

    fn email_addresses(recipients: &[SubmittedRecipientInput]) -> Vec<JmapEmailAddress> {
        recipients
            .iter()
            .map(|recipient| JmapEmailAddress {
                address: recipient.address.clone(),
                display_name: recipient.display_name.clone(),
            })
            .collect()
    }

    fn task(id: &str, task_list_id: &str, title: &str) -> ClientTask {
        let account = Self::account();
        ClientTask {
            id: Uuid::parse_str(id).unwrap(),
            owner_account_id: account.account_id,
            owner_email: account.email,
            owner_display_name: account.display_name,
            is_owned: true,
            rights: Self::rights(),
            task_list_id: Uuid::parse_str(task_list_id).unwrap(),
            task_list_sort_order: 0,
            title: title.to_string(),
            description: "Task body".to_string(),
            status: "needs-action".to_string(),
            due_at: Some("2026-05-05T09:00:00Z".to_string()),
            completed_at: None,
            sort_order: 10,
            updated_at: "2026-05-04T08:00:00Z".to_string(),
        }
    }
}

impl AccountAuthStore for FakeStore {
    fn fetch_account_session<'a>(
        &'a self,
        token: &'a str,
    ) -> StoreFuture<'a, Option<AuthenticatedAccount>> {
        let session = (token == "token").then(|| self.session.clone()).flatten();
        Box::pin(async move { Ok(session) })
    }

    fn fetch_account_login<'a>(&'a self, _email: &'a str) -> StoreFuture<'a, Option<AccountLogin>> {
        Box::pin(async move { Ok(None) })
    }

    fn fetch_active_account_app_passwords<'a>(
        &'a self,
        _email: &'a str,
    ) -> StoreFuture<'a, Vec<StoredAccountAppPassword>> {
        Box::pin(async move { Ok(Vec::new()) })
    }

    fn touch_account_app_password<'a>(
        &'a self,
        _email: &'a str,
        _app_password_id: Uuid,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move { Ok(()) })
    }

    fn append_audit_event<'a>(
        &'a self,
        _tenant_id: &'a str,
        _entry: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move { Ok(()) })
    }
}

impl ExchangeStore for FakeStore {
    fn fetch_address_book_entries<'a>(
        &'a self,
        principal_account_id: Uuid,
    ) -> StoreFuture<'a, Vec<ExchangeAddressBookEntry>> {
        let principal = self
            .session
            .clone()
            .filter(|account| account.account_id == principal_account_id);
        let mut accounts = self.directory_accounts.lock().unwrap().clone();
        if let Some(principal) = principal {
            if !self.omit_principal_from_directory
                && !accounts
                    .iter()
                    .any(|account| account.account_id == principal.account_id)
            {
                accounts.push(principal.clone());
            }
            accounts.retain(|account| account.tenant_id == principal.tenant_id);
        } else {
            accounts.clear();
        }
        let mut entries = accounts
            .into_iter()
            .map(|account| ExchangeAddressBookEntry {
                id: account.account_id,
                display_name: account.display_name,
                email: account.email,
                entry_kind: ExchangeAddressBookEntryKind::Account,
                directory_kind: ExchangeAddressBookDirectoryKind::Person,
            })
            .collect::<Vec<_>>();
        let visible_collection_ids = self
            .contact_collections
            .lock()
            .unwrap()
            .iter()
            .filter(|collection| {
                collection.owner_account_id == principal_account_id || collection.rights.may_read
            })
            .map(|collection| collection.id.clone())
            .collect::<Vec<_>>();
        entries.extend(
            self.contacts
                .lock()
                .unwrap()
                .iter()
                .filter(|contact| {
                    contact.owner_account_id == principal_account_id
                        || visible_collection_ids.contains(&contact.collection_id)
                })
                .map(|contact| ExchangeAddressBookEntry {
                    id: contact.id,
                    display_name: contact.name.clone(),
                    email: contact.email.clone(),
                    entry_kind: ExchangeAddressBookEntryKind::Contact,
                    directory_kind: ExchangeAddressBookDirectoryKind::Person,
                }),
        );
        entries.sort_by(|left, right| {
            left.display_name
                .cmp(&right.display_name)
                .then_with(|| left.email.cmp(&right.email))
        });
        Box::pin(async move { Ok(entries) })
    }

    fn fetch_accessible_contact_collections<'a>(
        &'a self,
        _principal_account_id: Uuid,
    ) -> StoreFuture<'a, Vec<CollaborationCollection>> {
        let collections = self.contact_collections.lock().unwrap().clone();
        Box::pin(async move { Ok(collections) })
    }

    fn fetch_accessible_calendar_collections<'a>(
        &'a self,
        _principal_account_id: Uuid,
    ) -> StoreFuture<'a, Vec<CollaborationCollection>> {
        let collections = self.calendar_collections.lock().unwrap().clone();
        Box::pin(async move { Ok(collections) })
    }

    fn fetch_accessible_task_collections<'a>(
        &'a self,
        _principal_account_id: Uuid,
    ) -> StoreFuture<'a, Vec<CollaborationCollection>> {
        let collections = self.task_collections.lock().unwrap().clone();
        Box::pin(async move { Ok(collections) })
    }

    fn fetch_accessible_contacts_in_collection<'a>(
        &'a self,
        _principal_account_id: Uuid,
        collection_id: &'a str,
    ) -> StoreFuture<'a, Vec<AccessibleContact>> {
        let contacts = self
            .contacts
            .lock()
            .unwrap()
            .iter()
            .filter(|contact| contact.collection_id == collection_id)
            .cloned()
            .collect();
        Box::pin(async move { Ok(contacts) })
    }

    fn fetch_contact_sync_versions<'a>(
        &'a self,
        _principal_account_id: Uuid,
        collection_id: &'a str,
    ) -> StoreFuture<'a, Vec<(Uuid, String)>> {
        let versions = self.contact_versions.lock().unwrap().clone();
        let contacts = self
            .contacts
            .lock()
            .unwrap()
            .iter()
            .filter(|contact| contact.collection_id == collection_id)
            .map(|contact| {
                (
                    contact.id,
                    versions
                        .get(&contact.id)
                        .copied()
                        .unwrap_or_default()
                        .to_string(),
                )
            })
            .collect();
        Box::pin(async move { Ok(contacts) })
    }

    fn fetch_accessible_events_in_collection<'a>(
        &'a self,
        _principal_account_id: Uuid,
        collection_id: &'a str,
    ) -> StoreFuture<'a, Vec<AccessibleEvent>> {
        let events = self
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event.collection_id == collection_id)
            .cloned()
            .collect();
        Box::pin(async move { Ok(events) })
    }

    fn fetch_event_sync_versions<'a>(
        &'a self,
        _principal_account_id: Uuid,
        collection_id: &'a str,
    ) -> StoreFuture<'a, Vec<(Uuid, String)>> {
        let versions = self.event_versions.lock().unwrap().clone();
        let events = self
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event.collection_id == collection_id)
            .map(|event| {
                (
                    event.id,
                    versions
                        .get(&event.id)
                        .copied()
                        .unwrap_or_default()
                        .to_string(),
                )
            })
            .collect();
        Box::pin(async move { Ok(events) })
    }

    fn fetch_accessible_tasks_in_collection<'a>(
        &'a self,
        _principal_account_id: Uuid,
        collection_id: &'a str,
    ) -> StoreFuture<'a, Vec<ClientTask>> {
        let tasks = self
            .tasks
            .lock()
            .unwrap()
            .iter()
            .filter(|task| {
                matches!(collection_id, "tasks" | "default")
                    || task.task_list_id.to_string() == collection_id
            })
            .cloned()
            .collect();
        Box::pin(async move { Ok(tasks) })
    }

    fn fetch_task_sync_versions<'a>(
        &'a self,
        _principal_account_id: Uuid,
        collection_id: &'a str,
    ) -> StoreFuture<'a, Vec<(Uuid, String)>> {
        let versions = self.task_versions.lock().unwrap().clone();
        let tasks = self
            .tasks
            .lock()
            .unwrap()
            .iter()
            .filter(|task| {
                matches!(collection_id, "tasks" | "default")
                    || task.task_list_id.to_string() == collection_id
            })
            .map(|task| {
                (
                    task.id,
                    versions
                        .get(&task.id)
                        .copied()
                        .map(|version| version.to_string())
                        .unwrap_or_else(|| task.updated_at.clone()),
                )
            })
            .collect();
        Box::pin(async move { Ok(tasks) })
    }

    fn fetch_accessible_contacts_by_ids<'a>(
        &'a self,
        _principal_account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<AccessibleContact>> {
        let contacts = self
            .contacts
            .lock()
            .unwrap()
            .iter()
            .filter(|contact| ids.contains(&contact.id))
            .cloned()
            .collect();
        Box::pin(async move { Ok(contacts) })
    }

    fn create_accessible_contact<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: Option<&'a str>,
        input: UpsertClientContactInput,
    ) -> StoreFuture<'a, AccessibleContact> {
        let account = Self::account();
        let contact = AccessibleContact {
            id: input.id.unwrap_or_else(|| {
                Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap()
            }),
            collection_id: collection_id.unwrap_or("default").to_string(),
            owner_account_id: principal_account_id,
            owner_email: account.email,
            owner_display_name: account.display_name,
            rights: Self::rights(),
            name: input.name,
            role: input.role,
            email: input.email,
            phone: input.phone,
            team: input.team,
            notes: input.notes,
        };
        self.contact_versions.lock().unwrap().insert(contact.id, 1);
        self.contacts.lock().unwrap().push(contact.clone());
        Box::pin(async move { Ok(contact) })
    }

    fn update_accessible_contact<'a>(
        &'a self,
        _principal_account_id: Uuid,
        contact_id: Uuid,
        input: UpsertClientContactInput,
    ) -> StoreFuture<'a, AccessibleContact> {
        let mut contacts = self.contacts.lock().unwrap();
        let contact = contacts
            .iter_mut()
            .find(|contact| contact.id == contact_id)
            .unwrap();
        contact.name = input.name;
        contact.role = input.role;
        contact.email = input.email;
        contact.phone = input.phone;
        contact.team = input.team;
        contact.notes = input.notes;
        let mut versions = self.contact_versions.lock().unwrap();
        let version = versions
            .get(&contact_id)
            .copied()
            .unwrap_or_default()
            .saturating_add(1);
        versions.insert(contact_id, version);
        let contact = contact.clone();
        Box::pin(async move { Ok(contact) })
    }

    fn delete_accessible_contact<'a>(
        &'a self,
        _principal_account_id: Uuid,
        contact_id: Uuid,
    ) -> StoreFuture<'a, ()> {
        self.deleted_contacts.lock().unwrap().push(contact_id);
        self.contacts
            .lock()
            .unwrap()
            .retain(|contact| contact.id != contact_id);
        Box::pin(async move { Ok(()) })
    }

    fn fetch_accessible_events_by_ids<'a>(
        &'a self,
        _principal_account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<AccessibleEvent>> {
        let events = self
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| ids.contains(&event.id))
            .cloned()
            .collect();
        Box::pin(async move { Ok(events) })
    }

    fn create_accessible_event<'a>(
        &'a self,
        principal_account_id: Uuid,
        collection_id: Option<&'a str>,
        input: UpsertClientEventInput,
    ) -> StoreFuture<'a, AccessibleEvent> {
        let account = Self::account();
        let event = AccessibleEvent {
            id: input.id.unwrap_or_else(|| {
                Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap()
            }),
            collection_id: collection_id.unwrap_or("default").to_string(),
            owner_account_id: principal_account_id,
            owner_email: account.email,
            owner_display_name: account.display_name,
            rights: Self::rights(),
            date: input.date,
            time: input.time,
            time_zone: input.time_zone,
            duration_minutes: input.duration_minutes,
            recurrence_rule: input.recurrence_rule,
            title: input.title,
            location: input.location,
            attendees: input.attendees,
            attendees_json: input.attendees_json,
            notes: input.notes,
        };
        self.event_versions.lock().unwrap().insert(event.id, 1);
        self.events.lock().unwrap().push(event.clone());
        Box::pin(async move { Ok(event) })
    }

    fn update_accessible_event<'a>(
        &'a self,
        _principal_account_id: Uuid,
        event_id: Uuid,
        input: UpsertClientEventInput,
    ) -> StoreFuture<'a, AccessibleEvent> {
        let mut events = self.events.lock().unwrap();
        let event = events
            .iter_mut()
            .find(|event| event.id == event_id)
            .unwrap();
        event.date = input.date;
        event.time = input.time;
        event.time_zone = input.time_zone;
        event.duration_minutes = input.duration_minutes;
        event.recurrence_rule = input.recurrence_rule;
        event.title = input.title;
        event.location = input.location;
        event.attendees = input.attendees;
        event.attendees_json = input.attendees_json;
        event.notes = input.notes;
        let mut versions = self.event_versions.lock().unwrap();
        let version = versions
            .get(&event_id)
            .copied()
            .unwrap_or_default()
            .saturating_add(1);
        versions.insert(event_id, version);
        let event = event.clone();
        Box::pin(async move { Ok(event) })
    }

    fn delete_accessible_event<'a>(
        &'a self,
        _principal_account_id: Uuid,
        event_id: Uuid,
    ) -> StoreFuture<'a, ()> {
        self.deleted_events.lock().unwrap().push(event_id);
        self.events
            .lock()
            .unwrap()
            .retain(|event| event.id != event_id);
        Box::pin(async move { Ok(()) })
    }

    fn fetch_accessible_tasks_by_ids<'a>(
        &'a self,
        _principal_account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<ClientTask>> {
        let tasks = self
            .tasks
            .lock()
            .unwrap()
            .iter()
            .filter(|task| ids.contains(&task.id))
            .cloned()
            .collect();
        Box::pin(async move { Ok(tasks) })
    }

    fn fetch_active_sieve_script<'a>(
        &'a self,
        _account_id: Uuid,
    ) -> StoreFuture<'a, Option<SieveScriptDocument>> {
        let content = self.active_sieve_script.lock().unwrap().clone();
        Box::pin(async move {
            Ok(content.map(|content| SieveScriptDocument {
                name: "jmap-vacation".to_string(),
                content,
                is_active: true,
                updated_at: "2026-05-05T08:00:00Z".to_string(),
            }))
        })
    }

    fn put_sieve_script<'a>(
        &'a self,
        _account_id: Uuid,
        name: &'a str,
        content: &'a str,
        activate: bool,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, SieveScriptDocument> {
        if activate {
            *self.active_sieve_script.lock().unwrap() = Some(content.to_string());
        }
        let script = SieveScriptDocument {
            name: name.to_string(),
            content: content.to_string(),
            is_active: activate,
            updated_at: "2026-05-05T08:00:00Z".to_string(),
        };
        Box::pin(async move { Ok(script) })
    }

    fn set_active_sieve_script<'a>(
        &'a self,
        _account_id: Uuid,
        name: Option<&'a str>,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, Option<String>> {
        if name.is_none() {
            *self.active_sieve_script.lock().unwrap() = None;
        }
        let active_name = name.map(str::to_string);
        Box::pin(async move { Ok(active_name) })
    }

    fn create_accessible_task<'a>(
        &'a self,
        principal_account_id: Uuid,
        input: UpsertClientTaskInput,
    ) -> StoreFuture<'a, ClientTask> {
        let account = Self::account();
        let task = ClientTask {
            id: input.id.unwrap_or_else(|| {
                Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap()
            }),
            owner_account_id: principal_account_id,
            owner_email: account.email,
            owner_display_name: account.display_name,
            is_owned: true,
            rights: Self::rights(),
            task_list_id: input.task_list_id.unwrap_or_else(|| {
                Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap()
            }),
            task_list_sort_order: 0,
            title: input.title,
            description: input.description,
            status: input.status,
            due_at: input.due_at,
            completed_at: input.completed_at,
            sort_order: input.sort_order,
            updated_at: "2026-05-05T08:00:00Z".to_string(),
        };
        self.task_versions.lock().unwrap().insert(task.id, 1);
        self.tasks.lock().unwrap().push(task.clone());
        Box::pin(async move { Ok(task) })
    }

    fn update_accessible_task<'a>(
        &'a self,
        _principal_account_id: Uuid,
        task_id: Uuid,
        input: UpsertClientTaskInput,
    ) -> StoreFuture<'a, ClientTask> {
        let mut tasks = self.tasks.lock().unwrap();
        let task = tasks.iter_mut().find(|task| task.id == task_id).unwrap();
        task.task_list_id = input.task_list_id.unwrap_or(task.task_list_id);
        task.title = input.title;
        task.description = input.description;
        task.status = input.status;
        task.due_at = input.due_at;
        task.completed_at = if task.status == "completed" {
            input
                .completed_at
                .or_else(|| Some("2026-05-05T10:00:00Z".to_string()))
        } else {
            None
        };
        task.sort_order = input.sort_order;
        let mut versions = self.task_versions.lock().unwrap();
        let version = versions
            .get(&task_id)
            .copied()
            .unwrap_or_default()
            .saturating_add(1);
        versions.insert(task_id, version);
        let task = task.clone();
        Box::pin(async move { Ok(task) })
    }

    fn delete_accessible_task<'a>(
        &'a self,
        _principal_account_id: Uuid,
        task_id: Uuid,
    ) -> StoreFuture<'a, ()> {
        self.deleted_tasks.lock().unwrap().push(task_id);
        self.tasks.lock().unwrap().retain(|task| task.id != task_id);
        Box::pin(async move { Ok(()) })
    }

    fn fetch_jmap_mailboxes<'a>(&'a self, _account_id: Uuid) -> StoreFuture<'a, Vec<JmapMailbox>> {
        let mailboxes = self.mailboxes.lock().unwrap().clone();
        Box::pin(async move { Ok(mailboxes) })
    }

    fn create_jmap_mailbox<'a>(
        &'a self,
        input: JmapMailboxCreateInput,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, JmapMailbox> {
        self.created_mailboxes.lock().unwrap().push(input.clone());
        let mailbox = JmapMailbox {
            id: Uuid::parse_str("44444444-4444-4444-4444-444444444444").unwrap(),
            role: "custom".to_string(),
            name: input.name,
            sort_order: input.sort_order.unwrap_or(40),
            total_emails: 0,
            unread_emails: 0,
        };
        self.mailboxes.lock().unwrap().push(mailbox.clone());
        Box::pin(async move { Ok(mailbox) })
    }

    fn destroy_jmap_mailbox<'a>(
        &'a self,
        _account_id: Uuid,
        mailbox_id: Uuid,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        self.destroyed_mailboxes.lock().unwrap().push(mailbox_id);
        Box::pin(async move { Ok(()) })
    }

    fn query_jmap_email_ids<'a>(
        &'a self,
        _account_id: Uuid,
        mailbox_id: Option<Uuid>,
        _search_text: Option<&'a str>,
        _position: u64,
        _limit: u64,
    ) -> StoreFuture<'a, JmapEmailQuery> {
        let ids = self
            .emails
            .lock()
            .unwrap()
            .iter()
            .filter(|email| mailbox_id.map_or(true, |mailbox_id| email.mailbox_id == mailbox_id))
            .map(|email| email.id)
            .collect::<Vec<_>>();
        Box::pin(async move {
            Ok(JmapEmailQuery {
                total: ids.len() as u64,
                ids,
            })
        })
    }

    fn fetch_jmap_emails<'a>(
        &'a self,
        _account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<JmapEmail>> {
        let emails = self
            .emails
            .lock()
            .unwrap()
            .iter()
            .filter(|email| ids.contains(&email.id))
            .cloned()
            .collect();
        Box::pin(async move { Ok(emails) })
    }

    fn fetch_message_attachments<'a>(
        &'a self,
        _account_id: Uuid,
        message_id: Uuid,
    ) -> StoreFuture<'a, Vec<ActiveSyncAttachment>> {
        let attachments = self
            .attachments
            .lock()
            .unwrap()
            .get(&message_id)
            .cloned()
            .unwrap_or_default();
        Box::pin(async move { Ok(attachments) })
    }

    fn fetch_attachment_content<'a>(
        &'a self,
        _account_id: Uuid,
        file_reference: &'a str,
    ) -> StoreFuture<'a, Option<ActiveSyncAttachmentContent>> {
        let content = self
            .attachment_contents
            .lock()
            .unwrap()
            .get(file_reference)
            .cloned();
        Box::pin(async move { Ok(content) })
    }

    fn add_message_attachment<'a>(
        &'a self,
        _account_id: Uuid,
        message_id: Uuid,
        attachment: AttachmentUploadInput,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, Option<(JmapEmail, ActiveSyncAttachment)>> {
        let mut emails = self.emails.lock().unwrap();
        let Some(email) = emails.iter_mut().find(|email| email.id == message_id) else {
            return Box::pin(async move { Ok(None) });
        };
        email.has_attachments = true;
        let email = email.clone();
        drop(emails);

        self.created_attachments
            .lock()
            .unwrap()
            .push(attachment.clone());
        let attachment_id = Uuid::parse_str("cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcdcd").unwrap();
        let file_reference = format!("attachment:{message_id}:{attachment_id}");
        let stored_attachment = ActiveSyncAttachment {
            id: attachment_id,
            message_id,
            file_name: attachment.file_name.clone(),
            media_type: attachment.media_type.clone(),
            size_octets: attachment.blob_bytes.len() as u64,
            file_reference: file_reference.clone(),
        };
        self.attachments
            .lock()
            .unwrap()
            .entry(message_id)
            .or_default()
            .push(stored_attachment.clone());
        self.attachment_contents.lock().unwrap().insert(
            file_reference.clone(),
            ActiveSyncAttachmentContent {
                file_reference,
                file_name: attachment.file_name,
                media_type: attachment.media_type,
                blob_bytes: attachment.blob_bytes,
            },
        );

        Box::pin(async move { Ok(Some((email, stored_attachment))) })
    }

    fn delete_message_attachment<'a>(
        &'a self,
        _account_id: Uuid,
        file_reference: &'a str,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, Option<JmapEmail>> {
        let Some((message_id, attachment_id)) = parse_attachment_reference(file_reference) else {
            return Box::pin(async move { Ok(None) });
        };
        let mut attachments = self.attachments.lock().unwrap();
        let Some(message_attachments) = attachments.get_mut(&message_id) else {
            return Box::pin(async move { Ok(None) });
        };
        let before_len = message_attachments.len();
        message_attachments.retain(|attachment| attachment.id != attachment_id);
        if message_attachments.len() == before_len {
            return Box::pin(async move { Ok(None) });
        }
        let has_attachments = !message_attachments.is_empty();
        drop(attachments);

        let mut emails = self.emails.lock().unwrap();
        let Some(email) = emails.iter_mut().find(|email| email.id == message_id) else {
            return Box::pin(async move { Ok(None) });
        };
        email.has_attachments = has_attachments;
        let email = email.clone();
        Box::pin(async move { Ok(Some(email)) })
    }

    fn import_jmap_email<'a>(
        &'a self,
        input: JmapImportedEmailInput,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail> {
        self.imported_emails.lock().unwrap().push(input.clone());
        let mailbox = self
            .mailboxes
            .lock()
            .unwrap()
            .iter()
            .find(|mailbox| mailbox.id == input.mailbox_id)
            .cloned();
        let mut email = FakeStore::email(
            "99999999-9999-9999-9999-999999999999",
            &input.mailbox_id.to_string(),
            mailbox
                .as_ref()
                .map(|mailbox| mailbox.role.as_str())
                .unwrap_or("custom"),
            &input.subject,
        );
        if let Some(mailbox) = mailbox {
            email.mailbox_name = mailbox.name;
        }
        email.from_address = input.from_address;
        email.from_display = input.from_display;
        email.sender_address = input.sender_address;
        email.sender_display = input.sender_display;
        email.submitted_by_account_id = input.submitted_by_account_id;
        email.to = FakeStore::email_addresses(&input.to);
        email.cc = FakeStore::email_addresses(&input.cc);
        email.bcc = FakeStore::email_addresses(&input.bcc);
        email.preview = input.body_text.clone();
        email.body_text = input.body_text;
        email.body_html_sanitized = input.body_html_sanitized;
        email.internet_message_id = input.internet_message_id;
        email.mime_blob_ref = Some(input.mime_blob_ref);
        email.size_octets = input.size_octets;
        email.received_at = input
            .received_at
            .unwrap_or_else(|| "2026-05-07T12:00:00Z".to_string());
        email.has_attachments = !input.attachments.is_empty();
        self.emails.lock().unwrap().push(email.clone());
        Box::pin(async move { Ok(email) })
    }

    fn move_jmap_email<'a>(
        &'a self,
        _account_id: Uuid,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail> {
        self.moved_emails
            .lock()
            .unwrap()
            .push((message_id, target_mailbox_id));
        let target = self
            .mailboxes
            .lock()
            .unwrap()
            .iter()
            .find(|mailbox| mailbox.id == target_mailbox_id)
            .cloned();
        let mut emails = self.emails.lock().unwrap();
        let email = emails
            .iter_mut()
            .find(|email| email.id == message_id)
            .unwrap();
        if let Some(target) = target {
            email.mailbox_id = target.id;
            email.mailbox_role = target.role;
            email.mailbox_name = target.name;
        } else {
            email.mailbox_id = target_mailbox_id;
        }
        let moved = email.clone();
        Box::pin(async move { Ok(moved) })
    }

    fn copy_jmap_email<'a>(
        &'a self,
        _account_id: Uuid,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail> {
        self.copied_emails
            .lock()
            .unwrap()
            .push((message_id, target_mailbox_id));
        let target = self
            .mailboxes
            .lock()
            .unwrap()
            .iter()
            .find(|mailbox| mailbox.id == target_mailbox_id)
            .cloned();
        let mut copied = self
            .emails
            .lock()
            .unwrap()
            .iter()
            .find(|email| email.id == message_id)
            .cloned()
            .unwrap();
        copied.id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap();
        if let Some(target) = target {
            copied.mailbox_id = target.id;
            copied.mailbox_role = target.role;
            copied.mailbox_name = target.name;
        } else {
            copied.mailbox_id = target_mailbox_id;
        }
        self.emails.lock().unwrap().push(copied.clone());
        Box::pin(async move { Ok(copied) })
    }

    fn update_jmap_email_flags<'a>(
        &'a self,
        _account_id: Uuid,
        message_id: Uuid,
        unread: Option<bool>,
        flagged: Option<bool>,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail> {
        let mut emails = self.emails.lock().unwrap();
        let email = emails
            .iter_mut()
            .find(|email| email.id == message_id)
            .unwrap();
        if let Some(unread) = unread {
            email.unread = unread;
        }
        if let Some(flagged) = flagged {
            email.flagged = flagged;
        }
        let updated = email.clone();
        Box::pin(async move { Ok(updated) })
    }

    fn delete_jmap_email<'a>(
        &'a self,
        _account_id: Uuid,
        message_id: Uuid,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        self.deleted_emails.lock().unwrap().push(message_id);
        self.emails
            .lock()
            .unwrap()
            .retain(|email| email.id != message_id);
        Box::pin(async move { Ok(()) })
    }

    fn save_draft_message<'a>(
        &'a self,
        input: SubmitMessageInput,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, SavedDraftMessage> {
        self.saved_drafts.lock().unwrap().push(input);
        Box::pin(async move {
            Ok(SavedDraftMessage {
                message_id: Uuid::parse_str("dddddddd-dddd-dddd-dddd-dddddddddddd").unwrap(),
                account_id: Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap(),
                submitted_by_account_id: Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
                    .unwrap(),
                draft_mailbox_id: Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap(),
                delivery_status: "draft".to_string(),
            })
        })
    }

    fn submit_message<'a>(
        &'a self,
        input: SubmitMessageInput,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, SubmittedMessage> {
        self.submitted_messages.lock().unwrap().push(input.clone());
        let sent_mailbox = self
            .mailboxes
            .lock()
            .unwrap()
            .iter()
            .find(|mailbox| mailbox.role == "sent")
            .cloned();
        let sent_mailbox_id = sent_mailbox
            .as_ref()
            .map(|mailbox| mailbox.id)
            .unwrap_or_else(|| Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap());
        let submitted = SubmittedMessage {
            message_id: Uuid::parse_str("ffffffff-ffff-ffff-ffff-ffffffffffff").unwrap(),
            thread_id: Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(),
            account_id: input.account_id,
            submitted_by_account_id: input.submitted_by_account_id,
            sent_mailbox_id,
            outbound_queue_id: Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap(),
            delivery_status: "queued".to_string(),
        };

        let mut sent = FakeStore::email(
            &submitted.message_id.to_string(),
            &sent_mailbox_id.to_string(),
            "sent",
            &input.subject,
        );
        sent.thread_id = submitted.thread_id;
        sent.mailbox_name = sent_mailbox
            .as_ref()
            .map(|mailbox| mailbox.name.clone())
            .unwrap_or_else(|| "Sent".to_string());
        sent.sent_at = Some("2026-05-07T12:00:00Z".to_string());
        sent.from_address = input.from_address;
        sent.from_display = input.from_display;
        sent.sender_address = input.sender_address;
        sent.sender_display = input.sender_display;
        sent.submitted_by_account_id = input.submitted_by_account_id;
        sent.to = FakeStore::email_addresses(&input.to);
        sent.cc = FakeStore::email_addresses(&input.cc);
        sent.bcc = FakeStore::email_addresses(&input.bcc);
        sent.preview = input.body_text.clone();
        sent.body_text = input.body_text;
        sent.body_html_sanitized = input.body_html_sanitized;
        sent.internet_message_id = input.internet_message_id;
        sent.mime_blob_ref = input.mime_blob_ref;
        sent.size_octets = input.size_octets;
        sent.unread = input.unread.unwrap_or(false);
        sent.flagged = input.flagged.unwrap_or(false);
        sent.has_attachments = !input.attachments.is_empty();
        sent.delivery_status = submitted.delivery_status.clone();

        let mut emails = self.emails.lock().unwrap();
        if let Some(draft_message_id) = input.draft_message_id {
            emails.retain(|email| email.id != draft_message_id);
        }
        emails.push(sent);

        Box::pin(async move { Ok(submitted) })
    }
}

fn bearer_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_static("Bearer token"),
    );
    headers
}

fn rpc_proxy_conn_a1_request_body(receive_window_size: u32) -> Vec<u8> {
    let mut body = Vec::with_capacity(76);
    body.extend_from_slice(&[0x05, 0x00, 0x14, 0x03, 0x10, 0x00, 0x00, 0x00]);
    body.extend_from_slice(&76u16.to_le_bytes());
    body.extend_from_slice(&0u16.to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes());
    body.extend_from_slice(&0u16.to_le_bytes());
    body.extend_from_slice(&4u16.to_le_bytes());
    body.extend_from_slice(&6u32.to_le_bytes());
    body.extend_from_slice(&1u32.to_le_bytes());
    body.extend_from_slice(&3u32.to_le_bytes());
    body.extend_from_slice(&[0x11; 16]);
    body.extend_from_slice(&3u32.to_le_bytes());
    body.extend_from_slice(&[0x22; 16]);
    body.extend_from_slice(&0u32.to_le_bytes());
    body.extend_from_slice(&receive_window_size.to_le_bytes());
    body
}

fn mapi_headers(request_type: &str) -> HeaderMap {
    let mut headers = bearer_headers();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/mapi-http"),
    );
    headers.insert(
        "x-requesttype",
        HeaderValue::from_str(request_type).unwrap(),
    );
    headers.insert("x-requestid", HeaderValue::from_static("request-1"));
    headers.insert("x-clientinfo", HeaderValue::from_static("client-info-1"));
    headers
}

fn mapi_headers_without_content_type(request_type: &str) -> HeaderMap {
    let mut headers = bearer_headers();
    headers.insert(
        "x-requesttype",
        HeaderValue::from_str(request_type).unwrap(),
    );
    headers.insert("x-requestid", HeaderValue::from_static("request-1"));
    headers.insert("x-clientinfo", HeaderValue::from_static("client-info-1"));
    headers
}

fn mapi_headers_with_content_type(request_type: &str, content_type: &'static str) -> HeaderMap {
    let mut headers = bearer_headers();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static(content_type),
    );
    headers.insert(
        "x-requesttype",
        HeaderValue::from_str(request_type).unwrap(),
    );
    headers.insert("x-requestid", HeaderValue::from_static("request-1"));
    headers.insert("x-clientinfo", HeaderValue::from_static("client-info-1"));
    headers
}

fn mapi_headers_without_request_id(request_type: &str) -> HeaderMap {
    let mut headers = bearer_headers();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/mapi-http"),
    );
    headers.insert(
        "x-requesttype",
        HeaderValue::from_str(request_type).unwrap(),
    );
    headers
}

fn execute_body(rop_buffer: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&0u32.to_le_bytes());
    body.extend_from_slice(&(rop_buffer.len() as u32).to_le_bytes());
    body.extend_from_slice(rop_buffer);
    body.extend_from_slice(&4096u32.to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes());
    body
}

fn rop_buffer(rops: &[u8], handles: &[u32]) -> Vec<u8> {
    let mut buffer = Vec::new();
    buffer.extend_from_slice(&(rops.len() as u16).to_le_bytes());
    buffer.extend_from_slice(rops);
    for handle in handles {
        buffer.extend_from_slice(&handle.to_le_bytes());
    }
    buffer
}

fn rca_wrapped_private_logon_execute_body(mailbox: &str, client: &str) -> Vec<u8> {
    let mut rops = Vec::new();
    rops.push(0xFE);
    rops.push(0x01);
    rops.push(0x00);
    rops.push(0x41);
    rops.extend_from_slice(&0x0100_040Cu32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&((mailbox.len() + 1) as u16).to_le_bytes());
    rops.extend_from_slice(&8u32.to_le_bytes());
    rops.extend_from_slice(mailbox.as_bytes());
    rops.push(0);
    rops.extend_from_slice(&0x001Fu16.to_le_bytes());
    rops.extend_from_slice(client.as_bytes());

    let mut payload = Vec::new();
    payload.extend_from_slice(&((rops.len() + 2) as u16).to_le_bytes());
    payload.extend_from_slice(&rops);
    payload.extend_from_slice(&u32::MAX.to_le_bytes());

    let mut rpc_header_ext = Vec::new();
    rpc_header_ext.extend_from_slice(&0u16.to_le_bytes());
    rpc_header_ext.extend_from_slice(&0x0004u16.to_le_bytes());
    rpc_header_ext.extend_from_slice(&(payload.len() as u16).to_le_bytes());
    rpc_header_ext.extend_from_slice(&(payload.len() as u16).to_le_bytes());
    rpc_header_ext.extend_from_slice(&payload);

    let mut body = Vec::new();
    body.extend_from_slice(&0u32.to_le_bytes());
    body.extend_from_slice(&(rpc_header_ext.len() as u32).to_le_bytes());
    body.extend_from_slice(&rpc_header_ext);
    body.extend_from_slice(&0x8007u32.to_le_bytes());
    body.extend_from_slice(&0u32.to_le_bytes());
    body
}

fn test_account_principal() -> AccountPrincipal {
    let account = FakeStore::account();
    AccountPrincipal {
        tenant_id: account.tenant_id,
        account_id: account.account_id,
        email: account.email,
        display_name: account.display_name,
    }
}

fn rpc_proxy_bootstrap_logon_execute_rop(mailbox: &str) -> Vec<u8> {
    let legacy_dn = format!("/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn={mailbox}\0");
    let mut rops = vec![0xFE, 0x00, 0x00, 0x01];
    rops.extend_from_slice(&0x0100_0004u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&(legacy_dn.len() as u16).to_le_bytes());
    rops.extend_from_slice(legacy_dn.as_bytes());
    rpc_proxy_wrapped_rop_buffer(&rops, &[u32::MAX])
}

fn rpc_proxy_wrapped_rop_buffer(rops: &[u8], handles: &[u32]) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&((rops.len() + 2) as u16).to_le_bytes());
    payload.extend_from_slice(rops);
    for handle in handles {
        payload.extend_from_slice(&handle.to_le_bytes());
    }

    let mut rpc_header_ext = Vec::new();
    rpc_header_ext.extend_from_slice(&0u16.to_le_bytes());
    rpc_header_ext.extend_from_slice(&0x0004u16.to_le_bytes());
    rpc_header_ext.extend_from_slice(&(payload.len() as u16).to_le_bytes());
    rpc_header_ext.extend_from_slice(&(payload.len() as u16).to_le_bytes());
    rpc_header_ext.extend_from_slice(&payload);
    rpc_header_ext
}

fn resolve_names_request(search_address: &str, columns: &[u32]) -> Vec<u8> {
    let mut request = Vec::new();
    request.extend_from_slice(&0u32.to_le_bytes());
    request.push(0xFF);
    request.extend_from_slice(&[0; 24]);
    request.extend_from_slice(&1252u32.to_le_bytes());
    request.extend_from_slice(&0x0409u32.to_le_bytes());
    request.extend_from_slice(&0x0409u32.to_le_bytes());
    request.push(0xFF);
    request.extend_from_slice(&(columns.len() as u32).to_le_bytes());
    for column in columns {
        request.extend_from_slice(&column.to_le_bytes());
    }
    request.push(0xFF);
    request.extend_from_slice(&1u32.to_le_bytes());
    let unresolved_name = utf16z(&format!("=SMTP:{search_address}"));
    request.extend_from_slice(&(unresolved_name.len() as u16).to_le_bytes());
    request.extend_from_slice(&unresolved_name);
    request.extend_from_slice(&0u32.to_le_bytes());
    request
}

async fn response_text(response: axum::response::Response) -> String {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

fn decoded_mime_content(response: &str) -> String {
    let encoded = response
        .split("<t:MimeContent CharacterSet=\"UTF-8\">")
        .nth(1)
        .and_then(|value| value.split("</t:MimeContent>").next())
        .unwrap();
    String::from_utf8(BASE64_STANDARD.decode(encoded.as_bytes()).unwrap()).unwrap()
}

async fn response_bytes(response: axum::response::Response) -> Vec<u8> {
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec();
    strip_mapi_http_envelope(bytes)
}

fn parse_attachment_reference(value: &str) -> Option<(Uuid, Uuid)> {
    let value = value.trim();
    let rest = value.strip_prefix("attachment:")?;
    let (message_id, attachment_id) = rest.split_once(':')?;
    Some((
        Uuid::parse_str(message_id).ok()?,
        Uuid::parse_str(attachment_id).ok()?,
    ))
}

fn strip_mapi_http_envelope(bytes: Vec<u8>) -> Vec<u8> {
    if !bytes.starts_with(b"PROCESSING\r\nDONE\r\n") {
        return bytes;
    }
    let Some(offset) = bytes.windows(4).position(|window| window == b"\r\n\r\n") else {
        return bytes;
    };
    bytes[offset + 4..].to_vec()
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

fn utf16z(value: &str) -> Vec<u8> {
    let mut bytes = value
        .encode_utf16()
        .flat_map(u16::to_le_bytes)
        .collect::<Vec<_>>();
    bytes.extend_from_slice(&0u16.to_le_bytes());
    bytes
}

fn append_mapi_utf16_property(values: &mut Vec<u8>, property_tag: u32, value: &str) {
    values.extend_from_slice(&property_tag.to_le_bytes());
    values.extend_from_slice(&utf16z(value));
}

fn append_mapi_binary_property(values: &mut Vec<u8>, property_tag: u32, value: &[u8]) {
    values.extend_from_slice(&property_tag.to_le_bytes());
    values.extend_from_slice(&(value.len() as u16).to_le_bytes());
    values.extend_from_slice(value);
}

fn append_mapi_i32_property(values: &mut Vec<u8>, property_tag: u32, value: i32) {
    values.extend_from_slice(&property_tag.to_le_bytes());
    values.extend_from_slice(&value.to_le_bytes());
}

fn append_mapi_i64_property(values: &mut Vec<u8>, property_tag: u32, value: i64) {
    values.extend_from_slice(&property_tag.to_le_bytes());
    values.extend_from_slice(&value.to_le_bytes());
}

fn append_rop_open_folder(rops: &mut Vec<u8>, input: u8, output: u8, folder_id: u64) {
    rops.extend_from_slice(&[0x02, input, 0x00, output]);
    rops.extend_from_slice(&folder_id.to_le_bytes());
    rops.push(0);
}

fn append_rop_create_message(rops: &mut Vec<u8>, input: u8, output: u8, folder_id: u64) {
    rops.extend_from_slice(&[0x06, input, 0x01, output]);
    rops.extend_from_slice(&1200u16.to_le_bytes());
    rops.extend_from_slice(&folder_id.to_le_bytes());
    rops.push(0);
}

fn append_rop_set_properties(
    rops: &mut Vec<u8>,
    input: u8,
    property_count: u16,
    property_values: &[u8],
) {
    rops.extend_from_slice(&[0x0A, 0x00, input]);
    rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&property_count.to_le_bytes());
    rops.extend_from_slice(property_values);
}

fn append_rop_modify_recipients(rops: &mut Vec<u8>, input: u8, rows: &[(u32, u8, &[u8])]) {
    rops.extend_from_slice(&[0x0E, 0x00, input]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x3003_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0C15_0003u32.to_le_bytes());
    rops.extend_from_slice(&(rows.len() as u16).to_le_bytes());
    for (row_id, recipient_type, row) in rows {
        rops.extend_from_slice(&row_id.to_le_bytes());
        rops.push(*recipient_type);
        rops.extend_from_slice(&(row.len() as u16).to_le_bytes());
        rops.extend_from_slice(row);
    }
}

fn append_rop_save_changes_message(rops: &mut Vec<u8>, input: u8, response: u8) {
    rops.extend_from_slice(&[0x0C, 0x00, input, response, 0x00]);
}

fn append_rop_sync_manifest_get_buffer(
    rops: &mut Vec<u8>,
    input: u8,
    output: u8,
    buffer_size: u16,
) {
    rops.extend_from_slice(&[
        0x70, 0x00, input, output, 0x01, 0x00, 0x00, 0x00, 0x4E, 0x00, output,
    ]);
    rops.extend_from_slice(&buffer_size.to_le_bytes());
}

fn append_rop_set_read_flags(rops: &mut Vec<u8>, input: u8, read_flags: u8, message_ids: &[u64]) {
    rops.extend_from_slice(&[0x66, 0x00, input, 0x00, read_flags]);
    rops.extend_from_slice(&(message_ids.len() as u16).to_le_bytes());
    for message_id in message_ids {
        rops.extend_from_slice(&message_id.to_le_bytes());
    }
}

fn append_rop_open_message(
    rops: &mut Vec<u8>,
    input: u8,
    output: u8,
    folder_id: u64,
    message_id: u64,
) {
    rops.extend_from_slice(&[0x03, 0x00, input, output]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    rops.extend_from_slice(&folder_id.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&message_id.to_le_bytes());
}

fn append_rop_submit_message(rops: &mut Vec<u8>, input: u8) {
    rops.extend_from_slice(&[0x32, 0x00, input, 0x00]);
}

fn append_rop_query_subject_rows(rops: &mut Vec<u8>, input: u8, output: u8, row_count: u16) {
    rops.extend_from_slice(&[
        0x05, 0x00, input, output, 0x00, // RopGetContentsTable
        0x12, 0x00, output, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[0x15, 0x00, output, 0x00, 0x01]);
    rops.extend_from_slice(&row_count.to_le_bytes());
}

async fn response_rops_from_execute_response(response: axum::response::Response) -> Vec<u8> {
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    rop_buffer[2..2 + response_rop_size].to_vec()
}

fn mapi_recipient_row(display_name: &str, address: &str, recipient_type: u8) -> Vec<u8> {
    let mut row = Vec::new();
    row.extend_from_slice(&utf16z(display_name));
    row.extend_from_slice(&utf16z(address));
    row.extend_from_slice(&(recipient_type as i32).to_le_bytes());
    row
}

fn mapi_content_restriction(property_tag: u32, value: &str) -> Vec<u8> {
    let mut restriction = vec![0x03];
    restriction.extend_from_slice(&0u32.to_le_bytes());
    restriction.extend_from_slice(&property_tag.to_le_bytes());
    restriction.extend_from_slice(&property_tag.to_le_bytes());
    restriction.extend_from_slice(&utf16z(value));
    restriction
}

fn test_mapi_message_id(id: &str) -> u64 {
    let uuid = Uuid::parse_str(id).unwrap();
    test_mapi_uuid_id(&uuid)
}

fn test_mapi_folder_id(global_counter: u64) -> u64 {
    ((global_counter & 0x0000_FFFF_FFFF_FFFF) << 16) | 1
}

fn test_mapi_uuid_id(uuid: &Uuid) -> u64 {
    let bytes = uuid.as_bytes();
    let value = u64::from_le_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]) & 0x0000_FFFF_FFFF_FFFF;
    test_mapi_folder_id(value.max(0x100))
}

fn append_rop_get_properties_specific(rops: &mut Vec<u8>, input: u8, property_tags: &[u32]) {
    rops.extend_from_slice(&[0x07, 0x00, input]);
    rops.extend_from_slice(&4096u16.to_le_bytes());
    rops.extend_from_slice(&(property_tags.len() as u16).to_le_bytes());
    for tag in property_tags {
        rops.extend_from_slice(&tag.to_le_bytes());
    }
}

fn append_rop_delete_messages(rops: &mut Vec<u8>, input: u8, message_ids: &[u64]) {
    rops.extend_from_slice(&[0x1E, 0x00, input, 0x00, 0x00]);
    rops.extend_from_slice(&(message_ids.len() as u16).to_le_bytes());
    for message_id in message_ids {
        rops.extend_from_slice(&message_id.to_le_bytes());
    }
}

fn test_filetime(date: &str, time: &str) -> i64 {
    let year = date
        .get(0..4)
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap();
    let month = date
        .get(5..7)
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap();
    let day = date
        .get(8..10)
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap();
    let hour = time
        .get(0..2)
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap();
    let minute = time
        .get(3..5)
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap();
    let year = i64::from(year) - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = year - era * 400;
    let month = i64::from(month);
    let day = i64::from(day);
    let doy = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let unix_seconds = days * 86_400 + i64::from(hour) * 3_600 + i64::from(minute) * 60;
    (unix_seconds + 11_644_473_600) * 10_000_000
}

fn utf16z_string_bytes(value: &[u8]) -> Vec<u8> {
    value
        .chunks_exact(2)
        .take_while(|chunk| *chunk != [0, 0])
        .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
        .collect::<Vec<_>>()
        .iter()
        .map(|unit| char::from_u32(*unit as u32).unwrap_or(char::REPLACEMENT_CHARACTER))
        .collect::<String>()
        .into_bytes()
}

#[tokio::test]
async fn mapi_over_http_contact_crud_uses_canonical_contacts() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        ..Default::default()
    };
    let contacts = store.contacts.clone();
    let deleted_contacts = store.deleted_contacts.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = HeaderValue::from_str(
        connect
            .headers()
            .get("set-cookie")
            .unwrap()
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap(),
    )
    .unwrap();

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x3001_001F, "RCA Contact");
    append_mapi_utf16_property(&mut property_values, 0x39FE_001F, "rca@example.test");
    append_mapi_utf16_property(&mut property_values, 0x3A1C_001F, "+49 30 123456");
    append_mapi_utf16_property(&mut property_values, 0x3A16_001F, "Interop Team");
    append_mapi_utf16_property(&mut property_values, 0x3A17_001F, "Coordinator");
    append_mapi_utf16_property(&mut property_values, 0x1000_001F, "Created through MAPI");
    let mut rops = Vec::new();
    append_rop_create_message(&mut rops, 0, 1, test_mapi_folder_id(15));
    append_rop_set_properties(&mut rops, 1, 6, &property_values);
    append_rop_save_changes_message(&mut rops, 1, 1);
    append_rop_get_properties_specific(&mut rops, 1, &[0x3001_001F, 0x39FE_001F, 0x3A1C_001F]);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", cookie.clone());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let _response_rops = response_rops_from_execute_response(response).await;
    {
        let stored = contacts.lock().unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].name, "RCA Contact");
        assert_eq!(stored[0].email, "rca@example.test");
        assert_eq!(stored[0].phone, "+49 30 123456");
        assert_eq!(stored[0].team, "Interop Team");
        assert_eq!(stored[0].role, "Coordinator");
        assert_eq!(stored[0].notes, "Created through MAPI");
    }

    let contact_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    let mut update_values = Vec::new();
    append_mapi_utf16_property(&mut update_values, 0x3001_001F, "Updated RCA Contact");
    append_mapi_utf16_property(&mut update_values, 0x39FE_001F, "updated@example.test");
    let mut update_rops = Vec::new();
    append_rop_open_folder(&mut update_rops, 0, 1, test_mapi_folder_id(15));
    append_rop_open_message(
        &mut update_rops,
        1,
        2,
        test_mapi_folder_id(15),
        test_mapi_uuid_id(&contact_id),
    );
    append_rop_set_properties(&mut update_rops, 2, 2, &update_values);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&update_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(!response_rops
        .windows(4)
        .any(|window| window == 0x8004_0102u32.to_le_bytes()));
    {
        let stored = contacts.lock().unwrap();
        assert_eq!(stored[0].name, "Updated RCA Contact");
        assert_eq!(stored[0].email, "updated@example.test");
    }

    let mut read_rops = Vec::new();
    append_rop_open_folder(&mut read_rops, 0, 1, test_mapi_folder_id(15));
    append_rop_open_message(
        &mut read_rops,
        1,
        2,
        test_mapi_folder_id(15),
        test_mapi_uuid_id(&contact_id),
    );
    append_rop_get_properties_specific(&mut read_rops, 2, &[0x3001_001F, 0x39FE_001F]);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&read_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &utf16z("Updated RCA Contact")
    ));
    assert!(contains_bytes(
        &response_rops,
        &utf16z("updated@example.test")
    ));

    let mut delete_rops = Vec::new();
    append_rop_open_folder(&mut delete_rops, 0, 1, test_mapi_folder_id(15));
    append_rop_delete_messages(&mut delete_rops, 1, &[test_mapi_uuid_id(&contact_id)]);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&delete_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(!response_rops
        .windows(4)
        .any(|window| window == 0x8004_0102u32.to_le_bytes()));
    assert!(contacts.lock().unwrap().is_empty());
    assert_eq!(deleted_contacts.lock().unwrap().as_slice(), &[contact_id]);
}

#[tokio::test]
async fn mapi_over_http_calendar_crud_uses_canonical_events() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "calendar", "Calendar",
        )])),
        ..Default::default()
    };
    let events = store.events.clone();
    let deleted_events = store.deleted_events.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = HeaderValue::from_str(
        connect
            .headers()
            .get("set-cookie")
            .unwrap()
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap(),
    )
    .unwrap();

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "RCA Calendar");
    append_mapi_i64_property(
        &mut property_values,
        0x0060_0040,
        test_filetime("2026-05-04", "09:30"),
    );
    append_mapi_i64_property(
        &mut property_values,
        0x0061_0040,
        test_filetime("2026-05-04", "10:15"),
    );
    append_mapi_utf16_property(&mut property_values, 0x3FFB_001F, "Room 1");
    append_mapi_utf16_property(&mut property_values, 0x1000_001F, "Agenda");
    let mut rops = Vec::new();
    append_rop_create_message(&mut rops, 0, 1, test_mapi_folder_id(16));
    append_rop_set_properties(&mut rops, 1, 5, &property_values);
    append_rop_save_changes_message(&mut rops, 1, 1);
    append_rop_get_properties_specific(&mut rops, 1, &[0x0037_001F, 0x0060_0040, 0x0061_0040]);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", cookie.clone());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let _response_rops = response_rops_from_execute_response(response).await;
    {
        let stored = events.lock().unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].title, "RCA Calendar");
        assert_eq!(stored[0].date, "2026-05-04");
        assert_eq!(stored[0].time, "09:30");
        assert_eq!(stored[0].duration_minutes, 45);
        assert_eq!(stored[0].location, "Room 1");
        assert_eq!(stored[0].notes, "Agenda");
        assert!(stored[0].recurrence_rule.is_empty());
    }

    let event_id = Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap();
    let mut update_values = Vec::new();
    append_mapi_utf16_property(&mut update_values, 0x0037_001F, "Updated RCA Calendar");
    append_mapi_utf16_property(&mut update_values, 0x3FFB_001F, "Room 2");
    let mut update_rops = Vec::new();
    append_rop_open_folder(&mut update_rops, 0, 1, test_mapi_folder_id(16));
    append_rop_open_message(
        &mut update_rops,
        1,
        2,
        test_mapi_folder_id(16),
        test_mapi_uuid_id(&event_id),
    );
    append_rop_set_properties(&mut update_rops, 2, 2, &update_values);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&update_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(!response_rops
        .windows(4)
        .any(|window| window == 0x8004_0102u32.to_le_bytes()));
    {
        let stored = events.lock().unwrap();
        assert_eq!(stored[0].title, "Updated RCA Calendar");
        assert_eq!(stored[0].location, "Room 2");
    }

    let mut read_rops = Vec::new();
    append_rop_open_folder(&mut read_rops, 0, 1, test_mapi_folder_id(16));
    append_rop_open_message(
        &mut read_rops,
        1,
        2,
        test_mapi_folder_id(16),
        test_mapi_uuid_id(&event_id),
    );
    append_rop_get_properties_specific(&mut read_rops, 2, &[0x0037_001F, 0x3FFB_001F]);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&read_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &utf16z("Updated RCA Calendar")
    ));
    assert!(contains_bytes(&response_rops, &utf16z("Room 2")));

    let mut delete_rops = Vec::new();
    append_rop_open_folder(&mut delete_rops, 0, 1, test_mapi_folder_id(16));
    append_rop_delete_messages(&mut delete_rops, 1, &[test_mapi_uuid_id(&event_id)]);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&delete_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(!response_rops
        .windows(4)
        .any(|window| window == 0x8004_0102u32.to_le_bytes()));
    assert!(events.lock().unwrap().is_empty());
    assert_eq!(deleted_events.lock().unwrap().as_slice(), &[event_id]);
}

#[tokio::test]
async fn mapi_over_http_connect_creates_emsmdb_session() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/mapi-http"
    );
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "Connect");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    assert_eq!(
        response.headers().get("x-clientinfo").unwrap(),
        "client-info-1"
    );
    assert_eq!(
        response.headers().get("x-expirationinfo").unwrap(),
        "1800000"
    );
    assert_eq!(response.headers().get("x-pendingperiod").unwrap(), "15000");
    let set_cookie = response
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(set_cookie.starts_with("lpe_mapi_emsmdb="));
    assert!(set_cookie.contains("Max-Age=1800"));
    assert!(set_cookie.contains("HttpOnly"));
    assert!(set_cookie.contains("Secure"));

    let raw_body = to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec();
    assert!(raw_body.starts_with(b"PROCESSING\r\nDONE\r\nX-ResponseCode: 0\r\n"));
    let body = strip_mapi_http_envelope(raw_body);
    assert_eq!(&body[0..8], &[0, 0, 0, 0, 0, 0, 0, 0]);
}

#[tokio::test]
async fn mapi_over_http_generates_request_id_when_client_omits_one() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &mapi_headers_without_request_id("Connect"),
            b"",
        )
        .await
        .unwrap();

    let request_id = response
        .headers()
        .get("x-requestid")
        .unwrap()
        .to_str()
        .unwrap();
    assert_ne!(request_id, "00000000-0000-0000-0000-000000000000");
    assert_ne!(request_id, "request-1");
    assert!(Uuid::parse_str(request_id).is_ok());
}

#[tokio::test]
async fn mapi_over_http_rejects_missing_content_type() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &mapi_headers_without_content_type("Connect"),
            b"",
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "Connect");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "4");
    let body = response_bytes(response).await;
    let message = String::from_utf8_lossy(&body);
    assert!(message.contains("Content-Type application/mapi-http"));
}

#[tokio::test]
async fn mapi_over_http_accepts_outlook_octet_stream_bind_probe() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle_mapi(
            MapiEndpoint::Nspi,
            &mapi_headers_with_content_type("Bind", "application/octet-stream"),
            &[0; 45],
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/mapi-http"
    );
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "Bind");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    assert_eq!(
        response.headers().get("x-clientinfo").unwrap(),
        "client-info-1"
    );
    assert_eq!(
        response.headers().get("x-expirationinfo").unwrap(),
        "1800000"
    );
    let set_cookie = response
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(set_cookie.starts_with("lpe_mapi_nspi="));
}

#[tokio::test]
async fn mapi_over_http_disconnect_consumes_emsmdb_session() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut disconnect_headers = mapi_headers("Disconnect");
    disconnect_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &disconnect_headers, b"")
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("x-requesttype").unwrap(),
        "Disconnect"
    );
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    assert!(response
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("Max-Age=0"));
}

#[tokio::test]
async fn mapi_over_http_execute_accepts_release_rop() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&[0x01, 0x00, 0x00], &[1]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "Execute");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[0..4].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[4..8].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[12..16].try_into().unwrap()), 2);
    assert_eq!(&body[16..18], &[0, 0]);
}

#[tokio::test]
async fn mapi_over_http_execute_returns_private_mailbox_logon() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let legacy_dn = b"/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=alice\0";
    let mut logon_rop = vec![0xFE, 0x00, 0x00, 0x01];
    logon_rop.extend_from_slice(&0x0100_0004u32.to_le_bytes());
    logon_rop.extend_from_slice(&0u32.to_le_bytes());
    logon_rop.extend_from_slice(&(legacy_dn.len() as u16).to_le_bytes());
    logon_rop.extend_from_slice(legacy_dn);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&logon_rop, &[]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rop = &rop_buffer[2..2 + response_rop_size];

    assert_eq!(response_rop[0], 0xFE);
    assert_eq!(response_rop[1], 0x00);
    assert_eq!(
        u32::from_le_bytes(response_rop[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(response_rop[6], 0x01);
    assert_eq!(response_rop_size, 166);
    assert_eq!(
        u32::from_le_bytes(
            rop_buffer[2 + response_rop_size..6 + response_rop_size]
                .try_into()
                .unwrap()
        ),
        1
    );
}

#[tokio::test]
async fn mapi_over_http_execute_accepts_rca_wrapped_private_mailbox_logon() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = rca_wrapped_private_logon_execute_body(
        "alice@example.test",
        "Client=MS Connectivity Analyzer",
    );
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    assert_eq!(u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()), 0);
    assert_eq!(
        u16::from_le_bytes(rop_buffer[2..4].try_into().unwrap()),
        0x0004
    );
    let payload_size = u16::from_le_bytes(rop_buffer[4..6].try_into().unwrap()) as usize;
    assert_eq!(
        u16::from_le_bytes(rop_buffer[6..8].try_into().unwrap()) as usize,
        payload_size
    );
    let payload = &rop_buffer[8..8 + payload_size];
    let response_rop_size = u16::from_le_bytes(payload[0..2].try_into().unwrap()) as usize;
    let response_rop = &payload[2..response_rop_size];

    assert_eq!(response_rop[0], 0xFE);
    assert_eq!(response_rop[1], 0x00);
    assert_eq!(
        u32::from_le_bytes(response_rop[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(response_rop[6] & 0x01, 0x01);
    assert_eq!(response_rop_size, response_rop.len() + 2);
    assert_eq!(
        u32::from_le_bytes(
            payload[response_rop_size..response_rop_size + 4]
                .try_into()
                .unwrap()
        ),
        1
    );
}

#[tokio::test]
async fn mapi_over_http_execute_opens_folder_and_gets_empty_hierarchy_table() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(1).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x04, 0x00, 0x01, 0x02, 0x04, // RopGetHierarchyTable
    ]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    assert_eq!(response_rops[0], 0x02);
    assert_eq!(response_rops[1], 0x01);
    assert_eq!(
        u32::from_le_bytes(response_rops[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(response_rops[8], 0x04);
    assert_eq!(response_rops[9], 0x02);
    assert_eq!(
        u32::from_le_bytes(response_rops[10..14].try_into().unwrap()),
        0
    );
    assert_eq!(
        u32::from_le_bytes(response_rops[14..18].try_into().unwrap()),
        0
    );
    assert_eq!(
        u32::from_le_bytes(
            rop_buffer[2 + response_rop_size..6 + response_rop_size]
                .try_into()
                .unwrap()
        ),
        2
    );
    assert_eq!(
        u32::from_le_bytes(
            rop_buffer[6 + response_rop_size..10 + response_rop_size]
                .try_into()
                .unwrap()
        ),
        3
    );
}

#[tokio::test]
async fn mapi_over_http_create_folder_creates_canonical_mailbox() {
    let created_mailboxes = Arc::new(Mutex::new(Vec::new()));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        created_mailboxes: created_mailboxes.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, Root
    ];
    rops.extend_from_slice(&test_mapi_folder_id(1).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x1C, 0x00, 0x01, 0x02, // RopCreateFolder
        0x01, // generic folder
        0x01, // Unicode names
        0x00, // do not open existing
        0x00, // reserved
    ]);
    rops.extend_from_slice(&utf16z("MAPI Projects"));
    rops.extend_from_slice(&utf16z(""));

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];
    let create = &response_rops[8..];

    assert_eq!(create[0], 0x1C);
    assert_eq!(create[1], 0x02);
    assert_eq!(u32::from_le_bytes(create[2..6].try_into().unwrap()), 0);
    assert_eq!(
        u64::from_le_bytes(create[6..14].try_into().unwrap()),
        test_mapi_uuid_id(&Uuid::parse_str("44444444-4444-4444-4444-444444444444").unwrap())
    );
    assert_eq!(create[14], 0);
    assert_eq!(create[15], 0);

    let created = created_mailboxes.lock().unwrap();
    assert_eq!(created.len(), 1);
    assert_eq!(created[0].account_id, FakeStore::account().account_id);
    assert_eq!(created[0].name, "MAPI Projects");
}

#[tokio::test]
async fn mapi_over_http_delete_folder_removes_custom_canonical_mailbox() {
    let custom_id = Uuid::parse_str("66666666-6666-6666-6666-666666666666").unwrap();
    let destroyed_mailboxes = Arc::new(Mutex::new(Vec::new()));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "66666666-6666-6666-6666-666666666666",
            "custom",
            "Archive",
        )])),
        destroyed_mailboxes: destroyed_mailboxes.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, Root
    ];
    rops.extend_from_slice(&test_mapi_folder_id(1).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x1D, 0x00, 0x01, // RopDeleteFolder
        0x00, // deletion flags
    ]);
    rops.extend_from_slice(&test_mapi_uuid_id(&custom_id).to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];
    let delete = &response_rops[8..];

    assert_eq!(delete[0], 0x1D);
    assert_eq!(delete[1], 0x01);
    assert_eq!(u32::from_le_bytes(delete[2..6].try_into().unwrap()), 0);
    assert_eq!(delete[6], 0);
    assert_eq!(destroyed_mailboxes.lock().unwrap().as_slice(), &[custom_id]);
}

#[tokio::test]
async fn mapi_over_http_delete_folder_rejects_system_mailbox() {
    let destroyed_mailboxes = Arc::new(Mutex::new(Vec::new()));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox",
        )])),
        destroyed_mailboxes: destroyed_mailboxes.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, Root
    ];
    rops.extend_from_slice(&test_mapi_folder_id(1).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x1D, 0x00, 0x01, // RopDeleteFolder
        0x00, // deletion flags
    ]);
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];
    let delete = &response_rops[8..];

    assert_eq!(delete[0], 0x1D);
    assert_eq!(delete[1], 0x01);
    assert_eq!(
        u32::from_le_bytes(delete[2..6].try_into().unwrap()),
        0x8007_0005
    );
    assert!(destroyed_mailboxes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_execute_sets_columns_and_queries_empty_rows() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ];
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x6748_0014u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&50u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[3]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    assert_eq!(response_rops[0], 0x12);
    assert_eq!(response_rops[1], 0x02);
    assert_eq!(
        u32::from_le_bytes(response_rops[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(response_rops[6], 0);
    assert_eq!(response_rops[7], 0x15);
    assert_eq!(response_rops[8], 0x02);
    assert_eq!(
        u32::from_le_bytes(response_rops[9..13].try_into().unwrap()),
        0
    );
    assert_eq!(response_rops[13], 0x02);
    assert_eq!(
        u16::from_le_bytes(response_rops[14..16].try_into().unwrap()),
        0
    );
}

#[tokio::test]
async fn mapi_over_http_query_rows_lists_canonical_mailbox_folders() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 7;
    inbox.unread_emails = 2;
    let mut archive =
        FakeStore::mailbox("66666666-6666-6666-6666-666666666666", "custom", "Archive");
    archive.total_emails = 3;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox, archive])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(1).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x04, 0x00, 0x01, 0x02, 0x04, // RopGetHierarchyTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x3602_0003u32.to_le_bytes());
    rops.extend_from_slice(&0x3603_0003u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&50u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];
    let query_offset = 8 + 10 + 7;

    assert_eq!(response_rops[query_offset], 0x15);
    assert_eq!(
        u16::from_le_bytes(
            response_rops[query_offset + 7..query_offset + 9]
                .try_into()
                .unwrap()
        ),
        2
    );
    assert!(contains_bytes(response_rops, &utf16z("Inbox")));
    assert!(contains_bytes(response_rops, &utf16z("Archive")));
}

#[tokio::test]
async fn mapi_over_http_contents_table_lists_canonical_messages() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let archive = FakeStore::mailbox("66666666-6666-6666-6666-666666666666", "custom", "Archive");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox, archive])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                "88888888-8888-8888-8888-888888888888",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Inbox message",
            ),
            FakeStore::email(
                "99999999-9999-9999-9999-999999999999",
                "66666666-6666-6666-6666-666666666666",
                "custom",
                "Archive message",
            ),
        ])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&7u16.to_le_bytes());
    rops.extend_from_slice(&0x674A_0014u32.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0C1F_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0E04_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0E08_0003u32.to_le_bytes());
    rops.extend_from_slice(&0x0E07_0003u32.to_le_bytes());
    rops.extend_from_slice(&0x0E1B_000Bu32.to_le_bytes());
    rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&50u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    let contents_offset = 8;
    assert_eq!(response_rops[contents_offset], 0x05);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[contents_offset + 6..contents_offset + 10]
                .try_into()
                .unwrap()
        ),
        1
    );
    let query_offset = contents_offset + 10 + 7;
    assert_eq!(response_rops[query_offset], 0x15);
    assert_eq!(
        u16::from_le_bytes(
            response_rops[query_offset + 7..query_offset + 9]
                .try_into()
                .unwrap()
        ),
        1
    );
    assert!(contains_bytes(response_rops, &utf16z("Inbox message")));
    assert!(contains_bytes(response_rops, &utf16z("alice@example.test")));
    assert!(contains_bytes(response_rops, &utf16z("Bob")));
    assert!(contains_bytes(response_rops, &128u32.to_le_bytes()));
    assert!(!contains_bytes(response_rops, &utf16z("Archive message")));
}

#[tokio::test]
async fn mapi_over_http_query_rows_advances_table_position() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 2;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                "81818181-8181-8181-8181-818181818181",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "First page message",
            ),
            FakeStore::email(
                "82828282-8282-8282-8282-828282828282",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Second page message",
            ),
        ])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x81, 0x00, 0x02, // RopResetTable
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];
    let query_offsets = response_rops
        .windows(7)
        .enumerate()
        .filter_map(|(offset, window)| (window == [0x15, 0x02, 0, 0, 0, 0, 0x02]).then_some(offset))
        .collect::<Vec<_>>();

    assert_eq!(query_offsets.len(), 3);
    let first_query = &response_rops[query_offsets[0]..query_offsets[1]];
    let second_query = &response_rops[query_offsets[1]..query_offsets[2]];
    let reset_query = &response_rops[query_offsets[2]..];
    assert_eq!(u16::from_le_bytes(first_query[7..9].try_into().unwrap()), 1);
    assert_eq!(
        u16::from_le_bytes(second_query[7..9].try_into().unwrap()),
        1
    );
    assert!(contains_bytes(first_query, &utf16z("First page message")));
    assert!(!contains_bytes(first_query, &utf16z("Second page message")));
    assert!(contains_bytes(second_query, &utf16z("Second page message")));
    assert!(contains_bytes(reset_query, &utf16z("First page message")));
    assert!(!contains_bytes(reset_query, &utf16z("Second page message")));
}

#[tokio::test]
async fn mapi_over_http_query_position_reports_table_cursor() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 2;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                "83838383-8383-8383-8383-838383838383",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Position first",
            ),
            FakeStore::email(
                "84848484-8484-8484-8484-848484848484",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Position second",
            ),
        ])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x17, 0x00, 0x02, // RopQueryPosition
    ]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    assert!(contains_bytes(
        response_rops,
        &[0x17, 0x02, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_query_rows_no_advance_preserves_table_position() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 2;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                "85858585-8585-8585-8585-858585858585",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "No advance first",
            ),
            FakeStore::email(
                "86868686-8686-8686-8686-868686868686",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "No advance second",
            ),
        ])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x15, 0x00, 0x02, 0x01, 0x01, // RopQueryRows, NoAdvance
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x17, 0x00, 0x02, // RopQueryPosition
    ]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    assert!(contains_bytes(
        response_rops,
        &[0x17, 0x02, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_sort_table_orders_contents_rows() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 2;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                "89898989-8989-8989-8989-898989898989",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Zulu sort",
            ),
            FakeStore::email(
                "90909090-9090-9090-9090-909090909090",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Alpha sort",
            ),
        ])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[
        0x13, 0x00, 0x02, 0x00, // RopSortTable
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&2u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];
    let alpha = utf16z("Alpha sort");
    let zulu = utf16z("Zulu sort");
    let alpha_offset = response_rops
        .windows(alpha.len())
        .position(|window| window == alpha)
        .unwrap();
    let zulu_offset = response_rops
        .windows(zulu.len())
        .position(|window| window == zulu)
        .unwrap();

    assert!(contains_bytes(response_rops, &[0x13, 0x02, 0, 0, 0, 0, 0]));
    assert!(alpha_offset < zulu_offset);
}

#[tokio::test]
async fn mapi_over_http_query_rows_reads_backward_from_table_position() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 2;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                "91919191-9191-9191-9191-919191919191",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Backward first",
            ),
            FakeStore::email(
                "92929292-9292-9292-9292-929292929292",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Backward second",
            ),
        ])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows, forward
    ]);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x00, // RopQueryRows, backward
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x17, 0x00, 0x02, // RopQueryPosition
    ]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];
    let query_offsets = response_rops
        .windows(7)
        .enumerate()
        .filter_map(|(offset, window)| (window == [0x15, 0x02, 0, 0, 0, 0, 0x02]).then_some(offset))
        .collect::<Vec<_>>();

    assert_eq!(query_offsets.len(), 2);
    let backward_query = &response_rops[query_offsets[1]..];
    assert!(contains_bytes(backward_query, &utf16z("Backward second")));
    assert!(!contains_bytes(backward_query, &utf16z("Backward first")));
    assert!(contains_bytes(
        response_rops,
        &[0x17, 0x02, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_restrict_filters_contents_table_rows() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 3;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                "93939393-9393-9393-9393-939393939393",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Quarter planning",
            ),
            FakeStore::email(
                "94949494-9494-9494-9494-949494949494",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Budget review",
            ),
            FakeStore::email(
                "95959595-9595-9595-9595-959595959595",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Planning followup",
            ),
        ])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();
    let restriction = mapi_content_restriction(0x0037_001F, "planning");

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[
        0x14, 0x00, 0x02, 0x00, // RopRestrict
    ]);
    rops.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    rops.extend_from_slice(&restriction);
    rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x17, 0x00, 0x02, // RopQueryPosition
    ]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    assert!(contains_bytes(response_rops, &[0x14, 0x02, 0, 0, 0, 0, 0]));
    assert!(contains_bytes(response_rops, &utf16z("Quarter planning")));
    assert!(!contains_bytes(response_rops, &utf16z("Budget review")));
    assert!(contains_bytes(response_rops, &utf16z("Planning followup")));
    assert!(contains_bytes(
        response_rops,
        &[0x17, 0x02, 0, 0, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_find_row_returns_matching_contents_row() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 3;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                "96969696-9696-9696-9696-969696969696",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Find first",
            ),
            FakeStore::email(
                "97979797-9797-9797-9797-979797979797",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Needle target",
            ),
            FakeStore::email(
                "98989898-9898-9898-9898-989898989898",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Find last",
            ),
        ])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();
    let restriction = mapi_content_restriction(0x0037_001F, "needle");

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[
        0x4F, 0x00, 0x02, 0x00, // RopFindRow
    ]);
    rops.extend_from_slice(&(restriction.len() as u16).to_le_bytes());
    rops.extend_from_slice(&restriction);
    rops.push(0);
    rops.extend_from_slice(&0u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    assert!(contains_bytes(
        response_rops,
        &[0x4F, 0x02, 0, 0, 0, 0, 0, 1]
    ));
    assert!(!contains_bytes(response_rops, &utf16z("Find first")));
    assert!(contains_bytes(response_rops, &utf16z("Needle target")));
    assert!(!contains_bytes(response_rops, &utf16z("Find last")));
}

#[tokio::test]
async fn mapi_over_http_table_bookmarks_restore_contents_cursor() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 2;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                "99999999-9999-9999-9999-999999999999",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Bookmark first",
            ),
            FakeStore::email(
                "9a9a9a9a-9a9a-9a9a-9a9a-9a9a9a9a9a9a",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Bookmark second",
            ),
        ])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut first_rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    first_rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    first_rops.push(0);
    first_rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    first_rops.extend_from_slice(&1u16.to_le_bytes());
    first_rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    first_rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    first_rops.extend_from_slice(&1u16.to_le_bytes());
    first_rops.extend_from_slice(&[
        0x1B, 0x00, 0x02, // RopCreateBookmark
    ]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let first_request = execute_body(&rop_buffer(&first_rops, &[1, u32::MAX, u32::MAX]));
    let first_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &first_request)
        .await
        .unwrap();
    let first_body = response_bytes(first_response).await;
    let first_rop_buffer_size = u32::from_le_bytes(first_body[12..16].try_into().unwrap()) as usize;
    let first_rop_buffer = &first_body[16..16 + first_rop_buffer_size];
    let first_response_rop_size =
        u16::from_le_bytes(first_rop_buffer[0..2].try_into().unwrap()) as usize;
    let first_response_rops = &first_rop_buffer[2..2 + first_response_rop_size];

    assert!(contains_bytes(
        first_response_rops,
        &utf16z("Bookmark first")
    ));
    assert!(contains_bytes(
        first_response_rops,
        &[0x1B, 0x02, 0, 0, 0, 0, 4, 0, 1, 0, 0, 0]
    ));

    let bookmark = 1u32.to_le_bytes();
    let mut second_rops = vec![0x19, 0x00, 0x02]; // RopSeekRowBookmark
    second_rops.extend_from_slice(&(bookmark.len() as u16).to_le_bytes());
    second_rops.extend_from_slice(&bookmark);
    second_rops.extend_from_slice(&0i32.to_le_bytes());
    second_rops.push(1);
    second_rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    second_rops.extend_from_slice(&1u16.to_le_bytes());
    second_rops.extend_from_slice(&[
        0x89, 0x00, 0x02, // RopFreeBookmark
    ]);
    second_rops.extend_from_slice(&(bookmark.len() as u16).to_le_bytes());
    second_rops.extend_from_slice(&bookmark);

    let second_request = execute_body(&rop_buffer(&second_rops, &[u32::MAX, u32::MAX, 3]));
    let second_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &second_request)
        .await
        .unwrap();

    assert_eq!(second_response.status(), StatusCode::OK);
    assert_eq!(
        second_response.headers().get("x-responsecode").unwrap(),
        "0"
    );
    let second_body = response_bytes(second_response).await;
    let second_rop_buffer_size =
        u32::from_le_bytes(second_body[12..16].try_into().unwrap()) as usize;
    let second_rop_buffer = &second_body[16..16 + second_rop_buffer_size];
    let second_response_rop_size =
        u16::from_le_bytes(second_rop_buffer[0..2].try_into().unwrap()) as usize;
    let second_response_rops = &second_rop_buffer[2..2 + second_response_rop_size];

    assert!(contains_bytes(
        second_response_rops,
        &[0x19, 0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
    assert!(!contains_bytes(
        second_response_rops,
        &utf16z("Bookmark first")
    ));
    assert!(contains_bytes(
        second_response_rops,
        &utf16z("Bookmark second")
    ));
    assert!(contains_bytes(
        second_response_rops,
        &[0x89, 0x02, 0, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_create_set_save_message_imports_canonical_email() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &inbox_id.to_string(),
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    let imported_emails = store.imported_emails.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "MAPI saved subject");
    append_mapi_utf16_property(&mut property_values, 0x1000_001F, "Body saved through MAPI");
    append_mapi_utf16_property(
        &mut property_values,
        0x1035_001F,
        "<mapi-save@example.test>",
    );

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, Inbox
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x06, 0x00, 0x01, 0x02, // RopCreateMessage
    ]);
    rops.extend_from_slice(&1200u16.to_le_bytes());
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x0A, 0x00, 0x02, // RopSetProperties
    ]);
    rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
    rops.extend_from_slice(&[
        0x07, 0x00, 0x02, // RopGetPropertiesSpecific on pending message
    ]);
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x1000_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[
        0x0C, 0x00, 0x01, 0x02, 0x00, // RopSaveChangesMessage
    ]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    assert!(contains_bytes(response_rops, &[0x06, 0x02, 0, 0, 0, 0, 0]));
    assert!(contains_bytes(
        response_rops,
        &[0x0A, 0x02, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(response_rops, &utf16z("MAPI saved subject")));
    assert!(contains_bytes(
        response_rops,
        &utf16z("Body saved through MAPI")
    ));
    assert!(contains_bytes(
        response_rops,
        &[0x0C, 0x01, 0, 0, 0, 0, 0x02]
    ));
    assert!(contains_bytes(
        response_rops,
        &test_mapi_message_id("99999999-9999-9999-9999-999999999999").to_le_bytes()
    ));

    let recorded = imported_emails.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].mailbox_id, inbox_id);
    assert_eq!(recorded[0].source, "mapi-save-message");
    assert_eq!(recorded[0].from_address, "alice@example.test");
    assert_eq!(recorded[0].from_display.as_deref(), Some("Alice"));
    assert_eq!(recorded[0].subject, "MAPI saved subject");
    assert_eq!(recorded[0].body_text, "Body saved through MAPI");
    assert_eq!(
        recorded[0].internet_message_id.as_deref(),
        Some("<mapi-save@example.test>")
    );
    assert!(recorded[0].to.is_empty());
    assert!(recorded[0].cc.is_empty());
    assert!(recorded[0].bcc.is_empty());
}

#[tokio::test]
async fn mapi_over_http_delete_properties_no_replicate_clears_pending_message_properties() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "Temporary subject");
    append_mapi_utf16_property(&mut property_values, 0x1000_001F, "Temporary body");

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, Inbox
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x06, 0x00, 0x01, 0x02, // RopCreateMessage
    ]);
    rops.extend_from_slice(&1200u16.to_le_bytes());
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x79, 0x00, 0x02, // RopSetPropertiesNoReplicate
    ]);
    rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
    rops.extend_from_slice(&[
        0x7A, 0x00, 0x02, // RopDeletePropertiesNoReplicate
    ]);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x1000_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[
        0x07, 0x00, 0x02, // RopGetPropertiesSpecific
    ]);
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x1000_001Fu32.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x79, 0x02, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x7A, 0x02, 0, 0, 0, 0, 0, 0]
    ));
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("Temporary subject")
    ));
    assert!(!contains_bytes(&response_rops, &utf16z("Temporary body")));
}

#[tokio::test]
async fn mapi_over_http_named_property_bootstrap_maps_session_property_ids() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let ps_mapi_guid = [
        0x28, 0x03, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x46,
    ];
    let ps_internet_headers_guid = [
        0x86, 0x03, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0xC0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x46,
    ];
    let named_header = utf16z("X-LPE-Test");

    let mut rops = vec![
        0xFE, 0x00, 0x00, 0x01, // RopLogon
    ];
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x56, 0x00, 0x00, 0x02, // RopGetPropertyIdsFromNames, create missing
    ]);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.push(0x00);
    rops.extend_from_slice(&ps_mapi_guid);
    rops.extend_from_slice(&0x1234u32.to_le_bytes());
    rops.push(0x01);
    rops.extend_from_slice(&ps_internet_headers_guid);
    rops.push(named_header.len() as u8);
    rops.extend_from_slice(&named_header);
    rops.extend_from_slice(&[
        0x55, 0x00, 0x00, // RopGetNamesFromPropertyIds
    ]);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x1234u16.to_le_bytes());
    rops.extend_from_slice(&0x8001u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x5F, 0x00, 0x00, 0x00, 0x00, // RopQueryNamedProperties
    ]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x56, 0x00, 0, 0, 0, 0, 2, 0, 0x34, 0x12, 0x01, 0x80]
    ));
    assert!(contains_bytes(&response_rops, &utf16z("x-lpe-test")));
    assert!(contains_bytes(
        &response_rops,
        &[0x5F, 0x00, 0, 0, 0, 0, 1, 0, 0x01, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_modify_recipients_imports_pending_message_recipients() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &inbox_id.to_string(),
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    let imported_emails = store.imported_emails.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "MAPI recipients");
    append_mapi_utf16_property(&mut property_values, 0x1000_001F, "Recipient body");

    let to_row = mapi_recipient_row("Bob", "bob@example.test", 0x01);
    let cc_row = mapi_recipient_row("Carol", "carol@example.test", 0x02);
    let bcc_row = mapi_recipient_row("Hidden", "hidden@example.test", 0x03);
    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, Inbox
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x06, 0x00, 0x01, 0x02, // RopCreateMessage
    ]);
    rops.extend_from_slice(&1200u16.to_le_bytes());
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x0A, 0x00, 0x02, // RopSetProperties
    ]);
    rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
    rops.extend_from_slice(&[
        0x0E, 0x00, 0x02, // RopModifyRecipients
    ]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x3003_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0C15_0003u32.to_le_bytes());
    rops.extend_from_slice(&3u16.to_le_bytes());
    for (row_id, recipient_type, row) in [
        (1u32, 0x01u8, to_row.as_slice()),
        (2u32, 0x02u8, cc_row.as_slice()),
        (3u32, 0x03u8, bcc_row.as_slice()),
    ] {
        rops.extend_from_slice(&row_id.to_le_bytes());
        rops.push(recipient_type);
        rops.extend_from_slice(&(row.len() as u16).to_le_bytes());
        rops.extend_from_slice(row);
    }
    rops.extend_from_slice(&[
        0x0F, 0x00, 0x02, // RopReadRecipients from pending message
    ]);
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x0C, 0x00, 0x01, 0x02, 0x00, // RopSaveChangesMessage
    ]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    assert!(contains_bytes(response_rops, &[0x0E, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(response_rops, &utf16z("bob@example.test")));
    assert!(contains_bytes(response_rops, &utf16z("Carol")));

    let recorded = imported_emails.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].to.len(), 1);
    assert_eq!(recorded[0].to[0].address, "bob@example.test");
    assert_eq!(recorded[0].to[0].display_name.as_deref(), Some("Bob"));
    assert_eq!(recorded[0].cc.len(), 1);
    assert_eq!(recorded[0].cc[0].address, "carol@example.test");
    assert_eq!(recorded[0].bcc.len(), 1);
    assert_eq!(recorded[0].bcc[0].address, "hidden@example.test");
}

#[tokio::test]
async fn mapi_over_http_remove_all_recipients_clears_pending_message_recipients() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &inbox_id.to_string(),
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    let imported_emails = store.imported_emails.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let row = mapi_recipient_row("Bob", "bob@example.test", 0x01);
    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, Inbox
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[0x06, 0x00, 0x01, 0x02]);
    rops.extend_from_slice(&1200u16.to_le_bytes());
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[0x0E, 0x00, 0x02]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x3003_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0C15_0003u32.to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&1u32.to_le_bytes());
    rops.push(0x01);
    rops.extend_from_slice(&(row.len() as u16).to_le_bytes());
    rops.extend_from_slice(&row);
    rops.extend_from_slice(&[
        0x0D, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, // RopRemoveAllRecipients
        0x0C, 0x00, 0x01, 0x02, 0x00, // RopSaveChangesMessage
    ]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    assert!(contains_bytes(response_rops, &[0x0D, 0x02, 0, 0, 0, 0]));
    let recorded = imported_emails.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert!(recorded[0].to.is_empty());
    assert!(recorded[0].cc.is_empty());
    assert!(recorded[0].bcc.is_empty());
}

#[tokio::test]
async fn mapi_over_http_submit_pending_message_uses_canonical_submission() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &inbox_id.to_string(),
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    let submitted_messages = store.submitted_messages.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "Submit from MAPI");
    append_mapi_utf16_property(&mut property_values, 0x1000_001F, "Canonical submit body");
    append_mapi_utf16_property(
        &mut property_values,
        0x1035_001F,
        "<mapi-submit@example.test>",
    );

    let to_row = mapi_recipient_row("Bob", "bob@example.test", 0x01);
    let bcc_row = mapi_recipient_row("Hidden", "hidden@example.test", 0x03);
    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, Inbox
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x06, 0x00, 0x01, 0x02, // RopCreateMessage
    ]);
    rops.extend_from_slice(&1200u16.to_le_bytes());
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x0A, 0x00, 0x02, // RopSetProperties
    ]);
    rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
    rops.extend_from_slice(&[
        0x0E, 0x00, 0x02, // RopModifyRecipients
    ]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x3003_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0C15_0003u32.to_le_bytes());
    rops.extend_from_slice(&2u16.to_le_bytes());
    for (row_id, recipient_type, row) in [
        (1u32, 0x01u8, to_row.as_slice()),
        (2u32, 0x03u8, bcc_row.as_slice()),
    ] {
        rops.extend_from_slice(&row_id.to_le_bytes());
        rops.push(recipient_type);
        rops.extend_from_slice(&(row.len() as u16).to_le_bytes());
        rops.extend_from_slice(row);
    }
    rops.extend_from_slice(&[
        0x32, 0x00, 0x02, 0x00, // RopSubmitMessage
    ]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    assert!(contains_bytes(response_rops, &[0x32, 0x02, 0, 0, 0, 0]));
    let recorded = submitted_messages.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].source, "mapi-submit-message");
    assert_eq!(recorded[0].draft_message_id, None);
    assert_eq!(recorded[0].subject, "Submit from MAPI");
    assert_eq!(recorded[0].body_text, "Canonical submit body");
    assert_eq!(recorded[0].from_address, "alice@example.test");
    assert_eq!(recorded[0].to.len(), 1);
    assert_eq!(recorded[0].to[0].address, "bob@example.test");
    assert_eq!(recorded[0].bcc.len(), 1);
    assert_eq!(recorded[0].bcc[0].address, "hidden@example.test");
    assert_eq!(
        recorded[0].internet_message_id.as_deref(),
        Some("<mapi-submit@example.test>")
    );
}

#[tokio::test]
async fn mapi_over_http_submit_opened_draft_uses_source_draft_id() {
    let draft_id = Uuid::parse_str("20202020-2020-2020-2020-202020202020").unwrap();
    let draft_mailbox_id = Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap();
    let mut draft = FakeStore::email(
        &draft_id.to_string(),
        &draft_mailbox_id.to_string(),
        "drafts",
        "Saved MAPI draft",
    );
    draft.body_text = "Draft body".to_string();
    draft.cc.push(JmapEmailAddress {
        address: "carol@example.test".to_string(),
        display_name: Some("Carol".to_string()),
    });
    draft.bcc.push(JmapEmailAddress {
        address: "hidden@example.test".to_string(),
        display_name: Some("Hidden".to_string()),
    });
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &draft_mailbox_id.to_string(),
            "drafts",
            "Drafts",
        )])),
        emails: Arc::new(Mutex::new(vec![draft])),
        ..Default::default()
    };
    let submitted_messages = store.submitted_messages.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, Drafts
    ];
    rops.extend_from_slice(&test_mapi_folder_id(14).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x03, 0x00, 0x01, 0x02, // RopOpenMessage
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    rops.extend_from_slice(&test_mapi_folder_id(14).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&test_mapi_message_id(&draft_id.to_string()).to_le_bytes());
    rops.extend_from_slice(&[
        0x32, 0x00, 0x02, 0x00, // RopSubmitMessage
    ]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    assert!(contains_bytes(response_rops, &[0x32, 0x02, 0, 0, 0, 0]));
    let recorded = submitted_messages.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].source, "mapi-submit-message");
    assert_eq!(recorded[0].draft_message_id, Some(draft_id));
    assert_eq!(recorded[0].subject, "Saved MAPI draft");
    assert_eq!(recorded[0].body_text, "Draft body");
    assert_eq!(recorded[0].to[0].address, "bob@example.test");
    assert_eq!(recorded[0].cc[0].address, "carol@example.test");
    assert_eq!(recorded[0].bcc[0].address, "hidden@example.test");
}

#[tokio::test]
async fn mapi_over_http_mail_lifecycle_uses_canonical_state_end_to_end() {
    let drafts_id = Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap();
    let sent_id = Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(&drafts_id.to_string(), "drafts", "Drafts"),
            FakeStore::mailbox(&sent_id.to_string(), "sent", "Sent"),
        ])),
        ..Default::default()
    };
    let imported_emails = store.imported_emails.clone();
    let submitted_messages = store.submitted_messages.clone();
    let emails = store.emails.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let lifecycle_subject = "Outlook day-two canonical draft";
    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, lifecycle_subject);
    append_mapi_utf16_property(
        &mut property_values,
        0x1000_001F,
        "Created through EMSMDB and submitted through canonical LPE",
    );
    append_mapi_utf16_property(
        &mut property_values,
        0x1035_001F,
        "<mapi-lifecycle@example.test>",
    );
    let to_row = mapi_recipient_row("Bob", "bob@example.test", 0x01);
    let bcc_row = mapi_recipient_row("Hidden", "hidden@example.test", 0x03);

    let mut create_rops = Vec::new();
    append_rop_open_folder(&mut create_rops, 0, 1, test_mapi_folder_id(14));
    append_rop_create_message(&mut create_rops, 1, 2, test_mapi_folder_id(14));
    append_rop_set_properties(&mut create_rops, 2, 3, &property_values);
    append_rop_modify_recipients(
        &mut create_rops,
        2,
        &[(1, 0x01, to_row.as_slice()), (2, 0x03, bcc_row.as_slice())],
    );
    append_rop_save_changes_message(&mut create_rops, 1, 2);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let create_request = execute_body(&rop_buffer(&create_rops, &[1, u32::MAX, u32::MAX]));
    let create_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &create_request)
        .await
        .unwrap();

    assert_eq!(create_response.status(), StatusCode::OK);
    let create_response_rops = response_rops_from_execute_response(create_response).await;
    assert!(contains_bytes(
        &create_response_rops,
        &[0x0C, 0x01, 0, 0, 0, 0, 0x02]
    ));
    assert_eq!(imported_emails.lock().unwrap().len(), 1);
    let draft_message_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
    let draft_mapi_message_id = test_mapi_message_id(&draft_message_id.to_string());
    {
        let canonical = emails.lock().unwrap();
        let draft = canonical
            .iter()
            .find(|email| email.id == draft_message_id)
            .expect("saved draft is visible in canonical store");
        assert_eq!(draft.mailbox_id, drafts_id);
        assert_eq!(draft.mailbox_role, "drafts");
        assert_eq!(draft.subject, lifecycle_subject);
        assert_eq!(draft.to[0].address, "bob@example.test");
        assert_eq!(draft.bcc[0].address, "hidden@example.test");
    }

    let mut sync_rops = Vec::new();
    append_rop_open_folder(&mut sync_rops, 0, 1, test_mapi_folder_id(14));
    append_rop_sync_manifest_get_buffer(&mut sync_rops, 1, 2, 4096);
    let sync_request = execute_body(&rop_buffer(&sync_rops, &[1, u32::MAX, u32::MAX]));
    let sync_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &sync_request)
        .await
        .unwrap();

    assert_eq!(sync_response.status(), StatusCode::OK);
    let sync_response_rops = response_rops_from_execute_response(sync_response).await;
    assert!(contains_bytes(&sync_response_rops, b"LPE-MAPI-SYNC\0"));
    assert!(contains_bytes(
        &sync_response_rops,
        lifecycle_subject.as_bytes()
    ));
    assert!(!contains_bytes(&sync_response_rops, b"hidden@example.test"));

    let mut flag_rops = Vec::new();
    append_rop_open_folder(&mut flag_rops, 0, 1, test_mapi_folder_id(14));
    append_rop_set_read_flags(&mut flag_rops, 1, 0x04, &[draft_mapi_message_id]);
    let flag_request = execute_body(&rop_buffer(&flag_rops, &[1, u32::MAX]));
    let flag_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &flag_request)
        .await
        .unwrap();

    assert_eq!(flag_response.status(), StatusCode::OK);
    let flag_response_rops = response_rops_from_execute_response(flag_response).await;
    assert!(contains_bytes(
        &flag_response_rops,
        &[0x66, 0x01, 0, 0, 0, 0, 0]
    ));
    assert!(
        emails
            .lock()
            .unwrap()
            .iter()
            .find(|email| email.id == draft_message_id)
            .unwrap()
            .unread
    );

    let mut submit_rops = Vec::new();
    append_rop_open_folder(&mut submit_rops, 0, 1, test_mapi_folder_id(14));
    append_rop_open_message(
        &mut submit_rops,
        1,
        2,
        test_mapi_folder_id(14),
        draft_mapi_message_id,
    );
    append_rop_submit_message(&mut submit_rops, 2);
    let submit_request = execute_body(&rop_buffer(&submit_rops, &[1, u32::MAX, u32::MAX]));
    let submit_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &submit_request)
        .await
        .unwrap();

    assert_eq!(submit_response.status(), StatusCode::OK);
    let submit_response_rops = response_rops_from_execute_response(submit_response).await;
    assert!(contains_bytes(
        &submit_response_rops,
        &[0x32, 0x02, 0, 0, 0, 0]
    ));
    {
        let recorded = submitted_messages.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].source, "mapi-submit-message");
        assert_eq!(recorded[0].draft_message_id, Some(draft_message_id));
        assert_eq!(recorded[0].subject, lifecycle_subject);
        assert_eq!(recorded[0].to[0].address, "bob@example.test");
        assert_eq!(recorded[0].bcc[0].address, "hidden@example.test");
    }
    {
        let canonical = emails.lock().unwrap();
        assert!(canonical.iter().all(|email| email.id != draft_message_id));
        let sent = canonical
            .iter()
            .find(|email| email.mailbox_role == "sent")
            .expect("submitted message is visible in canonical Sent");
        assert_eq!(sent.mailbox_id, sent_id);
        assert_eq!(sent.subject, lifecycle_subject);
        assert!(sent.unread);
    }

    let mut sent_table_rops = Vec::new();
    append_rop_open_folder(&mut sent_table_rops, 0, 1, test_mapi_folder_id(7));
    append_rop_query_subject_rows(&mut sent_table_rops, 1, 2, 10);
    let sent_table_request = execute_body(&rop_buffer(&sent_table_rops, &[1, u32::MAX, u32::MAX]));
    let sent_table_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &sent_table_request)
        .await
        .unwrap();

    assert_eq!(sent_table_response.status(), StatusCode::OK);
    let sent_table_response_rops = response_rops_from_execute_response(sent_table_response).await;
    assert!(contains_bytes(
        &sent_table_response_rops,
        &utf16z(lifecycle_subject)
    ));
    assert!(!contains_bytes(
        &sent_table_response_rops,
        &utf16z("hidden@example.test")
    ));
}

#[tokio::test]
async fn mapi_over_http_reload_cached_information_returns_pending_message_summary() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &inbox_id.to_string(),
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "Cached pending");
    let row = mapi_recipient_row("Bob", "bob@example.test", 0x01);
    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, Inbox
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[0x06, 0x00, 0x01, 0x02]);
    rops.extend_from_slice(&1200u16.to_le_bytes());
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[0x0A, 0x00, 0x02]);
    rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
    rops.extend_from_slice(&[0x0E, 0x00, 0x02]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x3003_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0C15_0003u32.to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&1u32.to_le_bytes());
    rops.push(0x01);
    rops.extend_from_slice(&(row.len() as u16).to_le_bytes());
    rops.extend_from_slice(&row);
    rops.extend_from_slice(&[0x10, 0x00, 0x02, 0x00, 0x00]); // RopReloadCachedInformation

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];
    let reload_offset = response_rops
        .windows(6)
        .position(|window| window == [0x10, 0x02, 0, 0, 0, 0].as_slice())
        .unwrap();
    assert_eq!(response_rops[reload_offset + 6], 0);
    assert_eq!(response_rops[reload_offset + 7], 0x01);
    assert_eq!(response_rops[reload_offset + 8], 0x04);
    let subject = utf16z("Cached pending");
    assert_eq!(
        &response_rops[reload_offset + 9..reload_offset + 9 + subject.len()],
        subject.as_slice()
    );
    let recipient_count_offset = reload_offset + 9 + subject.len();
    assert_eq!(
        u16::from_le_bytes(
            response_rops[recipient_count_offset..recipient_count_offset + 2]
                .try_into()
                .unwrap()
        ),
        1
    );
}

#[tokio::test]
async fn mapi_over_http_message_status_is_session_local() {
    let message_id = Uuid::parse_str("10101010-1010-1010-1010-101010101010").unwrap();
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            &message_id.to_string(),
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Status message",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mapi_message_id = test_mapi_message_id(&message_id.to_string());
    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, Inbox
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[0x1F, 0x00, 0x01]); // RopGetMessageStatus
    rops.extend_from_slice(&mapi_message_id.to_le_bytes());
    rops.extend_from_slice(&[0x20, 0x00, 0x01]); // RopSetMessageStatus
    rops.extend_from_slice(&mapi_message_id.to_le_bytes());
    rops.extend_from_slice(&0x20u32.to_le_bytes());
    rops.extend_from_slice(&0x20u32.to_le_bytes());
    rops.extend_from_slice(&[0x1F, 0x00, 0x01]); // RopGetMessageStatus
    rops.extend_from_slice(&mapi_message_id.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];
    assert_eq!(
        response_rops
            .windows(10)
            .filter(|window| *window == [0x20, 0x01, 0, 0, 0, 0, 0, 0, 0, 0].as_slice())
            .count(),
        2
    );
    assert!(contains_bytes(
        response_rops,
        &[0x20, 0x01, 0, 0, 0, 0, 0x20, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_move_copy_messages_uses_canonical_store() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let archive_id = Uuid::parse_str("66666666-6666-6666-6666-666666666666").unwrap();
    let move_message_id = Uuid::parse_str("9b9b9b9b-9b9b-9b9b-9b9b-9b9b9b9b9b9b").unwrap();
    let copy_message_id = Uuid::parse_str("9c9c9c9c-9c9c-9c9c-9c9c-9c9c9c9c9c9c").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox"),
            FakeStore::mailbox(&archive_id.to_string(), "archive", "Archive"),
        ])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                &move_message_id.to_string(),
                &inbox_id.to_string(),
                "inbox",
                "Move through MAPI",
            ),
            FakeStore::email(
                &copy_message_id.to_string(),
                &inbox_id.to_string(),
                "inbox",
                "Copy through MAPI",
            ),
        ])),
        ..Default::default()
    };
    let moved_emails = store.moved_emails.clone();
    let copied_emails = store.copied_emails.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, Inbox
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x02, 0x00, 0x00, 0x02, // RopOpenFolder, Archive
    ]);
    rops.extend_from_slice(&test_mapi_uuid_id(&archive_id).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x33, 0x00, 0x01, 0x02, // RopMoveCopyMessages, move
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&test_mapi_message_id(&move_message_id.to_string()).to_le_bytes());
    rops.push(0);
    rops.push(0);
    rops.extend_from_slice(&[
        0x33, 0x00, 0x01, 0x02, // RopMoveCopyMessages, copy
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&test_mapi_message_id(&copy_message_id.to_string()).to_le_bytes());
    rops.push(0);
    rops.push(1);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    assert_eq!(
        moved_emails.lock().unwrap().as_slice(),
        &[(move_message_id, archive_id)]
    );
    assert_eq!(
        copied_emails.lock().unwrap().as_slice(),
        &[(copy_message_id, archive_id)]
    );
    assert_eq!(
        response_rops
            .windows(7)
            .filter(|window| *window == [0x33, 0x01, 0, 0, 0, 0, 0])
            .count(),
        2
    );
}

#[tokio::test]
async fn mapi_over_http_delete_messages_uses_trash_and_hard_delete() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let trash_id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap();
    let soft_message_id = Uuid::parse_str("9d9d9d9d-9d9d-9d9d-9d9d-9d9d9d9d9d9d").unwrap();
    let hard_message_id = Uuid::parse_str("9e9e9e9e-9e9e-9e9e-9e9e-9e9e9e9e9e9e").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox"),
            FakeStore::mailbox(&trash_id.to_string(), "trash", "Deleted"),
        ])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                &soft_message_id.to_string(),
                &inbox_id.to_string(),
                "inbox",
                "Soft delete through MAPI",
            ),
            FakeStore::email(
                &hard_message_id.to_string(),
                &inbox_id.to_string(),
                "inbox",
                "Hard delete through MAPI",
            ),
        ])),
        ..Default::default()
    };
    let moved_emails = store.moved_emails.clone();
    let deleted_emails = store.deleted_emails.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, Inbox
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x1E, 0x00, 0x01, 0x00, 0x00, // RopDeleteMessages
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&test_mapi_message_id(&soft_message_id.to_string()).to_le_bytes());
    rops.extend_from_slice(&[
        0x91, 0x00, 0x01, 0x00, 0x00, // RopHardDeleteMessages
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&test_mapi_message_id(&hard_message_id.to_string()).to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    assert_eq!(
        moved_emails.lock().unwrap().as_slice(),
        &[(soft_message_id, trash_id)]
    );
    assert_eq!(
        deleted_emails.lock().unwrap().as_slice(),
        &[hard_message_id]
    );
    assert!(contains_bytes(response_rops, &[0x1E, 0x01, 0, 0, 0, 0, 0]));
    assert!(contains_bytes(response_rops, &[0x91, 0x01, 0, 0, 0, 0, 0]));
}

#[tokio::test]
async fn mapi_over_http_seek_row_moves_contents_table_cursor() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 2;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                "87878787-8787-8787-8787-878787878787",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Seek first",
            ),
            FakeStore::email(
                "88888888-8888-8888-8888-888888888888",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Seek second",
            ),
        ])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[
        0x18, 0x00, 0x02, 0x00, // RopSeekRow, BOOKMARK_BEGINNING
    ]);
    rops.extend_from_slice(&1i32.to_le_bytes());
    rops.push(1);
    rops.extend_from_slice(&[
        0x17, 0x00, 0x02, // RopQueryPosition
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    assert!(contains_bytes(
        response_rops,
        &[0x18, 0x02, 0, 0, 0, 0, 0, 1, 0, 0, 0]
    ));
    assert!(contains_bytes(
        response_rops,
        &[0x17, 0x02, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0]
    ));
    assert!(!contains_bytes(response_rops, &utf16z("Seek first")));
    assert!(contains_bytes(response_rops, &utf16z("Seek second")));
}

#[tokio::test]
async fn mapi_over_http_open_message_then_gets_canonical_message_properties() {
    let message_id = "11111111-1111-1111-1111-111111111111";
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            message_id,
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox message",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x03, 0x00, 0x01, 0x02, // RopOpenMessage
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&test_mapi_message_id(message_id).to_le_bytes());
    rops.extend_from_slice(&[
        0x07, 0x00, 0x02, // RopGetPropertiesSpecific
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());
    rops.extend_from_slice(&5u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x1000_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0C1F_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0E08_0003u32.to_le_bytes());
    rops.extend_from_slice(&0x0E07_0003u32.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    let open_message_offset = 8;
    assert_eq!(response_rops[open_message_offset], 0x03);
    assert_eq!(response_rops[open_message_offset + 1], 0x02);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[open_message_offset + 2..open_message_offset + 6]
                .try_into()
                .unwrap()
        ),
        0
    );
    let get_props_offset = response_rops
        .iter()
        .enumerate()
        .skip(open_message_offset + 6)
        .find_map(|(offset, byte)| (*byte == 0x07).then_some(offset))
        .unwrap();
    assert_eq!(response_rops[get_props_offset + 1], 0x02);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[get_props_offset + 2..get_props_offset + 6]
                .try_into()
                .unwrap()
        ),
        0
    );
    assert!(contains_bytes(response_rops, &utf16z("Inbox message")));
    assert!(contains_bytes(response_rops, &utf16z("Hello")));
    assert!(contains_bytes(response_rops, &utf16z("alice@example.test")));
    assert!(contains_bytes(response_rops, &128u32.to_le_bytes()));
}

#[tokio::test]
async fn mapi_over_http_get_properties_all_returns_message_projection() {
    let message_id = "24242424-2424-2424-2424-242424242424";
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            message_id,
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "All properties message",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x03, 0x00, 0x01, 0x02, // RopOpenMessage
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&test_mapi_message_id(message_id).to_le_bytes());
    rops.extend_from_slice(&[
        0x08, 0x00, 0x02, // RopGetPropertiesAll
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    assert!(contains_bytes(response_rops, &[0x08, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(response_rops, &0x0037_001Fu32.to_le_bytes()));
    assert!(contains_bytes(
        response_rops,
        &utf16z("All properties message")
    ));
    assert!(contains_bytes(response_rops, &0x1000_001Fu32.to_le_bytes()));
    assert!(contains_bytes(response_rops, &utf16z("Hello")));
}

#[tokio::test]
async fn mapi_over_http_read_recipients_returns_canonical_message_recipients() {
    let message_id = "22222222-2222-2222-2222-222222222222";
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Recipient message",
    );
    email.cc.push(JmapEmailAddress {
        address: "carol@example.test".to_string(),
        display_name: Some("Carol".to_string()),
    });
    email.bcc.push(JmapEmailAddress {
        address: "erin@example.test".to_string(),
        display_name: Some("Erin".to_string()),
    });
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x03, 0x00, 0x01, 0x02, // RopOpenMessage
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&test_mapi_message_id(message_id).to_le_bytes());
    rops.extend_from_slice(&[
        0x0F, 0x00, 0x02, // RopReadRecipients
    ]);
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];
    let read_recipients_offset = response_rops
        .iter()
        .enumerate()
        .find_map(|(offset, byte)| (*byte == 0x0F).then_some(offset))
        .unwrap();

    assert_eq!(response_rops[read_recipients_offset + 1], 0x02);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[read_recipients_offset + 2..read_recipients_offset + 6]
                .try_into()
                .unwrap()
        ),
        0
    );
    assert!(contains_bytes(response_rops, &0u32.to_le_bytes()));
    assert!(contains_bytes(response_rops, &1u32.to_le_bytes()));
    assert!(contains_bytes(response_rops, &utf16z("bob@example.test")));
    assert!(contains_bytes(response_rops, &utf16z("Bob")));
    assert!(contains_bytes(response_rops, &utf16z("carol@example.test")));
    assert!(contains_bytes(response_rops, &utf16z("Carol")));
    assert!(!contains_bytes(response_rops, &utf16z("erin@example.test")));
}

#[tokio::test]
async fn mapi_over_http_read_recipients_exposes_sent_message_bcc() {
    let message_id = "23232323-2323-2323-2323-232323232323";
    let mut sent = FakeStore::mailbox("77777777-7777-7777-7777-777777777777", "sent", "Sent");
    sent.total_emails = 1;
    let mut email = FakeStore::email(
        message_id,
        "77777777-7777-7777-7777-777777777777",
        "sent",
        "Sent recipient message",
    );
    email.bcc.push(JmapEmailAddress {
        address: "erin@example.test".to_string(),
        display_name: Some("Erin".to_string()),
    });
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![sent])),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(7).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x03, 0x00, 0x01, 0x02, // RopOpenMessage
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    rops.extend_from_slice(&test_mapi_folder_id(7).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&test_mapi_message_id(message_id).to_le_bytes());
    rops.extend_from_slice(&[
        0x0F, 0x00, 0x02, // RopReadRecipients
    ]);
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    assert!(contains_bytes(response_rops, &utf16z("bob@example.test")));
    assert!(contains_bytes(response_rops, &utf16z("erin@example.test")));
    assert!(contains_bytes(response_rops, &utf16z("Erin")));
}

#[tokio::test]
async fn mapi_over_http_attachment_table_lists_canonical_message_attachments() {
    let message_id = "33333333-3333-3333-3333-333333333333";
    let message_uuid = Uuid::parse_str(message_id).unwrap();
    let attachment_id = Uuid::parse_str("abababab-abab-abab-abab-abababababab").unwrap();
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Attachment message",
    );
    email.has_attachments = true;
    let file_reference = format!("attachment:{message_uuid}:{attachment_id}");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![email])),
        attachments: Arc::new(Mutex::new(HashMap::from([(
            message_uuid,
            vec![ActiveSyncAttachment {
                id: attachment_id,
                message_id: message_uuid,
                file_name: "brief.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                size_octets: 5,
                file_reference,
            }],
        )]))),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x03, 0x00, 0x01, 0x02, // RopOpenMessage
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&test_mapi_message_id(message_id).to_le_bytes());
    rops.extend_from_slice(&[
        0x21, 0x00, 0x02, 0x03, 0x00, // RopGetAttachmentTable
        0x12, 0x00, 0x03, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&5u16.to_le_bytes());
    rops.extend_from_slice(&0x0E21_0003u32.to_le_bytes());
    rops.extend_from_slice(&0x3707_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x370E_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0E20_0003u32.to_le_bytes());
    rops.extend_from_slice(&0x3705_0003u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x15, 0x00, 0x03, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&50u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    assert!(contains_bytes(
        response_rops,
        &[0x21, 0x03, 0, 0, 0, 0, 1, 0, 0, 0]
    ));
    assert!(contains_bytes(response_rops, &utf16z("brief.pdf")));
    assert!(contains_bytes(response_rops, &utf16z("application/pdf")));
    assert!(contains_bytes(response_rops, &5u32.to_le_bytes()));
}

#[tokio::test]
async fn mapi_over_http_open_attachment_returns_canonical_attachment_properties() {
    let message_id = "34343434-3434-3434-3434-343434343434";
    let message_uuid = Uuid::parse_str(message_id).unwrap();
    let attachment_id = Uuid::parse_str("bcbcbcbc-bcbc-bcbc-bcbc-bcbcbcbcbcbc").unwrap();
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Attachment open message",
    );
    email.has_attachments = true;
    let file_reference = format!("attachment:{message_uuid}:{attachment_id}");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![email])),
        attachments: Arc::new(Mutex::new(HashMap::from([(
            message_uuid,
            vec![ActiveSyncAttachment {
                id: attachment_id,
                message_id: message_uuid,
                file_name: "brief-open.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                size_octets: 9,
                file_reference,
            }],
        )]))),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x03, 0x00, 0x01, 0x02, // RopOpenMessage
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&test_mapi_message_id(message_id).to_le_bytes());
    rops.extend_from_slice(&[
        0x22, 0x00, 0x02, 0x03, 0x00, // RopOpenAttachment
    ]);
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x07, 0x00, 0x03, // RopGetPropertiesSpecific
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());
    rops.extend_from_slice(&4u16.to_le_bytes());
    rops.extend_from_slice(&0x0E21_0003u32.to_le_bytes());
    rops.extend_from_slice(&0x3707_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x370E_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x0E20_0003u32.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    assert!(contains_bytes(response_rops, &[0x22, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(response_rops, &utf16z("brief-open.pdf")));
    assert!(contains_bytes(response_rops, &utf16z("application/pdf")));
    assert!(contains_bytes(response_rops, &9u32.to_le_bytes()));
}

#[tokio::test]
async fn mapi_over_http_reads_canonical_attachment_data_stream() {
    let message_id = "35353535-3535-3535-3535-353535353535";
    let message_uuid = Uuid::parse_str(message_id).unwrap();
    let attachment_id = Uuid::parse_str("cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcdcd").unwrap();
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Attachment stream message",
    );
    email.has_attachments = true;
    let file_reference = format!("attachment:{message_uuid}:{attachment_id}");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![email])),
        attachments: Arc::new(Mutex::new(HashMap::from([(
            message_uuid,
            vec![ActiveSyncAttachment {
                id: attachment_id,
                message_id: message_uuid,
                file_name: "stream.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                size_octets: 11,
                file_reference: file_reference.clone(),
            }],
        )]))),
        attachment_contents: Arc::new(Mutex::new(HashMap::from([(
            file_reference.clone(),
            ActiveSyncAttachmentContent {
                file_reference,
                file_name: "stream.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                blob_bytes: b"hello-world".to_vec(),
            },
        )]))),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x03, 0x00, 0x01, 0x02, // RopOpenMessage
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&test_mapi_message_id(message_id).to_le_bytes());
    rops.extend_from_slice(&[
        0x22, 0x00, 0x02, 0x03, 0x00, // RopOpenAttachment
    ]);
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x2B, 0x00, 0x03, 0x04, // RopOpenStream
    ]);
    rops.extend_from_slice(&0x3701_0102u32.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x2C, 0x00, 0x04, // RopReadStream
    ]);
    rops.extend_from_slice(&5u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(
        &rops,
        &[1, u32::MAX, u32::MAX, u32::MAX, u32::MAX],
    ));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    assert!(contains_bytes(
        response_rops,
        &[0x2B, 0x04, 0, 0, 0, 0, 11, 0, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        response_rops,
        &[0x2C, 0x04, 0, 0, 0, 0, 5, 0, b'h', b'e', b'l', b'l', b'o']
    ));
}

#[tokio::test]
async fn mapi_over_http_create_attachment_saves_canonical_attachment_from_properties() {
    let message_id = "37373737-3737-3737-3737-373737373737";
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let created_attachments = Arc::new(Mutex::new(Vec::new()));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            message_id,
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "MAPI attachment message",
        )])),
        created_attachments: created_attachments.clone(),
        ..Default::default()
    };
    let service =
        ExchangeService::new_with_validator(store, Validator::new(FakeDetector::pdf(), 0.8));
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x3707_001F, "mapi-upload.pdf");
    append_mapi_utf16_property(&mut property_values, 0x370E_001F, "application/pdf");
    append_mapi_binary_property(&mut property_values, 0x3701_0102, b"%PDF-mapi");

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x03, 0x00, 0x01, 0x02, // RopOpenMessage
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&test_mapi_message_id(message_id).to_le_bytes());
    rops.extend_from_slice(&[
        0x23, 0x00, 0x02, 0x03, // RopCreateAttachment
        0x0A, 0x00, 0x03, // RopSetProperties
    ]);
    rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
    rops.extend_from_slice(&[
        0x25, 0x00, 0x02, 0x03, 0x00, // RopSaveChangesAttachment
    ]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    assert!(contains_bytes(
        response_rops,
        &[0x23, 0x03, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(response_rops, &[0x25, 0x02, 0, 0, 0, 0]));
    let created = created_attachments.lock().unwrap();
    assert_eq!(created.len(), 1);
    assert_eq!(created[0].file_name, "mapi-upload.pdf");
    assert_eq!(created[0].media_type, "application/pdf");
    assert_eq!(created[0].blob_bytes, b"%PDF-mapi");
}

#[tokio::test]
async fn mapi_over_http_write_stream_saves_canonical_attachment() {
    let message_id = "38383838-3838-3838-3838-383838383838";
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let created_attachments = Arc::new(Mutex::new(Vec::new()));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            message_id,
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "MAPI stream attachment message",
        )])),
        created_attachments: created_attachments.clone(),
        ..Default::default()
    };
    let service =
        ExchangeService::new_with_validator(store, Validator::new(FakeDetector::pdf(), 0.8));
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x3707_001F, "stream-upload.pdf");
    append_mapi_utf16_property(&mut property_values, 0x370E_001F, "application/pdf");

    let stream_bytes = b"%PDF-stream";
    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x03, 0x00, 0x01, 0x02, // RopOpenMessage
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&test_mapi_message_id(message_id).to_le_bytes());
    rops.extend_from_slice(&[
        0x23, 0x00, 0x02, 0x03, // RopCreateAttachment
        0x0A, 0x00, 0x03, // RopSetProperties
    ]);
    rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
    rops.extend_from_slice(&[
        0x2B, 0x00, 0x03, 0x04, // RopOpenStream
    ]);
    rops.extend_from_slice(&0x3701_0102u32.to_le_bytes());
    rops.push(1);
    rops.extend_from_slice(&[
        0x2D, 0x00, 0x04, // RopWriteStream
    ]);
    rops.extend_from_slice(&(stream_bytes.len() as u16).to_le_bytes());
    rops.extend_from_slice(stream_bytes);
    rops.extend_from_slice(&[
        0x5D, 0x00, 0x04, // RopCommitStream
        0x25, 0x00, 0x02, 0x03, 0x00, // RopSaveChangesAttachment
    ]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(
        &rops,
        &[1, u32::MAX, u32::MAX, u32::MAX, u32::MAX],
    ));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    assert!(contains_bytes(
        response_rops,
        &[0x2D, 0x04, 0, 0, 0, 0, stream_bytes.len() as u8, 0]
    ));
    assert!(contains_bytes(response_rops, &[0x5D, 0x04, 0, 0, 0, 0]));
    let created = created_attachments.lock().unwrap();
    assert_eq!(created.len(), 1);
    assert_eq!(created[0].file_name, "stream-upload.pdf");
    assert_eq!(created[0].blob_bytes, stream_bytes);
}

#[tokio::test]
async fn mapi_over_http_delete_attachment_removes_canonical_attachment() {
    let message_id = "39393939-3939-3939-3939-393939393939";
    let message_uuid = Uuid::parse_str(message_id).unwrap();
    let attachment_id = Uuid::parse_str("cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcdcd").unwrap();
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Attachment delete message",
    );
    email.has_attachments = true;
    let attachments = Arc::new(Mutex::new(HashMap::from([(
        message_uuid,
        vec![ActiveSyncAttachment {
            id: attachment_id,
            message_id: message_uuid,
            file_name: "delete.pdf".to_string(),
            media_type: "application/pdf".to_string(),
            size_octets: 11,
            file_reference: format!("attachment:{message_uuid}:{attachment_id}"),
        }],
    )])));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![email])),
        attachments: attachments.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x03, 0x00, 0x01, 0x02, // RopOpenMessage
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&test_mapi_message_id(message_id).to_le_bytes());
    rops.extend_from_slice(&[
        0x24, 0x00, 0x02, // RopDeleteAttachment
    ]);
    rops.extend_from_slice(&0u32.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    assert!(contains_bytes(response_rops, &[0x24, 0x02, 0, 0, 0, 0]));
    assert!(attachments.lock().unwrap()[&message_uuid].is_empty());
}

#[tokio::test]
async fn mapi_over_http_get_valid_attachments_lists_canonical_attachment_numbers() {
    let message_id = "40404040-4040-4040-4040-404040404040";
    let message_uuid = Uuid::parse_str(message_id).unwrap();
    let first_attachment_id = Uuid::parse_str("abababab-abab-abab-abab-abababababab").unwrap();
    let second_attachment_id = Uuid::parse_str("bcbcbcbc-bcbc-bcbc-bcbc-bcbcbcbcbcbc").unwrap();
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Valid attachments message",
    );
    email.has_attachments = true;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![email])),
        attachments: Arc::new(Mutex::new(HashMap::from([(
            message_uuid,
            vec![
                ActiveSyncAttachment {
                    id: first_attachment_id,
                    message_id: message_uuid,
                    file_name: "first.pdf".to_string(),
                    media_type: "application/pdf".to_string(),
                    size_octets: 11,
                    file_reference: format!("attachment:{message_uuid}:{first_attachment_id}"),
                },
                ActiveSyncAttachment {
                    id: second_attachment_id,
                    message_id: message_uuid,
                    file_name: "second.pdf".to_string(),
                    media_type: "application/pdf".to_string(),
                    size_octets: 22,
                    file_reference: format!("attachment:{message_uuid}:{second_attachment_id}"),
                },
            ],
        )]))),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x03, 0x00, 0x01, 0x02, // RopOpenMessage
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&test_mapi_message_id(message_id).to_le_bytes());
    rops.extend_from_slice(&[
        0x52, 0x00, 0x02, // RopGetValidAttachments
    ]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x52, 0x02, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 1, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_set_read_flags_updates_canonical_message_state() {
    let message_id = "36363636-3636-3636-3636-363636363636";
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    inbox.unread_emails = 1;
    let mut email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Read flag message",
    );
    email.unread = true;
    let emails = Arc::new(Mutex::new(vec![email]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: emails.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x66, 0x00, 0x01, 0x00, 0x01, // RopSetReadFlags, sync, rfSuppressReceipt
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&test_mapi_message_id(message_id).to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    assert!(contains_bytes(response_rops, &[0x66, 0x01, 0, 0, 0, 0, 0]));
    assert!(!emails.lock().unwrap()[0].unread);
}

#[tokio::test]
async fn mapi_over_http_set_message_read_flag_updates_open_message_state() {
    let message_id = "37373737-3737-3737-3737-373737373737";
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    inbox.unread_emails = 1;
    let mut email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Message read flag",
    );
    email.unread = true;
    let emails = Arc::new(Mutex::new(vec![email]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: emails.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x03, 0x00, 0x01, 0x02, // RopOpenMessage
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&test_mapi_message_id(message_id).to_le_bytes());
    rops.extend_from_slice(&[
        0x11, 0x00, 0x02, 0x02, 0x01, // RopSetMessageReadFlag
    ]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    assert!(contains_bytes(response_rops, &[0x11, 0x02, 0, 0, 0, 0, 1]));
    assert!(!emails.lock().unwrap()[0].unread);
}

#[tokio::test]
async fn mapi_over_http_set_properties_updates_open_message_flags() {
    let message_id = "38383838-3838-3838-3838-383838383838";
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    inbox.unread_emails = 1;
    let mut email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Message property flags",
    );
    email.unread = true;
    email.flagged = false;
    let emails = Arc::new(Mutex::new(vec![email]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: emails.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut property_values = Vec::new();
    append_mapi_i32_property(&mut property_values, 0x0E07_0003, 1);
    append_mapi_i32_property(&mut property_values, 0x1090_0003, 2);

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x03, 0x00, 0x01, 0x02, // RopOpenMessage
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&test_mapi_message_id(message_id).to_le_bytes());
    rops.extend_from_slice(&[
        0x0A, 0x00, 0x02, // RopSetProperties on opened message
    ]);
    rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&property_values);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x0A, 0x02, 0, 0, 0, 0, 0, 0]
    ));
    let updated = emails.lock().unwrap()[0].clone();
    assert!(!updated.unread);
    assert!(updated.flagged);
}

#[tokio::test]
async fn mapi_over_http_cached_mode_properties_include_canonical_change_keys() {
    let message_id = "39393939-3939-3939-3939-393939393939";
    let mailbox_id = "55555555-5555-5555-5555-555555555555";
    let mut inbox = FakeStore::mailbox(mailbox_id, "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(message_id, mailbox_id, "inbox", "Cached mode message");
    email.flagged = true;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x03, 0x00, 0x01, 0x02, // RopOpenMessage
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&test_mapi_message_id(message_id).to_le_bytes());
    rops.extend_from_slice(&[
        0x07, 0x00, 0x02, // RopGetPropertiesSpecific
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());
    rops.extend_from_slice(&5u16.to_le_bytes());
    rops.extend_from_slice(&0x65E0_0102u32.to_le_bytes());
    rops.extend_from_slice(&0x65E1_0102u32.to_le_bytes());
    rops.extend_from_slice(&0x65E2_0102u32.to_le_bytes());
    rops.extend_from_slice(&0x67A4_0014u32.to_le_bytes());
    rops.extend_from_slice(&0x1090_0003u32.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &mapi_mailstore::STORE_REPLICA_GUID
    ));
    assert!(contains_bytes(&response_rops, &2i32.to_le_bytes()));
}

#[tokio::test]
async fn mapi_over_http_sync_configure_returns_canonical_manifest_buffer() {
    let message_id = "40404040-4040-4040-4040-404040404040";
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Sync manifest message",
    );
    email.bcc.push(JmapEmailAddress {
        address: "hidden@example.test".to_string(),
        display_name: None,
    });
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, 0x00, 0x00, 0x00, 0x4E, 0x00, 0x02, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, b"LPE-MAPI-SYNC\0"));
    assert!(contains_bytes(&response_rops, b"Sync manifest message"));
    assert!(!contains_bytes(&response_rops, b"hidden@example.test"));
}

#[tokio::test]
async fn mapi_over_http_get_local_replica_ids_returns_replica_guid() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x7F, 0x00, 0x00, // RopGetLocalReplicaIds
    ];
    rops.extend_from_slice(&4u32.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &mapi_mailstore::STORE_REPLICA_GUID
    ));
    assert!(contains_bytes(&response_rops, &4u32.to_le_bytes()));
}

#[tokio::test]
async fn mapi_over_http_sync_upload_state_round_trips_as_transfer_state() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let state = b"client-uploaded-sync-state";
    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, // RopSynchronizationOpenCollector
    ]);
    rops.extend_from_slice(&[
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
    ]);
    rops.extend_from_slice(&0x65E2_0102u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x76, 0x00, 0x02, // RopSynchronizationUploadStateStreamContinue
    ]);
    rops.extend_from_slice(&(state.len() as u32).to_le_bytes());
    rops.extend_from_slice(state);
    rops.extend_from_slice(&[
        0x77, 0x00, 0x02, // RopSynchronizationUploadStateStreamEnd
        0x82, 0x00, 0x02, 0x03, // RopSynchronizationGetTransferState
        0x4E, 0x00, 0x03, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x7E, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x82, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, state));
}

#[tokio::test]
async fn mapi_over_http_sync_import_message_change_updates_canonical_flags() {
    let message_id = "41414141-4141-4141-4141-414141414141";
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Import message change",
    );
    email.unread = true;
    email.flagged = false;
    let emails = Arc::new(Mutex::new(vec![email]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: emails.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut property_values = Vec::new();
    append_mapi_i32_property(&mut property_values, 0x0E07_0003, 1);
    append_mapi_i32_property(&mut property_values, 0x1090_0003, 2);

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, // RopSynchronizationOpenCollector
        0x72, 0x00, 0x02, 0x03, // RopSynchronizationImportMessageChange
    ]);
    rops.extend_from_slice(&test_mapi_message_id(message_id).to_le_bytes());
    rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&property_values);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x72, 0x03, 0, 0, 0, 0]));
    let updated = emails.lock().unwrap()[0].clone();
    assert!(!updated.unread);
    assert!(updated.flagged);
}

#[tokio::test]
async fn mapi_over_http_sync_import_delete_and_read_state_use_canonical_store() {
    let read_message_id = "42424242-4242-4242-4242-424242424242";
    let delete_message_id = "43434343-4343-4343-4343-434343434343";
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 2;
    inbox.unread_emails = 1;
    let mut read_email = FakeStore::email(
        read_message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Read import",
    );
    read_email.unread = true;
    let delete_email = FakeStore::email(
        delete_message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Delete import",
    );
    let emails = Arc::new(Mutex::new(vec![read_email, delete_email]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: emails.clone(),
        ..Default::default()
    };
    let deleted_emails = store.deleted_emails.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, // RopSynchronizationOpenCollector
        0x80, 0x00, 0x02, // RopSynchronizationImportReadStateChanges
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&test_mapi_message_id(read_message_id).to_le_bytes());
    rops.push(1);
    rops.extend_from_slice(&[
        0x74, 0x00, 0x02, // RopSynchronizationImportDeletes
        0x00,
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&test_mapi_message_id(delete_message_id).to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x80, 0x02, 0, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x74, 0x02, 0, 0, 0, 0, 0]));
    assert!(!emails.lock().unwrap()[0].unread);
    assert_eq!(
        deleted_emails.lock().unwrap().as_slice(),
        &[Uuid::parse_str(delete_message_id).unwrap()]
    );
}

#[tokio::test]
async fn mapi_over_http_sync_import_move_uses_canonical_store() {
    let message_id = "44444444-4444-4444-4444-444444444444";
    let inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    let archive = FakeStore::mailbox("66666666-6666-6666-6666-666666666666", "archive", "Archive");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox, archive])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            message_id,
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Move import",
        )])),
        ..Default::default()
    };
    let moved_emails = store.moved_emails.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, // RopSynchronizationOpenCollector
        0x78, 0x00, 0x02, // RopSynchronizationImportMessageMove
    ]);
    rops.extend_from_slice(&test_mapi_message_id(message_id).to_le_bytes());
    rops.extend_from_slice(&test_mapi_folder_id(0x6666_6666_6666).to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x78, 0x02, 0, 0, 0, 0]));
    assert_eq!(
        moved_emails.lock().unwrap().as_slice(),
        &[(
            Uuid::parse_str(message_id).unwrap(),
            Uuid::parse_str("66666666-6666-6666-6666-666666666666").unwrap()
        )]
    );
}

#[tokio::test]
async fn mapi_over_http_sync_import_hierarchy_change_creates_canonical_mailbox() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    let created_mailboxes = store.created_mailboxes.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut hierarchy_values = Vec::new();
    append_mapi_binary_property(&mut hierarchy_values, 0x65E1_0102, &[]);
    append_mapi_binary_property(
        &mut hierarchy_values,
        0x65E0_0102,
        b"local-folder-source-key",
    );
    append_mapi_i64_property(&mut hierarchy_values, 0x3008_0040, 0);
    append_mapi_binary_property(&mut hierarchy_values, 0x65E2_0102, b"change-key");
    append_mapi_binary_property(&mut hierarchy_values, 0x65E3_0102, b"pcl");
    append_mapi_utf16_property(&mut hierarchy_values, 0x3001_001F, "Imported Sync Folder");

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x3001_001F, "Imported Sync Folder");

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, // RopSynchronizationOpenCollector
        0x73, 0x00, 0x02, // RopSynchronizationImportHierarchyChange
    ]);
    rops.extend_from_slice(&6u16.to_le_bytes());
    rops.extend_from_slice(&hierarchy_values);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&property_values);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x73, 0x02, 0, 0, 0, 0]));
    assert_eq!(
        created_mailboxes.lock().unwrap()[0].name,
        "Imported Sync Folder"
    );
}

#[tokio::test]
async fn mapi_over_http_sync_import_hierarchy_change_rejects_system_folder_mutation() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    let created_mailboxes = store.created_mailboxes.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut hierarchy_values = Vec::new();
    append_mapi_binary_property(&mut hierarchy_values, 0x65E1_0102, &[]);
    append_mapi_binary_property(&mut hierarchy_values, 0x65E0_0102, b"system-source-key");
    append_mapi_utf16_property(&mut hierarchy_values, 0x3001_001F, "Inbox");

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, // RopSynchronizationOpenCollector
        0x73, 0x00, 0x02, // RopSynchronizationImportHierarchyChange
    ]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&hierarchy_values);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&hierarchy_values);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x73, 0x02, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(created_mailboxes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_get_properties_specific_returns_folder_properties() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 7;
    inbox.unread_emails = 2;
    let folder_id = test_mapi_folder_id(5);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&folder_id.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x07, 0x00, 0x01, // RopGetPropertiesSpecific
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x3602_0003u32.to_le_bytes());
    rops.extend_from_slice(&0x3603_0003u32.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];
    let get_props_offset = 8;

    assert_eq!(response_rops[get_props_offset], 0x07);
    assert_eq!(response_rops[get_props_offset + 1], 0x01);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[get_props_offset + 2..get_props_offset + 6]
                .try_into()
                .unwrap()
        ),
        0
    );
    assert!(contains_bytes(response_rops, &utf16z("Inbox")));
    assert!(contains_bytes(response_rops, &7u32.to_le_bytes()));
    assert!(contains_bytes(response_rops, &2u32.to_le_bytes()));
}

#[tokio::test]
async fn mapi_over_http_execute_handles_mailbox_store_bootstrap_rops() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x09, 0x00, 0x01, // RopGetPropertiesList
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x16, 0x00, 0x02, // RopGetStatus
        0x17, 0x00, 0x02, // RopQueryPosition
        0x81, 0x00, 0x02, // RopResetTable
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&50u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    let props_list_offset = 8;
    assert_eq!(response_rops[props_list_offset], 0x09);
    assert_eq!(
        u16::from_le_bytes(
            response_rops[props_list_offset + 6..props_list_offset + 8]
                .try_into()
                .unwrap()
        ),
        13
    );
    assert!(contains_bytes(response_rops, &0x6748_0014u32.to_le_bytes()));

    let contents_offset = props_list_offset + 8 + 13 * 4;
    assert_eq!(response_rops[contents_offset], 0x05);
    assert_eq!(response_rops[contents_offset + 1], 0x02);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[contents_offset + 2..contents_offset + 6]
                .try_into()
                .unwrap()
        ),
        0
    );
    assert_eq!(
        u32::from_le_bytes(
            response_rops[contents_offset + 6..contents_offset + 10]
                .try_into()
                .unwrap()
        ),
        0
    );

    let status_offset = contents_offset + 10;
    assert_eq!(
        &response_rops[status_offset..status_offset + 7],
        &[0x16, 0x02, 0, 0, 0, 0, 0]
    );
    let position_offset = status_offset + 7;
    assert_eq!(response_rops[position_offset], 0x17);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[position_offset + 2..position_offset + 6]
                .try_into()
                .unwrap()
        ),
        0
    );
    let reset_offset = position_offset + 14;
    assert_eq!(
        &response_rops[reset_offset..reset_offset + 6],
        &[0x81, 0x02, 0, 0, 0, 0]
    );
    let query_offset = reset_offset + 6;
    assert_eq!(response_rops[query_offset], 0x15);
    assert_eq!(
        u16::from_le_bytes(
            response_rops[query_offset + 7..query_offset + 9]
                .try_into()
                .unwrap()
        ),
        0
    );
}

#[tokio::test]
async fn mapi_over_http_execute_returns_receive_folder_and_store_state() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rops = vec![0x27, 0x00, 0x00];
    rops.extend_from_slice(b"IPM.Note\0");
    rops.extend_from_slice(&[
        0x68, 0x00, 0x00, // RopGetReceiveFolderTable
        0x7B, 0x00, 0x00, // RopGetStoreState
    ]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rops = &rop_buffer[2..2 + response_rop_size];

    assert_eq!(response_rops[0], 0x27);
    assert_eq!(
        u64::from_le_bytes(response_rops[6..14].try_into().unwrap()),
        test_mapi_folder_id(5)
    );
    assert!(contains_bytes(response_rops, b"IPM.Note\0"));

    let table_offset = 23;
    assert_eq!(response_rops[table_offset], 0x68);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[table_offset + 6..table_offset + 10]
                .try_into()
                .unwrap()
        ),
        1
    );
    assert!(contains_bytes(response_rops, &utf16z("IPM.Note")));

    let store_offset = response_rops.len() - 10;
    assert_eq!(response_rops[store_offset], 0x7B);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[store_offset + 6..store_offset + 10]
                .try_into()
                .unwrap()
        ),
        0
    );
}

#[tokio::test]
async fn mapi_over_http_bind_creates_nspi_session() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &mapi_headers("Bind"), b"")
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "Bind");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    assert!(response
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .starts_with("lpe_mapi_nspi="));

    let body = response_bytes(response).await;
    assert_eq!(body.len(), 28);
    assert_eq!(&body[0..8], &[0, 0, 0, 0, 0, 0, 0, 0]);
    assert_ne!(&body[8..24], &[0; 16]);
}

#[tokio::test]
async fn mapi_over_http_returns_nspi_and_mailbox_urls() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let mut headers = mapi_headers("GetAddressBookUrl");
    headers.insert("host", HeaderValue::from_static("mail.example.test"));
    headers.insert("x-forwarded-proto", HeaderValue::from_static("https"));

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &headers, b"")
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("x-requesttype").unwrap(),
        "GetAddressBookUrl"
    );
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[0..4].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[4..8].try_into().unwrap()), 0);
    assert_eq!(
        utf16z_string_bytes(&body[8..]),
        b"https://mail.example.test/mapi/nspi/".to_vec()
    );
    assert!(body.ends_with(&[0, 0, 0, 0]));

    headers.insert("x-requesttype", HeaderValue::from_static("GetMailboxUrl"));
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &headers, b"")
        .await
        .unwrap();
    assert_eq!(
        response.headers().get("x-requesttype").unwrap(),
        "GetMailboxUrl"
    );
    let body = response_bytes(response).await;
    assert_eq!(
        utf16z_string_bytes(&body[8..]),
        b"https://mail.example.test/mapi/emsmdb/".to_vec()
    );
}

#[tokio::test]
async fn mapi_over_http_resolve_names_resolves_authenticated_mailbox() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &mapi_headers("ResolveNames"), &[0; 103])
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("x-requesttype").unwrap(),
        "ResolveNames"
    );
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[0..4].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[4..8].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[8..12].try_into().unwrap()), 1200);
    assert_eq!(body[12], 1);
    assert_eq!(u32::from_le_bytes(body[13..17].try_into().unwrap()), 1);
    assert_eq!(u32::from_le_bytes(body[17..21].try_into().unwrap()), 2);
    assert_eq!(body[21], 1);
    assert_eq!(u32::from_le_bytes(body[22..26].try_into().unwrap()), 8);
    assert_eq!(u32::from_le_bytes(body[58..62].try_into().unwrap()), 1);
    assert_eq!(body[62], 0);
    assert!(contains_bytes(&body, &utf16z("alice@example.test")));
    assert!(contains_bytes(&body, &utf16z("Alice")));
    assert!(contains_bytes(&body, &utf16z("SMTP")));
}

#[tokio::test]
async fn mapi_over_http_resolve_names_honors_requested_rca_columns() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let request = resolve_names_request("alice@example.test", &[0x3003_001F, 0x3001_001F]);

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &mapi_headers("ResolveNames"), &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[0..4].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[4..8].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[8..12].try_into().unwrap()), 1200);
    assert_eq!(body[12], 1);
    assert_eq!(u32::from_le_bytes(body[13..17].try_into().unwrap()), 1);
    assert_eq!(u32::from_le_bytes(body[17..21].try_into().unwrap()), 2);
    assert_eq!(body[21], 1);
    assert_eq!(u32::from_le_bytes(body[22..26].try_into().unwrap()), 2);
    assert_eq!(
        u32::from_le_bytes(body[26..30].try_into().unwrap()),
        0x3003_001F
    );
    assert_eq!(
        u32::from_le_bytes(body[30..34].try_into().unwrap()),
        0x3001_001F
    );
    assert_eq!(u32::from_le_bytes(body[34..38].try_into().unwrap()), 1);
    assert_eq!(body[38], 0);
    assert!(contains_bytes(&body, &utf16z("alice@example.test")));
    assert!(contains_bytes(&body, &utf16z("Alice")));
    assert!(!contains_bytes(&body, &utf16z("SMTP")));
    assert!(body.ends_with(&[0, 0, 0, 0]));
}

#[tokio::test]
async fn mapi_over_http_resolve_names_falls_back_to_authenticated_mailbox_for_rca() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        omit_principal_from_directory: true,
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let request = resolve_names_request("alice@example.test", &[0x3003_001F, 0x3001_001F]);

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &mapi_headers("ResolveNames"), &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[17..21].try_into().unwrap()), 2);
    assert_eq!(body[21], 1);
    assert!(contains_bytes(&body, &utf16z("alice@example.test")));
    assert!(contains_bytes(&body, &utf16z("Alice")));
}

#[tokio::test]
async fn mapi_over_http_resolve_names_resolves_canonical_contact() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        contacts: Arc::new(Mutex::new(vec![FakeStore::contact(
            "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "Bob Contact",
            "bob@example.test",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let request = resolve_names_request("bob@example.test", &[0x3003_001F, 0x3001_001F]);

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &mapi_headers("ResolveNames"), &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[17..21].try_into().unwrap()), 2);
    assert!(contains_bytes(&body, &utf16z("bob@example.test")));
    assert!(contains_bytes(&body, &utf16z("Bob Contact")));
}

#[tokio::test]
async fn mapi_over_http_hidden_authenticated_account_is_not_browsed_but_resolves_self() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        omit_principal_from_directory: true,
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &mapi_headers("QueryRows"), &[0; 32])
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert!(!contains_bytes(&body, &utf16z("alice@example.test")));

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &mapi_headers("GetMatches"), &[0; 32])
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert!(!contains_bytes(&body, &utf16z("alice@example.test")));

    let request = resolve_names_request("alice@example.test", &[0x3003_001F, 0x3001_001F]);
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &mapi_headers("ResolveNames"), &request)
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[17..21].try_into().unwrap()), 2);
    assert!(contains_bytes(&body, &utf16z("alice@example.test")));
}

#[tokio::test]
async fn mapi_over_http_query_rows_stays_in_authenticated_tenant() {
    let mut same_tenant = FakeStore::account();
    same_tenant.account_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    same_tenant.email = "bob@example.test".to_string();
    same_tenant.display_name = "Bob".to_string();

    let mut other_tenant = FakeStore::account();
    other_tenant.tenant_id = "tenant-b".to_string();
    other_tenant.account_id = Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap();
    other_tenant.email = "mallory@other.test".to_string();
    other_tenant.display_name = "Mallory".to_string();

    let store = FakeStore {
        session: Some(FakeStore::account()),
        directory_accounts: Arc::new(Mutex::new(vec![same_tenant, other_tenant])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &mapi_headers("QueryRows"), &[0; 32])
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_bytes(response).await;
    assert!(contains_bytes(&body, &utf16z("alice@example.test")));
    assert!(contains_bytes(&body, &utf16z("bob@example.test")));
    assert!(!contains_bytes(&body, &utf16z("mallory@other.test")));

    let request = resolve_names_request("mallory@other.test", &[0x3003_001F, 0x3001_001F]);
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &mapi_headers("ResolveNames"), &request)
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[17..21].try_into().unwrap()), 0);
    assert_eq!(body[21], 0);
    assert!(!contains_bytes(&body, &utf16z("mallory@other.test")));
}

#[tokio::test]
async fn mapi_over_http_resolve_names_returns_no_match_for_unknown_name() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let request = resolve_names_request("nobody@example.test", &[0x3003_001F, 0x3001_001F]);

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &mapi_headers("ResolveNames"), &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[0..4].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[4..8].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[17..21].try_into().unwrap()), 0);
    assert_eq!(body[21], 0);
    assert!(!contains_bytes(&body, &utf16z("alice@example.test")));
    assert!(!contains_bytes(&body, &utf16z("nobody@example.test")));
}

#[tokio::test]
async fn mapi_over_http_nspi_bootstrap_requests_return_success() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    for request_type in [
        "CompareMIds",
        "DNToMId",
        "GetMatches",
        "GetPropList",
        "GetProps",
        "GetSpecialTable",
        "GetTemplateInfo",
        "QueryColumns",
        "QueryRows",
        "ResortRestriction",
        "SeekEntries",
        "UpdateStat",
    ] {
        let response = service
            .handle_mapi(MapiEndpoint::Nspi, &mapi_headers(request_type), &[0; 32])
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK, "{request_type}");
        assert_eq!(
            response.headers().get("x-requesttype").unwrap(),
            request_type,
            "{request_type}"
        );
        assert_eq!(
            response.headers().get("x-responsecode").unwrap(),
            "0",
            "{request_type}"
        );
        let body = response_bytes(response).await;
        assert!(body.len() >= 12, "{request_type}");
        assert_eq!(
            u32::from_le_bytes(body[0..4].try_into().unwrap()),
            0,
            "{request_type}"
        );
        assert_eq!(
            u32::from_le_bytes(body[4..8].try_into().unwrap()),
            0,
            "{request_type}"
        );

        match request_type {
            "GetMatches" => {
                assert_eq!(body[8], 0, "{request_type}");
                assert_eq!(body[9], 1, "{request_type}");
                assert_eq!(
                    u32::from_le_bytes(body[10..14].try_into().unwrap()),
                    1,
                    "{request_type}"
                );
                assert_ne!(
                    u32::from_le_bytes(body[14..18].try_into().unwrap()),
                    0,
                    "{request_type}"
                );
                assert_eq!(body[18], 1, "{request_type}");
                assert!(contains_bytes(&body, &0x3001_001Fu32.to_le_bytes()));
                assert!(contains_bytes(&body, &0x39FE_001Fu32.to_le_bytes()));
                assert!(contains_bytes(&body, &utf16z("alice@example.test")));
                assert!(contains_bytes(&body, &utf16z("Alice")));
            }
            "QueryRows" | "SeekEntries" => {
                assert!(contains_bytes(&body, &0x3001_001Fu32.to_le_bytes()));
                assert!(contains_bytes(&body, &0x39FE_001Fu32.to_le_bytes()));
                assert!(contains_bytes(&body, &utf16z("alice@example.test")));
                assert!(contains_bytes(&body, &utf16z("Alice")));
            }
            "GetProps" | "GetTemplateInfo" => {
                assert_eq!(u32::from_le_bytes(body[8..12].try_into().unwrap()), 1200);
                assert_eq!(body[12], 1, "{request_type}");
                assert!(contains_bytes(&body, &utf16z("alice@example.test")));
                assert!(contains_bytes(&body, &utf16z("Alice")));
            }
            "ResortRestriction" => {
                assert!(body.len() >= 19, "{request_type}");
                assert_eq!(body[8], 0, "{request_type}");
                assert_eq!(body[9], 1, "{request_type}");
                assert_eq!(
                    u32::from_le_bytes(body[10..14].try_into().unwrap()),
                    1,
                    "{request_type}"
                );
                assert_ne!(
                    u32::from_le_bytes(body[14..18].try_into().unwrap()),
                    0,
                    "{request_type}"
                );
            }
            "GetPropList" | "QueryColumns" => {
                assert_eq!(body[8], 1, "{request_type}");
                assert!(contains_bytes(&body, &0x3001_001Fu32.to_le_bytes()));
                assert!(contains_bytes(&body, &0x39FE_001Fu32.to_le_bytes()));
            }
            "GetSpecialTable" => {
                assert_eq!(u32::from_le_bytes(body[8..12].try_into().unwrap()), 1200);
                assert!(contains_bytes(&body, &utf16z("Global Address List")));
            }
            "DNToMId" => {
                assert_ne!(u32::from_le_bytes(body[8..12].try_into().unwrap()), 0);
            }
            _ => {}
        }
    }
}

#[tokio::test]
async fn mapi_over_http_unbind_consumes_nspi_session() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let bind = service
        .handle_mapi(MapiEndpoint::Nspi, &mapi_headers("Bind"), b"")
        .await
        .unwrap();
    let cookie = bind
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut unbind_headers = mapi_headers("Unbind");
    unbind_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &unbind_headers, b"")
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "Unbind");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    assert!(response
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("Max-Age=0"));
}

#[tokio::test]
async fn mapi_over_http_rejects_missing_authentication() {
    let store = FakeStore::default();
    let service = ExchangeService::new(store);

    let error = service
        .handle_mapi(MapiEndpoint::Emsmdb, &HeaderMap::new(), b"")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("missing account authentication"));
}

#[tokio::test]
async fn rpc_proxy_challenges_missing_authentication_with_basic() {
    let store = FakeStore::default();
    let service = ExchangeService::new(store);
    let uri: Uri = "/rpc/rpcproxy.dll".parse().unwrap();

    let response = service
        .handle_rpc_proxy(&Method::GET, &uri, &HeaderMap::new(), b"")
        .await;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response.headers().get(axum::http::header::WWW_AUTHENTICATE),
        Some(&HeaderValue::from_static("Basic realm=\"LPE RPC\""))
    );
    let body = response_text(response).await;
    assert!(body.contains("missing account authentication"));
}

#[tokio::test]
async fn rpc_proxy_challenges_anonymous_msrpch_echo_ping() {
    let store = FakeStore::default();
    let service = ExchangeService::new(store);
    let method = Method::from_bytes(b"RPC_IN_DATA").expect("valid RPC method");
    let uri: Uri = "/rpc/rpcproxy.dll".parse().unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("user-agent", HeaderValue::from_static("MSRPC"));
    headers.insert("accept", HeaderValue::from_static("application/rpc"));

    let response = service.handle_rpc_proxy(&method, &uri, &headers, b"").await;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response.headers().get(axum::http::header::WWW_AUTHENTICATE),
        Some(&HeaderValue::from_static("Basic realm=\"LPE RPC\""))
    );
    let body = response_text(response).await;
    assert!(body.contains("missing account authentication"));
}

#[tokio::test]
async fn rpc_proxy_answers_authenticated_msrpch_echo_ping() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let method = Method::from_bytes(b"RPC_IN_DATA").expect("valid RPC method");
    let uri: Uri = "/rpc/rpcproxy.dll".parse().unwrap();
    let mut headers = bearer_headers();
    headers.insert("user-agent", HeaderValue::from_static("MSRPC"));
    headers.insert("accept", HeaderValue::from_static("application/rpc"));

    let response = service.handle_rpc_proxy(&method, &uri, &headers, b"").await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type"),
        Some(&HeaderValue::from_static("application/rpc"))
    );
    assert_eq!(
        response.headers().get("x-lpe-rpc-proxy-status"),
        Some(&HeaderValue::from_static("echo"))
    );
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(
        body.as_ref(),
        &[
            0x05, 0x00, 0x14, 0x03, 0x10, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x40, 0x00, 0x00, 0x00
        ]
    );
}

#[tokio::test]
async fn rpc_proxy_referral_endpoint_ping_returns_rts_connect_without_synthetic_bind_ack() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let method = Method::from_bytes(b"RPC_OUT_DATA").expect("valid RPC method");
    let uri: Uri = "/rpc/rpcproxy.dll?mail.example.test:6002".parse().unwrap();
    let mut headers = bearer_headers();
    headers.insert("user-agent", HeaderValue::from_static("MSRPC"));
    headers.insert("accept", HeaderValue::from_static("application/rpc"));

    let connect_body = rpc_proxy_conn_a1_request_body(0x0000_8000);
    let response = service
        .handle_rpc_proxy(&method, &uri, &headers, &connect_body)
        .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type"),
        Some(&HeaderValue::from_static("application/rpc"))
    );
    assert_eq!(
        response.headers().get("x-lpe-rpc-proxy-status"),
        Some(&HeaderValue::from_static("endpoint-ping"))
    );
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.len(), 72);
    assert_eq!(u16::from_le_bytes([body[8], body[9]]), 28);
    assert_eq!(u16::from_le_bytes([body[18], body[19]]), 1);
    assert_eq!(
        u32::from_le_bytes([body[20], body[21], body[22], body[23]]),
        2
    );
    assert_eq!(u16::from_le_bytes([body[36], body[37]]), 44);
    assert_eq!(u16::from_le_bytes([body[46], body[47]]), 3);
    assert_eq!(
        u32::from_le_bytes([body[48], body[49], body[50], body[51]]),
        6
    );
    assert_eq!(
        u32::from_le_bytes([body[60], body[61], body[62], body[63]]),
        0x0000_8000
    );
}

#[tokio::test]
async fn rpc_proxy_mailstore_endpoint_ping_includes_bind_ack_after_rts_connect() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let method = Method::from_bytes(b"RPC_OUT_DATA").expect("valid RPC method");
    let uri: Uri = "/rpc/rpcproxy.dll?mail.example.test:6001".parse().unwrap();
    let mut headers = bearer_headers();
    headers.insert("user-agent", HeaderValue::from_static("MSRPC"));
    headers.insert("accept", HeaderValue::from_static("application/rpc"));

    let connect_body = rpc_proxy_conn_a1_request_body(0x0000_8000);
    let response = service
        .handle_rpc_proxy(&method, &uri, &headers, &connect_body)
        .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type"),
        Some(&HeaderValue::from_static("application/rpc"))
    );
    assert_eq!(
        response.headers().get("x-lpe-rpc-proxy-status"),
        Some(&HeaderValue::from_static("endpoint-ping"))
    );
    assert_eq!(
        response.headers().get("content-length"),
        Some(&HeaderValue::from_static("131072"))
    );
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.len(), 184);
    assert_eq!(u16::from_le_bytes([body[8], body[9]]), 28);
    assert_eq!(u16::from_le_bytes([body[36], body[37]]), 44);
    assert_eq!(body[72], 0x05);
    assert_eq!(body[74], 0x0c);
    assert_eq!(body[75], 0x03);
    assert_eq!(u16::from_le_bytes([body[80], body[81]]), 112);
    assert_eq!(u16::from_le_bytes([body[82], body[83]]), 48);
    assert_eq!(
        &body[108..128],
        &[
            0x04, 0x5d, 0x88, 0x8a, 0xeb, 0x1c, 0xc9, 0x11, 0x9f, 0xe8, 0x08, 0x00, 0x2b, 0x10,
            0x48, 0x60, 0x02, 0x00, 0x00, 0x00
        ]
    );
    assert_eq!(body[128], 10);
    assert_eq!(body[129], 2);
    assert_eq!(&body[136..144], b"NTLMSSP\0");
    assert_eq!(
        u32::from_le_bytes([body[144], body[145], body[146], body[147]]),
        2
    );
}

#[tokio::test]
async fn rpc_proxy_address_book_endpoint_ping_returns_rts_connect_without_synthetic_bind_ack() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let method = Method::from_bytes(b"RPC_OUT_DATA").expect("valid RPC method");
    let uri: Uri = "/rpc/rpcproxy.dll?mail.example.test:6004".parse().unwrap();
    let mut headers = bearer_headers();
    headers.insert("user-agent", HeaderValue::from_static("MSRPC"));
    headers.insert("accept", HeaderValue::from_static("application/rpc"));

    let connect_body = rpc_proxy_conn_a1_request_body(0x0000_8000);
    let response = service
        .handle_rpc_proxy(&method, &uri, &headers, &connect_body)
        .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type"),
        Some(&HeaderValue::from_static("application/rpc"))
    );
    assert_eq!(
        response.headers().get("x-lpe-rpc-proxy-status"),
        Some(&HeaderValue::from_static("endpoint-ping"))
    );
    assert_eq!(
        response.headers().get("content-length"),
        Some(&HeaderValue::from_static("131072"))
    );
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.len(), 72);
    assert_eq!(u16::from_le_bytes([body[8], body[9]]), 28);
    assert_eq!(u16::from_le_bytes([body[36], body[37]]), 44);
    assert_eq!(
        u32::from_le_bytes([body[60], body[61], body[62], body[63]]),
        0x0000_8000
    );
}

#[tokio::test]
async fn rpc_proxy_opens_authenticated_in_data_channel_without_waiting_for_body_eof() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let method = Method::from_bytes(b"RPC_IN_DATA").expect("valid RPC method");
    let uri: Uri = "/rpc/rpcproxy.dll?mail.example.test:6001".parse().unwrap();
    let mut headers = bearer_headers();
    headers.insert("user-agent", HeaderValue::from_static("MSRPC"));
    headers.insert("accept", HeaderValue::from_static("application/rpc"));

    let response = service
        .handle_rpc_proxy_in_data_channel(&method, &uri, &headers, Body::from("bind-bytes"))
        .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type"),
        Some(&HeaderValue::from_static("application/rpc"))
    );
    assert_eq!(
        response.headers().get("x-lpe-rpc-proxy-status"),
        Some(&HeaderValue::from_static("in-channel-open"))
    );
    assert_eq!(
        response.headers().get("content-length"),
        Some(&HeaderValue::from_static("0"))
    );

    let response = service
        .handle_rpc_proxy_in_data_channel(&method, &uri, &headers, Body::from("bind-bytes"))
        .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-length"),
        Some(&HeaderValue::from_static("131072"))
    );
}

#[tokio::test]
async fn rpc_proxy_opens_authenticated_address_book_in_data_channel_without_waiting_for_body_eof() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let method = Method::from_bytes(b"RPC_IN_DATA").expect("valid RPC method");
    let uri: Uri = "/rpc/rpcproxy.dll?mail.example.test:6004".parse().unwrap();
    let mut headers = bearer_headers();
    headers.insert("user-agent", HeaderValue::from_static("MSRPC"));
    headers.insert("accept", HeaderValue::from_static("application/rpc"));

    let response = service
        .handle_rpc_proxy_in_data_channel(&method, &uri, &headers, Body::from("bind-bytes"))
        .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type"),
        Some(&HeaderValue::from_static("application/rpc"))
    );
    assert_eq!(
        response.headers().get("x-lpe-rpc-proxy-status"),
        Some(&HeaderValue::from_static("in-channel-open"))
    );
    assert_eq!(
        response.headers().get("content-length"),
        Some(&HeaderValue::from_static("131072"))
    );
}

#[tokio::test]
async fn rpc_proxy_opens_authenticated_referral_in_data_channel_without_buffering_body() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let method = Method::from_bytes(b"RPC_IN_DATA").expect("valid RPC method");
    let uri: Uri = "/rpc/rpcproxy.dll?mail.example.test:6002".parse().unwrap();
    let mut headers = bearer_headers();
    headers.insert("user-agent", HeaderValue::from_static("MSRPC"));
    headers.insert("accept", HeaderValue::from_static("application/rpc"));

    let response = service
        .handle_rpc_proxy_in_data_channel(&method, &uri, &headers, Body::from("bind-bytes"))
        .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type"),
        Some(&HeaderValue::from_static("application/rpc"))
    );
    assert_eq!(
        response.headers().get("x-lpe-rpc-proxy-status"),
        Some(&HeaderValue::from_static("in-channel-open"))
    );
    assert_eq!(
        response.headers().get("content-length"),
        Some(&HeaderValue::from_static("131072"))
    );
}

#[test]
fn rpc_proxy_classifies_referral_endpoint_as_streaming_in_data_channel() {
    let method = Method::from_bytes(b"RPC_IN_DATA").expect("valid RPC method");
    let uri: Uri = "/rpc/rpcproxy.dll?mail.example.test:6002".parse().unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("user-agent", HeaderValue::from_static("MSRPC"));
    headers.insert("accept", HeaderValue::from_static("application/rpc"));

    assert!(is_rpc_proxy_in_data_channel_request(
        &method, &uri, &headers
    ));
}

#[test]
fn rpc_proxy_in_channel_endpoint_ping_request_gets_success_response() {
    let request = [
        0x05, 0x00, 0x00, 0x03, 0x10, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00,
        0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00,
    ];

    let mut buffer = request.to_vec();
    let response =
        rpc_proxy_in_channel_response_for_buffer(&mut buffer).expect("endpoint response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(u16::from_le_bytes([response[8], response[9]]), 52);
    assert_eq!(u16::from_le_bytes([response[10], response[11]]), 0);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        2
    );
    assert_eq!(
        u32::from_le_bytes([response[16], response[17], response[18], response[19]]),
        28
    );
    assert_eq!(
        u32::from_le_bytes([response[24], response[25], response[26], response[27]]),
        4
    );
    assert_eq!(
        u32::from_le_bytes([response[28], response[29], response[30], response[31]]),
        4
    );
    assert_eq!(
        u32::from_le_bytes([response[48], response[49], response[50], response[51]]),
        0
    );
}

#[test]
fn rpc_proxy_in_channel_bind_request_gets_bind_ack_response() {
    let bind = hex_bytes(
        "05000b1310000000a400280003000000\
         f80ff80f010000000200000002000100\
         e0f544153c61d11193df00c04fd7bd0901000000\
         045d888aeb1cc9119fe808002b10486002000000\
         03000100e0f544153c61d11193df00c04fd7bd0901000000\
         33057171babe37498319b5dbef9ccc3601000000\
         0a020000000000004e544c4d5353500001000000078208a2\
         000000000000000000000000000000000a007c4f0000000f",
    );
    let mut buffer = bind;

    let response =
        rpc_proxy_in_channel_response_for_buffer(&mut buffer).expect("bind ack response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x0c, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        3
    );
    assert_eq!(u16::from_le_bytes([response[8], response[9]]), 136);
    assert_eq!(u16::from_le_bytes([response[10], response[11]]), 48);
    assert_eq!(response[28], 2);
    assert_eq!(
        &response[36..56],
        &[
            0x04, 0x5d, 0x88, 0x8a, 0xeb, 0x1c, 0xc9, 0x11, 0x9f, 0xe8, 0x08, 0x00, 0x2b, 0x10,
            0x48, 0x60, 0x02, 0x00, 0x00, 0x00
        ]
    );
    assert_eq!(u16::from_le_bytes([response[56], response[57]]), 2);
    assert_eq!(response[80], 10);
    assert_eq!(response[81], 2);
    assert_eq!(&response[88..96], b"NTLMSSP\0");
}

#[test]
fn rpc_proxy_in_channel_alter_context_request_gets_alter_context_response() {
    let alter_context = hex_bytes(
        "05000e03100000007400000004000000\
         f80ff80f010000000200000002000100\
         00dbf1a447ca6710b31f00dd010662da00005100\
         045d888aeb1cc9119fe808002b10486002000000\
         0300010000dbf1a447ca6710b31f00dd010662da00005100\
         33057171babe37498319b5dbef9ccc3601000000",
    );
    let mut buffer = alter_context;

    let response =
        rpc_proxy_in_channel_response_for_buffer(&mut buffer).expect("alter context response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x0f, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        4
    );
    assert_eq!(u16::from_le_bytes([response[8], response[9]]), 136);
    assert_eq!(u16::from_le_bytes([response[10], response[11]]), 48);
    assert_eq!(response[28], 2);
    assert_eq!(u16::from_le_bytes([response[56], response[57]]), 2);
    assert_eq!(&response[88..96], b"NTLMSSP\0");
}

#[test]
fn rpc_proxy_in_channel_emsmdb_connect_ex_gets_session_context_response() {
    let mut buffer = emsmdb_rpc_request(51, 10, 160);

    let response =
        rpc_proxy_in_channel_response_for_endpoint_query("mail.example.test:6001", &mut buffer)
            .expect("emsmdb connect response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        51
    );
    assert_eq!(&response[24..28], &[0; 4]);
    assert_eq!(&response[28..44], Uuid::nil().as_bytes());
    assert_eq!(
        u32::from_le_bytes(response[44..48].try_into().unwrap()),
        60_000
    );
    assert_eq!(*response.last().unwrap(), 0);
}

#[test]
fn rpc_proxy_in_channel_emsmdb_rpc_ext2_gets_logon_carrier_response() {
    let mut buffer = emsmdb_rpc_request(52, 11, 160);

    let response =
        rpc_proxy_in_channel_response_for_endpoint_query("mail.example.test:6001", &mut buffer)
            .expect("emsmdb rpc ext2 response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        52
    );
    assert_eq!(&response[24..28], &[0; 4]);
    assert_eq!(&response[28..44], Uuid::nil().as_bytes());
    assert!(response
        .windows(8)
        .any(|window| window == [0, 0, 4, 0, 0, 0, 0, 0]));
}

#[test]
fn rpc_proxy_in_channel_emsmdb_disconnect_clears_session_context() {
    let mut buffer = emsmdb_rpc_request(53, 1, 64);

    let response =
        rpc_proxy_in_channel_response_for_endpoint_query("mail.example.test:6001", &mut buffer)
            .expect("emsmdb disconnect response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        53
    );
    assert_eq!(&response[24..44], &[0; 20]);
    assert_eq!(u32::from_le_bytes(response[44..48].try_into().unwrap()), 0);
}

#[tokio::test]
async fn rpc_proxy_emsmdb_logon_uses_authenticated_canonical_principal() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let validator = Validator::new(FakeDetector::pdf(), 0.8);
    let principal = test_account_principal();
    let mut connect = emsmdb_rpc_request(61, 10, 160);
    let connect_response = rpc_proxy_in_channel_response_for_endpoint_query_with_store(
        &store,
        &validator,
        &principal,
        "mail.example.test:6001",
        &mut connect,
    )
    .await
    .expect("connect response");
    let context = rpc_response_context(&connect_response);

    let logon_request = rpc_proxy_bootstrap_logon_execute_rop(&principal.email);
    let mut execute = emsmdb_rpc_ext2_request(62, &context, &logon_request);
    let execute_response = rpc_proxy_in_channel_response_for_endpoint_query_with_store(
        &store,
        &validator,
        &principal,
        "mail.example.test:6001",
        &mut execute,
    )
    .await
    .expect("execute response");
    let rop_response = rpc_response_rpc_header_ext(&execute_response);

    let static_marker = [b"LPEEMSMDB".as_slice(), b"CTX0001".as_slice()].concat();
    assert!(contains_bytes(
        &rop_response,
        FakeStore::account().account_id.as_bytes()
    ));
    assert!(!contains_bytes(&execute_response, &static_marker));
}

#[tokio::test]
async fn rpc_proxy_emsmdb_query_rows_reads_canonical_mailboxes() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 7;
    let archive = FakeStore::mailbox("66666666-6666-6666-6666-666666666666", "custom", "Archive");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox, archive])),
        ..Default::default()
    };
    let validator = Validator::new(FakeDetector::pdf(), 0.8);
    let principal = test_account_principal();
    let mut connect = emsmdb_rpc_request(63, 10, 160);
    let connect_response = rpc_proxy_in_channel_response_for_endpoint_query_with_store(
        &store,
        &validator,
        &principal,
        "mail.example.test:6001",
        &mut connect,
    )
    .await
    .expect("connect response");
    let context = rpc_response_context(&connect_response);

    let logon_request = rpc_proxy_bootstrap_logon_execute_rop(&principal.email);
    let mut execute = emsmdb_rpc_ext2_request(64, &context, &logon_request);
    rpc_proxy_in_channel_response_for_endpoint_query_with_store(
        &store,
        &validator,
        &principal,
        "mail.example.test:6001",
        &mut execute,
    )
    .await
    .expect("logon response");

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(1).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x04, 0x00, 0x01, 0x02, 0x04, // RopGetHierarchyTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&50u16.to_le_bytes());
    let table_request = rpc_proxy_wrapped_rop_buffer(&rops, &[1, u32::MAX, u32::MAX]);
    let mut execute = emsmdb_rpc_ext2_request(65, &context, &table_request);
    let table_response = rpc_proxy_in_channel_response_for_endpoint_query_with_store(
        &store,
        &validator,
        &principal,
        "mail.example.test:6001",
        &mut execute,
    )
    .await
    .expect("table response");
    let rop_response = rpc_response_rpc_header_ext(&table_response);

    assert!(contains_bytes(&rop_response, &utf16z("Inbox")));
    assert!(contains_bytes(&rop_response, &utf16z("Archive")));
}

#[tokio::test]
async fn rpc_proxy_emsmdb_rpc_ext2_parse_failure_returns_protocol_fault() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let validator = Validator::new(FakeDetector::pdf(), 0.8);
    let principal = test_account_principal();
    let mut execute = emsmdb_rpc_request(66, 11, 160);

    let execute_response = rpc_proxy_in_channel_response_for_endpoint_query_with_store(
        &store,
        &validator,
        &principal,
        "mail.example.test:6001",
        &mut execute,
    )
    .await
    .expect("execute fault");

    assert_eq!(execute_response[0..4], [0x05, 0x00, 0x03, 0x03]);
    assert_eq!(rpc_response_call_id(&execute_response), 66);
    assert_eq!(rpc_response_fault_status(&execute_response), 5);
    assert!(!contains_bytes(
        &execute_response,
        &[0, 0, 4, 0, 0, 0, 0, 0]
    ));
}

#[tokio::test]
async fn rpc_proxy_emsmdb_rpc_ext2_requires_authenticated_context() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let validator = Validator::new(FakeDetector::pdf(), 0.8);
    let principal = test_account_principal();
    let context = [0u8; 20];
    let logon_request = rpc_proxy_bootstrap_logon_execute_rop(&principal.email);
    let mut execute = emsmdb_rpc_ext2_request(67, &context, &logon_request);

    let execute_response = rpc_proxy_in_channel_response_for_endpoint_query_with_store(
        &store,
        &validator,
        &principal,
        "mail.example.test:6001",
        &mut execute,
    )
    .await
    .expect("execute fault");

    assert_eq!(execute_response[0..4], [0x05, 0x00, 0x03, 0x03]);
    assert_eq!(rpc_response_call_id(&execute_response), 67);
    assert_eq!(rpc_response_fault_status(&execute_response), 5);
    assert!(!contains_bytes(
        &execute_response,
        FakeStore::account().account_id.as_bytes()
    ));
}

#[test]
fn rpc_proxy_mailstore_in_channel_skips_duplicate_bind_ack() {
    let endpoint_query = "mail.example.test:6001";
    mark_rpc_proxy_out_endpoint_bind_ack(endpoint_query);
    let bind = hex_bytes(
        "05000b1310000000a400280003000000\
         f80ff80f010000000200000002000100\
         e0f544153c61d11193df00c04fd7bd0901000000\
         045d888aeb1cc9119fe808002b10486002000000\
         03000100e0f544153c61d11193df00c04fd7bd0901000000\
         33057171babe37498319b5dbef9ccc3601000000\
         0a020000000000004e544c4d5353500001000000078208a2\
         000000000000000000000000000000000a007c4f0000000f",
    );
    let mut auth3 = vec![0u8; 250];
    auth3[0..8].copy_from_slice(&[0x05, 0x00, 0x10, 0x03, 0x10, 0x00, 0x00, 0x00]);
    auth3[8..10].copy_from_slice(&250u16.to_le_bytes());
    auth3[10..12].copy_from_slice(&0x00deu16.to_le_bytes());
    auth3[12..16].copy_from_slice(&2u32.to_le_bytes());
    let management = [
        0x05, 0x00, 0x00, 0x03, 0x10, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00,
        0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00,
    ];
    let mut buffer = Vec::new();
    buffer.extend_from_slice(&bind);
    buffer.extend_from_slice(&auth3);
    buffer.extend_from_slice(&management);

    let response = rpc_proxy_in_channel_response_for_endpoint_query(endpoint_query, &mut buffer)
        .expect("management response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        2
    );
    assert_eq!(
        u32::from_le_bytes([response[24], response[25], response[26], response[27]]),
        4
    );
}

#[test]
fn rpc_proxy_address_book_in_channel_answers_actual_bind_before_management_probe() {
    let endpoint_query = "mail.address-book-bind.example.test:6004";
    let bind = hex_bytes(
        "05000b0310000000d000280030000000\
         f80ff80f000000000300000000000100\
         80bda8af8a7dc911bef408002b10298901000000\
         045d888aeb1cc9119fe808002b10486002000000\
         0100010080bda8af8a7dc911bef408002b10298901000000\
         33057171babe37498319b5dbef9ccc3601000000\
         0200010080bda8af8a7dc911bef408002b10298901000000\
         2c1cb76c12984045030000000000000001000000\
         0a020000000000004e544c4d5353500001000000078208a2\
         000000000000000000000000000000000a007c4f0000000f",
    );
    let mut auth3 = vec![0u8; 250];
    auth3[0..8].copy_from_slice(&[0x05, 0x00, 0x10, 0x03, 0x10, 0x00, 0x00, 0x00]);
    auth3[8..10].copy_from_slice(&250u16.to_le_bytes());
    auth3[10..12].copy_from_slice(&0x00deu16.to_le_bytes());
    auth3[12..16].copy_from_slice(&48u32.to_le_bytes());
    let management = [
        0x05, 0x00, 0x00, 0x03, 0x10, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00, 0x30, 0x00, 0x00,
        0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00,
    ];
    let mut buffer = Vec::new();
    buffer.extend_from_slice(&bind);
    buffer.extend_from_slice(&auth3);
    buffer.extend_from_slice(&management);

    let bind_response =
        rpc_proxy_in_channel_response_for_endpoint_query(endpoint_query, &mut buffer)
            .expect("bind response");

    assert_eq!(bind_response[0..4], [0x05, 0x00, 0x0c, 0x03]);
    assert_eq!(
        u32::from_le_bytes([
            bind_response[12],
            bind_response[13],
            bind_response[14],
            bind_response[15]
        ]),
        48
    );

    let response = rpc_proxy_in_channel_response_for_endpoint_query(endpoint_query, &mut buffer)
        .expect("management response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        48
    );
    assert_eq!(
        u32::from_le_bytes([response[24], response[25], response[26], response[27]]),
        4
    );
}

#[test]
fn rpc_proxy_in_channel_nspi_bind_request_gets_context_handle_response() {
    let request = [
        0x05, 0x00, 0x00, 0x03, 0x10, 0x00, 0x00, 0x00, 0x60, 0x00, 0x10, 0x00, 0x03, 0x00, 0x00,
        0x00, 0x2c, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0xe4, 0x04, 0x00, 0x00, 0x09, 0x04, 0x00, 0x00, 0x09, 0x04, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a, 0x02, 0x04, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];
    let mut buffer = request.to_vec();

    let response =
        rpc_proxy_in_channel_response_for_buffer(&mut buffer).expect("nspi bind response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(u16::from_le_bytes([response[8], response[9]]), 52);
    assert_eq!(u16::from_le_bytes([response[10], response[11]]), 0);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        3
    );
    assert_eq!(
        u32::from_le_bytes([response[16], response[17], response[18], response[19]]),
        28
    );
    assert_eq!(
        u32::from_le_bytes([response[24], response[25], response[26], response[27]]),
        0
    );
    assert_eq!(
        u32::from_le_bytes([response[28], response[29], response[30], response[31]]),
        0
    );
    assert_eq!(&response[32..40], b"LPE\0NSPI");
    assert_eq!(
        u32::from_le_bytes([response[48], response[49], response[50], response[51]]),
        0
    );
}

#[test]
fn rpc_proxy_in_channel_nspi_update_stat_request_gets_success_response() {
    let request = [
        0x05, 0x00, 0x00, 0x03, 0x10, 0x00, 0x00, 0x00, 0x60, 0x00, 0x10, 0x00, 0x03, 0x00, 0x00,
        0x00, 0x2c, 0x00, 0x00, 0x00, 0x02, 0x00, 0x02, 0x00,
    ];
    let mut buffer = request.to_vec();
    buffer.resize(96, 0);

    let response =
        rpc_proxy_in_channel_response_for_buffer(&mut buffer).expect("nspi update stat response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(
        u16::from_le_bytes([response[8], response[9]]) as usize,
        response.len()
    );
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        3
    );
    assert_eq!(
        u32::from_le_bytes([response[16], response[17], response[18], response[19]]),
        44
    );
    assert_eq!(
        u32::from_le_bytes([response[48], response[49], response[50], response[51]]),
        0x04e4
    );
    assert_eq!(
        u32::from_le_bytes([response[56], response[57], response[58], response[59]]),
        0x0409
    );
}

#[test]
fn rpc_proxy_in_channel_nspi_resolve_names_w_request_gets_response() {
    let request = [
        0x05, 0x00, 0x00, 0x03, 0x10, 0x00, 0x00, 0x00, 0xd0, 0x00, 0x10, 0x00, 0x04, 0x00, 0x00,
        0x00, 0x98, 0x00, 0x00, 0x00, 0x02, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x4c, 0x50,
        0x45, 0x00, 0x4e, 0x53, 0x50, 0x49, 0x43, 0x54, 0x58, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
        0x00, 0x00, 0x00,
    ];
    let mut buffer = request.to_vec();
    buffer.resize(208, 0);
    buffer[72..76].copy_from_slice(&0x3003_001eu32.to_le_bytes());
    buffer[76..80].copy_from_slice(&0x3001_001eu32.to_le_bytes());
    let requested_name: Vec<u8> = "=SMTP:fabien@l-p-e.ch\0"
        .encode_utf16()
        .flat_map(|unit| unit.to_le_bytes())
        .collect();
    buffer[112..112 + requested_name.len()].copy_from_slice(&requested_name);

    let response =
        rpc_proxy_in_channel_response_for_buffer(&mut buffer).expect("resolve names response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(
        u16::from_le_bytes([response[8], response[9]]) as usize,
        response.len()
    );
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        4
    );
    assert_eq!(
        u32::from_le_bytes([response[16], response[17], response[18], response[19]]),
        (response.len() - 24) as u32
    );
    assert_eq!(
        u32::from_le_bytes([response[28], response[29], response[30], response[31]]),
        2
    );
    assert_eq!(
        u32::from_le_bytes([response[32], response[33], response[34], response[35]]),
        1
    );
    assert!(response
        .windows(b"fabien@l-p-e.ch".len())
        .any(|window| window == b"fabien@l-p-e.ch"));
    assert!(response
        .windows(b"Fabien".len())
        .any(|window| window == b"Fabien"));
    assert!(response.windows(12).any(|window| {
        window[0..4] == 0x3003_001eu32.to_le_bytes()
            && window[4..8] == 0u32.to_le_bytes()
            && window[8..12] == 0x001eu32.to_le_bytes()
    }));
    assert!(response.windows(12).any(|window| {
        window[0..4] == 0x3001_001eu32.to_le_bytes()
            && window[4..8] == 0u32.to_le_bytes()
            && window[8..12] == 0x001eu32.to_le_bytes()
    }));
    let return_offset = response.len() - 4;
    assert_eq!(
        u32::from_le_bytes([
            response[return_offset],
            response[return_offset + 1],
            response[return_offset + 2],
            response[return_offset + 3]
        ]),
        0
    );
}

#[test]
fn rpc_proxy_in_channel_scans_nspi_resolve_after_rts_pdu() {
    let chunk = hex_bytes(
        "05001403100000001c00000000000000020001000500000030750000\
         0500000310000000d000100009000000980000000200140000000000\
         4c5045004e535049435458000000000100000000000000000000000000000000000000000000000000000000\
         e404000009040000090400000000020003000000020000000000000002000000\
         1e0003301e000130010000000100000004000200140000000000000014000000\
         3d0053004d00540050003a00740065007300740040006c002d0070002d0065002e00630068000000\
         00000000000000000a0208000000000001000000000000000000000000000000",
    );
    let mut buffer = chunk;

    let response =
        rpc_proxy_in_channel_response_for_buffer(&mut buffer).expect("resolve names response");

    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        9
    );
    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
}

#[test]
fn rpc_proxy_in_channel_nspi_unbind_request_gets_success_response() {
    let request = [
        0x05, 0x00, 0x00, 0x03, 0x10, 0x00, 0x00, 0x00, 0x50, 0x00, 0x10, 0x00, 0x05, 0x00, 0x00,
        0x00, 0x18, 0x00, 0x00, 0x00, 0x02, 0x00, 0x01, 0x00,
    ];
    let mut buffer = request.to_vec();
    buffer.resize(80, 0);

    let response =
        rpc_proxy_in_channel_response_for_buffer(&mut buffer).expect("nspi unbind response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        5
    );
    assert_eq!(
        u32::from_le_bytes([response[16], response[17], response[18], response[19]]),
        24
    );
    assert_eq!(
        u32::from_le_bytes([response[44], response[45], response[46], response[47]]),
        0
    );
}

#[test]
fn rpc_proxy_in_channel_nspi_bootstrap_opnums_get_success_responses() {
    for (opnum, call_id) in [
        (3u16, 11u32),
        (4, 12),
        (5, 13),
        (6, 14),
        (7, 15),
        (8, 16),
        (9, 17),
        (10, 18),
        (12, 19),
        (13, 20),
        (16, 21),
        (17, 22),
        (18, 23),
        (19, 24),
    ] {
        let mut buffer = nspi_rpc_request(call_id, opnum, 96);

        let response = rpc_proxy_in_channel_response_for_buffer(&mut buffer)
            .unwrap_or_else(|| panic!("nspi opnum {opnum} response"));

        assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03], "opnum {opnum}");
        assert_eq!(
            u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
            call_id,
            "opnum {opnum}"
        );
        assert_eq!(
            u32::from_le_bytes([
                response[response.len() - 4],
                response[response.len() - 3],
                response[response.len() - 2],
                response[response.len() - 1]
            ]),
            0,
            "opnum {opnum}"
        );
    }
}

#[test]
fn rpc_proxy_in_channel_nspi_get_names_from_ids_gets_name_set_response() {
    let mut buffer = nspi_rpc_request(26, 17, 96);
    buffer[52..56].copy_from_slice(&0x3001_001fu32.to_le_bytes());
    buffer[56..60].copy_from_slice(&0x3003_001fu32.to_le_bytes());

    let response =
        rpc_proxy_in_channel_response_for_buffer(&mut buffer).expect("get names from ids response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        26
    );
    assert_eq!(
        u32::from_le_bytes([response[24], response[25], response[26], response[27]]),
        0
    );
    assert_ne!(
        u32::from_le_bytes([response[28], response[29], response[30], response[31]]),
        0
    );
    assert_eq!(
        u32::from_le_bytes([response[32], response[33], response[34], response[35]]),
        2
    );
    assert_eq!(
        u32::from_le_bytes([response[36], response[37], response[38], response[39]]),
        2
    );
    assert_eq!(
        u32::from_le_bytes([response[48], response[49], response[50], response[51]]),
        0x3001_001f
    );
    assert_eq!(
        u32::from_le_bytes([response[60], response[61], response[62], response[63]]),
        0x3003_001f
    );
    assert_eq!(
        u32::from_le_bytes([
            response[response.len() - 4],
            response[response.len() - 3],
            response[response.len() - 2],
            response[response.len() - 1]
        ]),
        0
    );
}

#[test]
fn rpc_proxy_in_channel_nspi_resolve_names_ascii_request_gets_response() {
    let mut buffer = nspi_rpc_request(27, 19, 160);
    buffer[72..76].copy_from_slice(&0x3003_001eu32.to_le_bytes());
    buffer[76..80].copy_from_slice(&0x3001_001eu32.to_le_bytes());
    buffer[96..117].copy_from_slice(b"=SMTP:alias@l-p-e.ch\0");

    let response =
        rpc_proxy_in_channel_response_for_buffer(&mut buffer).expect("resolve names response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        27
    );
    assert!(response
        .windows(b"alias@l-p-e.ch".len())
        .any(|window| window == b"alias@l-p-e.ch"));
    assert!(response
        .windows(b"Alias".len())
        .any(|window| window == b"Alias"));
}

#[test]
fn rpc_proxy_in_channel_referral_opnums_get_server_name_responses() {
    for (opnum, call_id) in [(0u16, 31u32), (1, 32)] {
        let mut buffer = rfri_rpc_request(call_id, opnum, 96);

        let response =
            rpc_proxy_in_channel_response_for_endpoint_query("mail.example.test:6002", &mut buffer)
                .unwrap_or_else(|| panic!("rfri opnum {opnum} response"));

        assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03], "opnum {opnum}");
        assert_eq!(
            u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
            call_id,
            "opnum {opnum}"
        );
        if opnum == 0 {
            assert_eq!(
                u32::from_le_bytes([response[24], response[25], response[26], response[27]]),
                0,
                "RfrGetNewDSA ppszUnused"
            );
            assert_ne!(
                u32::from_le_bytes([response[28], response[29], response[30], response[31]]),
                0,
                "RfrGetNewDSA ppszServer outer pointer"
            );
            assert_ne!(
                u32::from_le_bytes([response[32], response[33], response[34], response[35]]),
                0,
                "RfrGetNewDSA ppszServer string pointer"
            );
        } else {
            assert_ne!(
                u32::from_le_bytes([response[24], response[25], response[26], response[27]]),
                0,
                "RfrGetFQDNFromServerDN ppszServerFQDN string pointer"
            );
        }
        assert!(response
            .windows(b"mail.example.test".len())
            .any(|window| window == b"mail.example.test"));
        assert_eq!(
            u32::from_le_bytes([
                response[response.len() - 4],
                response[response.len() - 3],
                response[response.len() - 2],
                response[response.len() - 1]
            ]),
            0,
            "opnum {opnum}"
        );
    }
}

fn nspi_rpc_request(call_id: u32, opnum: u16, fragment_length: usize) -> Vec<u8> {
    rpc_request(call_id, 2, opnum, fragment_length)
}

fn rfri_rpc_request(call_id: u32, opnum: u16, fragment_length: usize) -> Vec<u8> {
    rpc_request(call_id, 0, opnum, fragment_length)
}

fn emsmdb_rpc_request(call_id: u32, opnum: u16, fragment_length: usize) -> Vec<u8> {
    rpc_request(call_id, 3, opnum, fragment_length)
}

fn emsmdb_rpc_ext2_request(call_id: u32, context: &[u8], rop_buffer: &[u8]) -> Vec<u8> {
    let mut stub = Vec::new();
    stub.extend_from_slice(context);
    stub.extend_from_slice(&(rop_buffer.len() as u32).to_le_bytes());
    stub.extend_from_slice(&0u32.to_le_bytes());
    stub.extend_from_slice(&(rop_buffer.len() as u32).to_le_bytes());
    stub.extend_from_slice(rop_buffer);
    while stub.len() % 4 != 0 {
        stub.push(0);
    }
    let fragment_length = 24 + stub.len();
    let mut request = rpc_request(call_id, 3, 11, fragment_length);
    request[16..20].copy_from_slice(&(stub.len() as u32).to_le_bytes());
    request[24..].copy_from_slice(&stub);
    request
}

fn rpc_request(call_id: u32, context_id: u16, opnum: u16, fragment_length: usize) -> Vec<u8> {
    let mut request = vec![0u8; fragment_length];
    request[0..8].copy_from_slice(&[0x05, 0x00, 0x00, 0x03, 0x10, 0x00, 0x00, 0x00]);
    request[8..10].copy_from_slice(&(fragment_length as u16).to_le_bytes());
    request[10..12].copy_from_slice(&0x0010u16.to_le_bytes());
    request[12..16].copy_from_slice(&call_id.to_le_bytes());
    request[16..20].copy_from_slice(&(fragment_length as u32 - 24).to_le_bytes());
    request[20..22].copy_from_slice(&context_id.to_le_bytes());
    request[22..24].copy_from_slice(&opnum.to_le_bytes());
    request
}

fn rpc_response_context(response: &[u8]) -> [u8; 20] {
    response[24..44].try_into().unwrap()
}

fn rpc_response_call_id(response: &[u8]) -> u32 {
    u32::from_le_bytes(response[12..16].try_into().unwrap())
}

fn rpc_response_fault_status(response: &[u8]) -> u32 {
    u32::from_le_bytes(response[24..28].try_into().unwrap())
}

fn rpc_response_rpc_header_ext(response: &[u8]) -> Vec<u8> {
    let offset = response
        .windows(4)
        .position(|window| window == [0, 0, 4, 0])
        .expect("RPC_HEADER_EXT response");
    let size = u16::from_le_bytes(response[offset + 4..offset + 6].try_into().unwrap()) as usize;
    response[offset..offset + 8 + size].to_vec()
}

fn hex_bytes(input: &str) -> Vec<u8> {
    let compact: String = input.chars().filter(|ch| !ch.is_whitespace()).collect();
    assert_eq!(compact.len() % 2, 0);
    compact
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_nibble(pair[0]);
            let low = hex_nibble(pair[1]);
            (high << 4) | low
        })
        .collect()
}

fn hex_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        b'A'..=b'F' => byte - b'A' + 10,
        _ => panic!("invalid hex byte"),
    }
}

#[test]
fn rpc_proxy_in_channel_scans_endpoint_ping_after_auth_fragment() {
    let auth = [
        0x05, 0x00, 0x10, 0x03, 0x10, 0x00, 0x00, 0x00, 0xfa, 0x00, 0xde, 0x00, 0x02, 0x00, 0x00,
        0x00,
    ];
    let request = [
        0x05, 0x00, 0x00, 0x03, 0x10, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00,
        0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00,
    ];
    let mut chunk = Vec::new();
    chunk.extend_from_slice(&auth);
    chunk.extend_from_slice(&[0u8; 234]);
    chunk.extend_from_slice(&request);

    let mut buffer = chunk;
    let response =
        rpc_proxy_in_channel_response_for_buffer(&mut buffer).expect("endpoint response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        3
    );
}

#[test]
fn rpc_proxy_in_channel_buffers_split_endpoint_ping_request() {
    let request = [
        0x05, 0x00, 0x00, 0x03, 0x10, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00,
        0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00,
    ];
    let mut buffer = request[..18].to_vec();

    assert!(rpc_proxy_in_channel_response_for_buffer(&mut buffer).is_none());
    assert_eq!(buffer, request[..18]);

    buffer.extend_from_slice(&request[18..]);
    let response =
        rpc_proxy_in_channel_response_for_buffer(&mut buffer).expect("endpoint response");

    assert!(buffer.is_empty());
    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        4
    );
}

#[tokio::test]
async fn rpc_proxy_accepts_authenticated_rca_probe_without_405() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let uri: Uri = "/rpc/rpcproxy.dll".parse().unwrap();

    let response = service
        .handle_rpc_proxy(&Method::GET, &uri, &bearer_headers(), b"")
        .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("x-lpe-rpc-proxy-status"),
        Some(&HeaderValue::from_static("auth-accepted"))
    );
    let body = response_text(response).await;
    assert!(body.contains("Use MAPI over HTTP for mailbox access"));
}

#[tokio::test]
async fn find_folder_lists_contact_and_calendar_folders() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "calendar", "Calendar",
        )])),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "44444444-4444-4444-4444-444444444444",
            "custom",
            "RCA Sync",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:FindFolder /></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<t:ServerVersionInfo"));
    assert!(body.contains("<m:FindFolderResponse>"));
    assert!(body.contains("<t:FolderClass>IPF.Contacts</t:FolderClass>"));
    assert!(body.contains("<t:FolderClass>IPF.Calendar</t:FolderClass>"));
    assert!(body.contains("<t:ContactsFolder>"));
    assert!(body.contains("<t:CalendarFolder>"));
    assert!(body.contains("<t:FolderId Id=\"mailbox:44444444-4444-4444-4444-444444444444\" ChangeKey=\"ck-44444444-4444-4444-4444-444444444444\"/>"));
    assert!(
        body.find("<t:FolderId Id=\"mailbox:44444444-4444-4444-4444-444444444444\"")
            .unwrap()
            < body.find("<t:ContactsFolder>").unwrap()
    );
    assert!(body.contains("<t:ParentFolderId Id=\"msgfolderroot\" ChangeKey=\"root\"/>"));
    assert!(body.contains("<t:DisplayName>RCA Sync</t:DisplayName>"));
    assert!(body.contains("<t:TotalCount>0</t:TotalCount>"));
    assert!(body.contains("<t:ChildFolderCount>0</t:ChildFolderCount>"));
    assert!(body.contains("<t:EffectiveRights>"));
    assert!(body.contains("<t:UnreadCount>0</t:UnreadCount>"));
}

#[tokio::test]
async fn sync_folder_hierarchy_lists_contact_and_calendar_folders() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "calendar", "Calendar",
        )])),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "44444444-4444-4444-4444-444444444444",
            "custom",
            "RCA Sync",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderHierarchy /></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:SyncFolderHierarchyResponse>"));
    assert!(body.contains("<m:IncludesLastFolderInRange>true</m:IncludesLastFolderInRange>"));
    assert!(body.contains("<t:Create><t:ContactsFolder>"));
    assert!(body.contains("<t:Create><t:CalendarFolder>"));
    assert!(body.contains("<t:Create><t:Folder>"));
    assert!(body.contains("<t:FolderClass>IPF.Contacts</t:FolderClass>"));
    assert!(body.contains("<t:FolderClass>IPF.Calendar</t:FolderClass>"));
    assert!(body.contains("<t:FolderId Id=\"mailbox:44444444-4444-4444-4444-444444444444\" ChangeKey=\"ck-44444444-4444-4444-4444-444444444444\"/>"));
    assert!(
        body.find("<t:Create><t:Folder>").unwrap()
            < body.find("<t:Create><t:ContactsFolder>").unwrap()
    );
    assert!(body.contains("<t:ParentFolderId Id=\"msgfolderroot\" ChangeKey=\"root\"/>"));
    assert!(body.contains("<t:DisplayName>RCA Sync</t:DisplayName>"));
    assert!(body.contains("<t:UnreadCount>0</t:UnreadCount>"));
}

#[tokio::test]
async fn get_folder_returns_msgfolderroot() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetFolder><m:FolderIds><t:DistinguishedFolderId Id="msgfolderroot"/></m:FolderIds></m:GetFolder></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:GetFolderResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:FolderId Id=\"msgfolderroot\" ChangeKey=\"root\"/>"));
    assert!(body.contains("<t:DisplayName>Root</t:DisplayName>"));
    assert!(body.contains("<t:TotalCount>0</t:TotalCount>"));
    assert!(body.contains("<t:ChildFolderCount>0</t:ChildFolderCount>"));
    assert!(body.contains("<t:EffectiveRights>"));
    assert!(body.contains("<t:UnreadCount>0</t:UnreadCount>"));
}

#[tokio::test]
async fn get_folder_root_reports_child_folders_for_client_bootstrap() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "calendar", "Calendar",
        )])),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetFolder><m:FolderIds><t:DistinguishedFolderId Id="msgfolderroot"/></m:FolderIds></m:GetFolder></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:GetFolderResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:FolderId Id=\"msgfolderroot\" ChangeKey=\"root\"/>"));
    assert!(body.contains("<t:ChildFolderCount>3</t:ChildFolderCount>"));
}

#[tokio::test]
async fn get_folder_returns_multiple_supported_folder_kinds() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "calendar", "Calendar",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetFolder><m:FolderIds><t:DistinguishedFolderId Id="contacts"/><t:DistinguishedFolderId Id="calendar"/></m:FolderIds></m:GetFolder></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:FolderClass>IPF.Contacts</t:FolderClass>"));
    assert!(body.contains("<t:FolderClass>IPF.Calendar</t:FolderClass>"));
}

#[tokio::test]
async fn create_folder_uses_canonical_mailbox_store() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let created_mailboxes = store.created_mailboxes.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:CreateFolder>
                  <m:ParentFolderId><t:DistinguishedFolderId Id="msgfolderroot"/></m:ParentFolderId>
                  <m:Folders><t:Folder><t:DisplayName>RCA Sync</t:DisplayName></t:Folder></m:Folders>
                </m:CreateFolder>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:CreateFolderResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:FolderId Id=\"mailbox:44444444-4444-4444-4444-444444444444\" ChangeKey=\"ck-44444444-4444-4444-4444-444444444444\"/>"));
    assert!(body.contains("<t:TotalCount>0</t:TotalCount>"));
    assert_eq!(created_mailboxes.lock().unwrap()[0].name, "RCA Sync");
}

#[tokio::test]
async fn delete_folder_uses_canonical_mailbox_destroy() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let destroyed_mailboxes = store.destroyed_mailboxes.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:DeleteFolder DeleteType="HardDelete">
                  <m:FolderIds><t:FolderId Id="mailbox:44444444-4444-4444-4444-444444444444"/></m:FolderIds>
                </m:DeleteFolder>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:DeleteFolderResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert_eq!(
        destroyed_mailboxes.lock().unwrap().as_slice(),
        &[Uuid::parse_str("44444444-4444-4444-4444-444444444444").unwrap()]
    );
}

#[tokio::test]
async fn get_folder_returns_ews_error_for_unsupported_folder_ids() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetFolder><m:FolderIds><t:DistinguishedFolderId Id="inbox"/></m:FolderIds></m:GetFolder></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:GetFolderResponseMessage ResponseClass=\"Error\">"));
    assert!(body.contains("<m:ResponseCode>ErrorFolderNotFound</m:ResponseCode>"));
    assert!(body.contains("<t:ServerVersionInfo"));
}

#[tokio::test]
async fn get_folder_returns_system_mailbox_by_distinguished_id() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetFolder><m:FolderIds><t:DistinguishedFolderId Id="inbox"/></m:FolderIds></m:GetFolder></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:GetFolderResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:FolderId Id=\"mailbox:55555555-5555-5555-5555-555555555555\" ChangeKey=\"ck-55555555-5555-5555-5555-555555555555\"/>"));
    assert!(body.contains("<t:DisplayName>Inbox</t:DisplayName>"));
}

#[tokio::test]
async fn get_server_time_zones_returns_minimal_definitions() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetServerTimeZones ReturnFullTimeZoneData="false"/></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:GetServerTimeZonesResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:TimeZoneDefinition Id=\"UTC\""));
    assert!(body.contains("<t:TimeZoneDefinition Id=\"W. Europe Standard Time\""));
}

#[tokio::test]
async fn resolve_names_returns_authenticated_mailbox_match() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:ResolveNames><m:UnresolvedEntry>alice</m:UnresolvedEntry></m:ResolveNames></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:ResolveNamesResponseMessage ResponseClass=\"Success\">"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<m:ResolutionSet TotalItemsInView=\"1\""));
    assert!(body.contains("<t:Name>Alice</t:Name>"));
    assert!(body.contains("<t:EmailAddress>alice@example.test</t:EmailAddress>"));
    assert!(body.contains("<t:MailboxType>Mailbox</t:MailboxType>"));
    assert!(body.contains("<t:ServerVersionInfo"));
}

#[tokio::test]
async fn resolve_names_returns_tenant_directory_account_match() {
    let mut bob = FakeStore::account();
    bob.account_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    bob.email = "bob@example.test".to_string();
    bob.display_name = "Bob Tenant".to_string();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        directory_accounts: Arc::new(Mutex::new(vec![bob])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:ResolveNames><m:UnresolvedEntry>bob@example.test</m:UnresolvedEntry></m:ResolveNames></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:ResolveNamesResponseMessage ResponseClass=\"Success\">"));
    assert!(body.contains("<t:Name>Bob Tenant</t:Name>"));
    assert!(body.contains("<t:EmailAddress>bob@example.test</t:EmailAddress>"));
    assert!(!body.contains("mallory@other.test"));
}

#[tokio::test]
async fn resolve_names_returns_accessible_contact_match() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        contacts: Arc::new(Mutex::new(vec![FakeStore::contact(
            "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "Bob Contact",
            "bob@example.test",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:ResolveNames><m:UnresolvedEntry>Bob Contact</m:UnresolvedEntry></m:ResolveNames></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:ResolveNamesResponseMessage ResponseClass=\"Success\">"));
    assert!(body.contains("<t:Name>Bob Contact</t:Name>"));
    assert!(body.contains("<t:EmailAddress>bob@example.test</t:EmailAddress>"));
    assert!(body.contains("<t:MailboxType>Contact</t:MailboxType>"));
}

#[tokio::test]
async fn resolve_names_hidden_authenticated_account_can_resolve_self() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        omit_principal_from_directory: true,
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:ResolveNames><m:UnresolvedEntry>alice@example.test</m:UnresolvedEntry></m:ResolveNames></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:ResolveNamesResponseMessage ResponseClass=\"Success\">"));
    assert!(body.contains("<t:EmailAddress>alice@example.test</t:EmailAddress>"));
}

#[tokio::test]
async fn resolve_names_returns_no_results_for_non_directory_names() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:ResolveNames><m:UnresolvedEntry>bob</m:UnresolvedEntry></m:ResolveNames></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:ResolveNamesResponseMessage ResponseClass=\"Error\">"));
    assert!(body.contains("<m:ResponseCode>ErrorNameResolutionNoResults</m:ResponseCode>"));
    assert!(!body.contains("bob@example.test"));
}

#[tokio::test]
async fn get_user_availability_returns_canonical_busy_events() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        events: Arc::new(Mutex::new(vec![
            AccessibleEvent {
                id: Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap(),
                collection_id: "default".to_string(),
                owner_account_id: FakeStore::account().account_id,
                owner_email: "alice@example.test".to_string(),
                owner_display_name: "Alice".to_string(),
                rights: FakeStore::rights(),
                date: "2026-05-04".to_string(),
                time: "09:30".to_string(),
                time_zone: "UTC".to_string(),
                duration_minutes: 45,
                recurrence_rule: String::new(),
                title: "Planning".to_string(),
                location: "Room 1".to_string(),
                attendees: String::new(),
                attendees_json: String::new(),
                notes: "Agenda".to_string(),
            },
            AccessibleEvent {
                id: Uuid::parse_str("ffffffff-ffff-ffff-ffff-ffffffffffff").unwrap(),
                collection_id: "default".to_string(),
                owner_account_id: FakeStore::account().account_id,
                owner_email: "alice@example.test".to_string(),
                owner_display_name: "Alice".to_string(),
                rights: FakeStore::rights(),
                date: "2026-05-07".to_string(),
                time: "09:30".to_string(),
                time_zone: "UTC".to_string(),
                duration_minutes: 45,
                recurrence_rule: String::new(),
                title: "Outside window".to_string(),
                location: String::new(),
                attendees: String::new(),
                attendees_json: String::new(),
                notes: String::new(),
            },
        ])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:GetUserAvailabilityRequest>
                  <m:MailboxDataArray>
                    <t:MailboxData>
                      <t:Email><t:Address>alice@example.test</t:Address></t:Email>
                    </t:MailboxData>
                  </m:MailboxDataArray>
                  <t:FreeBusyViewOptions>
                    <t:TimeWindow>
                      <t:StartTime>2026-05-04T00:00:00Z</t:StartTime>
                      <t:EndTime>2026-05-05T00:00:00Z</t:EndTime>
                    </t:TimeWindow>
                  </t:FreeBusyViewOptions>
                </m:GetUserAvailabilityRequest>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:GetUserAvailabilityResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<m:FreeBusyView>"));
    assert!(body.contains("</m:FreeBusyView>"));
    assert!(!body.contains("<t:FreeBusyView>"));
    assert!(body.contains("<t:FreeBusyViewType>Detailed</t:FreeBusyViewType>"));
    assert!(body.contains("<t:CalendarEventArray><t:CalendarEvent>"));
    assert!(body.contains("<t:StartTime>2026-05-04T09:30:00Z</t:StartTime>"));
    assert!(body.contains("<t:EndTime>2026-05-04T10:15:00Z</t:EndTime>"));
    assert!(!body.contains("2026-05-07T09:30:00Z"));
    assert!(body.contains("<t:ServerVersionInfo"));
}

#[tokio::test]
async fn get_user_availability_returns_suggestions_when_requested() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:GetUserAvailabilityRequest>
                  <m:MailboxDataArray>
                    <t:MailboxData>
                      <t:Email><t:Address>alice@example.test</t:Address></t:Email>
                    </t:MailboxData>
                  </m:MailboxDataArray>
                  <t:FreeBusyViewOptions>
                    <t:TimeWindow>
                      <t:StartTime>2026-05-15T00:00:00</t:StartTime>
                      <t:EndTime>2026-05-17T00:00:00</t:EndTime>
                    </t:TimeWindow>
                    <t:RequestedView>Detailed</t:RequestedView>
                  </t:FreeBusyViewOptions>
                  <t:SuggestionsViewOptions>
                    <t:MeetingDurationInMinutes>60</t:MeetingDurationInMinutes>
                    <t:DetailedSuggestionsWindow>
                      <t:StartTime>2026-05-15T00:00:00</t:StartTime>
                      <t:EndTime>2026-05-17T00:00:00</t:EndTime>
                    </t:DetailedSuggestionsWindow>
                  </t:SuggestionsViewOptions>
                </m:GetUserAvailabilityRequest>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:GetUserAvailabilityResponse>"));
    assert!(body.contains("<m:FreeBusyResponseArray>"));
    assert!(body.contains("<m:FreeBusyView>"));
    assert!(body.contains("<m:SuggestionsResponse>"));
    assert!(body.contains("<m:SuggestionDayResultArray>"));
    assert!(body.contains("<t:SuggestionDayResult>"));
    assert!(body.contains("<t:Date>2026-05-15T00:00:00Z</t:Date>"));
    assert!(body.contains("<t:SuggestionArray></t:SuggestionArray>"));
}

#[tokio::test]
async fn write_operations_return_ews_unsupported_errors() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    for operation in ["UpdateFolder"] {
        let request = format!("<s:Envelope><s:Body><m:{operation} /></s:Body></s:Envelope>");
        let response = service
            .handle(&bearer_headers(), request.as_bytes())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_text(response).await;
        assert!(body.contains(&format!("<m:{operation}Response>")));
        assert!(body.contains("ResponseClass=\"Error\""));
        assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
        assert!(body.contains("<t:ServerVersionInfo"));
    }
}

#[tokio::test]
async fn update_item_rejects_unsupported_item_ids() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:UpdateItem><m:ItemChanges><t:ItemChange><t:ItemId Id="message:dddddddd-dddd-dddd-dddd-dddddddddddd"/><t:Updates/></t:ItemChange></m:ItemChanges></m:UpdateItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:UpdateItemResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
}

#[tokio::test]
async fn update_item_updates_message_read_and_flag_state() {
    let mut email = FakeStore::email(
        "dddddddd-dddd-dddd-dddd-dddddddddddd",
        "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee",
        "inbox",
        "Mailbox message",
    );
    email.unread = true;
    email.flagged = false;
    let emails = Arc::new(Mutex::new(vec![email]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        emails: emails.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:UpdateItem>
                  <m:ItemChanges>
                    <t:ItemChange>
                      <t:ItemId Id="message:dddddddd-dddd-dddd-dddd-dddddddddddd"/>
                      <t:Updates>
                        <t:SetItemField>
                          <t:FieldURI FieldURI="message:IsRead"/>
                          <t:Message><t:IsRead>true</t:IsRead></t:Message>
                        </t:SetItemField>
                        <t:SetItemField>
                          <t:FieldURI FieldURI="message:Flag"/>
                          <t:Message><t:Flag><t:FlagStatus>Flagged</t:FlagStatus></t:Flag></t:Message>
                        </t:SetItemField>
                      </t:Updates>
                    </t:ItemChange>
                  </m:ItemChanges>
                </m:UpdateItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:UpdateItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:IsRead>true</t:IsRead>"));
    let updated = emails.lock().unwrap()[0].clone();
    assert!(!updated.unread);
    assert!(updated.flagged);
}

#[tokio::test]
async fn delete_item_hard_deletes_canonical_message() {
    let message_id = Uuid::parse_str("dddddddd-dddd-dddd-dddd-dddddddddddd").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            "dddddddd-dddd-dddd-dddd-dddddddddddd",
            "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee",
            "drafts",
            "Draft from EWS",
        )])),
        ..Default::default()
    };
    let deleted_emails = store.deleted_emails.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:DeleteItem DeleteType="HardDelete">
                  <m:ItemIds><t:ItemId Id="message:dddddddd-dddd-dddd-dddd-dddddddddddd"/></m:ItemIds>
                </m:DeleteItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:DeleteItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert_eq!(deleted_emails.lock().unwrap().as_slice(), &[message_id]);
}

#[tokio::test]
async fn delete_item_deletes_canonical_task() {
    let task_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        tasks: Arc::new(Mutex::new(vec![FakeStore::task(
            "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "aaaaaaaa-0000-0000-0000-000000000001",
            "Review task",
        )])),
        ..Default::default()
    };
    let deleted_tasks = store.deleted_tasks.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:DeleteItem><m:ItemIds><t:ItemId Id="task:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"/></m:ItemIds></m:DeleteItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:DeleteItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert_eq!(deleted_tasks.lock().unwrap().as_slice(), &[task_id]);
}

#[tokio::test]
async fn create_update_task_round_trips_through_sync_folder_items() {
    let task_list_id = Uuid::parse_str("aaaaaaaa-0000-0000-0000-000000000001").unwrap();
    let collection =
        FakeStore::collection("aaaaaaaa-0000-0000-0000-000000000001", "tasks", "Tasks");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        task_collections: Arc::new(Mutex::new(vec![collection])),
        ..Default::default()
    };
    let tasks = store.tasks.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:CreateItem>
                  <m:SavedItemFolderId><t:FolderId Id="aaaaaaaa-0000-0000-0000-000000000001"/></m:SavedItemFolderId>
                  <m:Items>
                    <t:Task>
                      <t:Subject>Review JMAP parity</t:Subject>
                      <t:Body BodyType="Text">Check EWS task coverage</t:Body>
                      <t:Status>InProgress</t:Status>
                      <t:DueDate>2026-05-06T09:00:00Z</t:DueDate>
                    </t:Task>
                  </m:Items>
                </m:CreateItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:CreateItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("task:eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"));
    assert_eq!(tasks.lock().unwrap()[0].task_list_id, task_list_id);
    assert_eq!(tasks.lock().unwrap()[0].status, "in-progress");

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:ItemShape><t:BaseShape>AllProperties</t:BaseShape></m:ItemShape><m:SyncFolderId><t:FolderId Id="aaaaaaaa-0000-0000-0000-000000000001"/></m:SyncFolderId><m:SyncState>tasks:aaaaaaaa-0000-0000-0000-000000000001:0</m:SyncState></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:Create><t:Task>"));
    assert!(body.contains("<t:Subject>Review JMAP parity</t:Subject>"));
    let old_sync_state = body
        .split("<m:SyncState>")
        .nth(1)
        .and_then(|rest| rest.split("</m:SyncState>").next())
        .unwrap()
        .to_string();

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:UpdateItem ConflictResolution="AlwaysOverwrite">
                  <m:ItemChanges>
                    <t:ItemChange>
                      <t:ItemId Id="task:eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"/>
                      <t:Updates>
                        <t:SetItemField>
                          <t:FieldURI FieldURI="task:Subject"/>
                          <t:Task>
                            <t:Subject>Complete JMAP parity review</t:Subject>
                            <t:Body BodyType="Text">Validated through EWS sync</t:Body>
                            <t:Status>Completed</t:Status>
                            <t:CompleteDate>2026-05-06T10:00:00Z</t:CompleteDate>
                          </t:Task>
                        </t:SetItemField>
                      </t:Updates>
                    </t:ItemChange>
                  </m:ItemChanges>
                </m:UpdateItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:UpdateItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:Subject>Complete JMAP parity review</t:Subject>"));
    assert!(body.contains("<t:Status>Completed</t:Status>"));

    let request = format!(
        r#"<s:Envelope><s:Body><m:SyncFolderItems><m:ItemShape><t:BaseShape>AllProperties</t:BaseShape></m:ItemShape><m:SyncFolderId><t:FolderId Id="aaaaaaaa-0000-0000-0000-000000000001"/></m:SyncFolderId><m:SyncState>{old_sync_state}</m:SyncState></m:SyncFolderItems></s:Body></s:Envelope>"#
    );
    let response = service
        .handle(&bearer_headers(), request.as_bytes())
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:Update><t:Task>"));
    assert!(body.contains("<t:Subject>Complete JMAP parity review</t:Subject>"));
    assert!(body.contains("<t:CompleteDate>2026-05-06T10:00:00Z</t:CompleteDate>"));
}

#[tokio::test]
async fn delete_item_rejects_unsupported_item_ids() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:DeleteItem><m:ItemIds><t:ItemId Id="note:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"/></m:ItemIds></m:DeleteItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:DeleteItemResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
}

#[tokio::test]
async fn delete_item_moves_canonical_message_to_trash_by_default() {
    let message_id = Uuid::parse_str("88888888-8888-8888-8888-888888888888").unwrap();
    let trash_id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "77777777-7777-7777-7777-777777777777",
            "trash",
            "Deleted",
        )])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            "88888888-8888-8888-8888-888888888888",
            "66666666-6666-6666-6666-666666666666",
            "inbox",
            "Inbox message",
        )])),
        ..Default::default()
    };
    let moved_emails = store.moved_emails.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:DeleteItem><m:ItemIds><t:ItemId Id="message:88888888-8888-8888-8888-888888888888"/></m:ItemIds></m:DeleteItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:DeleteItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert_eq!(
        moved_emails.lock().unwrap().as_slice(),
        &[(message_id, trash_id)]
    );
}

#[tokio::test]
async fn create_item_saveonly_stores_message_as_canonical_draft() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let saved_drafts = store.saved_drafts.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:CreateItem MessageDisposition="SaveOnly">
                  <m:SavedItemFolderId><t:DistinguishedFolderId Id="drafts"/></m:SavedItemFolderId>
                  <m:Items>
                    <t:Message>
                      <t:Subject>Draft from EWS</t:Subject>
                      <t:Body BodyType="Text">Hello from EWS</t:Body>
                      <t:ToRecipients>
                        <t:Mailbox><t:Name>Bob</t:Name><t:EmailAddress>bob@example.test</t:EmailAddress></t:Mailbox>
                      </t:ToRecipients>
                    </t:Message>
                  </m:Items>
                </m:CreateItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:CreateItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("message:dddddddd-dddd-dddd-dddd-dddddddddddd"));
    let recorded = saved_drafts.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].source, "ews-createitem");
    assert_eq!(recorded[0].subject, "Draft from EWS");
    assert_eq!(recorded[0].body_text, "Hello from EWS");
    assert_eq!(recorded[0].from_address, "alice@example.test");
    assert_eq!(recorded[0].to[0].address, "bob@example.test");
    assert_eq!(recorded[0].to[0].display_name.as_deref(), Some("Bob"));
}

#[tokio::test]
async fn create_item_send_and_save_uses_canonical_submission() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let submitted_messages = store.submitted_messages.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:CreateItem MessageDisposition="SendAndSaveCopy">
                  <m:Items>
                    <t:Message>
                      <t:Subject>Send from EWS</t:Subject>
                      <t:Body BodyType="HTML">&lt;p&gt;Hello&lt;/p&gt;</t:Body>
                      <t:ToRecipients>
                        <t:Mailbox><t:EmailAddress>carol@example.test</t:EmailAddress></t:Mailbox>
                      </t:ToRecipients>
                    </t:Message>
                  </m:Items>
                </m:CreateItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:CreateItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("message:ffffffff-ffff-ffff-ffff-ffffffffffff"));
    let recorded = submitted_messages.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].source, "ews-createitem");
    assert_eq!(recorded[0].subject, "Send from EWS");
    assert_eq!(recorded[0].body_text, "Hello");
    assert_eq!(recorded[0].to[0].address, "carol@example.test");
}

#[tokio::test]
async fn get_item_returns_ews_error_for_unsupported_message_ids() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:GetItem>
                  <m:ItemShape><t:BaseShape>Default</t:BaseShape></m:ItemShape>
                  <m:ItemIds><t:ItemId Id="message:dddddddd-dddd-dddd-dddd-dddddddddddd"/></m:ItemIds>
                </m:GetItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:GetItemResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("<m:ResponseCode>ErrorItemNotFound</m:ResponseCode>"));
    assert!(body.contains("<t:ServerVersionInfo"));
}

#[tokio::test]
async fn out_of_scope_bootstrap_operations_return_ews_unsupported_errors() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    for operation in [
        "GetRoomLists",
        "FindPeople",
        "ExpandDL",
        "GetDelegate",
        "GetUserConfiguration",
        "GetSharingMetadata",
        "GetSharingFolder",
    ] {
        let request = format!("<s:Envelope><s:Body><m:{operation} /></s:Body></s:Envelope>");
        let response = service
            .handle(&bearer_headers(), request.as_bytes())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_text(response).await;
        assert!(body.contains(&format!("<m:{operation}Response>")));
        assert!(body.contains("ResponseClass=\"Error\""));
        assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
        assert!(body.contains("<t:ServerVersionInfo"));
    }
}

#[tokio::test]
async fn pull_subscription_get_events_and_unsubscribe_return_status_flow() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "44444444-4444-4444-4444-444444444444",
            "inbox",
            "Inbox",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:Subscribe>
                  <m:PullSubscriptionRequest>
                    <t:EventTypes><t:EventType>NewMailEvent</t:EventType><t:EventType>DeletedEvent</t:EventType></t:EventTypes>
                    <t:Timeout>10</t:Timeout>
                  </m:PullSubscriptionRequest>
                </m:Subscribe>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:SubscribeResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    let subscription_id = body
        .split("<m:SubscriptionId>")
        .nth(1)
        .and_then(|rest| rest.split("</m:SubscriptionId>").next())
        .unwrap()
        .to_string();
    let watermark = body
        .split("<m:Watermark>")
        .nth(1)
        .and_then(|rest| rest.split("</m:Watermark>").next())
        .unwrap()
        .to_string();

    let request = format!(
        r#"<s:Envelope><s:Body><m:GetEvents><m:SubscriptionId>{subscription_id}</m:SubscriptionId><m:Watermark>{watermark}</m:Watermark></m:GetEvents></s:Body></s:Envelope>"#
    );
    let response = service
        .handle(&bearer_headers(), request.as_bytes())
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:GetEventsResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<m:Notification>"));
    assert!(body.contains(&format!(
        "<t:SubscriptionId>{subscription_id}</t:SubscriptionId>"
    )));
    assert!(body.contains(&format!(
        "<t:PreviousWatermark>{watermark}</t:PreviousWatermark>"
    )));
    assert!(body.contains("<t:MoreEvents>false</t:MoreEvents>"));
    assert!(body.contains("<t:StatusEvent>"));
    assert!(!body.contains("<t:StatusEvent><t:Watermark>") || !body.contains("<t:TimeStamp>"));

    let request = format!(
        r#"<s:Envelope><s:Body><m:Unsubscribe><m:SubscriptionId>{subscription_id}</m:SubscriptionId></m:Unsubscribe></s:Body></s:Envelope>"#
    );
    let response = service
        .handle(&bearer_headers(), request.as_bytes())
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:UnsubscribeResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
}

#[tokio::test]
async fn pull_subscription_get_events_returns_created_for_empty_watermarked_mailbox() {
    let mailbox_id = "12121212-1212-1212-1212-121212121212";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            mailbox_id, "inbox", "Inbox",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:Subscribe>
                  <m:PullSubscriptionRequest>
                    <t:FolderIds><t:DistinguishedFolderId Id="inbox"/></t:FolderIds>
                    <t:EventTypes><t:EventType>CreatedEvent</t:EventType></t:EventTypes>
                    <t:Timeout>10</t:Timeout>
                  </m:PullSubscriptionRequest>
                </m:Subscribe>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    let subscription_id = body
        .split("<m:SubscriptionId>")
        .nth(1)
        .and_then(|rest| rest.split("</m:SubscriptionId>").next())
        .unwrap()
        .to_string();
    let watermark = body
        .split("<m:Watermark>")
        .nth(1)
        .and_then(|rest| rest.split("</m:Watermark>").next())
        .unwrap()
        .to_string();

    let request = format!(
        r#"<s:Envelope><s:Body><m:GetEvents><m:SubscriptionId>{subscription_id}</m:SubscriptionId><m:Watermark>{watermark}</m:Watermark></m:GetEvents></s:Body></s:Envelope>"#
    );
    let response = service
        .handle(&bearer_headers(), request.as_bytes())
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:GetEventsResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:CreatedEvent>"));
    assert!(!body.contains("<t:StatusEvent>"));
    assert!(body.contains(&format!("<t:ParentFolderId Id=\"mailbox:{mailbox_id}\"")));
    assert!(body.contains("ChangeKey=\"notification\""));
}

#[tokio::test]
async fn pull_subscription_get_events_returns_queued_create_after_subscribe() {
    let mailbox_id = "55555555-5555-5555-5555-555555555555";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            mailbox_id, "inbox", "Inbox",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:Subscribe>
                  <m:PullSubscriptionRequest>
                    <t:FolderIds><t:FolderId Id="mailbox:55555555-5555-5555-5555-555555555555"/></t:FolderIds>
                    <t:EventTypes><t:EventType>CreatedEvent</t:EventType></t:EventTypes>
                    <t:Timeout>10</t:Timeout>
                  </m:PullSubscriptionRequest>
                </m:Subscribe>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    let subscription_id = body
        .split("<m:SubscriptionId>")
        .nth(1)
        .and_then(|rest| rest.split("</m:SubscriptionId>").next())
        .unwrap()
        .to_string();
    let watermark = body
        .split("<m:Watermark>")
        .nth(1)
        .and_then(|rest| rest.split("</m:Watermark>").next())
        .unwrap()
        .to_string();

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:CreateItem MessageDisposition="SaveOnly">
                  <m:SavedItemFolderId><t:FolderId Id="mailbox:55555555-5555-5555-5555-555555555555"/></m:SavedItemFolderId>
                  <m:Items><t:Message><t:Subject>RCA pull create</t:Subject></t:Message></m:Items>
                </m:CreateItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));

    let request = format!(
        r#"<s:Envelope><s:Body><m:GetEvents><m:SubscriptionId>{subscription_id}</m:SubscriptionId><m:Watermark>{watermark}</m:Watermark></m:GetEvents></s:Body></s:Envelope>"#
    );
    let response = service
        .handle(&bearer_headers(), request.as_bytes())
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:CreatedEvent>"));
    assert!(body.contains("<t:ItemId Id=\"message:99999999-9999-9999-9999-999999999999\""));
    assert!(body.contains(&format!("<t:ParentFolderId Id=\"mailbox:{mailbox_id}\"")));
    assert!(!body.contains("<t:StatusEvent>"));
}

#[tokio::test]
async fn pull_subscription_get_events_returns_queued_delete_after_subscribe() {
    let mailbox_id = "66666666-6666-6666-6666-666666666666";
    let message_id = "77777777-7777-7777-7777-777777777777";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            mailbox_id, "inbox", "Inbox",
        )])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            message_id,
            mailbox_id,
            "inbox",
            "RCA pull delete",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:Subscribe>
                  <m:PullSubscriptionRequest>
                    <t:FolderIds><t:FolderId Id="mailbox:66666666-6666-6666-6666-666666666666"/></t:FolderIds>
                    <t:EventTypes><t:EventType>DeletedEvent</t:EventType></t:EventTypes>
                    <t:Timeout>10</t:Timeout>
                  </m:PullSubscriptionRequest>
                </m:Subscribe>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    let subscription_id = body
        .split("<m:SubscriptionId>")
        .nth(1)
        .and_then(|rest| rest.split("</m:SubscriptionId>").next())
        .unwrap()
        .to_string();
    let watermark = body
        .split("<m:Watermark>")
        .nth(1)
        .and_then(|rest| rest.split("</m:Watermark>").next())
        .unwrap()
        .to_string();

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:DeleteItem DeleteType="HardDelete"><m:ItemIds><t:ItemId Id="message:77777777-7777-7777-7777-777777777777"/></m:ItemIds></m:DeleteItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));

    let request = format!(
        r#"<s:Envelope><s:Body><m:GetEvents><m:SubscriptionId>{subscription_id}</m:SubscriptionId><m:Watermark>{watermark}</m:Watermark></m:GetEvents></s:Body></s:Envelope>"#
    );
    let response = service
        .handle(&bearer_headers(), request.as_bytes())
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:DeletedEvent>"));
    assert!(body.contains(&format!("<t:ItemId Id=\"message:{message_id}\"")));
    assert!(body.contains(&format!("<t:ParentFolderId Id=\"mailbox:{mailbox_id}\"")));
    assert!(!body.contains("<t:StatusEvent>"));
}

#[tokio::test]
async fn pull_subscription_watermark_replays_delete_after_resubscribe() {
    let mailbox_id = "88888888-8888-8888-8888-888888888888";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            mailbox_id, "inbox", "Inbox",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:Subscribe>
                  <m:PullSubscriptionRequest>
                    <t:FolderIds><t:FolderId Id="mailbox:88888888-8888-8888-8888-888888888888"/></t:FolderIds>
                    <t:EventTypes><t:EventType>CreatedEvent</t:EventType></t:EventTypes>
                    <t:Timeout>10</t:Timeout>
                  </m:PullSubscriptionRequest>
                </m:Subscribe>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    let subscription_id = body
        .split("<m:SubscriptionId>")
        .nth(1)
        .and_then(|rest| rest.split("</m:SubscriptionId>").next())
        .unwrap()
        .to_string();
    let watermark = body
        .split("<m:Watermark>")
        .nth(1)
        .and_then(|rest| rest.split("</m:Watermark>").next())
        .unwrap()
        .to_string();

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:CreateItem MessageDisposition="SaveOnly">
                  <m:SavedItemFolderId><t:FolderId Id="mailbox:88888888-8888-8888-8888-888888888888"/></m:SavedItemFolderId>
                  <m:Items><t:Message><t:Subject>RCA replay delete</t:Subject></t:Message></m:Items>
                </m:CreateItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));

    let request = format!(
        r#"<s:Envelope><s:Body><m:GetEvents><m:SubscriptionId>{subscription_id}</m:SubscriptionId><m:Watermark>{watermark}</m:Watermark></m:GetEvents></s:Body></s:Envelope>"#
    );
    let response = service
        .handle(&bearer_headers(), request.as_bytes())
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:CreatedEvent>"));
    let created_watermark = body
        .split("<t:CreatedEvent>")
        .nth(1)
        .and_then(|rest| rest.split("<t:Watermark>").nth(1))
        .and_then(|rest| rest.split("</t:Watermark>").next())
        .unwrap()
        .to_string();

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:DeleteItem DeleteType="HardDelete"><m:ItemIds><t:ItemId Id="message:99999999-9999-9999-9999-999999999999"/></m:ItemIds></m:DeleteItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));

    let subscribe = format!(
        r#"
        <s:Envelope>
          <s:Body>
            <m:Subscribe>
              <m:PullSubscriptionRequest>
                <t:FolderIds><t:FolderId Id="mailbox:{mailbox_id}"/></t:FolderIds>
                <t:EventTypes><t:EventType>DeletedEvent</t:EventType></t:EventTypes>
                <t:Watermark>{created_watermark}</t:Watermark>
                <t:Timeout>10</t:Timeout>
              </m:PullSubscriptionRequest>
            </m:Subscribe>
          </s:Body>
        </s:Envelope>
        "#
    );
    let response = service
        .handle(&bearer_headers(), subscribe.as_bytes())
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    let replay_subscription_id = body
        .split("<m:SubscriptionId>")
        .nth(1)
        .and_then(|rest| rest.split("</m:SubscriptionId>").next())
        .unwrap()
        .to_string();
    let replay_watermark = body
        .split("<m:Watermark>")
        .nth(1)
        .and_then(|rest| rest.split("</m:Watermark>").next())
        .unwrap()
        .to_string();
    assert_eq!(replay_watermark, created_watermark);

    let request = format!(
        r#"<s:Envelope><s:Body><m:GetEvents><m:SubscriptionId>{replay_subscription_id}</m:SubscriptionId><m:Watermark>{replay_watermark}</m:Watermark></m:GetEvents></s:Body></s:Envelope>"#
    );
    let response = service
        .handle(&bearer_headers(), request.as_bytes())
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:DeletedEvent>"));
    assert!(body.contains("<t:ItemId Id=\"message:99999999-9999-9999-9999-999999999999\""));
    assert!(!body.contains("<t:CreatedEvent>"));
}

#[tokio::test]
async fn pull_subscription_get_events_returns_new_mail_for_watermarked_mailbox() {
    let mailbox_id = "13131313-1313-1313-1313-131313131313";
    let message_id = "14141414-1414-1414-1414-141414141414";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            mailbox_id, "inbox", "Inbox",
        )])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            message_id,
            mailbox_id,
            "inbox",
            "RCA Notification",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:Subscribe>
                  <m:PullSubscriptionRequest>
                    <t:FolderIds><t:DistinguishedFolderId Id="inbox"/></t:FolderIds>
                    <t:EventTypes><t:EventType>NewMailEvent</t:EventType></t:EventTypes>
                    <t:Timeout>10</t:Timeout>
                  </m:PullSubscriptionRequest>
                </m:Subscribe>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    let subscription_id = body
        .split("<m:SubscriptionId>")
        .nth(1)
        .and_then(|rest| rest.split("</m:SubscriptionId>").next())
        .unwrap()
        .to_string();
    let watermark = body
        .split("<m:Watermark>")
        .nth(1)
        .and_then(|rest| rest.split("</m:Watermark>").next())
        .unwrap()
        .to_string();

    let request = format!(
        r#"<s:Envelope><s:Body><m:GetEvents><m:SubscriptionId>{subscription_id}</m:SubscriptionId><m:Watermark>{watermark}</m:Watermark></m:GetEvents></s:Body></s:Envelope>"#
    );
    let response = service
        .handle(&bearer_headers(), request.as_bytes())
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:GetEventsResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:CreatedEvent>"));
    assert!(body.contains("<t:NewMailEvent>"));
    assert!(body.contains(&format!("<t:ItemId Id=\"message:{message_id}\"")));
    assert!(body.contains(&format!("<t:ParentFolderId Id=\"mailbox:{mailbox_id}\"")));
}

#[tokio::test]
async fn get_user_oof_settings_returns_disabled_without_active_vacation() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetUserOofSettingsRequest /></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:GetUserOofSettingsResponse>"));
    assert!(!body.contains("<m:GetUserOofSettingsRequestResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:OofSettings>"));
    assert!(body.contains("</t:OofSettings>"));
    assert!(!body.contains("<m:OofSettings>"));
    assert!(body.contains("<t:OofState>Disabled</t:OofState>"));
    assert!(body.contains("<t:ExternalAudience>None</t:ExternalAudience>"));
    assert!(body.contains("<m:AllowExternalOof>None</m:AllowExternalOof>"));
    assert!(body.contains("<t:ServerVersionInfo"));
}

#[tokio::test]
async fn get_user_oof_settings_projects_canonical_sieve_vacation() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        active_sieve_script: Arc::new(Mutex::new(Some(
            r#"require ["vacation"];
               vacation :subject "Out" :days 3 "Away until Monday";"#
                .to_string(),
        ))),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetUserOofSettings /></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:GetUserOofSettingsResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:OofState>Enabled</t:OofState>"));
    assert!(body.contains("<t:ExternalAudience>All</t:ExternalAudience>"));
    assert!(body.contains("<t:InternalReply><t:Message>Away until Monday</t:Message>"));
    assert!(body.contains("<t:ExternalReply><t:Message>Away until Monday</t:Message>"));
    assert!(body.contains("<t:ServerVersionInfo"));
}

#[tokio::test]
async fn set_user_oof_settings_writes_canonical_sieve_vacation() {
    let active_sieve_script = Arc::new(Mutex::new(None));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        active_sieve_script: active_sieve_script.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:SetUserOofSettings>
                  <t:OofSettings>
                    <t:OofState>Enabled</t:OofState>
                    <t:InternalReply><t:Message>Back next week</t:Message></t:InternalReply>
                  </t:OofSettings>
                </m:SetUserOofSettings>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:SetUserOofSettingsResponse>"));
    assert!(body.contains("<m:ResponseMessage ResponseClass=\"Success\">"));
    assert!(!body.contains("<m:ResponseMessages>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    let script = active_sieve_script.lock().unwrap().clone().unwrap();
    assert!(script.contains("vacation :days 7 \"Back next week\";"));
}

#[tokio::test]
async fn set_user_oof_settings_scheduled_round_trips_canonical_sieve_metadata() {
    let active_sieve_script = Arc::new(Mutex::new(None));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        active_sieve_script: active_sieve_script.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:SetUserOofSettingsRequest>
                  <t:Mailbox><t:Address>alice@example.test</t:Address></t:Mailbox>
                  <t:UserOofSettings>
                    <t:OofState>Scheduled</t:OofState>
                    <t:ExternalAudience>Known</t:ExternalAudience>
                    <t:Duration>
                      <t:StartTime>2026-05-15T00:00:00</t:StartTime>
                      <t:EndTime>2026-05-17T00:00:00</t:EndTime>
                    </t:Duration>
                    <t:InternalReply><t:Message>Back Monday</t:Message></t:InternalReply>
                    <t:ExternalReply><t:Message>Back Monday external</t:Message></t:ExternalReply>
                  </t:UserOofSettings>
                </m:SetUserOofSettingsRequest>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:SetUserOofSettingsResponse>"));
    assert!(body.contains("<m:ResponseMessage ResponseClass=\"Success\">"));
    assert!(!body.contains("<m:ResponseMessages>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));

    let script = active_sieve_script.lock().unwrap().clone().unwrap();
    assert!(script.contains("# LPE-EWS-OOF-State: Scheduled"));
    assert!(script.contains("# LPE-EWS-OOF-ExternalAudience: Known"));
    assert!(script.contains("# LPE-EWS-OOF-StartTime: 2026-05-15T00:00:00"));
    assert!(script.contains("# LPE-EWS-OOF-EndTime: 2026-05-17T00:00:00"));
    assert!(script.contains("vacation :days 7 \"Back Monday\";"));

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetUserOofSettingsRequest /></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:OofState>Scheduled</t:OofState>"));
    assert!(body.contains("<t:ExternalAudience>Known</t:ExternalAudience>"));
    assert!(body.contains("<t:Duration>"));
    assert!(body.contains("<t:StartTime>2026-05-15T00:00:00</t:StartTime>"));
    assert!(body.contains("<t:EndTime>2026-05-17T00:00:00</t:EndTime>"));
    assert!(body.contains("<t:InternalReply><t:Message>Back Monday</t:Message>"));
}

#[tokio::test]
async fn set_user_oof_settings_disables_active_sieve_script() {
    let active_sieve_script = Arc::new(Mutex::new(Some(
        r#"require ["vacation"]; vacation "Away";"#.to_string(),
    )));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        active_sieve_script: active_sieve_script.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SetUserOofSettings><t:OofSettings><t:OofState>Disabled</t:OofState></t:OofSettings></m:SetUserOofSettings></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:SetUserOofSettingsResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(active_sieve_script.lock().unwrap().is_none());
}

#[tokio::test]
async fn set_user_oof_settings_errors_use_single_response_message_shape() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:SetUserOofSettingsRequest>
                  <t:UserOofSettings>
                    <t:OofState>Scheduled</t:OofState>
                    <t:InternalReply><t:Message>Back Monday</t:Message></t:InternalReply>
                  </t:UserOofSettings>
                </m:SetUserOofSettingsRequest>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:SetUserOofSettingsResponse>"));
    assert!(body.contains("<m:ResponseMessage ResponseClass=\"Error\">"));
    assert!(!body.contains("<m:ResponseMessages>"));
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(body.contains("Duration is required when OofState is Scheduled"));
}

#[tokio::test]
async fn unknown_ews_operations_return_parseable_invalid_operation_errors() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    for operation in [
        "SendItem",
        "GetMailTips",
        "GetInboxRules",
        "ConvertId",
        "FindConversation",
        "GetConversationItems",
        "GetStreamingEvents",
    ] {
        let request = format!(
            concat!(
                "<s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\" ",
                "xmlns:m=\"http://schemas.microsoft.com/exchange/services/2006/messages\">",
                "<s:Body><m:{operation} /></s:Body>",
                "</s:Envelope>"
            ),
            operation = operation
        );
        let response = service
            .handle(&bearer_headers(), request.as_bytes())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_text(response).await;
        assert!(body.contains(&format!("<m:{operation}Response>")));
        assert!(body.contains("ResponseClass=\"Error\""));
        assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
        assert!(body.contains("<t:ServerVersionInfo"));
    }
}

#[tokio::test]
async fn authentication_errors_return_basic_challenge() {
    let response = error_response(&anyhow::anyhow!("missing account authentication"));

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response
            .headers()
            .get("www-authenticate")
            .and_then(|value| value.to_str().ok()),
        Some("Basic realm=\"LPE EWS\"")
    );
    let body = response_text(response).await;
    assert!(body.contains("<s:Fault>"));
    assert!(body.contains("missing account authentication"));
}

#[tokio::test]
async fn sync_folder_items_returns_contacts_from_canonical_store() {
    let contact_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    let collection = FakeStore::collection("default", "contacts", "Contacts");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![collection.clone()])),
        contacts: Arc::new(Mutex::new(vec![AccessibleContact {
            id: contact_id,
            collection_id: collection.id.clone(),
            owner_account_id: collection.owner_account_id,
            owner_email: collection.owner_email.clone(),
            owner_display_name: collection.owner_display_name.clone(),
            rights: collection.rights.clone(),
            name: "Bob Example".to_string(),
            role: "Manager".to_string(),
            email: "bob@example.test".to_string(),
            phone: "+491234".to_string(),
            team: "Ops".to_string(),
            notes: "VIP".to_string(),
        }])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:DistinguishedFolderId Id="contacts"/></m:SyncFolderId></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:SyncFolderItemsResponse>"));
    assert!(body.contains("<t:Contact>"));
    assert!(body.contains("contact:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"));
    assert!(body.contains("bob@example.test"));
}

#[tokio::test]
async fn create_delete_contact_round_trips_through_sync_folder_items() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        ..Default::default()
    };
    let deleted_contacts = store.deleted_contacts.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:CreateItem>
                  <m:SavedItemFolderId><t:FolderId Id="default"/></m:SavedItemFolderId>
                  <m:Items>
                    <t:Contact>
                      <t:DisplayName>RCA Contact</t:DisplayName>
                      <t:GivenName>RCA</t:GivenName>
                      <t:Surname>Contact</t:Surname>
                      <t:EmailAddresses>
                        <t:Entry Key="EmailAddress1">rca@example.test</t:Entry>
                      </t:EmailAddresses>
                      <t:PhoneNumbers>
                        <t:Entry Key="MobilePhone">+41000000000</t:Entry>
                      </t:PhoneNumbers>
                      <t:CompanyName>LPE</t:CompanyName>
                      <t:JobTitle>Tester</t:JobTitle>
                      <t:Body BodyType="Text">Created by RCA</t:Body>
                    </t:Contact>
                  </m:Items>
                </m:CreateItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:CreateItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("contact:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"));

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:ItemShape><t:BaseShape>AllProperties</t:BaseShape></m:ItemShape><m:SyncFolderId><t:FolderId Id="default"/></m:SyncFolderId><m:SyncState>contacts:default:0</m:SyncState><m:MaxChangesReturned>10</m:MaxChangesReturned></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:Create><t:Contact>"));
    assert!(body.contains("contact:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"));
    assert!(body.contains("<t:DisplayName>RCA Contact</t:DisplayName>"));
    assert!(
        body.contains("<m:SyncState>contacts:default:v2:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb=ck-")
    );

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:DeleteItem DeleteType="HardDelete"><m:ItemIds><t:ItemId Id="contact:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"/></m:ItemIds></m:DeleteItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:DeleteItemResponse>"));
    assert_eq!(
        deleted_contacts.lock().unwrap().as_slice(),
        &[Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap()]
    );

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="default"/></m:SyncFolderId><m:SyncState>contacts:default:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb</m:SyncState></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(
        body.contains("<t:Delete><t:ItemId Id=\"contact:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb\"")
    );
    assert!(body.contains("<m:SyncState>contacts:default:v2:0</m:SyncState>"));
}

#[tokio::test]
async fn create_contact_syncs_from_current_empty_rca_sync_state() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:CreateItem>
                  <m:SavedItemFolderId><t:FolderId Id="default" ChangeKey="ck-default"/></m:SavedItemFolderId>
                  <m:Items>
                    <t:Contact>
                      <t:DisplayName>RCA Contact</t:DisplayName>
                      <t:EmailAddresses>
                        <t:Entry Key="EmailAddress1">rca@example.test</t:Entry>
                      </t:EmailAddresses>
                    </t:Contact>
                  </m:Items>
                </m:CreateItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:ItemShape><t:BaseShape>AllProperties</t:BaseShape></m:ItemShape><m:SyncFolderId><t:FolderId Id="default" ChangeKey="ck-default"/></m:SyncFolderId><m:SyncState>contacts:default:v2:0</m:SyncState><m:MaxChangesReturned>10</m:MaxChangesReturned></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:Create><t:Contact>"));
    assert!(body.contains("contact:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"));
    assert!(body.contains("<t:DisplayName>RCA Contact</t:DisplayName>"));
    assert!(!body.contains("<m:SyncState>contacts:default:v2:0</m:SyncState>"));
}

#[tokio::test]
async fn create_contact_without_saved_folder_ignores_unrelated_folder_ids() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "contacts", "Contacts",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:CreateItem>
                  <m:Items>
                    <t:Contact>
                      <t:FolderId Id="shared-contacts-aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"/>
                      <t:DisplayName>Unscoped RCA Contact</t:DisplayName>
                      <t:EmailAddresses>
                        <t:Entry Key="EmailAddress1">unscoped@example.test</t:Entry>
                      </t:EmailAddresses>
                    </t:Contact>
                  </m:Items>
                </m:CreateItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="default" ChangeKey="ck-default"/></m:SyncFolderId><m:SyncState>contacts:default:v2:0</m:SyncState></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:Create><t:Contact>"));
    assert!(body.contains("<t:DisplayName>Unscoped RCA Contact</t:DisplayName>"));
}

#[tokio::test]
async fn sync_folder_items_returns_contact_update_for_legacy_id_only_sync_state() {
    let contact_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    let collection = FakeStore::collection("default", "contacts", "Contacts");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![collection.clone()])),
        contacts: Arc::new(Mutex::new(vec![AccessibleContact {
            id: contact_id,
            collection_id: collection.id.clone(),
            owner_account_id: collection.owner_account_id,
            owner_email: collection.owner_email.clone(),
            owner_display_name: collection.owner_display_name.clone(),
            rights: collection.rights.clone(),
            name: "Updated RCA Contact".to_string(),
            role: "Manager".to_string(),
            email: "rca@example.test".to_string(),
            phone: "+41000000000".to_string(),
            team: "LPE".to_string(),
            notes: "Changed after legacy sync state".to_string(),
        }])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:ItemShape><t:BaseShape>AllProperties</t:BaseShape></m:ItemShape><m:SyncFolderId><t:FolderId Id="default" ChangeKey="ck-default"/></m:SyncFolderId><m:SyncState>contacts:default:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb</m:SyncState><m:MaxChangesReturned>10</m:MaxChangesReturned></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<t:Update><t:Contact>"));
    assert!(body.contains("contact:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"));
    assert!(body.contains("<t:DisplayName>Updated RCA Contact</t:DisplayName>"));
    assert!(
        body.contains("<m:SyncState>contacts:default:v2:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb=ck-")
    );
}

#[tokio::test]
async fn sync_folder_items_returns_contact_update_for_legacy_keyed_sync_state() {
    let contact_id = Uuid::parse_str("e77d919d-df4f-488d-bb4c-2defdfd8d6ec").unwrap();
    let collection = FakeStore::collection("default", "contacts", "Contacts");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![collection.clone()])),
        contacts: Arc::new(Mutex::new(vec![AccessibleContact {
            id: contact_id,
            collection_id: collection.id.clone(),
            owner_account_id: collection.owner_account_id,
            owner_email: collection.owner_email.clone(),
            owner_display_name: collection.owner_display_name.clone(),
            rights: collection.rights.clone(),
            name: "RCA Contact".to_string(),
            role: "Tester".to_string(),
            email: "rca@example.test".to_string(),
            phone: "+41000000000".to_string(),
            team: "LPE".to_string(),
            notes: "RCA sync verification".to_string(),
        }])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:ItemShape><t:BaseShape>AllProperties</t:BaseShape></m:ItemShape><m:SyncFolderId><t:FolderId Id="default" ChangeKey="ck-default"/></m:SyncFolderId><m:SyncState>contacts:default:e77d919d-df4f-488d-bb4c-2defdfd8d6ec=ck-d21173e54e57cc77</m:SyncState><m:MaxChangesReturned>10</m:MaxChangesReturned></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<t:Update><t:Contact>"));
    assert!(body.contains("contact:e77d919d-df4f-488d-bb4c-2defdfd8d6ec"));
    assert!(
        body.contains("<m:SyncState>contacts:default:v2:e77d919d-df4f-488d-bb4c-2defdfd8d6ec=ck-")
    );
}

#[tokio::test]
async fn sync_folder_items_returns_no_contact_change_for_current_keyed_sync_state() {
    let contact_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    let collection = FakeStore::collection("default", "contacts", "Contacts");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![collection.clone()])),
        contacts: Arc::new(Mutex::new(vec![AccessibleContact {
            id: contact_id,
            collection_id: collection.id.clone(),
            owner_account_id: collection.owner_account_id,
            owner_email: collection.owner_email.clone(),
            owner_display_name: collection.owner_display_name.clone(),
            rights: collection.rights.clone(),
            name: "RCA Contact".to_string(),
            role: "Tester".to_string(),
            email: "rca@example.test".to_string(),
            phone: "+41000000000".to_string(),
            team: "LPE".to_string(),
            notes: "No change".to_string(),
        }])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="default"/></m:SyncFolderId><m:SyncState>contacts:default:0</m:SyncState></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    let current_sync_state = body
        .split("<m:SyncState>")
        .nth(1)
        .and_then(|rest| rest.split("</m:SyncState>").next())
        .unwrap()
        .to_string();
    assert!(current_sync_state.starts_with("contacts:default:v2:"));

    let request = format!(
        r#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="default"/></m:SyncFolderId><m:SyncState>{current_sync_state}</m:SyncState></m:SyncFolderItems></s:Body></s:Envelope>"#
    );
    let response = service
        .handle(&bearer_headers(), request.as_bytes())
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(!body.contains("<t:Create>"));
    assert!(!body.contains("<t:Update>"));
    assert!(!body.contains("<t:Delete>"));
    assert!(body.contains(&format!("<m:SyncState>{current_sync_state}</m:SyncState>")));
}

#[tokio::test]
async fn update_contact_round_trips_through_sync_folder_items() {
    let contact_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    let collection = FakeStore::collection("default", "contacts", "Contacts");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![collection.clone()])),
        contacts: Arc::new(Mutex::new(vec![AccessibleContact {
            id: contact_id,
            collection_id: collection.id.clone(),
            owner_account_id: collection.owner_account_id,
            owner_email: collection.owner_email.clone(),
            owner_display_name: collection.owner_display_name.clone(),
            rights: collection.rights.clone(),
            name: "RCA Contact".to_string(),
            role: "Tester".to_string(),
            email: "rca@example.test".to_string(),
            phone: "+41000000000".to_string(),
            team: "LPE".to_string(),
            notes: "Created by RCA".to_string(),
        }])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="default"/></m:SyncFolderId><m:SyncState>contacts:default:0</m:SyncState></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    let old_sync_state = body
        .split("<m:SyncState>")
        .nth(1)
        .and_then(|rest| rest.split("</m:SyncState>").next())
        .unwrap()
        .to_string();

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:UpdateItem ConflictResolution="AlwaysOverwrite">
                  <m:ItemChanges>
                    <t:ItemChange>
                      <t:ItemId Id="contact:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"/>
                      <t:Updates>
                        <t:SetItemField>
                          <t:FieldURI FieldURI="contacts:DisplayName"/>
                          <t:Contact>
                            <t:DisplayName>Updated RCA Contact</t:DisplayName>
                            <t:JobTitle>Manager</t:JobTitle>
                            <t:Body BodyType="Text">Updated by RCA</t:Body>
                          </t:Contact>
                        </t:SetItemField>
                      </t:Updates>
                    </t:ItemChange>
                  </m:ItemChanges>
                </m:UpdateItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:UpdateItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:DisplayName>Updated RCA Contact</t:DisplayName>"));

    let request = format!(
        r#"<s:Envelope><s:Body><m:SyncFolderItems><m:ItemShape><t:BaseShape>AllProperties</t:BaseShape></m:ItemShape><m:SyncFolderId><t:FolderId Id="default" ChangeKey="ck-default"/></m:SyncFolderId><m:SyncState>{old_sync_state}</m:SyncState><m:MaxChangesReturned>10</m:MaxChangesReturned></m:SyncFolderItems></s:Body></s:Envelope>"#
    );
    let response = service
        .handle(&bearer_headers(), request.as_bytes())
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:Update><t:Contact>"));
    assert!(body.contains("<t:DisplayName>Updated RCA Contact</t:DisplayName>"));
    assert!(body.contains("<t:JobTitle>Manager</t:JobTitle>"));
    assert!(body.contains("<t:Body BodyType=\"Text\">Updated by RCA</t:Body>"));
}

#[tokio::test]
async fn update_contact_unmapped_field_still_advances_sync_folder_items() {
    let contact_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    let collection = FakeStore::collection("default", "contacts", "Contacts");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![collection.clone()])),
        contacts: Arc::new(Mutex::new(vec![AccessibleContact {
            id: contact_id,
            collection_id: collection.id.clone(),
            owner_account_id: collection.owner_account_id,
            owner_email: collection.owner_email.clone(),
            owner_display_name: collection.owner_display_name.clone(),
            rights: collection.rights.clone(),
            name: "RCA Contact".to_string(),
            role: "Tester".to_string(),
            email: "rca@example.test".to_string(),
            phone: "+41000000000".to_string(),
            team: "LPE".to_string(),
            notes: "Created by RCA".to_string(),
        }])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="default"/></m:SyncFolderId><m:SyncState>contacts:default:0</m:SyncState></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    let old_sync_state = body
        .split("<m:SyncState>")
        .nth(1)
        .and_then(|rest| rest.split("</m:SyncState>").next())
        .unwrap()
        .to_string();

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope>
              <s:Body>
                <m:UpdateItem ConflictResolution="AlwaysOverwrite">
                  <m:ItemChanges>
                    <t:ItemChange>
                      <t:ItemId Id="contact:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"/>
                      <t:Updates>
                        <t:SetItemField>
                          <t:FieldURI FieldURI="contacts:AssistantName"/>
                          <t:Contact>
                            <t:AssistantName>RCA Assistant</t:AssistantName>
                          </t:Contact>
                        </t:SetItemField>
                      </t:Updates>
                    </t:ItemChange>
                  </m:ItemChanges>
                </m:UpdateItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:UpdateItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));

    let request = format!(
        r#"<s:Envelope><s:Body><m:SyncFolderItems><m:ItemShape><t:BaseShape>AllProperties</t:BaseShape></m:ItemShape><m:SyncFolderId><t:FolderId Id="default" ChangeKey="ck-default"/></m:SyncFolderId><m:SyncState>{old_sync_state}</m:SyncState><m:MaxChangesReturned>10</m:MaxChangesReturned></m:SyncFolderItems></s:Body></s:Envelope>"#
    );
    let response = service
        .handle(&bearer_headers(), request.as_bytes())
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:Update><t:Contact>"));
    assert!(body.contains("<t:DisplayName>RCA Contact</t:DisplayName>"));
    assert!(!body.contains(&format!("<m:SyncState>{old_sync_state}</m:SyncState>")));
}

#[tokio::test]
async fn create_delete_calendar_item_round_trips_through_sync_folder_items() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "calendar", "Calendar",
        )])),
        ..Default::default()
    };
    let deleted_events = store.deleted_events.clone();
    let events = store.events.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Header>
                <t:TimeZoneContext><t:TimeZoneDefinition Id="UTC" /></t:TimeZoneContext>
              </s:Header>
              <s:Body>
                <m:CreateItem>
                  <m:SavedItemFolderId><t:FolderId Id="default"/></m:SavedItemFolderId>
                  <m:Items>
                    <t:CalendarItem>
                      <t:Subject>RCA Calendar</t:Subject>
                      <t:Location>Room 1</t:Location>
                      <t:Start>2026-05-04T09:30:00Z</t:Start>
                      <t:End>2026-05-04T10:15:00Z</t:End>
                      <t:Recurrence>
                        <t:WeeklyRecurrence>
                          <t:Interval>1</t:Interval>
                          <t:DaysOfWeek>Monday Wednesday</t:DaysOfWeek>
                        </t:WeeklyRecurrence>
                        <t:NumberedRecurrence>
                          <t:StartDate>2026-05-04</t:StartDate>
                          <t:NumberOfOccurrences>5</t:NumberOfOccurrences>
                        </t:NumberedRecurrence>
                      </t:Recurrence>
                      <t:RequiredAttendees>
                        <t:Attendee><t:Mailbox><t:Name>Bob</t:Name><t:EmailAddress>bob@example.test</t:EmailAddress></t:Mailbox><t:ResponseType>Accept</t:ResponseType></t:Attendee>
                      </t:RequiredAttendees>
                      <t:OptionalAttendees>
                        <t:Attendee><t:Mailbox><t:Name>Carol</t:Name><t:EmailAddress>carol@example.test</t:EmailAddress></t:Mailbox><t:ResponseType>Tentative</t:ResponseType></t:Attendee>
                      </t:OptionalAttendees>
                      <t:Body BodyType="Text">Created by RCA</t:Body>
                    </t:CalendarItem>
                  </m:Items>
                </m:CreateItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:CreateItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("event:cccccccc-cccc-cccc-cccc-cccccccccccc"));
    let created_events = events.lock().unwrap();
    assert_eq!(
        created_events[0].recurrence_rule,
        "FREQ=WEEKLY;BYDAY=MO,WE;COUNT=5"
    );
    assert_eq!(created_events[0].attendees, "Bob, Carol");
    assert!(created_events[0]
        .attendees_json
        .contains("alice@example.test"));
    assert!(created_events[0]
        .attendees_json
        .contains("bob@example.test"));
    assert!(created_events[0]
        .attendees_json
        .contains("carol@example.test"));
    assert!(created_events[0].attendees_json.contains("accepted"));
    assert!(created_events[0].attendees_json.contains("tentative"));
    drop(created_events);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:ItemShape><t:BaseShape>AllProperties</t:BaseShape></m:ItemShape><m:SyncFolderId><t:FolderId Id="default"/></m:SyncFolderId><m:SyncState>calendar:default:0</m:SyncState><m:MaxChangesReturned>10</m:MaxChangesReturned></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:Create><t:CalendarItem>"));
    assert!(body.contains("event:cccccccc-cccc-cccc-cccc-cccccccccccc"));
    assert!(body.contains("<t:Subject>RCA Calendar</t:Subject>"));
    assert!(body.contains("<t:Start>2026-05-04T09:30:00Z</t:Start>"));
    assert!(body.contains("<t:End>2026-05-04T10:15:00Z</t:End>"));
    assert!(body.contains("<t:WeeklyRecurrence>"));
    assert!(body.contains("<t:DaysOfWeek>Monday Wednesday</t:DaysOfWeek>"));
    assert!(body.contains("<t:NumberOfOccurrences>5</t:NumberOfOccurrences>"));
    assert!(body.contains("<t:RequiredAttendees>"));
    assert!(body.contains("<t:OptionalAttendees>"));
    assert!(body.contains("<t:EmailAddress>bob@example.test</t:EmailAddress>"));
    assert!(body.contains("<t:EmailAddress>carol@example.test</t:EmailAddress>"));
    assert!(body.contains("<t:ResponseType>Accept</t:ResponseType>"));
    assert!(body.contains("<t:ResponseType>Tentative</t:ResponseType>"));
    assert!(
        body.contains("<m:SyncState>calendar:default:v2:cccccccc-cccc-cccc-cccc-cccccccccccc=ck-")
    );

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:DeleteItem DeleteType="HardDelete"><m:ItemIds><t:ItemId Id="event:cccccccc-cccc-cccc-cccc-cccccccccccc"/></m:ItemIds></m:DeleteItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<m:DeleteItemResponse>"));
    assert_eq!(
        deleted_events.lock().unwrap().as_slice(),
        &[Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap()]
    );

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="default"/></m:SyncFolderId><m:SyncState>calendar:default:cccccccc-cccc-cccc-cccc-cccccccccccc</m:SyncState></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:Delete><t:ItemId Id=\"event:cccccccc-cccc-cccc-cccc-cccccccccccc\""));
    assert!(body.contains("<m:SyncState>calendar:default:v2:0</m:SyncState>"));
}

#[tokio::test]
async fn sync_folder_items_returns_empty_sync_for_custom_mailbox_folder() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "44444444-4444-4444-4444-444444444444",
            "custom",
            "RCA Sync",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="mailbox:44444444-4444-4444-4444-444444444444"/></m:SyncFolderId></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:SyncFolderItemsResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(
        body.contains("<m:SyncState>mailbox:44444444-4444-4444-4444-444444444444:</m:SyncState>")
    );
    assert!(body.contains("<m:Changes></m:Changes>"));
}

#[tokio::test]
async fn sync_folder_items_accepts_any_folder_id_namespace_prefix() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "44444444-4444-4444-4444-444444444444",
            "custom",
            "RCA Sync",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><x:FolderId Id="mailbox:44444444-4444-4444-4444-444444444444"/></m:SyncFolderId></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:SyncFolderItemsResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(
        body.contains("<m:SyncState>mailbox:44444444-4444-4444-4444-444444444444:</m:SyncState>")
    );
}

#[tokio::test]
async fn sync_folder_items_uses_mailbox_id_from_sync_state_when_folder_id_is_omitted() {
    let emails = Arc::new(Mutex::new(vec![FakeStore::email(
        "99999999-9999-9999-9999-999999999999",
        "44444444-4444-4444-4444-444444444444",
        "custom",
        "RCA folder item",
    )]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "44444444-4444-4444-4444-444444444444",
            "custom",
            "RCA Sync",
        )])),
        emails,
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncState>mailbox:44444444-4444-4444-4444-444444444444:</m:SyncState></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<t:Create><t:Message>"));
    assert!(body.contains("message:99999999-9999-9999-9999-999999999999"));
}

#[tokio::test]
async fn sync_folder_items_accepts_utf16_soap_requests() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "44444444-4444-4444-4444-444444444444",
            "custom",
            "RCA Sync",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let mut headers = bearer_headers();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("text/xml; charset=utf-16"),
    );
    let request = r#"<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types"><s:Body><m:SyncFolderItems><m:ItemShape><t:BaseShape>IdOnly</t:BaseShape></m:ItemShape><m:SyncFolderId><t:FolderId Id="mailbox:44444444-4444-4444-4444-444444444444"/></m:SyncFolderId><m:MaxChangesReturned>512</m:MaxChangesReturned></m:SyncFolderItems></s:Body></s:Envelope>"#;
    let mut body = vec![0xff, 0xfe];
    body.extend(request.encode_utf16().flat_map(u16::to_le_bytes));

    let response = service.handle(&headers, &body).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:SyncFolderItemsResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
}

#[tokio::test]
async fn create_item_saveonly_imports_message_into_custom_mailbox_folder() {
    let mailbox_id = Uuid::parse_str("44444444-4444-4444-4444-444444444444").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "44444444-4444-4444-4444-444444444444",
            "custom",
            "RCA Sync",
        )])),
        ..Default::default()
    };
    let imported_emails = store.imported_emails.clone();
    let saved_drafts = store.saved_drafts.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" xmlns:m="http://schemas.microsoft.com/exchange/services/2006/messages" xmlns:t="http://schemas.microsoft.com/exchange/services/2006/types">
              <s:Body>
                <m:CreateItem MessageDisposition="SaveOnly">
                  <m:SavedItemFolderId><t:FolderId Id="mailbox:44444444-4444-4444-4444-444444444444"/></m:SavedItemFolderId>
                  <m:Items>
                    <t:Message>
                      <t:Subject>RCA folder item</t:Subject>
                      <t:Body BodyType="Text">Hello from EWS</t:Body>
                    </t:Message>
                  </m:Items>
                </m:CreateItem>
              </s:Body>
            </s:Envelope>
            "#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("message:99999999-9999-9999-9999-999999999999"));
    assert!(saved_drafts.lock().unwrap().is_empty());
    let recorded = imported_emails.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].mailbox_id, mailbox_id);
    assert_eq!(recorded[0].subject, "RCA folder item");
}

#[tokio::test]
async fn find_item_lists_custom_mailbox_messages() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            "99999999-9999-9999-9999-999999999999",
            "44444444-4444-4444-4444-444444444444",
            "custom",
            "RCA folder item",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:FindItem><m:ParentFolderIds><t:FolderId Id="mailbox:44444444-4444-4444-4444-444444444444"/></m:ParentFolderIds></m:FindItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:FindItemResponse>"));
    assert!(body.contains("<t:Message>"));
    assert!(body.contains("message:99999999-9999-9999-9999-999999999999"));
    assert!(body.contains("<t:Subject>RCA folder item</t:Subject>"));
}

#[tokio::test]
async fn find_item_lists_system_mailbox_messages_by_distinguished_id() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox",
        )])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            "88888888-8888-8888-8888-888888888888",
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox message",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:FindItem><m:ParentFolderIds><t:DistinguishedFolderId Id="inbox"/></m:ParentFolderIds></m:FindItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:FindItemResponse>"));
    assert!(body.contains("<t:Message>"));
    assert!(body.contains("message:88888888-8888-8888-8888-888888888888"));
    assert!(body.contains("<t:Subject>Inbox message</t:Subject>"));
}

#[tokio::test]
async fn sync_folder_items_reports_custom_mailbox_create_and_delete_changes() {
    let emails = Arc::new(Mutex::new(vec![FakeStore::email(
        "99999999-9999-9999-9999-999999999999",
        "44444444-4444-4444-4444-444444444444",
        "custom",
        "RCA folder item",
    )]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        emails: emails.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:FolderId Id="mailbox:44444444-4444-4444-4444-444444444444"/></m:SyncFolderId></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(body.contains("<t:Create><t:Message>"));
    assert!(body.contains("message:99999999-9999-9999-9999-999999999999"));
    assert!(body.contains("<m:SyncState>mailbox:44444444-4444-4444-4444-444444444444:99999999-9999-9999-9999-999999999999</m:SyncState>"));

    emails.lock().unwrap().clear();
    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncState>mailbox:44444444-4444-4444-4444-444444444444:99999999-9999-9999-9999-999999999999</m:SyncState><m:SyncFolderId><t:FolderId Id="mailbox:44444444-4444-4444-4444-444444444444"/></m:SyncFolderId></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();
    let body = response_text(response).await;
    assert!(
        body.contains("<t:Delete><t:ItemId Id=\"message:99999999-9999-9999-9999-999999999999\"")
    );
}

#[tokio::test]
async fn sync_folder_items_reports_system_mailbox_messages() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox",
        )])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            "88888888-8888-8888-8888-888888888888",
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox message",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:SyncFolderItems><m:SyncFolderId><t:DistinguishedFolderId Id="inbox"/></m:SyncFolderId></m:SyncFolderItems></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:SyncFolderItemsResponse>"));
    assert!(body.contains("<t:Create><t:Message>"));
    assert!(body.contains("message:88888888-8888-8888-8888-888888888888"));
    assert!(body.contains("<m:SyncState>mailbox:55555555-5555-5555-5555-555555555555:88888888-8888-8888-8888-888888888888</m:SyncState>"));
}

#[tokio::test]
async fn get_item_returns_custom_mailbox_message_body() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            "99999999-9999-9999-9999-999999999999",
            "44444444-4444-4444-4444-444444444444",
            "custom",
            "RCA folder item",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetItem><m:ItemIds><t:ItemId Id="message:99999999-9999-9999-9999-999999999999"/></m:ItemIds></m:GetItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:GetItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:Body BodyType=\"Text\">Hello</t:Body>"));
}

#[tokio::test]
async fn get_item_returns_system_mailbox_message_body() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            "88888888-8888-8888-8888-888888888888",
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox message",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetItem><m:ItemIds><t:ItemId Id="message:88888888-8888-8888-8888-888888888888"/></m:ItemIds></m:GetItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:GetItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:Subject>Inbox message</t:Subject>"));
    assert!(body.contains("<t:Body BodyType=\"Text\">Hello</t:Body>"));
}

#[tokio::test]
async fn get_item_returns_requested_mime_content_without_leaking_bcc_for_normal_mailbox() {
    let mut email = FakeStore::email(
        "99999999-9999-9999-9999-999999999999",
        "44444444-4444-4444-4444-444444444444",
        "custom",
        "RCA folder item",
    );
    email.bcc.push(JmapEmailAddress {
        address: "hidden@example.test".to_string(),
        display_name: Some("Hidden".to_string()),
    });
    let store = FakeStore {
        session: Some(FakeStore::account()),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetItem><m:ItemShape><t:AdditionalProperties><t:FieldURI FieldURI="item:MimeContent"/></t:AdditionalProperties></m:ItemShape><m:ItemIds><t:ItemId Id="message:99999999-9999-9999-9999-999999999999"/></m:ItemIds></m:GetItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<t:MimeContent CharacterSet=\"UTF-8\">"));
    let mime = decoded_mime_content(&body);
    assert!(mime.contains("Subject: RCA folder item"));
    assert!(mime.contains("Content-Type: text/plain; charset=UTF-8"));
    assert!(mime.ends_with("Hello"));
    assert!(!mime.contains("Bcc:"));
}

#[tokio::test]
async fn get_item_mime_content_preserves_bcc_for_sent_message() {
    let mut email = FakeStore::email(
        "99999999-9999-9999-9999-999999999999",
        "44444444-4444-4444-4444-444444444444",
        "sent",
        "Sent folder item",
    );
    email.bcc.push(JmapEmailAddress {
        address: "hidden@example.test".to_string(),
        display_name: Some("Hidden".to_string()),
    });
    let store = FakeStore {
        session: Some(FakeStore::account()),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetItem><m:ItemShape><t:AdditionalProperties><t:FieldURI FieldURI="item:MimeContent"/></t:AdditionalProperties></m:ItemShape><m:ItemIds><t:ItemId Id="message:99999999-9999-9999-9999-999999999999"/></m:ItemIds></m:GetItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    let mime = decoded_mime_content(&body);
    assert!(mime.contains("Subject: Sent folder item"));
    assert!(mime.contains("Bcc: Hidden <hidden@example.test>"));
}

#[tokio::test]
async fn get_item_includes_attachment_references_for_message() {
    let message_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
    let attachment_id = Uuid::parse_str("abababab-abab-abab-abab-abababababab").unwrap();
    let mut email = FakeStore::email(
        "99999999-9999-9999-9999-999999999999",
        "44444444-4444-4444-4444-444444444444",
        "custom",
        "RCA folder item",
    );
    email.has_attachments = true;
    let file_reference = format!("attachment:{message_id}:{attachment_id}");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        emails: Arc::new(Mutex::new(vec![email])),
        attachments: Arc::new(Mutex::new(HashMap::from([(
            message_id,
            vec![ActiveSyncAttachment {
                id: attachment_id,
                message_id,
                file_name: "brief.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                size_octets: 5,
                file_reference: file_reference.clone(),
            }],
        )]))),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetItem><m:ItemIds><t:ItemId Id="message:99999999-9999-9999-9999-999999999999"/></m:ItemIds></m:GetItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:GetItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:Attachments>"));
    assert!(body.contains("<t:FileAttachment>"));
    assert!(body.contains(&format!("<t:AttachmentId Id=\"{file_reference}\"/>")));
    assert!(body.contains("<t:Name>brief.pdf</t:Name>"));
    assert!(body.contains("<t:ContentType>application/pdf</t:ContentType>"));
    assert!(body.contains("<t:Size>5</t:Size>"));
    assert!(!body.contains("<t:Content>"));
}

#[tokio::test]
async fn get_item_mime_content_includes_canonical_attachments() {
    let message_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
    let attachment_id = Uuid::parse_str("abababab-abab-abab-abab-abababababab").unwrap();
    let mut email = FakeStore::email(
        "99999999-9999-9999-9999-999999999999",
        "44444444-4444-4444-4444-444444444444",
        "custom",
        "RCA folder item",
    );
    email.has_attachments = true;
    let file_reference = format!("attachment:{message_id}:{attachment_id}");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        emails: Arc::new(Mutex::new(vec![email])),
        attachments: Arc::new(Mutex::new(HashMap::from([(
            message_id,
            vec![ActiveSyncAttachment {
                id: attachment_id,
                message_id,
                file_name: "brief.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                size_octets: 5,
                file_reference: file_reference.clone(),
            }],
        )]))),
        attachment_contents: Arc::new(Mutex::new(HashMap::from([(
            file_reference.clone(),
            ActiveSyncAttachmentContent {
                file_reference,
                file_name: "brief.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                blob_bytes: b"hello".to_vec(),
            },
        )]))),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetItem><m:ItemShape><t:AdditionalProperties><t:FieldURI FieldURI="item:MimeContent"/></t:AdditionalProperties></m:ItemShape><m:ItemIds><t:ItemId Id="message:99999999-9999-9999-9999-999999999999"/></m:ItemIds></m:GetItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    let mime = decoded_mime_content(&body);
    assert!(mime.contains("Content-Type: multipart/mixed; boundary=\"lpe-ews-mixed-99999999999999999999999999999999\""));
    assert!(mime.contains("Content-Disposition: attachment; filename=\"brief.pdf\""));
    assert!(mime.contains("Content-Type: application/pdf; name=\"brief.pdf\""));
    assert!(mime.contains("aGVsbG8="));
}

#[tokio::test]
async fn get_attachment_returns_canonical_attachment_content() {
    let message_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
    let attachment_id = Uuid::parse_str("abababab-abab-abab-abab-abababababab").unwrap();
    let file_reference = format!("attachment:{message_id}:{attachment_id}");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        attachment_contents: Arc::new(Mutex::new(HashMap::from([(
            file_reference.clone(),
            ActiveSyncAttachmentContent {
                file_reference: file_reference.clone(),
                file_name: "brief.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                blob_bytes: b"hello".to_vec(),
            },
        )]))),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let request = format!(
        r#"<s:Envelope><s:Body><m:GetAttachment><m:AttachmentIds><t:AttachmentId Id="{file_reference}"/></m:AttachmentIds></m:GetAttachment></s:Body></s:Envelope>"#
    );

    let response = service
        .handle(&bearer_headers(), request.as_bytes())
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:GetAttachmentResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains(&format!("<t:AttachmentId Id=\"{file_reference}\"/>")));
    assert!(body.contains("<t:Name>brief.pdf</t:Name>"));
    assert!(body.contains("<t:ContentType>application/pdf</t:ContentType>"));
    assert!(body.contains("<t:Size>5</t:Size>"));
    assert!(body.contains("<t:Content>aGVsbG8=</t:Content>"));
}

#[tokio::test]
async fn get_attachment_rejects_unknown_attachment_id() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetAttachment><m:AttachmentIds><t:AttachmentId Id="attachment:99999999-9999-9999-9999-999999999999:abababab-abab-abab-abab-abababababab"/></m:AttachmentIds></m:GetAttachment></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:GetAttachmentResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("<m:ResponseCode>ErrorAttachmentNotFound</m:ResponseCode>"));
}

#[tokio::test]
async fn create_attachment_validates_and_adds_canonical_attachment() {
    let message_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            "99999999-9999-9999-9999-999999999999",
            "44444444-4444-4444-4444-444444444444",
            "custom",
            "RCA folder item",
        )])),
        ..Default::default()
    };
    let created_attachments = store.created_attachments.clone();
    let attachments = store.attachments.clone();
    let emails = store.emails.clone();
    let service =
        ExchangeService::new_with_validator(store, Validator::new(FakeDetector::pdf(), 0.8));

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:CreateAttachment><m:ParentItemId><t:ItemId Id="message:99999999-9999-9999-9999-999999999999"/></m:ParentItemId><m:Attachments><t:FileAttachment><t:Name>brief.pdf</t:Name><t:ContentType>application/pdf</t:ContentType><t:Content>aGVsbG8=</t:Content></t:FileAttachment></m:Attachments></m:CreateAttachment></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:CreateAttachmentResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("<t:AttachmentId Id=\"attachment:99999999-9999-9999-9999-999999999999:cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcdcd\"/>"));
    assert!(body.contains("RootItemId=\"message:99999999-9999-9999-9999-999999999999\""));
    assert_eq!(created_attachments.lock().unwrap().len(), 1);
    let attachment = &created_attachments.lock().unwrap()[0];
    assert_eq!(attachment.file_name, "brief.pdf");
    assert_eq!(attachment.media_type, "application/pdf");
    assert_eq!(attachment.blob_bytes, b"hello");
    assert_eq!(
        attachments.lock().unwrap().get(&message_id).unwrap().len(),
        1
    );
    assert!(emails.lock().unwrap()[0].has_attachments);
}

#[tokio::test]
async fn create_attachment_rejects_magika_blocked_payload() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            "99999999-9999-9999-9999-999999999999",
            "44444444-4444-4444-4444-444444444444",
            "custom",
            "RCA folder item",
        )])),
        ..Default::default()
    };
    let created_attachments = store.created_attachments.clone();
    let service =
        ExchangeService::new_with_validator(store, Validator::new(FakeDetector::executable(), 0.8));

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:CreateAttachment><m:ParentItemId><t:ItemId Id="message:99999999-9999-9999-9999-999999999999"/></m:ParentItemId><m:Attachments><t:FileAttachment><t:Name>brief.pdf</t:Name><t:ContentType>application/pdf</t:ContentType><t:Content>aGVsbG8=</t:Content></t:FileAttachment></m:Attachments></m:CreateAttachment></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:CreateAttachmentResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(created_attachments.lock().unwrap().is_empty());
}

#[tokio::test]
async fn create_attachment_rejects_unknown_parent_message() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service =
        ExchangeService::new_with_validator(store, Validator::new(FakeDetector::pdf(), 0.8));

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:CreateAttachment><m:ParentItemId><t:ItemId Id="message:99999999-9999-9999-9999-999999999999"/></m:ParentItemId><m:Attachments><t:FileAttachment><t:Name>brief.pdf</t:Name><t:ContentType>application/pdf</t:ContentType><t:Content>aGVsbG8=</t:Content></t:FileAttachment></m:Attachments></m:CreateAttachment></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:CreateAttachmentResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("<m:ResponseCode>ErrorItemNotFound</m:ResponseCode>"));
}

#[tokio::test]
async fn delete_attachment_removes_canonical_attachment_reference() {
    let message_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
    let attachment_id = Uuid::parse_str("abababab-abab-abab-abab-abababababab").unwrap();
    let mut email = FakeStore::email(
        "99999999-9999-9999-9999-999999999999",
        "44444444-4444-4444-4444-444444444444",
        "custom",
        "RCA folder item",
    );
    email.has_attachments = true;
    let file_reference = format!("attachment:{message_id}:{attachment_id}");
    let attachments = Arc::new(Mutex::new(HashMap::from([(
        message_id,
        vec![ActiveSyncAttachment {
            id: attachment_id,
            message_id,
            file_name: "brief.pdf".to_string(),
            media_type: "application/pdf".to_string(),
            size_octets: 5,
            file_reference: file_reference.clone(),
        }],
    )])));
    let emails = Arc::new(Mutex::new(vec![email]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        emails: emails.clone(),
        attachments: attachments.clone(),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let request = format!(
        r#"<s:Envelope><s:Body><m:DeleteAttachment><m:AttachmentIds><t:AttachmentId Id="{file_reference}"/></m:AttachmentIds></m:DeleteAttachment></s:Body></s:Envelope>"#
    );

    let response = service
        .handle(&bearer_headers(), request.as_bytes())
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:DeleteAttachmentResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("RootItemId=\"message:99999999-9999-9999-9999-999999999999\""));
    assert!(attachments
        .lock()
        .unwrap()
        .get(&message_id)
        .unwrap()
        .is_empty());
    assert!(!emails.lock().unwrap()[0].has_attachments);
}

#[tokio::test]
async fn delete_attachment_rejects_unknown_attachment_id() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:DeleteAttachment><m:AttachmentIds><t:AttachmentId Id="attachment:99999999-9999-9999-9999-999999999999:abababab-abab-abab-abab-abababababab"/></m:AttachmentIds></m:DeleteAttachment></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:DeleteAttachmentResponse>"));
    assert!(body.contains("ResponseClass=\"Error\""));
    assert!(body.contains("<m:ResponseCode>ErrorAttachmentNotFound</m:ResponseCode>"));
}

#[tokio::test]
async fn delete_item_removes_custom_mailbox_message() {
    let message_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            "99999999-9999-9999-9999-999999999999",
            "44444444-4444-4444-4444-444444444444",
            "custom",
            "RCA folder item",
        )])),
        ..Default::default()
    };
    let deleted_emails = store.deleted_emails.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:DeleteItem DeleteType="HardDelete"><m:ItemIds><t:ItemId Id="message:99999999-9999-9999-9999-999999999999"/></m:ItemIds></m:DeleteItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:DeleteItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert_eq!(deleted_emails.lock().unwrap().as_slice(), &[message_id]);
}

#[tokio::test]
async fn move_item_moves_custom_mailbox_message_to_target_folder() {
    let message_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
    let target_mailbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox("44444444-4444-4444-4444-444444444444", "custom", "RCA Sync"),
            FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "custom", "Archive"),
        ])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            "99999999-9999-9999-9999-999999999999",
            "44444444-4444-4444-4444-444444444444",
            "custom",
            "RCA folder item",
        )])),
        ..Default::default()
    };
    let moved_emails = store.moved_emails.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:MoveItem><m:ToFolderId><t:FolderId Id="mailbox:55555555-5555-5555-5555-555555555555"/></m:ToFolderId><m:ItemIds><t:ItemId Id="message:99999999-9999-9999-9999-999999999999"/></m:ItemIds></m:MoveItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:MoveItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("message:99999999-9999-9999-9999-999999999999"));
    assert!(body.contains("mailbox:55555555-5555-5555-5555-555555555555"));
    assert_eq!(
        moved_emails.lock().unwrap().as_slice(),
        &[(message_id, target_mailbox_id)]
    );
}

#[tokio::test]
async fn move_item_rejects_non_message_ids() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "custom",
            "Archive",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:MoveItem><m:ToFolderId><t:FolderId Id="mailbox:55555555-5555-5555-5555-555555555555"/></m:ToFolderId><m:ItemIds><t:ItemId Id="contact:cccccccc-cccc-cccc-cccc-cccccccccccc"/></m:ItemIds></m:MoveItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:MoveItemResponse>"));
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(body.contains("supports only canonical message ids"));
}

#[tokio::test]
async fn copy_item_copies_custom_mailbox_message_to_target_folder() {
    let message_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
    let target_mailbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox("44444444-4444-4444-4444-444444444444", "custom", "RCA Sync"),
            FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "custom", "Archive"),
        ])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            "99999999-9999-9999-9999-999999999999",
            "44444444-4444-4444-4444-444444444444",
            "custom",
            "RCA folder item",
        )])),
        ..Default::default()
    };
    let copied_emails = store.copied_emails.clone();
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:CopyItem><m:ToFolderId><t:FolderId Id="mailbox:55555555-5555-5555-5555-555555555555"/></m:ToFolderId><m:ItemIds><t:ItemId Id="message:99999999-9999-9999-9999-999999999999"/></m:ItemIds></m:CopyItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:CopyItemResponse>"));
    assert!(body.contains("<m:ResponseCode>NoError</m:ResponseCode>"));
    assert!(body.contains("message:77777777-7777-7777-7777-777777777777"));
    assert!(body.contains("mailbox:55555555-5555-5555-5555-555555555555"));
    assert_eq!(
        copied_emails.lock().unwrap().as_slice(),
        &[(message_id, target_mailbox_id)]
    );
}

#[tokio::test]
async fn copy_item_rejects_non_message_ids() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "custom",
            "Archive",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:CopyItem><m:ToFolderId><t:FolderId Id="mailbox:55555555-5555-5555-5555-555555555555"/></m:ToFolderId><m:ItemIds><t:ItemId Id="event:cccccccc-cccc-cccc-cccc-cccccccccccc"/></m:ItemIds></m:CopyItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:CopyItemResponse>"));
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(body.contains("supports only canonical message ids"));
}

#[tokio::test]
async fn find_item_returns_calendar_items_from_canonical_store() {
    let event_id = Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap();
    let collection = FakeStore::collection("default", "calendar", "Calendar");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        calendar_collections: Arc::new(Mutex::new(vec![collection.clone()])),
        events: Arc::new(Mutex::new(vec![AccessibleEvent {
            id: event_id,
            collection_id: collection.id.clone(),
            owner_account_id: collection.owner_account_id,
            owner_email: collection.owner_email.clone(),
            owner_display_name: collection.owner_display_name.clone(),
            rights: collection.rights.clone(),
            date: "2026-05-04".to_string(),
            time: "09:30".to_string(),
            time_zone: "UTC".to_string(),
            duration_minutes: 45,
            recurrence_rule: String::new(),
            title: "Planning".to_string(),
            location: "Room 1".to_string(),
            attendees: String::new(),
            attendees_json: String::new(),
            notes: "Agenda".to_string(),
        }])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:FindItem><m:ParentFolderIds><t:DistinguishedFolderId Id="calendar"/></m:ParentFolderIds></m:FindItem></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    let body = response_text(response).await;
    assert!(body.contains("<m:FindItemResponse>"));
    assert!(body.contains("<t:CalendarItem>"));
    assert!(body.contains("event:cccccccc-cccc-cccc-cccc-cccccccccccc"));
    assert!(body.contains("<t:Start>2026-05-04T09:30:00Z</t:Start>"));
    assert!(body.contains("<t:End>2026-05-04T10:15:00Z</t:End>"));
}
