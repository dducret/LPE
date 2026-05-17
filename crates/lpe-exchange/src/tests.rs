use axum::body::{to_bytes, Body};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use lpe_magika::{DetectionSource, Detector, MagikaDetection, Validator};
use lpe_mail_auth::{AccountAuthStore, AccountPrincipal, StoreFuture};
use lpe_storage::{
    AccessibleContact, AccessibleEvent, AccountLogin, ActiveSyncAttachment,
    ActiveSyncAttachmentContent, AttachmentUploadInput, AuthenticatedAccount, ClientTask,
    CollaborationCollection, CollaborationRights, JmapEmail, JmapEmailAddress,
    JmapEmailMailboxState, JmapEmailQuery, JmapImportedEmailInput, JmapMailbox,
    JmapMailboxCreateInput, SavedDraftMessage, SieveScriptDocument, StoredAccountAppPassword,
    SubmitMessageInput, SubmittedMessage, SubmittedRecipientInput, UpsertClientContactInput,
    UpsertClientEventInput, UpsertClientTaskInput,
};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};
use uuid::Uuid;

use crate::{
    mapi::{permissions::MapiFolderPermission, MapiEndpoint},
    mapi_mailstore,
    mapi_store::MapiStore,
    service::{
        error_response, is_rpc_proxy_in_data_channel_request, mark_rpc_proxy_out_endpoint_bind_ack,
        rpc_proxy_in_channel_response_for_buffer, rpc_proxy_in_channel_response_for_endpoint_query,
        rpc_proxy_in_channel_response_for_endpoint_query_with_store, ExchangeService,
    },
    store::{
        ExchangeAddressBookDirectoryKind, ExchangeAddressBookEntry, ExchangeAddressBookEntryKind,
        ExchangeStore, MapiCheckpointKind, MapiContentTableQuery, MapiContentTableQueryResult,
        MapiContentTableSortField, MapiIdentityLookupRecord, MapiIdentityObjectKind,
        MapiIdentityRecord, MapiIdentityRequest, MapiNotificationPoll, MapiSyncChangeSet,
        MapiSyncCheckpoint,
    },
};

static MAPI_TEST_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

#[tokio::test]
async fn mapi_identity_mapping_survives_restart_style_store_reload() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        ..FakeStore::default()
    };
    let mailbox = FakeStore::mailbox(
        "44444444-4444-4444-4444-444444444444",
        "custom",
        "Durable IDs",
    );
    let email = FakeStore::email(
        "99999999-9999-9999-9999-999999999999",
        &mailbox.id.to_string(),
        "custom",
        "Stable identity",
    );
    store.mailboxes.lock().unwrap().push(mailbox);
    store.emails.lock().unwrap().push(email);

    let first = store
        .load_mapi_mail_store(account.account_id, 500)
        .await
        .unwrap();
    let second = store
        .load_mapi_mail_store(account.account_id, 500)
        .await
        .unwrap();

    assert_eq!(first.folders()[0].id, second.folders()[0].id);
    assert_eq!(first.messages()[0].id, second.messages()[0].id);
    assert_eq!(
        crate::mapi::identity::object_id_from_long_term_id(
            &crate::mapi::identity::long_term_id_from_object_id(first.messages()[0].id).unwrap()
        ),
        Some(second.messages()[0].id)
    );
}

#[tokio::test]
async fn mapi_identity_source_key_lookup_and_checkpoints_round_trip() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        ..FakeStore::default()
    };
    let mailbox = FakeStore::mailbox(
        "44444444-4444-4444-4444-444444444444",
        "custom",
        "Source key",
    );
    store.mailboxes.lock().unwrap().push(mailbox.clone());
    let allocated = store
        .fetch_or_allocate_mapi_identities(
            account.account_id,
            &[MapiIdentityRequest {
                object_kind: MapiIdentityObjectKind::Mailbox,
                canonical_id: mailbox.id,
                reserved_global_counter: None,
            }],
        )
        .await
        .unwrap();
    let source_key = crate::mapi::identity::source_key_for_object_id(allocated[0].object_id);
    let looked_up = store
        .fetch_mapi_identities_by_source_keys(account.account_id, &[source_key])
        .await
        .unwrap();

    assert_eq!(looked_up[0].canonical_id, mailbox.id);

    let checkpoint = store
        .store_mapi_sync_checkpoint(
            account.account_id,
            Some(mailbox.id),
            MapiCheckpointKind::Content,
            42,
            7,
            serde_json::json!({"last": "message"}),
        )
        .await
        .unwrap();
    let fetched = store
        .fetch_mapi_sync_checkpoint(
            account.account_id,
            Some(mailbox.id),
            MapiCheckpointKind::Content,
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(fetched, checkpoint);
}

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
    queried_jmap_email_ids: Arc<AtomicU64>,
    created_mailboxes: Arc<Mutex<Vec<JmapMailboxCreateInput>>>,
    destroyed_mailboxes: Arc<Mutex<Vec<Uuid>>>,
    directory_accounts: Arc<Mutex<Vec<AuthenticatedAccount>>>,
    mapi_identities: Arc<Mutex<HashMap<Uuid, u64>>>,
    mapi_checkpoints: Arc<Mutex<HashMap<(Option<Uuid>, MapiCheckpointKind), MapiSyncCheckpoint>>>,
    mapi_sync_changes: Arc<Mutex<MapiSyncChangeSet>>,
    mapi_folder_permissions: Arc<Mutex<Vec<MapiFolderPermission>>>,
    mapi_notification_cursor: Arc<Mutex<Option<i64>>>,
    mapi_notification_polls: Arc<Mutex<Vec<MapiNotificationPoll>>>,
    next_mapi_global_counter: Arc<Mutex<u64>>,
    omit_principal_from_directory: bool,
    mapi_mail_store_load_started: Option<Arc<tokio::sync::Notify>>,
    mapi_mail_store_load_continue: Option<Arc<tokio::sync::Notify>>,
}

fn display_to_for_test(email: &JmapEmail) -> String {
    email
        .to
        .iter()
        .map(|address| {
            address
                .display_name
                .as_deref()
                .unwrap_or(&address.address)
                .to_string()
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn test_message_flags(email: &JmapEmail) -> u32 {
    let mut flags = 0u32;
    if !email.unread {
        flags |= 0x0000_0001;
    }
    if email.has_attachments {
        flags |= 0x0000_0010;
    }
    flags
}

fn jmap_search_matches(email: &JmapEmail, search_text: &str) -> bool {
    let needle = search_text.trim().to_ascii_lowercase();
    if needle.is_empty() {
        return true;
    }

    let contains = |value: &str| value.to_ascii_lowercase().contains(&needle);
    contains(&email.subject)
        || contains(&email.preview)
        || contains(&email.body_text)
        || contains(&email.from_address)
        || email.from_display.as_deref().is_some_and(contains)
        || email.sender_address.as_deref().is_some_and(contains)
        || email.sender_display.as_deref().is_some_and(contains)
        || email.to.iter().chain(email.cc.iter()).any(|recipient| {
            contains(&recipient.address) || recipient.display_name.as_deref().is_some_and(contains)
        })
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
            tenant_id: Uuid::from_u128(0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa),
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
            parent_id: None,
            role: role.to_string(),
            name: name.to_string(),
            sort_order: 40,
            modseq: 40,
            total_emails: 0,
            unread_emails: 0,
            is_subscribed: true,
        }
    }

    fn email(id: &str, mailbox_id: &str, mailbox_role: &str, subject: &str) -> JmapEmail {
        let account = Self::account();
        let mailbox_id = Uuid::parse_str(mailbox_id).unwrap();
        JmapEmail {
            id: Uuid::parse_str(id).unwrap(),
            thread_id: Uuid::parse_str("12121212-1212-1212-1212-121212121212").unwrap(),
            mailbox_id,
            mailbox_role: mailbox_role.to_string(),
            mailbox_name: "RCA Sync".to_string(),
            modseq: 41,
            mailbox_ids: vec![mailbox_id],
            mailbox_states: vec![JmapEmailMailboxState {
                mailbox_id,
                role: mailbox_role.to_string(),
                name: "RCA Sync".to_string(),
                modseq: 41,
                unread: false,
                flagged: false,
                draft: mailbox_role == "drafts",
            }],
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

    fn fake_mapi_identity_lookup_for_object_id(
        &self,
        object_id: u64,
    ) -> Option<MapiIdentityLookupRecord> {
        let identities = self.mapi_identities.lock().unwrap().clone();
        let mailbox_match = self
            .mailboxes
            .lock()
            .unwrap()
            .iter()
            .find(|mailbox| {
                identities.get(&mailbox.id).copied() == Some(object_id)
                    || crate::mapi::identity::legacy_migration_object_id(&mailbox.id) == object_id
                    || crate::mapi_store::reserved_folder_counter_for_role(&mailbox.role)
                        .map(crate::mapi::identity::mapi_store_id)
                        == Some(object_id)
            })
            .map(|mailbox| (MapiIdentityObjectKind::Mailbox, mailbox.id));
        let message_match = self
            .emails
            .lock()
            .unwrap()
            .iter()
            .find(|email| {
                identities.get(&email.id).copied() == Some(object_id)
                    || crate::mapi::identity::legacy_migration_object_id(&email.id) == object_id
            })
            .map(|email| (MapiIdentityObjectKind::Message, email.id));
        let contact_match = self
            .contacts
            .lock()
            .unwrap()
            .iter()
            .find(|contact| {
                identities.get(&contact.id).copied() == Some(object_id)
                    || crate::mapi::identity::legacy_migration_object_id(&contact.id) == object_id
            })
            .map(|contact| (MapiIdentityObjectKind::Contact, contact.id));
        let event_match = self
            .events
            .lock()
            .unwrap()
            .iter()
            .find(|event| {
                identities.get(&event.id).copied() == Some(object_id)
                    || crate::mapi::identity::legacy_migration_object_id(&event.id) == object_id
            })
            .map(|event| (MapiIdentityObjectKind::CalendarEvent, event.id));
        let task_match = self
            .tasks
            .lock()
            .unwrap()
            .iter()
            .find(|task| {
                identities.get(&task.id).copied() == Some(object_id)
                    || crate::mapi::identity::legacy_migration_object_id(&task.id) == object_id
            })
            .map(|task| (MapiIdentityObjectKind::Task, task.id));

        let (object_kind, canonical_id) = mailbox_match
            .or(message_match)
            .or(contact_match)
            .or(event_match)
            .or(task_match)?;
        Some(MapiIdentityLookupRecord {
            object_kind,
            canonical_id,
            object_id,
            source_key: crate::mapi::identity::source_key_for_object_id(object_id),
        })
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
        _tenant_id: &'a Uuid,
        _entry: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move { Ok(()) })
    }
}

impl ExchangeStore for FakeStore {
    fn fetch_or_allocate_mapi_identities<'a>(
        &'a self,
        _account_id: Uuid,
        requests: &'a [MapiIdentityRequest],
    ) -> StoreFuture<'a, Vec<MapiIdentityRecord>> {
        Box::pin(async move {
            let mut identities = self.mapi_identities.lock().unwrap();
            let mut next_counter = self.next_mapi_global_counter.lock().unwrap();
            if *next_counter < crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER {
                *next_counter = crate::mapi::identity::FIRST_DYNAMIC_GLOBAL_COUNTER;
            }
            let mut records = Vec::with_capacity(requests.len());
            for request in requests {
                let object_id = if let Some(existing) = identities.get(&request.canonical_id) {
                    *existing
                } else {
                    let counter = request.reserved_global_counter.unwrap_or_else(|| {
                        if request.object_kind == MapiIdentityObjectKind::Account {
                            let value = *next_counter;
                            *next_counter = next_counter.saturating_add(1);
                            value
                        } else {
                            crate::mapi::identity::global_counter_from_store_id(
                                crate::mapi::identity::legacy_migration_object_id(
                                    &request.canonical_id,
                                ),
                            )
                            .unwrap_or_else(|| {
                                let value = *next_counter;
                                *next_counter = next_counter.saturating_add(1);
                                value
                            })
                        }
                    });
                    let object_id = crate::mapi::identity::mapi_store_id(counter);
                    identities.insert(request.canonical_id, object_id);
                    object_id
                };
                records.push(MapiIdentityRecord {
                    canonical_id: request.canonical_id,
                    object_id,
                });
            }
            Ok(records)
        })
    }

    fn fetch_mapi_identities_by_object_ids<'a>(
        &'a self,
        _account_id: Uuid,
        object_ids: &'a [u64],
    ) -> StoreFuture<'a, Vec<MapiIdentityLookupRecord>> {
        let records = object_ids
            .iter()
            .filter_map(|object_id| self.fake_mapi_identity_lookup_for_object_id(*object_id))
            .collect::<Vec<_>>();
        Box::pin(async move { Ok(records) })
    }

    fn fetch_mapi_identities_by_source_keys<'a>(
        &'a self,
        _account_id: Uuid,
        source_keys: &'a [Vec<u8>],
    ) -> StoreFuture<'a, Vec<MapiIdentityLookupRecord>> {
        let records = source_keys
            .iter()
            .filter_map(|source_key| {
                crate::mapi::identity::object_id_from_source_key(source_key)
                    .and_then(|object_id| self.fake_mapi_identity_lookup_for_object_id(object_id))
            })
            .collect::<Vec<_>>();
        Box::pin(async move { Ok(records) })
    }

    fn fetch_mapi_sync_checkpoint<'a>(
        &'a self,
        _account_id: Uuid,
        mailbox_id: Option<Uuid>,
        checkpoint_kind: MapiCheckpointKind,
    ) -> StoreFuture<'a, Option<MapiSyncCheckpoint>> {
        let checkpoint = self
            .mapi_checkpoints
            .lock()
            .unwrap()
            .get(&(mailbox_id, checkpoint_kind))
            .cloned();
        Box::pin(async move { Ok(checkpoint) })
    }

    fn store_mapi_sync_checkpoint<'a>(
        &'a self,
        _account_id: Uuid,
        mailbox_id: Option<Uuid>,
        checkpoint_kind: MapiCheckpointKind,
        last_change_sequence: u64,
        last_modseq: u64,
        cursor_json: serde_json::Value,
    ) -> StoreFuture<'a, MapiSyncCheckpoint> {
        let checkpoint = MapiSyncCheckpoint {
            mailbox_id,
            checkpoint_kind,
            last_change_sequence,
            last_modseq,
            cursor_json,
        };
        self.mapi_checkpoints
            .lock()
            .unwrap()
            .insert((mailbox_id, checkpoint_kind), checkpoint.clone());
        Box::pin(async move { Ok(checkpoint) })
    }

    fn fetch_mapi_sync_changes<'a>(
        &'a self,
        _account_id: Uuid,
        _mailbox_id: Option<Uuid>,
        _checkpoint_kind: MapiCheckpointKind,
        _after_change_sequence: u64,
    ) -> StoreFuture<'a, MapiSyncChangeSet> {
        let changes = self.mapi_sync_changes.lock().unwrap().clone();
        Box::pin(async move { Ok(changes) })
    }

    fn fetch_mapi_folder_permissions<'a>(
        &'a self,
        account_id: Uuid,
        mailbox_ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<MapiFolderPermission>> {
        let mut permissions = self.mapi_folder_permissions.lock().unwrap().clone();
        if permissions.is_empty() {
            let principal = self.session.clone().unwrap_or_else(FakeStore::account);
            permissions.extend(mailbox_ids.iter().copied().map(|mailbox_id| {
                crate::mapi::permissions::owner_permission(
                    mailbox_id,
                    &AccountPrincipal {
                        tenant_id: principal.tenant_id,
                        account_id,
                        email: principal.email.clone(),
                        display_name: principal.display_name.clone(),
                    },
                )
            }));
        }
        Box::pin(async move { Ok(permissions) })
    }

    fn fetch_mapi_notification_cursor<'a>(
        &'a self,
        _account_id: Uuid,
    ) -> StoreFuture<'a, Option<i64>> {
        let cursor = *self.mapi_notification_cursor.lock().unwrap();
        Box::pin(async move { Ok(cursor) })
    }

    fn poll_mapi_notifications<'a>(
        &'a self,
        _account_id: Uuid,
        _after_cursor: i64,
    ) -> StoreFuture<'a, MapiNotificationPoll> {
        let poll = self
            .mapi_notification_polls
            .lock()
            .unwrap()
            .pop()
            .unwrap_or(MapiNotificationPoll {
                event_pending: false,
                cursor: None,
            });
        Box::pin(async move { Ok(poll) })
    }

    fn fetch_address_book_entries<'a>(
        &'a self,
        principal: &'a AccountPrincipal,
    ) -> StoreFuture<'a, Vec<ExchangeAddressBookEntry>> {
        let principal_account = self.session.clone().filter(|account| {
            account.tenant_id == principal.tenant_id && account.account_id == principal.account_id
        });
        let mut accounts = self.directory_accounts.lock().unwrap().clone();
        if let Some(principal) = &principal_account {
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
        let principal_account_id = principal_account
            .as_ref()
            .map(|account| account.account_id)
            .unwrap_or_default();
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
            uid: input.uid,
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
        let load_started = self.mapi_mail_store_load_started.clone();
        let load_continue = self.mapi_mail_store_load_continue.clone();
        Box::pin(async move {
            if let Some(load_started) = load_started {
                load_started.notify_one();
            }
            if let Some(load_continue) = load_continue {
                load_continue.notified().await;
            }
            Ok(mailboxes)
        })
    }

    fn ensure_jmap_system_mailboxes<'a>(
        &'a self,
        account_id: Uuid,
    ) -> StoreFuture<'a, Vec<JmapMailbox>> {
        self.fetch_jmap_mailboxes(account_id)
    }

    fn create_jmap_mailbox<'a>(
        &'a self,
        input: JmapMailboxCreateInput,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, JmapMailbox> {
        self.created_mailboxes.lock().unwrap().push(input.clone());
        let mailbox = JmapMailbox {
            id: Uuid::parse_str("44444444-4444-4444-4444-444444444444").unwrap(),
            parent_id: input.parent_id,
            role: "custom".to_string(),
            name: input.name,
            sort_order: input.sort_order.unwrap_or(40),
            modseq: 40,
            total_emails: 0,
            unread_emails: 0,
            is_subscribed: input.is_subscribed,
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
        search_text: Option<&'a str>,
        _position: u64,
        _limit: u64,
    ) -> StoreFuture<'a, JmapEmailQuery> {
        self.queried_jmap_email_ids.fetch_add(1, Ordering::SeqCst);
        let ids = self
            .emails
            .lock()
            .unwrap()
            .iter()
            .filter(|email| mailbox_id.map_or(true, |mailbox_id| email.mailbox_id == mailbox_id))
            .filter(|email| {
                search_text.map_or(true, |search_text| jmap_search_matches(email, search_text))
            })
            .map(|email| email.id)
            .collect::<Vec<_>>();
        Box::pin(async move {
            Ok(JmapEmailQuery {
                total: ids.len() as u64,
                ids,
            })
        })
    }

    fn query_mapi_content_table_ids<'a>(
        &'a self,
        _account_id: Uuid,
        query: MapiContentTableQuery,
    ) -> StoreFuture<'a, MapiContentTableQueryResult> {
        let mut emails = self
            .emails
            .lock()
            .unwrap()
            .iter()
            .filter(|email| email.mailbox_id == query.mailbox_id)
            .cloned()
            .collect::<Vec<_>>();
        if !query.sort_orders.is_empty() {
            emails.sort_by(|left, right| {
                for sort in &query.sort_orders {
                    let ordering = match sort.field {
                        MapiContentTableSortField::ReceivedAt => {
                            left.received_at.cmp(&right.received_at)
                        }
                        MapiContentTableSortField::Subject => left
                            .subject
                            .to_ascii_lowercase()
                            .cmp(&right.subject.to_ascii_lowercase()),
                        MapiContentTableSortField::SenderName => left
                            .from_display
                            .as_deref()
                            .unwrap_or(&left.from_address)
                            .to_ascii_lowercase()
                            .cmp(
                                &right
                                    .from_display
                                    .as_deref()
                                    .unwrap_or(&right.from_address)
                                    .to_ascii_lowercase(),
                            ),
                        MapiContentTableSortField::SenderEmail => left
                            .from_address
                            .to_ascii_lowercase()
                            .cmp(&right.from_address.to_ascii_lowercase()),
                        MapiContentTableSortField::DisplayTo => display_to_for_test(left)
                            .to_ascii_lowercase()
                            .cmp(&display_to_for_test(right).to_ascii_lowercase()),
                        MapiContentTableSortField::MessageSize => {
                            left.size_octets.cmp(&right.size_octets)
                        }
                        MapiContentTableSortField::HasAttachments => {
                            left.has_attachments.cmp(&right.has_attachments)
                        }
                        MapiContentTableSortField::MessageFlags => {
                            test_message_flags(left).cmp(&test_message_flags(right))
                        }
                    };
                    let ordering = if sort.descending {
                        ordering.reverse()
                    } else {
                        ordering
                    };
                    if ordering != std::cmp::Ordering::Equal {
                        return ordering;
                    }
                }
                right.id.cmp(&left.id)
            });
        }
        let total = emails.len() as u64;
        let ids = emails
            .into_iter()
            .skip(query.position as usize)
            .take(query.limit as usize)
            .map(|email| email.id)
            .collect();
        Box::pin(async move { Ok(MapiContentTableQueryResult { ids, total }) })
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
            .map(|mut email| {
                email.bcc.clear();
                email
            })
            .collect();
        Box::pin(async move { Ok(emails) })
    }

    fn fetch_jmap_emails_with_protected_bcc<'a>(
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

    fn move_jmap_email_from_mailbox<'a>(
        &'a self,
        account_id: Uuid,
        _source_mailbox_id: Uuid,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail> {
        self.move_jmap_email(account_id, message_id, target_mailbox_id, audit)
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

    fn delete_jmap_email_from_mailbox<'a>(
        &'a self,
        account_id: Uuid,
        _mailbox_id: Uuid,
        message_id: Uuid,
        audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        self.delete_jmap_email(account_id, message_id, audit)
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
        sent.mailbox_ids = vec![sent_mailbox_id];
        sent.mailbox_states = vec![JmapEmailMailboxState {
            mailbox_id: sent_mailbox_id,
            role: "sent".to_string(),
            name: sent.mailbox_name.clone(),
            modseq: sent.modseq,
            unread: sent.unread,
            flagged: sent.flagged,
            draft: false,
        }];

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
    insert_mapi_content_length(&mut headers);
    headers.insert(
        "x-requesttype",
        HeaderValue::from_str(request_type).unwrap(),
    );
    headers.insert(
        "x-requestid",
        HeaderValue::from_str(&mapi_request_id()).unwrap(),
    );
    headers.insert(
        "x-clientinfo",
        HeaderValue::from_str(&mapi_client_info()).unwrap(),
    );
    headers.insert("host", HeaderValue::from_static("mail.example.test"));
    headers
}

fn insert_mapi_content_length(headers: &mut HeaderMap) {
    headers.insert(
        axum::http::header::CONTENT_LENGTH,
        HeaderValue::from_static("0"),
    );
}

fn renew_mapi_request_id(headers: &mut HeaderMap) {
    headers.insert(
        "x-requestid",
        HeaderValue::from_str(&mapi_request_id()).unwrap(),
    );
}

fn mapi_request_id() -> String {
    format!(
        "{{11111111-2222-3333-4444-555555555555}}:{}",
        MAPI_TEST_REQUEST_ID.fetch_add(1, Ordering::Relaxed)
    )
}

fn mapi_client_info() -> String {
    format!(
        "{{aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee}}:{}",
        MAPI_TEST_REQUEST_ID.fetch_add(1, Ordering::Relaxed)
    )
}

fn mapi_headers_without_content_type(request_type: &str) -> HeaderMap {
    let mut headers = bearer_headers();
    insert_mapi_content_length(&mut headers);
    headers.insert(
        "x-requesttype",
        HeaderValue::from_str(request_type).unwrap(),
    );
    headers.insert(
        "x-requestid",
        HeaderValue::from_str(&mapi_request_id()).unwrap(),
    );
    headers.insert(
        "x-clientinfo",
        HeaderValue::from_str(&mapi_client_info()).unwrap(),
    );
    headers.insert("host", HeaderValue::from_static("mail.example.test"));
    headers
}

fn mapi_headers_with_content_type(request_type: &str, content_type: &'static str) -> HeaderMap {
    let mut headers = bearer_headers();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static(content_type),
    );
    insert_mapi_content_length(&mut headers);
    headers.insert(
        "x-requesttype",
        HeaderValue::from_str(request_type).unwrap(),
    );
    headers.insert(
        "x-requestid",
        HeaderValue::from_str(&mapi_request_id()).unwrap(),
    );
    headers.insert(
        "x-clientinfo",
        HeaderValue::from_str(&mapi_client_info()).unwrap(),
    );
    headers.insert("host", HeaderValue::from_static("mail.example.test"));
    headers
}

fn mapi_headers_without_request_id(request_type: &str) -> HeaderMap {
    let mut headers = bearer_headers();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/mapi-http"),
    );
    insert_mapi_content_length(&mut headers);
    headers.insert(
        "x-requesttype",
        HeaderValue::from_str(request_type).unwrap(),
    );
    headers.insert(
        "x-clientinfo",
        HeaderValue::from_str(&mapi_client_info()).unwrap(),
    );
    headers.insert("host", HeaderValue::from_static("mail.example.test"));
    headers
}

fn mapi_headers_without_request_type() -> HeaderMap {
    let mut headers = bearer_headers();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/mapi-http"),
    );
    insert_mapi_content_length(&mut headers);
    headers.insert(
        "x-requestid",
        HeaderValue::from_str(&mapi_request_id()).unwrap(),
    );
    headers.insert(
        "x-clientinfo",
        HeaderValue::from_str(&mapi_client_info()).unwrap(),
    );
    headers.insert("host", HeaderValue::from_static("mail.example.test"));
    headers
}

fn mapi_headers_without_client_info(request_type: &str) -> HeaderMap {
    let mut headers = bearer_headers();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/mapi-http"),
    );
    insert_mapi_content_length(&mut headers);
    headers.insert(
        "x-requesttype",
        HeaderValue::from_str(request_type).unwrap(),
    );
    headers.insert(
        "x-requestid",
        HeaderValue::from_str(&mapi_request_id()).unwrap(),
    );
    headers.insert("host", HeaderValue::from_static("mail.example.test"));
    headers
}

fn mapi_headers_with_request_id(request_type: &str, request_id: &'static str) -> HeaderMap {
    let mut headers = mapi_headers(request_type);
    headers.insert("x-requestid", HeaderValue::from_static(request_id));
    headers
}

fn mapi_headers_with_client_info(request_type: &str, client_info: &'static str) -> HeaderMap {
    let mut headers = mapi_headers(request_type);
    headers.insert("x-clientinfo", HeaderValue::from_static(client_info));
    headers
}

fn mapi_headers_without_host(request_type: &str) -> HeaderMap {
    let mut headers = mapi_headers(request_type);
    headers.remove("host");
    headers
}

fn mapi_headers_without_content_length(request_type: &str) -> HeaderMap {
    let mut headers = mapi_headers(request_type);
    headers.remove(axum::http::header::CONTENT_LENGTH);
    headers
}

fn mapi_headers_with_content_length(request_type: &str, content_length: &'static str) -> HeaderMap {
    let mut headers = mapi_headers(request_type);
    headers.insert(
        axum::http::header::CONTENT_LENGTH,
        HeaderValue::from_static(content_length),
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

fn mapi_cookie_header(response: &axum::response::Response) -> String {
    response
        .headers()
        .get_all("set-cookie")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .filter_map(|value| value.split(';').next())
        .collect::<Vec<_>>()
        .join("; ")
}

fn mapi_cookie_header_with_mismatched_sequence(response: &axum::response::Response) -> String {
    mapi_cookie_header(response)
        .split("; ")
        .map(|part| {
            if part.starts_with("MapiSequence=") {
                "MapiSequence=00000000-0000-0000-0000-000000000000".to_string()
            } else {
                part.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("; ")
}

async fn nspi_bound_headers(service: &ExchangeService<FakeStore>, request_type: &str) -> HeaderMap {
    let bind = service
        .handle_mapi(MapiEndpoint::Nspi, &mapi_headers("Bind"), b"")
        .await
        .unwrap();
    let mut headers = mapi_headers(request_type);
    headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&bind)).unwrap(),
    );
    headers
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

fn mapi_sync_manifest_counts(bytes: &[u8]) -> Option<(u32, u32)> {
    let change_marker = 0x4012_0003u32.to_le_bytes();
    let message_marker = 0x4015_0003u32.to_le_bytes();
    let state_marker = 0x403A_0003u32.to_le_bytes();
    let end_marker = 0x4014_0003u32.to_le_bytes();
    let mut folder_count = 0;
    let mut message_count = 0;
    let mut offset = 0;
    while offset + 4 <= bytes.len() {
        if bytes[offset..offset + 4] == change_marker {
            let next_change = bytes[offset + 4..]
                .windows(4)
                .position(|window| {
                    window == change_marker || window == state_marker || window == end_marker
                })
                .map(|position| offset + 4 + position)
                .unwrap_or(bytes.len());
            if bytes[offset + 4..next_change]
                .windows(4)
                .any(|window| window == message_marker)
            {
                message_count += 1;
            } else {
                folder_count += 1;
            }
            offset = next_change;
            continue;
        }
        offset += 1;
    }
    if folder_count == 0 && message_count == 0 {
        None
    } else {
        Some((folder_count, message_count))
    }
}

fn assert_content_final_state_includes(bytes: &[u8], message_ids: &[Uuid], change_numbers: &[u64]) {
    let idset_given = mapi_binary_property_value(bytes, META_TAG_IDSET_GIVEN);
    for message_id in message_ids {
        assert!(
            strict_replguid_globset_contains_counter(
                idset_given,
                &globcnt_bytes(mapi_message_global_counter(message_id))
            )
            .unwrap(),
            "final MetaTagIdsetGiven missing message {message_id}"
        );
    }

    for tag in [
        META_TAG_CNSET_SEEN,
        META_TAG_CNSET_SEEN_FAI,
        META_TAG_CNSET_READ,
    ] {
        let cnset = mapi_binary_property_value(bytes, tag);
        for change_number in change_numbers {
            assert!(
                strict_replguid_globset_contains_counter(cnset, &globcnt_bytes(*change_number))
                    .unwrap(),
                "final content CNSET 0x{tag:08x} missing change {change_number}"
            );
        }
    }
}

fn mapi_binary_property_value(bytes: &[u8], property_tag: u32) -> &[u8] {
    let tag = property_tag.to_le_bytes();
    let offset = bytes
        .windows(tag.len())
        .position(|window| window == tag)
        .expect("MAPI binary property is present");
    let length = u32::from_le_bytes(bytes[offset + 4..offset + 8].try_into().unwrap()) as usize;
    &bytes[offset + 8..offset + 8 + length]
}

const FX_INCR_SYNC_CHG: u32 = 0x4012_0003;
const FX_INCR_SYNC_END: u32 = 0x4014_0003;
const FX_INCR_SYNC_STATE_BEGIN: u32 = 0x403A_0003;
const FX_INCR_SYNC_STATE_END: u32 = 0x403B_0003;
const PID_TAG_DISPLAY_NAME_W: u32 = 0x3001_001F;
const PID_TAG_CONTENT_COUNT: u32 = 0x3602_0003;
const PID_TAG_CONTENT_UNREAD_COUNT: u32 = 0x3603_0003;
const PID_TAG_SUBFOLDERS: u32 = 0x360A_000B;
const PID_TAG_CONTAINER_CLASS_W: u32 = 0x3613_001F;
const PID_TAG_LAST_MODIFICATION_TIME: u32 = 0x3008_0040;
const PID_TAG_LOCAL_COMMIT_TIME_MAX: u32 = 0x670A_0040;
const PID_TAG_SOURCE_KEY: u32 = 0x65E0_0102;
const PID_TAG_PARENT_SOURCE_KEY: u32 = 0x65E1_0102;
const PID_TAG_CHANGE_KEY: u32 = 0x65E2_0102;
const PID_TAG_PREDECESSOR_CHANGE_LIST: u32 = 0x65E3_0102;
const PID_TAG_MID: u32 = 0x674A_0014;
const META_TAG_IDSET_GIVEN: u32 = 0x4017_0102;
const META_TAG_IDSET_DELETED: u32 = 0x4018_0102;
const META_TAG_CNSET_SEEN: u32 = 0x6796_0102;
const META_TAG_CNSET_SEEN_FAI: u32 = 0x67DA_0102;
const META_TAG_CNSET_READ: u32 = 0x67D2_0102;

#[derive(Debug)]
struct StrictHierarchySyncStream {
    folder_changes: Vec<StrictHierarchyFolderChange>,
    idset_given: Vec<u8>,
    cnset_seen: Vec<u8>,
}

#[derive(Debug)]
struct StrictHierarchyFolderChange {
    source_key: Vec<u8>,
    parent_source_key: Vec<u8>,
    change_key: Vec<u8>,
    display_name: String,
    content_count: Option<u32>,
    content_unread_count: Option<u32>,
    local_commit_time_max: Option<u64>,
}

#[derive(Debug, Default)]
struct StrictHierarchyFolderBuilder {
    tags: Vec<u32>,
    source_key: Option<Vec<u8>>,
    parent_source_key: Option<Vec<u8>>,
    change_key: Option<Vec<u8>>,
    display_name: Option<String>,
    content_count: Option<u32>,
    content_unread_count: Option<u32>,
    local_commit_time_max: Option<u64>,
}

struct StrictFastTransferProperty {
    tag: u32,
    value: Vec<u8>,
    next_offset: usize,
}

fn strict_hierarchy_sync_transfer_from_response(
    response_rops: &[u8],
) -> Result<StrictHierarchySyncStream, String> {
    let chunks = mapi_fast_transfer_chunks(response_rops);
    if chunks.len() != 1 {
        return Err(format!(
            "expected one completed FastTransfer chunk, got {}",
            chunks.len()
        ));
    }
    if chunks[0].0 != 0x0003 {
        return Err(format!(
            "expected completed FastTransfer status 0x0003, got 0x{:04x}",
            chunks[0].0
        ));
    }
    strict_decode_hierarchy_sync_stream(&chunks[0].1)
}

fn strict_decode_hierarchy_sync_stream(bytes: &[u8]) -> Result<StrictHierarchySyncStream, String> {
    let mut offset = 0;
    let mut current_folder: Option<StrictHierarchyFolderBuilder> = None;
    let mut folder_changes = Vec::new();
    let mut seen_source_keys: Vec<Vec<u8>> = vec![
        mapi_mailstore::source_key_for_store_id(crate::mapi::identity::ROOT_FOLDER_ID),
        mapi_mailstore::source_key_for_store_id(crate::mapi::identity::IPM_SUBTREE_FOLDER_ID),
    ];
    let mut in_state = false;
    let mut state_closed = false;
    let mut idset_given = None;
    let mut cnset_seen = None;

    while offset < bytes.len() {
        let tag = read_strict_u32(bytes, offset)?;
        if strict_hierarchy_marker(tag) {
            match tag {
                FX_INCR_SYNC_CHG => {
                    if in_state || state_closed {
                        return Err(
                            "folderChange marker appears after final ICS state starts".into()
                        );
                    }
                    if let Some(folder) = current_folder.take() {
                        strict_finish_folder_change(
                            folder,
                            &mut seen_source_keys,
                            &mut folder_changes,
                        )?;
                    }
                    current_folder = Some(StrictHierarchyFolderBuilder::default());
                }
                FX_INCR_SYNC_STATE_BEGIN => {
                    if let Some(folder) = current_folder.take() {
                        strict_finish_folder_change(
                            folder,
                            &mut seen_source_keys,
                            &mut folder_changes,
                        )?;
                    }
                    if in_state || state_closed {
                        return Err("duplicate final ICS state boundary".into());
                    }
                    in_state = true;
                }
                FX_INCR_SYNC_STATE_END => {
                    if !in_state {
                        return Err("IncrSyncStateEnd without IncrSyncStateBegin".into());
                    }
                    if idset_given.is_none() || cnset_seen.is_none() {
                        return Err("final ICS state is missing hierarchy IDSET or CNSET".into());
                    }
                    in_state = false;
                    state_closed = true;
                }
                FX_INCR_SYNC_END => {
                    if current_folder.is_some() {
                        return Err("IncrSyncEnd appears inside an open folderChange".into());
                    }
                    if !state_closed {
                        return Err("IncrSyncEnd appears before final ICS state is complete".into());
                    }
                    offset += 4;
                    if offset != bytes.len() {
                        return Err("trailing bytes after IncrSyncEnd".into());
                    }
                    break;
                }
                _ => unreachable!(),
            }
            offset += 4;
            continue;
        }

        let property = strict_parse_fast_transfer_property(bytes, offset)?;
        offset = property.next_offset;
        if let Some(folder) = current_folder.as_mut() {
            strict_record_folder_property(folder, property)?;
        } else if in_state {
            match property.tag {
                META_TAG_IDSET_GIVEN => {
                    if idset_given.replace(property.value).is_some() {
                        return Err("duplicate MetaTagIdsetGiven in final ICS state".into());
                    }
                }
                META_TAG_CNSET_SEEN => {
                    if cnset_seen.replace(property.value).is_some() {
                        return Err("duplicate MetaTagCnsetSeen in final ICS state".into());
                    }
                }
                tag => {
                    return Err(format!(
                        "unexpected property 0x{tag:08x} in hierarchy final ICS state"
                    ));
                }
            }
        } else {
            return Err(format!(
                "property 0x{:08x} appears outside folderChange or final state",
                property.tag
            ));
        }
    }

    if offset != bytes.len() {
        return Err("FastTransfer stream ended on a partial atom".into());
    }
    if folder_changes.is_empty() {
        return Err("hierarchy sync stream contained no folderChange rows".into());
    }
    let idset_given = idset_given.ok_or("missing MetaTagIdsetGiven")?;
    let cnset_seen = cnset_seen.ok_or("missing MetaTagCnsetSeen")?;
    strict_validate_replguid_globset(&idset_given)?;
    strict_validate_replguid_globset(&cnset_seen)?;
    for folder in &folder_changes {
        strict_validate_source_or_change_key(&folder.source_key)?;
        strict_validate_source_or_change_key(&folder.change_key)?;
        if !strict_replguid_globset_contains_counter(&idset_given, &folder.source_key[16..22])? {
            return Err(format!(
                "final MetaTagIdsetGiven does not include folder {}",
                folder.display_name
            ));
        }
        if !strict_replguid_globset_contains_counter(&cnset_seen, &folder.change_key[16..22])? {
            return Err(format!(
                "final MetaTagCnsetSeen does not include folder {} change key",
                folder.display_name
            ));
        }
    }

    Ok(StrictHierarchySyncStream {
        folder_changes,
        idset_given,
        cnset_seen,
    })
}

fn strict_hierarchy_marker(tag: u32) -> bool {
    matches!(
        tag,
        FX_INCR_SYNC_CHG | FX_INCR_SYNC_STATE_BEGIN | FX_INCR_SYNC_STATE_END | FX_INCR_SYNC_END
    )
}

fn strict_parse_fast_transfer_property(
    bytes: &[u8],
    offset: usize,
) -> Result<StrictFastTransferProperty, String> {
    let tag = read_strict_u32(bytes, offset)?;
    let property_type = tag & 0x0000_FFFF;
    let value_start = offset + 4;
    let (value_start, value_len) = match property_type {
        0x0002 => (value_start, 2),
        0x0003 => (value_start, 4),
        0x000B => {
            let value = read_strict_slice(bytes, value_start, 2)?;
            if value != [0, 0] && value != [1, 0] {
                return Err(format!(
                    "PtypBoolean property 0x{tag:08x} has invalid FastTransfer value"
                ));
            }
            (value_start, 2)
        }
        0x0014 | 0x0040 => (value_start, 8),
        0x001E | 0x001F | 0x0102 => {
            let len = read_strict_u32(bytes, value_start)? as usize;
            let value_start = value_start + 4;
            if property_type == 0x001E {
                let value = read_strict_slice(bytes, value_start, len)?;
                if value.is_empty() || value.last() != Some(&0) {
                    return Err(format!(
                        "PtypString8 property 0x{tag:08x} is not null-terminated"
                    ));
                }
            }
            if property_type == 0x001F {
                let value = read_strict_slice(bytes, value_start, len)?;
                if value.len() < 2 || value.len() % 2 != 0 || value[value.len() - 2..] != [0, 0] {
                    return Err(format!("PtypString property 0x{tag:08x} is not UTF-16Z"));
                }
            }
            (value_start, len)
        }
        _ => {
            return Err(format!(
                "unsupported FastTransfer property type in 0x{tag:08x}"
            ))
        }
    };
    let value = read_strict_slice(bytes, value_start, value_len)?.to_vec();
    Ok(StrictFastTransferProperty {
        tag,
        value,
        next_offset: value_start + value_len,
    })
}

fn strict_record_folder_property(
    folder: &mut StrictHierarchyFolderBuilder,
    property: StrictFastTransferProperty,
) -> Result<(), String> {
    if property.tag == PID_TAG_MID {
        return Err("message change property appears inside hierarchy folderChange".into());
    }
    if folder.tags.contains(&property.tag) {
        return Err(format!(
            "duplicate property 0x{:08x} inside folderChange",
            property.tag
        ));
    }
    folder.tags.push(property.tag);
    match property.tag {
        PID_TAG_PARENT_SOURCE_KEY => folder.parent_source_key = Some(property.value),
        PID_TAG_SOURCE_KEY => folder.source_key = Some(property.value),
        PID_TAG_CHANGE_KEY => folder.change_key = Some(property.value),
        PID_TAG_DISPLAY_NAME_W => {
            folder.display_name = Some(strict_decode_utf16z(&property.value)?)
        }
        PID_TAG_CONTENT_COUNT => {
            folder.content_count = Some(strict_decode_u32_property(&property)?);
        }
        PID_TAG_CONTENT_UNREAD_COUNT => {
            folder.content_unread_count = Some(strict_decode_u32_property(&property)?);
        }
        PID_TAG_LOCAL_COMMIT_TIME_MAX => {
            folder.local_commit_time_max = Some(strict_decode_u64_property(&property)?);
        }
        PID_TAG_SUBFOLDERS => {
            if property.value.len() != 2 {
                return Err("PidTagSubfolders was not encoded as a two-byte PtypBoolean".into());
            }
        }
        PID_TAG_CONTAINER_CLASS_W => {
            let _ = strict_decode_utf16z(&property.value)?;
        }
        _ => {}
    }
    Ok(())
}

fn strict_decode_u32_property(property: &StrictFastTransferProperty) -> Result<u32, String> {
    if property.value.len() != 4 {
        return Err(format!(
            "property 0x{:08x} was not encoded as a four-byte integer",
            property.tag
        ));
    }
    Ok(u32::from_le_bytes(
        property.value.as_slice().try_into().unwrap(),
    ))
}

fn strict_decode_u64_property(property: &StrictFastTransferProperty) -> Result<u64, String> {
    if property.value.len() != 8 {
        return Err(format!(
            "property 0x{:08x} was not encoded as an eight-byte integer",
            property.tag
        ));
    }
    Ok(u64::from_le_bytes(
        property.value.as_slice().try_into().unwrap(),
    ))
}

fn strict_finish_folder_change(
    folder: StrictHierarchyFolderBuilder,
    seen_source_keys: &mut Vec<Vec<u8>>,
    folder_changes: &mut Vec<StrictHierarchyFolderChange>,
) -> Result<(), String> {
    let required_prefix = [
        PID_TAG_PARENT_SOURCE_KEY,
        PID_TAG_SOURCE_KEY,
        PID_TAG_LAST_MODIFICATION_TIME,
        PID_TAG_CHANGE_KEY,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        PID_TAG_DISPLAY_NAME_W,
    ];
    if folder.tags.len() < required_prefix.len()
        || folder.tags[..required_prefix.len()] != required_prefix
    {
        return Err(format!(
            "folderChange required property prefix was not in documented order: {:x?}",
            folder.tags
        ));
    }
    let source_key = folder
        .source_key
        .ok_or("folderChange missing PidTagSourceKey")?;
    let parent_source_key = folder
        .parent_source_key
        .ok_or("folderChange missing PidTagParentSourceKey")?;
    let change_key = folder
        .change_key
        .ok_or("folderChange missing PidTagChangeKey")?;
    let display_name = folder
        .display_name
        .ok_or("folderChange missing PidTagDisplayName")?;
    strict_validate_source_or_change_key(&source_key)?;
    strict_validate_source_or_change_key(&change_key)?;
    if !parent_source_key.is_empty() {
        strict_validate_source_or_change_key(&parent_source_key)?;
        if !seen_source_keys
            .iter()
            .any(|source_key| source_key.as_slice() == parent_source_key.as_slice())
        {
            return Err(format!(
                "folderChange for {display_name} appeared before its parent folder"
            ));
        }
    }
    seen_source_keys.push(source_key.clone());
    folder_changes.push(StrictHierarchyFolderChange {
        source_key,
        parent_source_key,
        change_key,
        display_name,
        content_count: folder.content_count,
        content_unread_count: folder.content_unread_count,
        local_commit_time_max: folder.local_commit_time_max,
    });
    Ok(())
}

fn strict_decode_utf16z(bytes: &[u8]) -> Result<String, String> {
    if bytes.len() < 2 || bytes.len() % 2 != 0 || bytes[bytes.len() - 2..] != [0, 0] {
        return Err("UTF-16 property is not null-terminated".into());
    }
    let units = bytes[..bytes.len() - 2]
        .chunks_exact(2)
        .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
        .collect::<Vec<_>>();
    String::from_utf16(&units).map_err(|_| "UTF-16 property contains invalid data".into())
}

fn strict_validate_source_or_change_key(value: &[u8]) -> Result<(), String> {
    if value.len() != 22 || !value.starts_with(&mapi_mailstore::STORE_REPLICA_GUID) {
        return Err("source/change key is not a 22-byte REPLGUID-scoped XID".into());
    }
    if value[16..22] == [0; 6] {
        return Err("source/change key contains a zero GLOBCNT".into());
    }
    Ok(())
}

fn strict_validate_replguid_globset(value: &[u8]) -> Result<(), String> {
    let _ = strict_replguid_globset_ranges(value)?;
    Ok(())
}

fn strict_replguid_globset_contains_counter(value: &[u8], counter: &[u8]) -> Result<bool, String> {
    let counter = strict_globcnt_to_u64(counter)?;
    Ok(strict_replguid_globset_ranges(value)?
        .into_iter()
        .any(|(low, high)| low <= counter && counter <= high))
}

fn strict_replguid_globset_ranges(value: &[u8]) -> Result<Vec<(u64, u64)>, String> {
    if value.len() < 17 || !value.starts_with(&mapi_mailstore::STORE_REPLICA_GUID) {
        return Err("REPLGUID-based IDSET/CNSET is missing the store replica GUID".into());
    }
    let mut ranges = Vec::new();
    let mut offset = 16;
    loop {
        let command = *value
            .get(offset)
            .ok_or("REPLGUID-based IDSET/CNSET missing end command")?;
        offset += 1;
        match command {
            0x00 => {
                if offset != value.len() {
                    return Err("trailing bytes after GLOBSET end command".into());
                }
                return Ok(ranges);
            }
            0x52 => {
                let low = strict_globcnt_to_u64(read_strict_slice(value, offset, 6)?)?;
                offset += 6;
                let high = strict_globcnt_to_u64(read_strict_slice(value, offset, 6)?)?;
                offset += 6;
                if low == 0 || high < low {
                    return Err("invalid GLOBSET range".into());
                }
                ranges.push((low, high));
            }
            _ => return Err(format!("unsupported GLOBSET command 0x{command:02x}")),
        }
    }
}

fn strict_globcnt_to_u64(bytes: &[u8]) -> Result<u64, String> {
    if bytes.len() != 6 {
        return Err("GLOBCNT must be six bytes".into());
    }
    Ok(bytes
        .iter()
        .fold(0u64, |value, byte| (value << 8) | u64::from(*byte)))
}

fn read_strict_u32(bytes: &[u8], offset: usize) -> Result<u32, String> {
    let slice = read_strict_slice(bytes, offset, 4)?;
    Ok(u32::from_le_bytes(slice.try_into().unwrap()))
}

fn read_strict_slice(bytes: &[u8], offset: usize, len: usize) -> Result<&[u8], String> {
    bytes
        .get(offset..offset.saturating_add(len))
        .ok_or_else(|| format!("FastTransfer atom at offset {offset} overruns stream"))
}

fn strict_test_xid(counter: u64) -> Vec<u8> {
    let mut value = mapi_mailstore::STORE_REPLICA_GUID.to_vec();
    value.extend_from_slice(&globcnt_bytes(counter));
    value
}

fn strict_test_replguid_globset(counters: &[u64]) -> Vec<u8> {
    let mut value = mapi_mailstore::STORE_REPLICA_GUID.to_vec();
    for counter in counters {
        value.push(0x52);
        value.extend_from_slice(&globcnt_bytes(*counter));
        value.extend_from_slice(&globcnt_bytes(*counter));
    }
    value.push(0);
    value
}

fn strict_test_replid_globset(counters: &[u64]) -> Vec<u8> {
    let mut value = 1u16.to_le_bytes().to_vec();
    for counter in counters {
        value.push(0x52);
        value.extend_from_slice(&globcnt_bytes(*counter));
        value.extend_from_slice(&globcnt_bytes(*counter));
    }
    value.push(0);
    value
}

fn mapi_binary_property(tag: u32, value: &[u8]) -> Vec<u8> {
    let mut property = tag.to_le_bytes().to_vec();
    property.extend_from_slice(&(value.len() as u32).to_le_bytes());
    property.extend_from_slice(value);
    property
}

fn mapi_message_global_counter(id: &Uuid) -> u64 {
    test_mapi_uuid_id(id) >> 16
}

fn mapi_message_cnset_property(tag: u32, changes: &[u64]) -> Vec<u8> {
    mapi_binary_property(tag, &strict_test_replguid_globset(changes))
}

fn mapi_deleted_message_idset_property(ids: &[Uuid]) -> Vec<u8> {
    let counters = ids
        .iter()
        .map(mapi_message_global_counter)
        .collect::<Vec<_>>();
    mapi_binary_property(
        META_TAG_IDSET_DELETED,
        &strict_test_replid_globset(&counters),
    )
}

fn strict_push_binary_property(bytes: &mut Vec<u8>, tag: u32, value: &[u8]) {
    bytes.extend_from_slice(&tag.to_le_bytes());
    bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
    bytes.extend_from_slice(value);
}

fn strict_push_i32_property(bytes: &mut Vec<u8>, tag: u32, value: i32) {
    bytes.extend_from_slice(&tag.to_le_bytes());
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn strict_push_i64_property(bytes: &mut Vec<u8>, tag: u32, value: i64) {
    bytes.extend_from_slice(&tag.to_le_bytes());
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn strict_push_utf16_property(bytes: &mut Vec<u8>, tag: u32, value: &str) {
    bytes.extend_from_slice(&tag.to_le_bytes());
    let value = utf16z(value);
    bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&value);
}

fn strict_push_folder_change(
    bytes: &mut Vec<u8>,
    parent_source_key: &[u8],
    source_counter: u64,
    change_counter: u64,
    name: &str,
    boolean_width: usize,
) {
    bytes.extend_from_slice(&FX_INCR_SYNC_CHG.to_le_bytes());
    strict_push_binary_property(bytes, PID_TAG_PARENT_SOURCE_KEY, parent_source_key);
    strict_push_binary_property(bytes, PID_TAG_SOURCE_KEY, &strict_test_xid(source_counter));
    strict_push_i64_property(bytes, PID_TAG_LAST_MODIFICATION_TIME, 1);
    strict_push_binary_property(bytes, PID_TAG_CHANGE_KEY, &strict_test_xid(change_counter));
    strict_push_binary_property(
        bytes,
        PID_TAG_PREDECESSOR_CHANGE_LIST,
        &strict_test_xid(change_counter),
    );
    strict_push_utf16_property(bytes, PID_TAG_DISPLAY_NAME_W, name);
    strict_push_i32_property(bytes, 0x3602_0003, 0);
    bytes.extend_from_slice(&PID_TAG_SUBFOLDERS.to_le_bytes());
    bytes.extend_from_slice(&1u16.to_le_bytes());
    if boolean_width > 2 {
        bytes.extend(std::iter::repeat_n(0, boolean_width - 2));
    }
    strict_push_utf16_property(bytes, PID_TAG_CONTAINER_CLASS_W, "IPF.Note");
}

fn strict_push_final_hierarchy_state(bytes: &mut Vec<u8>, source_ids: &[u64], changes: &[u64]) {
    bytes.extend_from_slice(&FX_INCR_SYNC_STATE_BEGIN.to_le_bytes());
    strict_push_binary_property(
        bytes,
        META_TAG_IDSET_GIVEN,
        &strict_test_replguid_globset(source_ids),
    );
    strict_push_binary_property(
        bytes,
        META_TAG_CNSET_SEEN,
        &strict_test_replguid_globset(changes),
    );
    bytes.extend_from_slice(&FX_INCR_SYNC_STATE_END.to_le_bytes());
    bytes.extend_from_slice(&FX_INCR_SYNC_END.to_le_bytes());
}

#[test]
fn strict_hierarchy_decoder_rejects_child_before_parent() {
    let parent_source_key = strict_test_xid(5);
    let mut bytes = Vec::new();
    strict_push_folder_change(&mut bytes, &parent_source_key, 6, 200, "Archive", 2);
    strict_push_folder_change(&mut bytes, &[], 5, 100, "Projects", 2);
    strict_push_final_hierarchy_state(&mut bytes, &[5, 6], &[100, 200]);

    let error = strict_decode_hierarchy_sync_stream(&bytes).unwrap_err();
    assert!(error.contains("before its parent"));
}

#[test]
fn strict_hierarchy_decoder_rejects_misaligned_boolean_lexical_size() {
    let mut bytes = Vec::new();
    strict_push_folder_change(&mut bytes, &[], 5, 100, "Projects", 4);
    strict_push_final_hierarchy_state(&mut bytes, &[5], &[100]);

    assert!(strict_decode_hierarchy_sync_stream(&bytes).is_err());
}

#[test]
fn strict_hierarchy_decoder_rejects_missing_final_cnset() {
    let mut bytes = Vec::new();
    strict_push_folder_change(&mut bytes, &[], 5, 100, "Projects", 2);
    bytes.extend_from_slice(&FX_INCR_SYNC_STATE_BEGIN.to_le_bytes());
    strict_push_binary_property(
        &mut bytes,
        META_TAG_IDSET_GIVEN,
        &strict_test_replguid_globset(&[5]),
    );
    bytes.extend_from_slice(&FX_INCR_SYNC_STATE_END.to_le_bytes());
    bytes.extend_from_slice(&FX_INCR_SYNC_END.to_le_bytes());

    let error = strict_decode_hierarchy_sync_stream(&bytes).unwrap_err();
    assert!(error.contains("missing hierarchy IDSET or CNSET"));
}

#[test]
fn strict_hierarchy_decoder_rejects_folder_change_after_final_state() {
    let mut bytes = Vec::new();
    strict_push_folder_change(&mut bytes, &[], 5, 100, "Projects", 2);
    bytes.extend_from_slice(&FX_INCR_SYNC_STATE_BEGIN.to_le_bytes());
    strict_push_binary_property(
        &mut bytes,
        META_TAG_IDSET_GIVEN,
        &strict_test_replguid_globset(&[5]),
    );
    strict_push_binary_property(
        &mut bytes,
        META_TAG_CNSET_SEEN,
        &strict_test_replguid_globset(&[100]),
    );
    bytes.extend_from_slice(&FX_INCR_SYNC_STATE_END.to_le_bytes());
    strict_push_folder_change(&mut bytes, &[], 6, 200, "Late", 2);
    bytes.extend_from_slice(&FX_INCR_SYNC_END.to_le_bytes());

    let error = strict_decode_hierarchy_sync_stream(&bytes).unwrap_err();
    assert!(error.contains("after final ICS state"));
}

#[test]
fn strict_hierarchy_decoder_rejects_duplicate_folder_property() {
    let mut bytes = Vec::new();
    strict_push_folder_change(&mut bytes, &[], 5, 100, "Projects", 2);
    strict_push_utf16_property(&mut bytes, PID_TAG_DISPLAY_NAME_W, "Duplicate");
    strict_push_final_hierarchy_state(&mut bytes, &[5], &[100]);

    let error = strict_decode_hierarchy_sync_stream(&bytes).unwrap_err();
    assert!(error.contains("duplicate property"));
}

#[test]
fn strict_hierarchy_decoder_rejects_message_change_in_hierarchy_stream() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&FX_INCR_SYNC_CHG.to_le_bytes());
    strict_push_i64_property(&mut bytes, PID_TAG_MID, 123);
    strict_push_final_hierarchy_state(&mut bytes, &[], &[]);

    let error = strict_decode_hierarchy_sync_stream(&bytes).unwrap_err();
    assert!(error.contains("message change property"));
}

#[test]
fn strict_hierarchy_decoder_rejects_final_state_missing_folder_id() {
    let mut bytes = Vec::new();
    strict_push_folder_change(&mut bytes, &[], 5, 100, "Projects", 2);
    strict_push_final_hierarchy_state(&mut bytes, &[], &[100]);

    let error = strict_decode_hierarchy_sync_stream(&bytes).unwrap_err();
    assert!(error.contains("does not include folder"));
}

#[test]
fn strict_hierarchy_decoder_rejects_non_replguid_final_state() {
    let mut bytes = Vec::new();
    let mut wrong_idset = vec![0xAA; 16];
    wrong_idset.push(0);
    strict_push_folder_change(&mut bytes, &[], 5, 100, "Projects", 2);
    bytes.extend_from_slice(&FX_INCR_SYNC_STATE_BEGIN.to_le_bytes());
    strict_push_binary_property(&mut bytes, META_TAG_IDSET_GIVEN, &wrong_idset);
    strict_push_binary_property(
        &mut bytes,
        META_TAG_CNSET_SEEN,
        &strict_test_replguid_globset(&[100]),
    );
    bytes.extend_from_slice(&FX_INCR_SYNC_STATE_END.to_le_bytes());
    bytes.extend_from_slice(&FX_INCR_SYNC_END.to_le_bytes());

    let error = strict_decode_hierarchy_sync_stream(&bytes).unwrap_err();
    assert!(error.contains("missing the store replica GUID"));
}

fn mapi_last_binary_property(bytes: &[u8], property_tag: u32) -> Option<&[u8]> {
    let tag = property_tag.to_le_bytes();
    let offset = bytes.windows(tag.len()).rposition(|window| window == tag)?;
    let length = u32::from_le_bytes(bytes.get(offset + 4..offset + 8)?.try_into().ok()?);
    bytes.get(offset + 8..offset + 8 + length as usize)
}

fn mapi_sync_manifest_message_state(bytes: &[u8], subject: &str) -> Option<(u32, u32)> {
    let subject = subject.as_bytes();
    let subject_start = bytes
        .windows(subject.len())
        .position(|window| window == subject)?;
    let flags_tag = 0x0E07_0003u32.to_le_bytes();
    let flag_status_tag = 0x1090_0003u32.to_le_bytes();
    let flags_start = bytes[..subject_start]
        .windows(flags_tag.len())
        .rposition(|window| window == flags_tag)?;
    let flag_status_start = bytes[..subject_start]
        .windows(flag_status_tag.len())
        .rposition(|window| window == flag_status_tag)?;
    Some((
        u32::from_le_bytes(
            bytes
                .get(flags_start + 4..flags_start + 8)?
                .try_into()
                .ok()?,
        ),
        u32::from_le_bytes(
            bytes
                .get(flag_status_start + 4..flag_status_start + 8)?
                .try_into()
                .ok()?,
        ),
    ))
}

fn mapi_fast_transfer_chunks(bytes: &[u8]) -> Vec<(u16, Vec<u8>)> {
    let mut chunks = Vec::new();
    let mut offset = 0;
    while offset + 15 <= bytes.len() {
        if bytes[offset] != 0x4E {
            offset += 1;
            continue;
        }
        let return_value = u32::from_le_bytes([
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
            bytes[offset + 5],
        ]);
        if return_value != 0 {
            offset += 1;
            continue;
        }
        let status = u16::from_le_bytes([bytes[offset + 6], bytes[offset + 7]]);
        let transfer_buffer_size =
            u16::from_le_bytes([bytes[offset + 13], bytes[offset + 14]]) as usize;
        let transfer_buffer_start = offset + 15;
        let transfer_buffer_end = transfer_buffer_start.saturating_add(transfer_buffer_size);
        let Some(chunk) = bytes.get(transfer_buffer_start..transfer_buffer_end) else {
            offset += 1;
            continue;
        };
        chunks.push((status, chunk.to_vec()));
        offset = transfer_buffer_end;
    }
    chunks
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

fn append_mapi_string8_property(values: &mut Vec<u8>, property_tag: u32, value: &str) {
    values.extend_from_slice(&property_tag.to_le_bytes());
    values.extend_from_slice(value.as_bytes());
    values.push(0);
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
    append_rop_sync_manifest_get_buffer_with_state(rops, input, output, buffer_size, &[]);
}

fn append_rop_sync_manifest_get_buffer_with_state(
    rops: &mut Vec<u8>,
    input: u8,
    output: u8,
    buffer_size: u16,
    state: &[u8],
) {
    rops.extend_from_slice(&[
        0x70, 0x00, input, output, 0x01, 0x00, 0x00, 0x00, // RopSynchronizationConfigure
        0x00, 0x00, // RestrictionDataSize
        0x00, 0x00, 0x00, 0x00, // SynchronizationExtraFlags
        0x00, 0x00, // PropertyTagCount
        0x75, 0x00, output, // RopSynchronizationUploadStateStreamBegin
    ]);
    rops.extend_from_slice(&0x4017_0102u32.to_le_bytes());
    rops.extend_from_slice(&(state.len() as u32).to_le_bytes());
    if !state.is_empty() {
        rops.extend_from_slice(&[
            0x76, 0x00, output, // RopSynchronizationUploadStateStreamContinue
        ]);
        rops.extend_from_slice(&(state.len() as u32).to_le_bytes());
        rops.extend_from_slice(state);
    }
    rops.extend_from_slice(&[
        0x77, 0x00, output, // RopSynchronizationUploadStateStreamEnd
        0x4E, 0x00, output, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&buffer_size.to_le_bytes());
}

async fn content_sync_response_rops(
    store: FakeStore,
    folder_global_counter: u64,
    client_state: &[u8],
) -> Vec<u8> {
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

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(folder_global_counter));
    append_rop_sync_manifest_get_buffer_with_state(&mut rops, 1, 2, 4096, client_state);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    response_rops_from_execute_response(response).await
}

fn append_rop_outlook_hierarchy_sync_manifest_get_buffer(
    rops: &mut Vec<u8>,
    input: u8,
    output: u8,
    buffer_size: u16,
) {
    append_rop_outlook_hierarchy_sync_manifest_get_buffer_with_state(
        rops,
        input,
        output,
        buffer_size,
        &[],
    );
}

fn append_rop_outlook_hierarchy_sync_manifest_get_buffer_with_state(
    rops: &mut Vec<u8>,
    input: u8,
    output: u8,
    buffer_size: u16,
    state: &[u8],
) {
    rops.extend_from_slice(&[
        0x70, 0x00, input, output, // RopSynchronizationConfigure
        0x02,   // hierarchy sync
        0x09,   // SendOptions
        0x01, 0x01, // SynchronizationFlags
        0x00, 0x00, // RestrictionDataSize
        0x00, 0x00, 0x00, 0x00, // SynchronizationExtraFlags
        0x08, 0x00, // PropertyTagCount
        0x03, 0x00, 0x01, 0x36, // PidTagFolderType
        0x03, 0x00, 0x02, 0x36, // PidTagContentCount
        0x03, 0x00, 0x03, 0x36, // PidTagContentUnreadCount
        0x03, 0x00, 0x08, 0x0e, // PidTagMessageSize
        0x03, 0x00, 0xf4, 0x0f, // PidTagAccess
        0x02, 0x01, 0xe0, 0x3f, // PidTagMappingSignature
        0x02, 0x01, 0xe1, 0x3f, // PidTagRecordKey
        0x02, 0x01, 0x27, 0x0e, // PidTagOrdinalMost
        0x75, 0x00, output, // RopSynchronizationUploadStateStreamBegin
    ]);
    rops.extend_from_slice(&0x4017_0003u32.to_le_bytes());
    rops.extend_from_slice(&(state.len() as u32).to_le_bytes());
    if !state.is_empty() {
        rops.extend_from_slice(&[
            0x76, 0x00, output, // RopSynchronizationUploadStateStreamContinue
        ]);
        rops.extend_from_slice(&(state.len() as u32).to_le_bytes());
        rops.extend_from_slice(state);
    }
    rops.extend_from_slice(&[
        0x77, 0x00, output, // RopSynchronizationUploadStateStreamEnd
        0x75, 0x00, output, // RopSynchronizationUploadStateStreamBegin
    ]);
    rops.extend_from_slice(&0x6796_0102u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x77, 0x00, output, // RopSynchronizationUploadStateStreamEnd
        0x4E, 0x00, output, // RopFastTransferSourceGetBuffer
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

fn append_rop_transport_send(rops: &mut Vec<u8>, input: u8) {
    rops.extend_from_slice(&[0x4A, 0x00, input]);
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

fn globcnt_bytes(value: u64) -> [u8; 6] {
    crate::mapi::identity::globcnt_bytes(value)
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
    renew_mapi_request_id(&mut execute_headers);
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
    renew_mapi_request_id(&mut execute_headers);
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
    renew_mapi_request_id(&mut execute_headers);
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
    renew_mapi_request_id(&mut execute_headers);
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
    renew_mapi_request_id(&mut execute_headers);
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
    renew_mapi_request_id(&mut execute_headers);
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
async fn mapi_over_http_task_crud_uses_canonical_tasks() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        task_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "tasks", "Tasks",
        )])),
        ..Default::default()
    };
    let tasks = store.tasks.clone();
    let deleted_tasks = store.deleted_tasks.clone();
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
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "RCA Task");
    append_mapi_utf16_property(&mut property_values, 0x1000_001F, "Created through MAPI");
    let mut rops = Vec::new();
    append_rop_create_message(&mut rops, 0, 1, test_mapi_folder_id(19));
    append_rop_set_properties(&mut rops, 1, 2, &property_values);
    append_rop_save_changes_message(&mut rops, 1, 1);
    append_rop_get_properties_specific(&mut rops, 1, &[0x0037_001F, 0x1000_001F]);
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
        let stored = tasks.lock().unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].title, "RCA Task");
        assert_eq!(stored[0].description, "Created through MAPI");
        assert_eq!(stored[0].status, "needs-action");
    }

    let task_id = Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap();
    let mut update_values = Vec::new();
    append_mapi_utf16_property(&mut update_values, 0x0037_001F, "Updated RCA Task");
    append_mapi_i32_property(&mut update_values, 0x1090_0003, 1);
    let mut update_rops = Vec::new();
    append_rop_open_folder(&mut update_rops, 0, 1, test_mapi_folder_id(19));
    append_rop_open_message(
        &mut update_rops,
        1,
        2,
        test_mapi_folder_id(19),
        test_mapi_uuid_id(&task_id),
    );
    append_rop_set_properties(&mut update_rops, 2, 2, &update_values);
    renew_mapi_request_id(&mut execute_headers);
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
        let stored = tasks.lock().unwrap();
        assert_eq!(stored[0].title, "Updated RCA Task");
        assert_eq!(stored[0].status, "completed");
    }

    let mut delete_rops = Vec::new();
    append_rop_open_folder(&mut delete_rops, 0, 1, test_mapi_folder_id(19));
    append_rop_delete_messages(&mut delete_rops, 1, &[test_mapi_uuid_id(&task_id)]);
    renew_mapi_request_id(&mut execute_headers);
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
    assert!(tasks.lock().unwrap().is_empty());
    assert_eq!(deleted_tasks.lock().unwrap().as_slice(), &[task_id]);
}

#[tokio::test]
async fn mapi_over_http_task_contents_table_lists_canonical_tasks() {
    let task_list_id = "99999999-9999-9999-9999-999999999999";
    let task = FakeStore::task(
        "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee",
        task_list_id,
        "Existing RCA Task",
    );
    let store = FakeStore {
        session: Some(FakeStore::account()),
        task_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default", "tasks", "Tasks",
        )])),
        tasks: Arc::new(Mutex::new(vec![task])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(19).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x001A_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&10u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &utf16z("Existing RCA Task")));
    assert!(contains_bytes(&response_rops, &utf16z("IPM.Task")));
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
        response.headers().get("x-serverapplication").unwrap(),
        "Exchange/15.20.0485.000"
    );
    assert!(response
        .headers()
        .get("x-clientinfo")
        .unwrap()
        .to_str()
        .unwrap()
        .starts_with("{aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee}:"));
    assert_eq!(
        response.headers().get("x-expirationinfo").unwrap(),
        "1800000"
    );
    assert_eq!(response.headers().get("x-pendingperiod").unwrap(), "15000");
    let content_length = response
        .headers()
        .get("content-length")
        .unwrap()
        .to_str()
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let set_cookie = response
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(set_cookie.starts_with("MapiContext="));
    assert!(set_cookie.contains("Max-Age=1800"));
    assert!(set_cookie.contains("HttpOnly"));
    assert!(set_cookie.contains("Secure"));
    let set_cookies = response
        .headers()
        .get_all("set-cookie")
        .iter()
        .map(|value| value.to_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(set_cookies.len(), 2);
    assert!(set_cookies
        .iter()
        .any(|cookie| cookie.starts_with("MapiContext=")));
    assert!(set_cookies
        .iter()
        .any(|cookie| cookie.starts_with("MapiSequence=")));

    let raw_body = to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec();
    assert_eq!(content_length, raw_body.len());
    assert!(raw_body.starts_with(b"PROCESSING\r\nDONE\r\nX-ResponseCode: 0\r\n"));
    let body = strip_mapi_http_envelope(raw_body);
    assert_eq!(&body[0..8], &[0, 0, 0, 0, 0, 0, 0, 0]);
    assert_eq!(&body[8..12], &60_000u32.to_le_bytes());
    assert_eq!(&body[12..16], &6u32.to_le_bytes());
    assert_eq!(&body[16..20], &10_000u32.to_le_bytes());
    assert!(body[20..].starts_with(b"/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=\0"));
    assert_eq!(
        &body[body.len() - 20..body.len() - 16],
        &16u32.to_le_bytes()
    );
    assert_eq!(
        &body[body.len() - 16..],
        &[
            0x00, 0x00, // RPC_HEADER_EXT Version
            0x04, 0x00, // Last flag
            0x08, 0x00, // Payload size
            0x08, 0x00, // Uncompressed payload size
            0x08, 0x00, // AUX_HEADER Size
            0x01, // AUX_HEADER Version
            0x17, // AUX_EXORGINFO
            0x00, 0x00, 0x00, 0x00, // OrgFlags
        ]
    );
}

#[tokio::test]
async fn mapi_over_http_connect_reestablishes_session_context_with_open_sync_handle() {
    let mailbox_id = "55555555-5555-5555-5555-555555555555";
    let mut inbox = FakeStore::mailbox(mailbox_id, "inbox", "Inbox");
    inbox.total_emails = 1;
    let email = FakeStore::email(
        "48484848-4848-4848-4848-484848484848",
        mailbox_id,
        "inbox",
        "Reconnect sync context message",
    );
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
    let first_cookie = connect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut configure_rops = Vec::new();
    append_rop_open_folder(&mut configure_rops, 0, 1, test_mapi_folder_id(5));
    configure_rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, 0x00, 0x00, 0x00, // content sync
        0x00, 0x00, // RestrictionDataSize
        0x00, 0x00, 0x00, 0x00, // SynchronizationExtraFlags
        0x00, 0x00, // PropertyTagCount
    ]);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&first_cookie).unwrap());
    let configure_request = execute_body(&rop_buffer(&configure_rops, &[1, u32::MAX, u32::MAX]));
    let configure_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &configure_request)
        .await
        .unwrap();
    assert_eq!(configure_response.status(), StatusCode::OK);
    assert_eq!(
        configure_response.headers().get("x-responsecode").unwrap(),
        "0"
    );

    let mut reconnect_headers = mapi_headers("Connect");
    reconnect_headers.insert("cookie", HeaderValue::from_str(&first_cookie).unwrap());
    let reconnect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &reconnect_headers, b"")
        .await
        .unwrap();
    assert_eq!(reconnect.status(), StatusCode::OK);
    assert_eq!(reconnect.headers().get("x-responsecode").unwrap(), "0");
    let reconnected_cookie = reconnect
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();
    assert_ne!(reconnected_cookie, first_cookie);

    let mut get_buffer_rops = Vec::new();
    get_buffer_rops.extend_from_slice(&[0x4E, 0x00, 0x00]);
    get_buffer_rops.extend_from_slice(&4096u16.to_le_bytes());
    let mut reconnected_execute_headers = mapi_headers("Execute");
    reconnected_execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&reconnected_cookie).unwrap(),
    );
    let get_buffer_request = execute_body(&rop_buffer(&get_buffer_rops, &[3]));
    let get_buffer_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &reconnected_execute_headers,
            &get_buffer_request,
        )
        .await
        .unwrap();

    assert_eq!(get_buffer_response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(get_buffer_response).await;
    assert_eq!(mapi_sync_manifest_counts(&response_rops), Some((1, 1)));
    assert!(contains_bytes(
        &response_rops,
        b"Reconnect sync context message"
    ));
}

#[tokio::test]
async fn mapi_over_http_connect_ignores_mismatched_sequence_cookie_on_reconnect() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut reconnect_headers = mapi_headers("Connect");
    reconnect_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header_with_mismatched_sequence(&connect)).unwrap(),
    );

    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &reconnect_headers, b"")
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "Connect");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let set_cookies = response
        .headers()
        .get_all("set-cookie")
        .iter()
        .map(|value| value.to_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(set_cookies.len(), 2);
    assert!(set_cookies
        .iter()
        .any(|cookie| cookie.starts_with("MapiContext=")));
    assert!(set_cookies
        .iter()
        .any(|cookie| cookie.starts_with("MapiSequence=")));
}

#[tokio::test]
async fn mapi_over_http_rejects_missing_request_id_with_parseable_error() {
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

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "Connect");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "7");
    let body = String::from_utf8(response_bytes(response).await).unwrap();
    assert!(body.contains("missing MAPI X-RequestId header"));
}

#[tokio::test]
async fn mapi_over_http_rejects_missing_request_type_with_parseable_error() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &mapi_headers_without_request_type(),
            b"",
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "Unknown");
    assert!(response
        .headers()
        .get("x-requestid")
        .unwrap()
        .to_str()
        .unwrap()
        .starts_with("{11111111-2222-3333-4444-555555555555}:"));
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "7");
    let body = String::from_utf8(response_bytes(response).await).unwrap();
    assert!(body.contains("missing MAPI X-RequestType header"));
}

#[tokio::test]
async fn mapi_over_http_rejects_unknown_request_type_with_parseable_error() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("BogusRequest"), b"")
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("x-requesttype").unwrap(),
        "BogusRequest"
    );
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "5");
    let body = String::from_utf8(response_bytes(response).await).unwrap();
    assert!(body.contains("invalid MAPI X-RequestType header"));
}

#[tokio::test]
async fn mapi_over_http_rejects_missing_client_info_with_parseable_error() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &mapi_headers_without_client_info("Connect"),
            b"",
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "Connect");
    assert!(response
        .headers()
        .get("x-requestid")
        .unwrap()
        .to_str()
        .unwrap()
        .starts_with("{11111111-2222-3333-4444-555555555555}:"));
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "7");
    assert!(response.headers().get("x-clientinfo").is_none());
    let body = String::from_utf8(response_bytes(response).await).unwrap();
    assert!(body.contains("missing MAPI X-ClientInfo header"));
}

#[tokio::test]
async fn mapi_over_http_rejects_invalid_client_info_with_parseable_error() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &mapi_headers_with_client_info("Connect", "not-a-guid-counter"),
            b"",
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "Connect");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "4");
    assert_eq!(
        response.headers().get("x-clientinfo").unwrap(),
        "not-a-guid-counter"
    );
    let body = String::from_utf8(response_bytes(response).await).unwrap();
    assert!(body.contains("invalid MAPI X-ClientInfo header"));
}

#[tokio::test]
async fn mapi_over_http_rejects_missing_host_with_parseable_error() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &mapi_headers_without_host("Connect"),
            b"",
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "Connect");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "7");
    let body = String::from_utf8(response_bytes(response).await).unwrap();
    assert!(body.contains("missing MAPI Host header"));
}

#[tokio::test]
async fn mapi_over_http_rejects_missing_content_length_with_parseable_error() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &mapi_headers_without_content_length("Connect"),
            b"",
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "Connect");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "7");
    let body = String::from_utf8(response_bytes(response).await).unwrap();
    assert!(body.contains("missing MAPI Content-Length header"));
}

#[tokio::test]
async fn mapi_over_http_rejects_invalid_content_length_with_parseable_error() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &mapi_headers_with_content_length("Connect", "not-a-length"),
            b"",
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "Connect");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "4");
    let body = String::from_utf8(response_bytes(response).await).unwrap();
    assert!(body.contains("invalid MAPI Content-Length header"));
}

#[tokio::test]
async fn mapi_over_http_rejects_invalid_request_id_with_parseable_error() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &mapi_headers_with_request_id("Connect", "not-a-guid-counter"),
            b"",
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "Connect");
    assert_eq!(
        response.headers().get("x-requestid").unwrap(),
        "not-a-guid-counter"
    );
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "4");
    let body = String::from_utf8(response_bytes(response).await).unwrap();
    assert!(body.contains("invalid MAPI X-RequestId header"));
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
    assert!(response
        .headers()
        .get("x-clientinfo")
        .unwrap()
        .to_str()
        .unwrap()
        .starts_with("{aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee}:"));
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
    assert!(set_cookie.starts_with("MapiContext="));
    let set_cookies = response
        .headers()
        .get_all("set-cookie")
        .iter()
        .map(|value| value.to_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(set_cookies.len(), 2);
    assert!(set_cookies
        .iter()
        .any(|cookie| cookie.starts_with("MapiContext=")));
    assert!(set_cookies
        .iter()
        .any(|cookie| cookie.starts_with("MapiSequence=")));
}

#[tokio::test]
async fn mapi_over_http_accepts_rca_octet_stream_emsmdb_connect() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let mut headers = mapi_headers_with_content_type("Connect", "application/octet-stream");
    headers.insert(
        axum::http::header::CONTENT_LENGTH,
        HeaderValue::from_static("214"),
    );
    headers.insert(
        "x-requestid",
        HeaderValue::from_static("3e93d512-7b7b-495a-9eb5-40b5adc4696a:1"),
    );
    headers.insert(
        "x-clientinfo",
        HeaderValue::from_static("c9a1f6bb-76d3-41a1-8abb-fc60106a4a97:1"),
    );

    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &headers, &[0; 214])
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "Connect");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/mapi-http"
    );
    let set_cookies = response
        .headers()
        .get_all("set-cookie")
        .iter()
        .map(|value| value.to_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(set_cookies.len(), 2);
}

#[tokio::test]
async fn mapi_over_http_accepts_rca_octet_stream_resolve_names_probe() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let mut headers = nspi_bound_headers(&service, "ResolveNames").await;
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    headers.insert(
        axum::http::header::CONTENT_LENGTH,
        HeaderValue::from_static("103"),
    );
    headers.insert(
        "x-requestid",
        HeaderValue::from_static("520bfd13-f3a9-45c4-abec-6ef0a2541db9:2"),
    );
    headers.insert(
        "x-clientinfo",
        HeaderValue::from_static("c9a1f6bb-76d3-41a1-8abb-fc60106a4a97:1"),
    );

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &headers, &[0; 103])
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("x-requesttype").unwrap(),
        "ResolveNames"
    );
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
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
async fn mapi_over_http_notification_wait_refreshes_emsmdb_session_cookie() {
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

    let mut wait_headers = mapi_headers("NotificationWait");
    wait_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &wait_headers, b"")
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("x-requesttype").unwrap(),
        "NotificationWait"
    );
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let set_cookies = response
        .headers()
        .get_all("set-cookie")
        .iter()
        .map(|value| value.to_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(set_cookies.len(), 2);
    assert!(set_cookies
        .iter()
        .any(|cookie| cookie.starts_with("MapiContext=") && cookie.contains("Max-Age=1800")));
    assert!(set_cookies
        .iter()
        .any(|cookie| cookie.starts_with("MapiSequence=") && cookie.contains("Max-Age=1800")));
    let body = response_bytes(response).await;
    assert_eq!(body.len(), 16);
    assert_eq!(u32::from_le_bytes(body[0..4].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[4..8].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[8..12].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[12..16].try_into().unwrap()), 0);
}

#[tokio::test]
async fn mapi_over_http_ping_requires_and_refreshes_session_cookie() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut ping_headers = mapi_headers("PING");
    ping_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &ping_headers, b"")
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "PING");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    assert_eq!(
        response.headers().get("x-expirationinfo").unwrap(),
        "1800000"
    );
    assert!(response_bytes(response).await.is_empty());

    let missing_cookie = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("PING"), b"")
        .await
        .unwrap();
    assert_eq!(
        missing_cookie.headers().get("x-responsecode").unwrap(),
        "13"
    );
    assert!(String::from_utf8(response_bytes(missing_cookie).await)
        .unwrap()
        .contains("missing MAPI session cookie"));

    let mut invalid_body_headers = mapi_headers("PING");
    invalid_body_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let invalid_body = service
        .handle_mapi(MapiEndpoint::Emsmdb, &invalid_body_headers, b"not-empty")
        .await
        .unwrap();
    assert_eq!(invalid_body.headers().get("x-responsecode").unwrap(), "12");
}

#[tokio::test]
async fn mapi_over_http_ping_rejects_mismatched_sequence_cookie() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let bad_cookie = mapi_cookie_header_with_mismatched_sequence(&connect);

    let mut ping_headers = mapi_headers("PING");
    ping_headers.insert("cookie", HeaderValue::from_str(&bad_cookie).unwrap());
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &ping_headers, b"")
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "PING");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "6");
    let body = String::from_utf8(response_bytes(response).await).unwrap();
    assert!(body.contains("invalid MAPI request sequence cookie"));
}

#[tokio::test]
async fn mapi_over_http_ping_rejects_nonzero_content_length() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut ping_headers = mapi_headers_with_content_length("PING", "1");
    ping_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &ping_headers, b"")
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "PING");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "4");
    let body = String::from_utf8(response_bytes(response).await).unwrap();
    assert!(body.contains("PING requests must use Content-Length 0"));
}

#[tokio::test]
async fn mapi_over_http_ping_refreshes_nspi_session_cookie() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let bind = service
        .handle_mapi(MapiEndpoint::Nspi, &mapi_headers("Bind"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&bind);

    let mut ping_headers = mapi_headers("PING");
    ping_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &ping_headers, b"")
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "PING");
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    assert_eq!(
        response.headers().get("x-expirationinfo").unwrap(),
        "1800000"
    );
    assert!(response_bytes(response).await.is_empty());
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
async fn mapi_over_http_execute_stops_batch_after_reserved_rop() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut rops = vec![0x28, 0x00, 0x00, 0xAA];
    rops.extend_from_slice(&[0x01, 0x00, 0x00]);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    assert_eq!(
        response_rops_from_execute_response(response).await,
        vec![0x28, 0x00, 0x02, 0x01, 0x04, 0x80]
    );
}

#[tokio::test]
async fn mapi_over_http_execute_and_replay_refresh_session_cookies() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&[0x01, 0x00, 0x00], &[1]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let execute_cookie = mapi_cookie_header(&response);
    assert!(execute_cookie.contains("MapiContext="));
    assert!(execute_cookie.contains("MapiSequence="));

    let mut replay_headers = execute_headers;
    replay_headers.insert("cookie", HeaderValue::from_str(&execute_cookie).unwrap());
    let replay = service
        .handle_mapi(MapiEndpoint::Emsmdb, &replay_headers, &request)
        .await
        .unwrap();
    assert_eq!(replay.headers().get("x-responsecode").unwrap(), "0");
    let replay_cookie = mapi_cookie_header(&replay);
    assert!(replay_cookie.contains("MapiContext="));
    assert!(replay_cookie.contains("MapiSequence="));
}

#[tokio::test]
async fn mapi_over_http_rejects_concurrent_session_request_with_invalid_sequence() {
    let load_started = Arc::new(tokio::sync::Notify::new());
    let load_continue = Arc::new(tokio::sync::Notify::new());
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mapi_mail_store_load_started: Some(load_started.clone()),
        mapi_mail_store_load_continue: Some(load_continue.clone()),
        ..Default::default()
    };
    let service = Arc::new(ExchangeService::new(store));
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&[0x01, 0x00, 0x00], &[1]));
    let execute_service = service.clone();
    let first_execute = tokio::spawn(async move {
        execute_service
            .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
            .await
            .unwrap()
    });
    load_started.notified().await;

    let mut ping_headers = mapi_headers("PING");
    ping_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let ping = service
        .handle_mapi(MapiEndpoint::Emsmdb, &ping_headers, b"")
        .await
        .unwrap();

    assert_eq!(ping.status(), StatusCode::OK);
    assert_eq!(ping.headers().get("x-requesttype").unwrap(), "PING");
    assert_eq!(ping.headers().get("x-responsecode").unwrap(), "15");
    let body = String::from_utf8(response_bytes(ping).await).unwrap();
    assert!(body.contains("MAPI session already has an active request"));

    load_continue.notify_waiters();
    let execute = first_execute.await.unwrap();
    assert_eq!(execute.headers().get("x-requesttype").unwrap(), "Execute");
    assert_eq!(execute.headers().get("x-responsecode").unwrap(), "0");
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
    assert_eq!(response_rop[111], 0x07);
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
async fn mapi_over_http_public_folder_logon_is_deferred_without_store_handle() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let legacy_dn = b"/o=LPE/ou=Exchange Administrative Group/cn=Public Folders\0";
    let mut logon_rop = vec![0xFE, 0x00, 0x00, 0x00];
    logon_rop.extend_from_slice(&0u32.to_le_bytes());
    logon_rop.extend_from_slice(&0u32.to_le_bytes());
    logon_rop.extend_from_slice(&(legacy_dn.len() as u16).to_le_bytes());
    logon_rop.extend_from_slice(legacy_dn);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&logon_rop, &[])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let response_rop_size = u16::from_le_bytes(rop_buffer[0..2].try_into().unwrap()) as usize;
    let response_rop = &rop_buffer[2..2 + response_rop_size];

    assert_eq!(response_rop, &[0xFE, 0x00, 0x02, 0x01, 0x04, 0x80]);
    assert_eq!(rop_buffer.len(), 2 + response_rop_size);
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
    assert_eq!(response_rop[111], 0x07);
    assert_eq!(
        &response_rop[112..128],
        &FakeStore::account().account_id.to_bytes_le()
    );
    assert_eq!(&response_rop[128..130], &1u16.to_le_bytes());
    assert_eq!(&response_rop[130..146], &mapi_mailstore::STORE_REPLICA_GUID);
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
async fn mapi_over_http_execute_returns_logon_replid_guid_map_for_outlook_bootstrap() {
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
    let logon_request = hex_bytes(
        "0200000063000000000004005b005b005700fe0000010c0400210000000047002f6f3d4c50452f6f753d45786368616e67652041646d696e6973747261746976652047726f75702f636e3d526563697069656e74732f636e3d746573742d6c2d702d652d636800ffffffff0780000000000000",
    );
    let logon_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &logon_request)
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    assert_eq!(logon_response.headers().get("x-responsecode").unwrap(), "0");

    renew_mapi_request_id(&mut execute_headers);
    let replid_request = hex_bytes(
        "020000001b00000000000400130013000f0007000000000000010002013866010000000780000000000000",
    );
    let replid_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &replid_request)
        .await
        .unwrap();

    assert_eq!(replid_response.status(), StatusCode::OK);
    assert_eq!(
        replid_response.headers().get("x-responsecode").unwrap(),
        "0"
    );
    let body = response_bytes(replid_response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    assert_eq!(&rop_buffer[0..4], &[0, 0, 4, 0]);
    let payload_size = u16::from_le_bytes(rop_buffer[4..6].try_into().unwrap()) as usize;
    let payload = &rop_buffer[8..8 + payload_size];
    let response_rop_size = u16::from_le_bytes(payload[0..2].try_into().unwrap()) as usize;
    let response_rop = &payload[2..response_rop_size];

    assert_eq!(response_rop[0], 0x07);
    assert_eq!(response_rop[1], 0x00);
    assert_eq!(
        u32::from_le_bytes(response_rop[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(response_rop[6], 0);
    assert_eq!(
        u16::from_le_bytes(response_rop[7..9].try_into().unwrap()),
        18
    );
    assert_eq!(&response_rop[9..11], &1u16.to_le_bytes());
    assert_eq!(&response_rop[11..27], &mapi_mailstore::STORE_REPLICA_GUID);
    assert_eq!(response_rop_size, response_rop.len() + 2);
    assert_eq!(&payload[response_rop_size..], &1u32.to_le_bytes());

    renew_mapi_request_id(&mut execute_headers);
    let named_property_request = hex_bytes(
        "020000003e00000000000400360036003200560000020200000820060000000000c00000000000004680850000000820060000000000c00000000000004681850000010000000780000000000000",
    );
    let named_property_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &named_property_request,
        )
        .await
        .unwrap();

    assert_eq!(named_property_response.status(), StatusCode::OK);
    let body = response_bytes(named_property_response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    assert_eq!(&rop_buffer[0..4], &[0, 0, 4, 0]);
    let payload_size = u16::from_le_bytes(rop_buffer[4..6].try_into().unwrap()) as usize;
    let payload = &rop_buffer[8..8 + payload_size];
    let response_rop_size = u16::from_le_bytes(payload[0..2].try_into().unwrap()) as usize;
    let response_rop = &payload[2..response_rop_size];

    assert_eq!(response_rop[0], 0x56);
    assert_eq!(response_rop[1], 0x00);
    assert_eq!(
        u32::from_le_bytes(response_rop[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(
        u16::from_le_bytes(response_rop[6..8].try_into().unwrap()),
        2
    );
    assert_eq!(&response_rop[8..10], &0x8001u16.to_le_bytes());
    assert_eq!(&response_rop[10..12], &0x8002u16.to_le_bytes());
    assert_eq!(response_rop_size, response_rop.len() + 2);
    assert_eq!(&payload[response_rop_size..], &1u32.to_le_bytes());

    renew_mapi_request_id(&mut execute_headers);
    let ipm_subtree_property_request = hex_bytes(
        "020000002c00000000000400240024001c00020000010100040000000000000700010000000001000201047c01000000ffffffff0780000000000000",
    );
    let ipm_subtree_property_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &ipm_subtree_property_request,
        )
        .await
        .unwrap();

    assert_eq!(ipm_subtree_property_response.status(), StatusCode::OK);
    let body = response_bytes(ipm_subtree_property_response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    assert_eq!(&rop_buffer[0..4], &[0, 0, 4, 0]);
    let payload_size = u16::from_le_bytes(rop_buffer[4..6].try_into().unwrap()) as usize;
    let payload = &rop_buffer[8..8 + payload_size];
    let response_rop_size = u16::from_le_bytes(payload[0..2].try_into().unwrap()) as usize;
    let response_rops = &payload[2..response_rop_size];

    assert_eq!(response_rops[0], 0x02);
    assert_eq!(response_rops[1], 0x01);
    assert_eq!(
        u32::from_le_bytes(response_rops[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(response_rops[8], 0x07);
    assert_eq!(response_rops[9], 0x01);
    assert_eq!(
        u32::from_le_bytes(response_rops[10..14].try_into().unwrap()),
        0
    );
    assert_eq!(response_rop_size, response_rops.len() + 2);
    assert_eq!(
        &payload[response_rop_size..response_rop_size + 8],
        &[1, 0, 0, 0, 2, 0, 0, 0]
    );

    renew_mapi_request_id(&mut execute_headers);
    let folder_set_properties_request = hex_bytes(
        "020000002f000000000004002700270023000a00001c0001000201047c14003bccd33e05e40d41a4e87c7d9d249ff501000000020000000780000000000000",
    );
    let folder_set_properties_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &folder_set_properties_request,
        )
        .await
        .unwrap();

    assert_eq!(folder_set_properties_response.status(), StatusCode::OK);
    let body = response_bytes(folder_set_properties_response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    assert_eq!(&rop_buffer[0..4], &[0, 0, 4, 0]);
    let payload_size = u16::from_le_bytes(rop_buffer[4..6].try_into().unwrap()) as usize;
    let payload = &rop_buffer[8..8 + payload_size];
    let response_rop_size = u16::from_le_bytes(payload[0..2].try_into().unwrap()) as usize;
    let response_rop = &payload[2..response_rop_size];

    assert_eq!(response_rop[0], 0x0A);
    assert_eq!(response_rop[1], 0x00);
    assert_eq!(
        u32::from_le_bytes(response_rop[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(
        u16::from_le_bytes(response_rop[6..8].try_into().unwrap()),
        0
    );
    assert_eq!(response_rop_size, response_rop.len() + 2);
    assert_eq!(&payload[response_rop_size..], &2u32.to_le_bytes());

    renew_mapi_request_id(&mut execute_headers);
    let release_and_local_replica_ids_request = hex_bytes(
        "020000001c00000000000400140014000c000100007f00010000010002000000010000000780000000000000",
    );
    let release_and_local_replica_ids_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &release_and_local_replica_ids_request,
        )
        .await
        .unwrap();

    assert_eq!(
        release_and_local_replica_ids_response.status(),
        StatusCode::OK
    );
    let body = response_bytes(release_and_local_replica_ids_response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    assert_eq!(&rop_buffer[0..4], &[0, 0, 4, 0]);
    let payload_size = u16::from_le_bytes(rop_buffer[4..6].try_into().unwrap()) as usize;
    let payload = &rop_buffer[8..8 + payload_size];
    let response_rop_size = u16::from_le_bytes(payload[0..2].try_into().unwrap()) as usize;
    let response_rop = &payload[2..response_rop_size];

    assert_eq!(response_rop[0], 0x7F);
    assert_eq!(response_rop[1], 0x01);
    assert_eq!(
        u32::from_le_bytes(response_rop[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(&response_rop[6..22], &mapi_mailstore::STORE_REPLICA_GUID);
    assert_eq!(response_rop.len(), 28);
    assert!(response_rop[22..28].iter().any(|byte| *byte != 0));
    assert_eq!(response_rop_size, response_rop.len() + 2);
    assert_eq!(
        &payload[response_rop_size..response_rop_size + 8],
        &[0xff, 0xff, 0xff, 0xff, 1, 0, 0, 0]
    );

    renew_mapi_request_id(&mut execute_headers);
    let release_and_receive_folder_request = hex_bytes(
        "0200000019000000000004001100110009000100002700010003000000010000000780000000000000",
    );
    let release_and_receive_folder_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &release_and_receive_folder_request,
        )
        .await
        .unwrap();

    assert_eq!(release_and_receive_folder_response.status(), StatusCode::OK);
    let body = response_bytes(release_and_receive_folder_response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    assert_eq!(&rop_buffer[0..4], &[0, 0, 4, 0]);
    let payload_size = u16::from_le_bytes(rop_buffer[4..6].try_into().unwrap()) as usize;
    let payload = &rop_buffer[8..8 + payload_size];
    let response_rop_size = u16::from_le_bytes(payload[0..2].try_into().unwrap()) as usize;
    let response_rop = &payload[2..response_rop_size];

    assert_eq!(response_rop[0], 0x27);
    assert_eq!(response_rop[1], 0x01);
    assert_eq!(
        u32::from_le_bytes(response_rop[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(
        u64::from_le_bytes(response_rop[6..14].try_into().unwrap()),
        test_mapi_folder_id(5)
    );
    assert_eq!(response_rop.get(14), Some(&0));
    assert_eq!(response_rop_size, response_rop.len() + 2);
    assert_eq!(
        &payload[response_rop_size..response_rop_size + 8],
        &[0xff, 0xff, 0xff, 0xff, 1, 0, 0, 0]
    );

    renew_mapi_request_id(&mut execute_headers);
    let mut hierarchy_sync_rops = Vec::new();
    append_rop_open_folder(&mut hierarchy_sync_rops, 0, 1, test_mapi_folder_id(1));
    hierarchy_sync_rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x02, 0x09, 0x01, 0x01, // hierarchy sync, Unicode send/options
        0x00, 0x00, // RestrictionDataSize
        0x00, 0x00, 0x00, 0x00, // SynchronizationExtraFlags
    ]);
    let sync_property_tags = [
        0x3601_0003u32,
        0x3602_0003,
        0x3603_0003,
        0x0E08_0003,
        0x0FF4_0003,
        0x3FE0_0102,
        0x3FE1_0102,
        0x0E27_0102,
    ];
    hierarchy_sync_rops.extend_from_slice(&(sync_property_tags.len() as u16).to_le_bytes());
    for tag in sync_property_tags {
        hierarchy_sync_rops.extend_from_slice(&tag.to_le_bytes());
    }
    hierarchy_sync_rops.extend_from_slice(&[
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
    ]);
    hierarchy_sync_rops.extend_from_slice(&0x4017_0003u32.to_le_bytes());
    hierarchy_sync_rops.extend_from_slice(&0u32.to_le_bytes());
    hierarchy_sync_rops.extend_from_slice(&[
        0x77, 0x00, 0x02, // RopSynchronizationUploadStateStreamEnd
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
    ]);
    hierarchy_sync_rops.extend_from_slice(&0x6796_0102u32.to_le_bytes());
    hierarchy_sync_rops.extend_from_slice(&0u32.to_le_bytes());
    hierarchy_sync_rops.extend_from_slice(&[
        0x77, 0x00, 0x02, // RopSynchronizationUploadStateStreamEnd
    ]);
    hierarchy_sync_rops.extend_from_slice(&[
        0x4E, 0x00, 0x02, // RopFastTransferSourceGetBuffer
    ]);
    hierarchy_sync_rops.extend_from_slice(&0xBABEu16.to_le_bytes());
    hierarchy_sync_rops.extend_from_slice(&0x7BC0u16.to_le_bytes());
    let hierarchy_sync_configure_request = execute_body(&crate::tests::rop_buffer(
        &hierarchy_sync_rops,
        &[1, u32::MAX, u32::MAX],
    ));
    let hierarchy_sync_configure_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &hierarchy_sync_configure_request,
        )
        .await
        .unwrap();

    assert_eq!(hierarchy_sync_configure_response.status(), StatusCode::OK);
    let response_rops =
        response_rops_from_execute_response(hierarchy_sync_configure_response).await;

    assert!(contains_bytes(
        &response_rops,
        &[0x70, 0x02, 0x00, 0x00, 0x00, 0x00]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x70, 0x02, 0x00, 0x00, 0x00, 0x00, 0x75, 0x02, 0x00, 0x00, 0x00, 0x00,]
    ));
    assert!(!contains_bytes(
        &response_rops,
        &[0x70, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x75,]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x75, 0x02, 0x00, 0x00, 0x00, 0x00]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x77, 0x02, 0x00, 0x00, 0x00, 0x00]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x4E, 0x02, 0x00, 0x00, 0x00, 0x00]
    ));
    assert!(!contains_bytes(&response_rops, &[0x02, 0x01, 0x04, 0x80]));
    assert!(contains_bytes(
        &response_rops,
        &0x403A_0003u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x4017_0102u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x6796_0102u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x403B_0003u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x4014_0003u32.to_le_bytes()
    ));
    assert!(!contains_bytes(&response_rops, b"LPE-MAPI-SYNC\0"));

    renew_mapi_request_id(&mut execute_headers);
    let address_types_request =
        hex_bytes("020000001100000000000400090009000500490000010000000780000000000000");
    let address_types_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &address_types_request,
        )
        .await
        .unwrap();

    assert_eq!(address_types_response.status(), StatusCode::OK);
    let body = response_bytes(address_types_response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    assert_eq!(&rop_buffer[0..4], &[0, 0, 4, 0]);
    let payload_size = u16::from_le_bytes(rop_buffer[4..6].try_into().unwrap()) as usize;
    let payload = &rop_buffer[8..8 + payload_size];
    let response_rop_size = u16::from_le_bytes(payload[0..2].try_into().unwrap()) as usize;
    let response_rop = &payload[2..response_rop_size];

    assert_eq!(response_rop[0], 0x49);
    assert_eq!(response_rop[1], 0x00);
    assert_eq!(
        u32::from_le_bytes(response_rop[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(
        u16::from_le_bytes(response_rop[6..8].try_into().unwrap()),
        2
    );
    assert_eq!(
        u16::from_le_bytes(response_rop[8..10].try_into().unwrap()) as usize,
        b"EX\0SMTP\0".len()
    );
    assert_eq!(&response_rop[10..], b"EX\0SMTP\0");
    assert_eq!(&payload[response_rop_size..], &1u32.to_le_bytes());
}

#[tokio::test]
async fn mapi_over_http_execute_returns_logon_owner_and_status_properties() {
    let mut account = FakeStore::account();
    account.account_id = Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
    account.email = "bob@example.test".to_string();
    account.display_name = "Bob Store".to_string();
    let store = FakeStore {
        session: Some(account.clone()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let logon_request = hex_bytes(
        "0200000063000000000004005b005b005700fe0000010c0400210000000047002f6f3d4c50452f6f753d45786368616e67652041646d696e6973747261746976652047726f75702f636e3d526563697069656e74732f636e3d746573742d6c2d702d652d636800ffffffff0780000000000000",
    );
    let logon_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &logon_request)
        .await
        .unwrap();
    assert_eq!(logon_response.status(), StatusCode::OK);
    assert_eq!(logon_response.headers().get("x-responsecode").unwrap(), "0");

    renew_mapi_request_id(&mut execute_headers);
    let store_properties_request = hex_bytes(
        "0200000037000000000004002f002f002b000700000000000008001f001c6602011b661f001d3402011e3402011f340b005c0e03006f3402010767010000000780000000000000",
    );
    let store_properties_response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &store_properties_request,
        )
        .await
        .unwrap();

    assert_eq!(store_properties_response.status(), StatusCode::OK);
    assert_eq!(
        store_properties_response
            .headers()
            .get("x-responsecode")
            .unwrap(),
        "0"
    );
    let body = response_bytes(store_properties_response).await;
    let rop_buffer_size = u32::from_le_bytes(body[12..16].try_into().unwrap()) as usize;
    let rop_buffer = &body[16..16 + rop_buffer_size];
    let payload_size = u16::from_le_bytes(rop_buffer[4..6].try_into().unwrap()) as usize;
    let payload = &rop_buffer[8..8 + payload_size];
    let response_rop_size = u16::from_le_bytes(payload[0..2].try_into().unwrap()) as usize;
    let response_rops = &payload[2..response_rop_size];

    assert_eq!(response_rops[0], 0x07);
    assert_eq!(response_rops[1], 0);
    assert_eq!(
        u32::from_le_bytes(response_rops[2..6].try_into().unwrap()),
        0
    );
    let mut offset = 6;
    assert_eq!(response_rops[offset], 0);
    offset += 1;

    let owner_name = utf16z("Bob Store");
    assert_eq!(
        &response_rops[offset..offset + owner_name.len()],
        owner_name.as_slice()
    );
    offset += owner_name.len();

    let entry_id_len =
        u16::from_le_bytes(response_rops[offset..offset + 2].try_into().unwrap()) as usize;
    assert!(entry_id_len > 0);
    offset += 2 + entry_id_len;

    let server_name = utf16z("LPE");
    assert_eq!(
        &response_rops[offset..offset + server_name.len()],
        server_name.as_slice()
    );
    offset += server_name.len();

    for _ in 0..2 {
        assert_eq!(
            u16::from_le_bytes(response_rops[offset..offset + 2].try_into().unwrap()),
            0
        );
        offset += 2;
    }

    assert_eq!(response_rops[offset], 0);
    offset += 1;
    assert_eq!(
        u32::from_le_bytes(response_rops[offset..offset + 4].try_into().unwrap()),
        0
    );
    offset += 4;

    assert_eq!(
        u16::from_le_bytes(response_rops[offset..offset + 2].try_into().unwrap()),
        16
    );
    offset += 2;
    assert_eq!(
        &response_rops[offset..offset + 16],
        account.account_id.as_bytes()
    );
    offset += 16;
    assert_eq!(offset, response_rops.len());

    assert!(contains_bytes(&response_rops, &utf16z("Bob Store")));
    assert!(contains_bytes(&response_rops, b"acct-bob-example-test\0"));
    assert!(contains_bytes(&response_rops, &utf16z("LPE")));
    assert!(contains_bytes(
        &response_rops,
        account.account_id.as_bytes().as_slice()
    ));
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
        1
    );
    assert_eq!(
        u32::from_le_bytes(
            rop_buffer[6 + response_rop_size..10 + response_rop_size]
                .try_into()
                .unwrap()
        ),
        2
    );
    assert_eq!(
        u32::from_le_bytes(
            rop_buffer[10 + response_rop_size..14 + response_rop_size]
                .try_into()
                .unwrap()
        ),
        3
    );
}

#[tokio::test]
async fn mapi_over_http_query_columns_all_reports_canonical_table_columns() {
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
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, Inbox
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x37, 0x00, 0x02, // RopQueryColumnsAll
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

    let query_columns_offset = 18;
    assert_eq!(response_rops[query_columns_offset], 0x37);
    assert_eq!(response_rops[query_columns_offset + 1], 0x02);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[query_columns_offset + 2..query_columns_offset + 6]
                .try_into()
                .unwrap()
        ),
        0
    );
    let column_count = u16::from_le_bytes(
        response_rops[query_columns_offset + 6..query_columns_offset + 8]
            .try_into()
            .unwrap(),
    );
    assert!(column_count >= 10);
    assert!(contains_bytes(response_rops, &0x0037_001Fu32.to_le_bytes()));
    assert!(contains_bytes(response_rops, &0x65E0_0102u32.to_le_bytes()));
    assert!(contains_bytes(response_rops, &0x65E2_0102u32.to_le_bytes()));
}

#[tokio::test]
async fn mapi_over_http_ipm_subtree_reports_distinct_folder_identity() {
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

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, IPM subtree
    ];
    rops.extend_from_slice(&test_mapi_folder_id(4).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x07, 0x00, 0x01, // RopGetPropertiesSpecific
    ]);
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&7u16.to_le_bytes());
    for tag in [
        0x3001_001F,
        0x6748_0014,
        0x6749_0014,
        0x3613_001F,
        0x001A_001F,
        0x65E0_0102,
        0x65E1_0102,
    ] as [u32; 7]
    {
        rops.extend_from_slice(&tag.to_le_bytes());
    }

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
    let properties = &response_rops[8..];

    assert_eq!(properties[0], 0x07);
    assert_eq!(properties[1], 0x01);
    assert_eq!(u32::from_le_bytes(properties[2..6].try_into().unwrap()), 0);
    assert!(contains_bytes(
        properties,
        &utf16z("Top of Information Store")
    ));
    assert!(contains_bytes(
        properties,
        &test_mapi_folder_id(4).to_le_bytes()
    ));
    assert!(contains_bytes(
        properties,
        &test_mapi_folder_id(1).to_le_bytes()
    ));
    assert!(contains_bytes(properties, &utf16z("IPF.Note")));
}

#[tokio::test]
async fn mapi_over_http_advertised_special_folder_reports_own_identity() {
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
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, Outbox
    ];
    rops.extend_from_slice(&test_mapi_folder_id(6).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x07, 0x00, 0x01, // RopGetPropertiesSpecific
    ]);
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&5u16.to_le_bytes());
    for tag in [
        0x3001_001F,
        0x6748_0014,
        0x6749_0014,
        0x3613_001F,
        0x001A_001F,
    ] as [u32; 5]
    {
        rops.extend_from_slice(&tag.to_le_bytes());
    }

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    let properties = &response_rops[8..];

    assert_eq!(properties[0], 0x07);
    assert_eq!(properties[1], 0x01);
    assert_eq!(u32::from_le_bytes(properties[2..6].try_into().unwrap()), 0);
    assert!(contains_bytes(properties, &utf16z("Outbox")));
    assert!(contains_bytes(
        properties,
        &test_mapi_folder_id(6).to_le_bytes()
    ));
    assert!(contains_bytes(
        properties,
        &test_mapi_folder_id(4).to_le_bytes()
    ));
    assert!(contains_bytes(properties, &utf16z("IPF.Note")));
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
async fn mapi_over_http_folder_move_copy_rops_return_parseable_errors_without_corrupting_batch() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let folder_id = test_mapi_folder_id(5);
    let mut rops = vec![
        0x35, 0x00, 0x00, 0x01, // RopMoveFolder
        0x00, // synchronous
        0x01, // Unicode name
    ];
    rops.extend_from_slice(&folder_id.to_le_bytes());
    rops.extend_from_slice(&utf16z("Moved Folder"));
    rops.extend_from_slice(&[
        0x36, 0x00, 0x00, 0x01, // RopCopyFolder
        0x00, // synchronous
        0x01, // recursive
        0x00, // multibyte name
    ]);
    rops.extend_from_slice(&folder_id.to_le_bytes());
    rops.extend_from_slice(b"Copied Folder\0");
    rops.extend_from_slice(&[0x7B, 0x00, 0x00]); // RopGetStoreState proves the batch stayed aligned.

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, 1]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x35, 0x00, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x36, 0x00, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x7B, 0x00, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
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
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x6748_0014u32.to_le_bytes());
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
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x12, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &[0x15, 0x02, 0, 0, 0, 0, 0x02, 0, 0]
    ));
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
async fn mapi_over_http_sort_table_orders_combined_hierarchy_rows() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Zulu Mail",
        )])),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "default",
            "contacts",
            "Alpha Contacts",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder, Root
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
        0x13, 0x00, 0x02, 0x00, // RopSortTable
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows
    ]);
    rops.extend_from_slice(&2u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x15, 0x02, 0, 0, 0, 0, 0x02, 2, 0]
    ));
    let alpha = utf16z("Alpha Contacts");
    let zulu = utf16z("Zulu Mail");
    let alpha_offset = response_rops
        .windows(alpha.len())
        .position(|window| window == alpha)
        .unwrap();
    let zulu_offset = response_rops
        .windows(zulu.len())
        .position(|window| window == zulu)
        .unwrap();

    assert!(alpha_offset < zulu_offset);
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
async fn mapi_over_http_seek_row_fractional_moves_table_cursor() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 4;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                "87878787-8787-8787-8787-878787878787",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Fractional first",
            ),
            FakeStore::email(
                "88888888-8888-8888-8888-888888888888",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Fractional second",
            ),
            FakeStore::email(
                "89898989-8989-8989-8989-898989898989",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Fractional third",
            ),
            FakeStore::email(
                "8a8a8a8a-8a8a-8a8a-8a8a-8a8a8a8a8a8a",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Fractional fourth",
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
        0x1A, 0x00, 0x02, // RopSeekRowFractional
    ]);
    rops.extend_from_slice(&1u32.to_le_bytes());
    rops.extend_from_slice(&2u32.to_le_bytes());
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
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x1A, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &[0x17, 0x02, 0, 0, 0, 0, 2, 0, 0, 0, 4, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_categorized_table_rops_return_rop_specific_protocol_errors() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            "87878787-8787-8787-8787-878787878787",
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Categorized table probe",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[0x05, 0x00, 0x01, 0x02, 0x00]);
    rops.extend_from_slice(&[0x59, 0x00, 0x02]);
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(
        &test_mapi_message_id("87878787-8787-8787-8787-878787878787").to_le_bytes(),
    );
    rops.extend_from_slice(&[0x5A, 0x00, 0x02]);
    rops.extend_from_slice(
        &test_mapi_message_id("87878787-8787-8787-8787-878787878787").to_le_bytes(),
    );
    rops.extend_from_slice(&[0x6B, 0x00, 0x02]);
    rops.extend_from_slice(
        &test_mapi_message_id("87878787-8787-8787-8787-878787878787").to_le_bytes(),
    );
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&[0x6C, 0x00, 0x02]);
    rops.extend_from_slice(&4u16.to_le_bytes());
    rops.extend_from_slice(b"LPEC");

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x59, 0x02, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x5A, 0x02, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x6B, 0x02, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x6C, 0x02, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(!contains_bytes(
        &response_rops,
        &[0x00, 0x00, 0x02, 0x01, 0x04, 0x80]
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

    renew_mapi_request_id(&mut execute_headers);
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
    let stream_body = utf16z("Body stream saved through MAPI");
    let html_stream_body = b"<p>HTML stream saved through MAPI</p>";

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
        0x2B, 0x00, 0x02, 0x03, // RopOpenStream, create body stream
    ]);
    rops.extend_from_slice(&0x1000_001Fu32.to_le_bytes());
    rops.push(2);
    rops.extend_from_slice(&[
        0x2F, 0x00, 0x03, // RopSetStreamSize
    ]);
    rops.extend_from_slice(&(stream_body.len() as u64).to_le_bytes());
    rops.extend_from_slice(&[
        0x5E, 0x00, 0x03, // RopGetStreamSize
    ]);
    rops.extend_from_slice(&[
        0x2D, 0x00, 0x03, // RopWriteStream
    ]);
    rops.extend_from_slice(&(stream_body.len() as u16).to_le_bytes());
    rops.extend_from_slice(&stream_body);
    rops.extend_from_slice(&[
        0x5D, 0x00, 0x03, // RopCommitStream
    ]);
    rops.extend_from_slice(&[
        0x2B, 0x00, 0x02, 0x04, // RopOpenStream, create HTML body stream
    ]);
    rops.extend_from_slice(&0x1013_0102u32.to_le_bytes());
    rops.push(2);
    rops.extend_from_slice(&[
        0xA3, 0x00, 0x04, // RopWriteStreamExtended
    ]);
    rops.extend_from_slice(&(html_stream_body.len() as u16).to_le_bytes());
    rops.extend_from_slice(html_stream_body);
    rops.extend_from_slice(&[
        0x5D, 0x00, 0x04, // RopCommitStream
    ]);
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

    assert!(contains_bytes(response_rops, &[0x06, 0x02, 0, 0, 0, 0, 0]));
    assert!(contains_bytes(
        response_rops,
        &[0x0A, 0x02, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        response_rops,
        &[0x2B, 0x03, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(response_rops, &[0x2F, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(
        response_rops,
        &[0x5E, 0x03, 0, 0, 0, 0, stream_body.len() as u8, 0, 0, 0]
    ));
    assert!(contains_bytes(
        response_rops,
        &[0x2D, 0x03, 0, 0, 0, 0, stream_body.len() as u8, 0]
    ));
    assert!(contains_bytes(response_rops, &[0x5D, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(
        response_rops,
        &[
            0xA3,
            0x04,
            0,
            0,
            0,
            0,
            html_stream_body.len() as u8,
            0,
            0,
            0
        ]
    ));
    assert!(contains_bytes(response_rops, &[0x5D, 0x04, 0, 0, 0, 0]));
    assert!(contains_bytes(response_rops, &utf16z("MAPI saved subject")));
    assert!(contains_bytes(
        response_rops,
        &utf16z("Body stream saved through MAPI")
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
    assert_eq!(recorded[0].body_text, "Body stream saved through MAPI");
    assert_eq!(
        recorded[0].body_html_sanitized.as_deref(),
        Some("<p>HTML stream saved through MAPI</p>")
    );
    assert_eq!(
        recorded[0].internet_message_id.as_deref(),
        Some("<mapi-save@example.test>")
    );
    assert!(recorded[0].to.is_empty());
    assert!(recorded[0].cc.is_empty());
    assert!(recorded[0].bcc.is_empty());
}

#[tokio::test]
async fn mapi_over_http_string8_property_tags_round_trip_through_canonical_unicode_property() {
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
    append_mapi_string8_property(&mut property_values, 0x0037_001E, "String8 subject");

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_create_message(&mut rops, 1, 2, test_mapi_folder_id(5));
    append_rop_set_properties(&mut rops, 2, 1, &property_values);
    rops.extend_from_slice(&[0x07, 0x00, 0x02]);
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Eu32.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    append_rop_save_changes_message(&mut rops, 1, 2);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, b"String8 subject\0"));
    assert!(contains_bytes(&response_rops, &utf16z("String8 subject")));

    let recorded = imported_emails.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].subject, "String8 subject");
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
async fn mapi_over_http_modify_recipients_string8_rows_save_canonically() {
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

    let mut to_row = Vec::new();
    to_row.extend_from_slice(b"Bob\0");
    to_row.extend_from_slice(b"bob@example.test\0");
    to_row.extend_from_slice(&1i32.to_le_bytes());
    let mut bcc_row = Vec::new();
    bcc_row.extend_from_slice(b"Hidden\0");
    bcc_row.extend_from_slice(b"hidden@example.test\0");
    bcc_row.extend_from_slice(&3i32.to_le_bytes());

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "String8 recipients");

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
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
    rops.extend_from_slice(&[
        0x0E, 0x00, 0x02, // RopModifyRecipients
    ]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&0x3001_001Eu32.to_le_bytes());
    rops.extend_from_slice(&0x3003_001Eu32.to_le_bytes());
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
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x0E, 0x02, 0, 0, 0, 0]));

    let recorded = imported_emails.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].to.len(), 1);
    assert_eq!(recorded[0].to[0].address, "bob@example.test");
    assert_eq!(recorded[0].to[0].display_name.as_deref(), Some("Bob"));
    assert_eq!(recorded[0].bcc.len(), 1);
    assert_eq!(recorded[0].bcc[0].address, "hidden@example.test");
    assert_eq!(recorded[0].bcc[0].display_name.as_deref(), Some("Hidden"));
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
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox"),
            FakeStore::mailbox("22222222-2222-2222-2222-222222222222", "sent", "Sent"),
        ])),
        ..Default::default()
    };
    let submitted_messages = store.submitted_messages.clone();
    let emails = store.emails.clone();
    let projection_store = store.clone();
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
    {
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

    let sent = {
        let canonical = emails.lock().unwrap();
        let sent = canonical
            .iter()
            .filter(|email| email.mailbox_role == "sent" && email.subject == "Submit from MAPI")
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(sent.len(), 1);
        assert!(canonical.iter().all(|email| email.mailbox_role != "outbox"));
        sent[0].clone()
    };
    assert_eq!(
        sent.mailbox_id,
        Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap()
    );
    assert_eq!(sent.mailbox_ids, vec![sent.mailbox_id]);
    assert_eq!(sent.mailbox_states.len(), 1);
    assert_eq!(sent.mailbox_states[0].mailbox_id, sent.mailbox_id);
    assert_eq!(sent.mailbox_states[0].modseq, sent.modseq);
    assert_eq!(sent.delivery_status, "queued");

    let visible = projection_store
        .fetch_jmap_emails(FakeStore::account().account_id, &[sent.id])
        .await
        .unwrap();
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].mailbox_role, "sent");
    assert!(visible[0].bcc.is_empty());
    let protected = projection_store
        .fetch_jmap_emails_with_protected_bcc(FakeStore::account().account_id, &[sent.id])
        .await
        .unwrap();
    assert_eq!(protected[0].bcc[0].address, "hidden@example.test");
    let hidden_search = projection_store
        .query_jmap_email_ids(
            FakeStore::account().account_id,
            Some(sent.mailbox_id),
            Some("hidden@example.test"),
            0,
            10,
        )
        .await
        .unwrap();
    assert!(hidden_search.ids.is_empty());
    let subject_search = projection_store
        .query_jmap_email_ids(
            FakeStore::account().account_id,
            Some(sent.mailbox_id),
            Some("Submit from MAPI"),
            0,
            10,
        )
        .await
        .unwrap();
    assert_eq!(subject_search.ids, vec![sent.id]);
}

#[tokio::test]
async fn mapi_over_http_transport_send_uses_canonical_submission() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox"),
            FakeStore::mailbox("22222222-2222-2222-2222-222222222222", "sent", "Sent"),
        ])),
        ..Default::default()
    };
    let submitted_messages = store.submitted_messages.clone();
    let emails = store.emails.clone();
    let projection_store = store.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut property_values = Vec::new();
    append_mapi_utf16_property(
        &mut property_values,
        0x0037_001F,
        "Transport send from MAPI",
    );
    append_mapi_utf16_property(
        &mut property_values,
        0x1000_001F,
        "Canonical transport body",
    );
    let to_row = mapi_recipient_row("Bob", "bob@example.test", 0x01);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_create_message(&mut rops, 1, 2, test_mapi_folder_id(5));
    append_rop_set_properties(&mut rops, 2, 2, &property_values);
    append_rop_modify_recipients(&mut rops, 2, &[(1, 0x01, to_row.as_slice())]);
    append_rop_transport_send(&mut rops, 2);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x4A, 0x02, 0, 0, 0, 0, 0, 0]
    ));

    let recorded = submitted_messages.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].source, "mapi-submit-message");
    assert_eq!(recorded[0].draft_message_id, None);
    assert_eq!(recorded[0].subject, "Transport send from MAPI");
    assert_eq!(recorded[0].body_text, "Canonical transport body");
    assert_eq!(recorded[0].from_address, "alice@example.test");
    assert_eq!(recorded[0].to.len(), 1);
    assert_eq!(recorded[0].to[0].address, "bob@example.test");
    drop(recorded);

    let sent = {
        let canonical = emails.lock().unwrap();
        let sent = canonical
            .iter()
            .filter(|email| {
                email.mailbox_role == "sent" && email.subject == "Transport send from MAPI"
            })
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(sent.len(), 1);
        assert!(canonical.iter().all(|email| email.mailbox_role != "outbox"));
        sent[0].clone()
    };
    assert_eq!(sent.delivery_status, "queued");
    assert_eq!(sent.mailbox_states[0].modseq, sent.modseq);
    let visible = projection_store
        .fetch_jmap_emails(FakeStore::account().account_id, &[sent.id])
        .await
        .unwrap();
    assert_eq!(visible[0].mailbox_role, "sent");
    assert!(visible[0].bcc.is_empty());
}

#[tokio::test]
async fn mapi_over_http_transport_send_opened_draft_preserves_canonical_attachment_and_bcc_guards()
{
    let draft_id = Uuid::parse_str("20202020-2020-2020-2020-202020202020").unwrap();
    let draft_mailbox_id = Uuid::parse_str("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee").unwrap();
    let sent_mailbox_id = Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();
    let mut draft = FakeStore::email(
        &draft_id.to_string(),
        &draft_mailbox_id.to_string(),
        "drafts",
        "Transport saved draft",
    );
    draft.body_text = "Draft body for transport send".to_string();
    draft.bcc.push(JmapEmailAddress {
        address: "transport-hidden@example.test".to_string(),
        display_name: Some("Transport Hidden".to_string()),
    });
    draft.has_attachments = true;
    let attachment_id = Uuid::parse_str("abababab-abab-abab-abab-abababababab").unwrap();
    let attachment_reference = format!("attachment:{draft_id}:{attachment_id}");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(&draft_mailbox_id.to_string(), "drafts", "Drafts"),
            FakeStore::mailbox(&sent_mailbox_id.to_string(), "sent", "Sent"),
        ])),
        attachments: Arc::new(Mutex::new(HashMap::from([(
            draft_id,
            vec![ActiveSyncAttachment {
                id: attachment_id,
                message_id: draft_id,
                file_name: "transport.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                size_octets: 7,
                file_reference: attachment_reference.clone(),
            }],
        )]))),
        attachment_contents: Arc::new(Mutex::new(HashMap::from([(
            attachment_reference.clone(),
            ActiveSyncAttachmentContent {
                file_reference: attachment_reference,
                file_name: "transport.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                blob_bytes: b"PDFDATA".to_vec(),
            },
        )]))),
        emails: Arc::new(Mutex::new(vec![draft])),
        ..Default::default()
    };
    let submitted_messages = store.submitted_messages.clone();
    let emails = store.emails.clone();
    let projection_store = store.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(14));
    append_rop_open_message(
        &mut rops,
        1,
        2,
        test_mapi_folder_id(14),
        test_mapi_message_id(&draft_id.to_string()),
    );
    append_rop_transport_send(&mut rops, 2);

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
        &[0x4A, 0x02, 0, 0, 0, 0, 0, 0]
    ));

    {
        let recorded = submitted_messages.lock().unwrap();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].source, "mapi-submit-message");
        assert_eq!(recorded[0].draft_message_id, Some(draft_id));
        assert_eq!(recorded[0].subject, "Transport saved draft");
        assert_eq!(recorded[0].bcc[0].address, "transport-hidden@example.test");
        assert_eq!(recorded[0].attachments.len(), 1);
        assert_eq!(recorded[0].attachments[0].file_name, "transport.pdf");
        assert_eq!(recorded[0].attachments[0].media_type, "application/pdf");
        assert_eq!(recorded[0].attachments[0].blob_bytes, b"PDFDATA");
    }

    let sent = {
        let canonical = emails.lock().unwrap();
        assert!(canonical.iter().all(|email| email.id != draft_id));
        let sent = canonical
            .iter()
            .filter(|email| {
                email.mailbox_role == "sent" && email.subject == "Transport saved draft"
            })
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(sent.len(), 1);
        assert!(canonical.iter().all(|email| email.mailbox_role != "outbox"));
        sent[0].clone()
    };
    assert_eq!(sent.mailbox_id, sent_mailbox_id);
    assert_eq!(sent.mailbox_ids, vec![sent_mailbox_id]);
    assert_eq!(sent.mailbox_states[0].mailbox_id, sent_mailbox_id);
    assert_eq!(sent.mailbox_states[0].modseq, sent.modseq);
    assert!(sent.has_attachments);
    assert_eq!(sent.delivery_status, "queued");

    let visible = projection_store
        .fetch_jmap_emails(FakeStore::account().account_id, &[sent.id])
        .await
        .unwrap();
    assert_eq!(visible.len(), 1);
    assert!(visible[0].bcc.is_empty());
    assert!(visible[0].has_attachments);
    let protected = projection_store
        .fetch_jmap_emails_with_protected_bcc(FakeStore::account().account_id, &[sent.id])
        .await
        .unwrap();
    assert_eq!(protected[0].bcc[0].address, "transport-hidden@example.test");
    let hidden_search = projection_store
        .query_jmap_email_ids(
            FakeStore::account().account_id,
            Some(sent_mailbox_id),
            Some("transport-hidden@example.test"),
            0,
            10,
        )
        .await
        .unwrap();
    assert!(hidden_search.ids.is_empty());
    let subject_search = projection_store
        .query_jmap_email_ids(
            FakeStore::account().account_id,
            Some(sent_mailbox_id),
            Some("Transport saved draft"),
            0,
            10,
        )
        .await
        .unwrap();
    assert_eq!(subject_search.ids, vec![sent.id]);
}

#[tokio::test]
async fn mapi_over_http_replayed_execute_request_id_does_not_resubmit_message() {
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
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "Retry-safe submit");
    append_mapi_utf16_property(&mut property_values, 0x1000_001F, "Retry body");
    let to_row = mapi_recipient_row("Bob", "bob@example.test", 0x01);
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_create_message(&mut rops, 1, 2, test_mapi_folder_id(5));
    append_rop_set_properties(&mut rops, 2, 2, &property_values);
    append_rop_modify_recipients(&mut rops, 2, &[(1, 0x01, to_row.as_slice())]);
    append_rop_submit_message(&mut rops, 2);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    execute_headers.insert(
        "x-requestid",
        HeaderValue::from_static("{11111111-2222-3333-4444-555555555555}:999999"),
    );
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let first = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();
    let first_body = response_bytes(first).await;
    let second = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();
    let second_body = response_bytes(second).await;

    assert_eq!(first_body, second_body);
    let recorded = submitted_messages.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].subject, "Retry-safe submit");
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
    draft.has_attachments = true;
    let attachment_id = Uuid::parse_str("abababab-abab-abab-abab-abababababab").unwrap();
    let attachment_reference = format!("attachment:{draft_id}:{attachment_id}");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            &draft_mailbox_id.to_string(),
            "drafts",
            "Drafts",
        )])),
        attachments: Arc::new(Mutex::new(HashMap::from([(
            draft_id,
            vec![ActiveSyncAttachment {
                id: attachment_id,
                message_id: draft_id,
                file_name: "draft.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                size_octets: 7,
                file_reference: attachment_reference.clone(),
            }],
        )]))),
        attachment_contents: Arc::new(Mutex::new(HashMap::from([(
            attachment_reference.clone(),
            ActiveSyncAttachmentContent {
                file_reference: attachment_reference,
                file_name: "draft.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                blob_bytes: b"PDFDATA".to_vec(),
            },
        )]))),
        emails: Arc::new(Mutex::new(vec![draft])),
        ..Default::default()
    };
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
    assert_eq!(recorded[0].attachments.len(), 1);
    assert_eq!(recorded[0].attachments[0].file_name, "draft.pdf");
    assert_eq!(recorded[0].attachments[0].media_type, "application/pdf");
    assert_eq!(recorded[0].attachments[0].blob_bytes, b"PDFDATA");
    let canonical = emails.lock().unwrap();
    let sent = canonical
        .iter()
        .find(|email| email.mailbox_role == "sent")
        .expect("submitted draft is visible in canonical Sent");
    assert!(sent.has_attachments);
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
    renew_mapi_request_id(&mut execute_headers);
    let sync_request = execute_body(&rop_buffer(&sync_rops, &[1, u32::MAX, u32::MAX]));
    let sync_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &sync_request)
        .await
        .unwrap();

    assert_eq!(sync_response.status(), StatusCode::OK);
    let sync_response_rops = response_rops_from_execute_response(sync_response).await;
    assert!(!contains_bytes(&sync_response_rops, b"LPE-MAPI-SYNC\0"));
    assert!(contains_bytes(
        &sync_response_rops,
        lifecycle_subject.as_bytes()
    ));
    assert!(!contains_bytes(&sync_response_rops, b"hidden@example.test"));

    let mut flag_rops = Vec::new();
    append_rop_open_folder(&mut flag_rops, 0, 1, test_mapi_folder_id(14));
    append_rop_set_read_flags(&mut flag_rops, 1, 0x04, &[draft_mapi_message_id]);
    renew_mapi_request_id(&mut execute_headers);
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
    renew_mapi_request_id(&mut execute_headers);
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
        assert_eq!(
            canonical
                .iter()
                .filter(|email| email.mailbox_role == "sent")
                .count(),
            1
        );
        assert!(canonical.iter().all(|email| email.mailbox_role != "outbox"));
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
    renew_mapi_request_id(&mut execute_headers);
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
async fn mapi_over_http_open_message_uses_targeted_store_lookup() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 2;
    let target = FakeStore::email(
        "87878787-8787-8787-8787-878787878787",
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Targeted open",
    );
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![
            target.clone(),
            FakeStore::email(
                "88888888-8888-8888-8888-888888888888",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Unopened message",
            ),
        ])),
        ..Default::default()
    };
    let queried_jmap_email_ids = store.queried_jmap_email_ids.clone();
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

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_open_message(
        &mut rops,
        1,
        2,
        test_mapi_folder_id(5),
        test_mapi_message_id(&target.id.to_string()),
    );
    append_rop_get_properties_specific(&mut rops, 2, &[0x0037_001F]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &utf16z("Targeted open")));
    assert_eq!(queried_jmap_email_ids.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn mapi_over_http_query_rows_uses_paged_content_table_lookup() {
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 3;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                "87878787-8787-8787-8787-878787878787",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Paged first",
            ),
            FakeStore::email(
                "88888888-8888-8888-8888-888888888888",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Paged second",
            ),
            FakeStore::email(
                "89898989-8989-8989-8989-898989898989",
                "55555555-5555-5555-5555-555555555555",
                "inbox",
                "Paged third",
            ),
        ])),
        ..Default::default()
    };
    let queried_jmap_email_ids = store.queried_jmap_email_ids.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_query_subject_rows(&mut rops, 1, 2, 1);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &utf16z("Paged first")));
    assert!(!contains_bytes(&response_rops, &utf16z("Paged second")));
    assert_eq!(queried_jmap_email_ids.load(Ordering::SeqCst), 0);
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
async fn mapi_over_http_read_recipients_hides_sent_message_bcc_by_default() {
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
    assert!(!contains_bytes(response_rops, &utf16z("erin@example.test")));
    assert!(!contains_bytes(response_rops, &utf16z("Erin")));
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
        0x3B, 0x00, 0x04, 0x05, // RopCloneStream
    ]);
    rops.extend_from_slice(&[
        0x2C, 0x00, 0x04, // RopReadStream
    ]);
    rops.extend_from_slice(&0xBABEu16.to_le_bytes());
    rops.extend_from_slice(&5u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x2C, 0x00, 0x05, // RopReadStream from cloned stream
    ]);
    rops.extend_from_slice(&5u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x2E, 0x00, 0x04, 0x00, // RopSeekStream, stream beginning
    ]);
    rops.extend_from_slice(&6i64.to_le_bytes());
    rops.extend_from_slice(&[
        0x2C, 0x00, 0x04, // RopReadStream
    ]);
    rops.extend_from_slice(&5u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x5B, 0x00, 0x04, // RopLockRegionStream
    ]);
    rops.extend_from_slice(&0u64.to_le_bytes());
    rops.extend_from_slice(&5u64.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x5C, 0x00, 0x04, // RopUnlockRegionStream
    ]);
    rops.extend_from_slice(&0u64.to_le_bytes());
    rops.extend_from_slice(&5u64.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(
        &rops,
        &[1, u32::MAX, u32::MAX, u32::MAX, u32::MAX, u32::MAX],
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
        &[0x2B, 0x04, 0, 0, 0, 0, 11, 0, 0, 0]
    ));
    assert!(contains_bytes(response_rops, &[0x3B, 0x05, 0, 0, 0, 0]));
    assert!(contains_bytes(
        response_rops,
        &[0x2C, 0x04, 0, 0, 0, 0, 5, 0, b'h', b'e', b'l', b'l', b'o']
    ));
    assert!(contains_bytes(
        response_rops,
        &[0x2C, 0x05, 0, 0, 0, 0, 5, 0, b'h', b'e', b'l', b'l', b'o']
    ));
    assert!(contains_bytes(
        response_rops,
        &[0x2E, 0x04, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        response_rops,
        &[0x2C, 0x04, 0, 0, 0, 0, 5, 0, b'w', b'o', b'r', b'l', b'd']
    ));
    assert!(contains_bytes(
        response_rops,
        &[0x5B, 0x04, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        response_rops,
        &[0x5C, 0x04, 0x02, 0x01, 0x04, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_reads_canonical_message_body_stream() {
    let message_id = "42424242-4242-4242-4242-424242424242";
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "Message body stream",
    );
    email.body_text = "Canonical body stream".to_string();
    email.body_html_sanitized = Some("<p>Canonical <b>HTML</b> stream</p>".to_string());
    let body_bytes = utf16z(&email.body_text);
    let html_bytes = email.body_html_sanitized.clone().unwrap().into_bytes();
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
        0x2B, 0x00, 0x02, 0x03, // RopOpenStream
    ]);
    rops.extend_from_slice(&0x1000_001Fu32.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x2C, 0x00, 0x03, // RopReadStream
    ]);
    rops.extend_from_slice(&0xBABEu16.to_le_bytes());
    rops.extend_from_slice(&(body_bytes.len() as u32).to_le_bytes());
    rops.extend_from_slice(&[
        0x2B, 0x00, 0x02, 0x04, // RopOpenStream, PidTagHtml
    ]);
    rops.extend_from_slice(&0x1013_0102u32.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x2C, 0x00, 0x04, // RopReadStream
    ]);
    rops.extend_from_slice(&0xBABEu16.to_le_bytes());
    rops.extend_from_slice(&(html_bytes.len() as u32).to_le_bytes());

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
    let response_rops = response_rops_from_execute_response(response).await;

    assert!(contains_bytes(
        &response_rops,
        &[0x2B, 0x03, 0, 0, 0, 0, body_bytes.len() as u8, 0, 0, 0]
    ));
    let mut read_response = vec![0x2C, 0x03, 0, 0, 0, 0];
    read_response.extend_from_slice(&(body_bytes.len() as u16).to_le_bytes());
    read_response.extend_from_slice(&body_bytes);
    assert!(contains_bytes(&response_rops, &read_response));
    assert!(contains_bytes(
        &response_rops,
        &[0x2B, 0x04, 0, 0, 0, 0, html_bytes.len() as u8, 0, 0, 0]
    ));
    let mut html_response = vec![0x2C, 0x04, 0, 0, 0, 0];
    html_response.extend_from_slice(&(html_bytes.len() as u16).to_le_bytes());
    html_response.extend_from_slice(&html_bytes);
    assert!(contains_bytes(&response_rops, &html_response));
}

#[tokio::test]
async fn mapi_over_http_string8_body_stream_writes_canonical_message_body() {
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
    let stream_body = b"String8 body stream\0";

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_create_message(&mut rops, 1, 2, test_mapi_folder_id(5));
    rops.extend_from_slice(&[
        0x2B, 0x00, 0x02, 0x03, // RopOpenStream, PidTagBody String8
    ]);
    rops.extend_from_slice(&0x1000_001Eu32.to_le_bytes());
    rops.push(2);
    rops.extend_from_slice(&[0x2D, 0x00, 0x03]); // RopWriteStream
    rops.extend_from_slice(&(stream_body.len() as u16).to_le_bytes());
    rops.extend_from_slice(stream_body);
    rops.extend_from_slice(&[
        0x2E, 0x00, 0x03, 0x00, // RopSeekStream, stream beginning
    ]);
    rops.extend_from_slice(&0i64.to_le_bytes());
    rops.extend_from_slice(&[0x2C, 0x00, 0x03]); // RopReadStream
    rops.extend_from_slice(&(stream_body.len() as u16).to_le_bytes());
    rops.extend_from_slice(&[0x5D, 0x00, 0x03]); // RopCommitStream
    append_rop_save_changes_message(&mut rops, 1, 2);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, stream_body));
    assert!(contains_bytes(
        &response_rops,
        &[0x0C, 0x01, 0, 0, 0, 0, 0x02]
    ));

    let recorded = imported_emails.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].body_text, "String8 body stream");
}

#[tokio::test]
async fn mapi_over_http_copy_to_stream_saves_canonical_message_body() {
    let source_message_id = "43434343-4343-4343-4343-434343434343";
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let mut inbox = FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut source = FakeStore::email(
        source_message_id,
        &inbox_id.to_string(),
        "inbox",
        "Source message body",
    );
    source.body_text = "Copied canonical body stream".to_string();
    let source_body = utf16z(&source.body_text);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![source])),
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

    let mut subject_values = Vec::new();
    append_mapi_utf16_property(&mut subject_values, 0x0037_001F, "Copied body destination");

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x03, 0x00, 0x01, 0x02, // RopOpenMessage, source
    ]);
    rops.extend_from_slice(&0x0FFFu16.to_le_bytes());
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&test_mapi_message_id(source_message_id).to_le_bytes());
    rops.extend_from_slice(&[
        0x2B, 0x00, 0x02, 0x03, // RopOpenStream, source body
    ]);
    rops.extend_from_slice(&0x1000_001Fu32.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x06, 0x00, 0x01, 0x04, // RopCreateMessage, destination
    ]);
    rops.extend_from_slice(&1200u16.to_le_bytes());
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x0A, 0x00, 0x04, // RopSetProperties, destination subject
    ]);
    rops.extend_from_slice(&((subject_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&subject_values);
    rops.extend_from_slice(&[
        0x2B, 0x00, 0x04, 0x05, // RopOpenStream, destination body
    ]);
    rops.extend_from_slice(&0x1000_001Fu32.to_le_bytes());
    rops.push(2);
    rops.extend_from_slice(&[
        0x3A, 0x00, 0x03, 0x05, // RopCopyToStream
    ]);
    rops.extend_from_slice(&(source_body.len() as u64).to_le_bytes());
    rops.extend_from_slice(&[
        0x0C, 0x00, 0x01, 0x04, 0x00, // RopSaveChangesMessage
    ]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(
        &rops,
        &[1, u32::MAX, u32::MAX, u32::MAX, u32::MAX, u32::MAX],
    ));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[
            0x3A,
            0x03,
            0,
            0,
            0,
            0,
            source_body.len() as u8,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            source_body.len() as u8,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
        ]
    ));

    let recorded = imported_emails.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].mailbox_id, inbox_id);
    assert_eq!(recorded[0].subject, "Copied body destination");
    assert_eq!(recorded[0].body_text, "Copied canonical body stream");
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
        0x2B, 0x00, 0x03, 0x04, // RopOpenStream, read-only
    ]);
    rops.extend_from_slice(&0x3701_0102u32.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x2D, 0x00, 0x04, // RopWriteStream
    ]);
    rops.extend_from_slice(&4u16.to_le_bytes());
    rops.extend_from_slice(b"fake");
    rops.extend_from_slice(&[
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
        &[0x23, 0x03, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        response_rops,
        &[0x2B, 0x04, 0, 0, 0, 0, 9, 0, 0, 0]
    ));
    assert!(contains_bytes(
        response_rops,
        &[0x2D, 0x04, 0x05, 0x00, 0x03, 0x80]
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
        0x2F, 0x00, 0x04, // RopSetStreamSize
    ]);
    rops.extend_from_slice(&(stream_bytes.len() as u64).to_le_bytes());
    rops.extend_from_slice(&[
        0x5E, 0x00, 0x04, // RopGetStreamSize
    ]);
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

    assert!(contains_bytes(response_rops, &[0x2F, 0x04, 0, 0, 0, 0]));
    assert!(contains_bytes(
        response_rops,
        &[0x5E, 0x04, 0, 0, 0, 0, stream_bytes.len() as u8, 0, 0, 0]
    ));
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
async fn mapi_over_http_copy_to_stream_saves_canonical_attachment() {
    let message_id = "41414141-4141-4141-4141-414141414141";
    let message_uuid = Uuid::parse_str(message_id).unwrap();
    let source_attachment_id = Uuid::parse_str("dadadada-dada-dada-dada-dadadadadada").unwrap();
    let mut inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        message_id,
        "55555555-5555-5555-5555-555555555555",
        "inbox",
        "MAPI stream copy attachment message",
    );
    email.has_attachments = true;
    let source_reference = format!("attachment:{message_uuid}:{source_attachment_id}");
    let source_bytes = b"%PDF-copy-source";
    let created_attachments = Arc::new(Mutex::new(Vec::new()));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![email])),
        attachments: Arc::new(Mutex::new(HashMap::from([(
            message_uuid,
            vec![ActiveSyncAttachment {
                id: source_attachment_id,
                message_id: message_uuid,
                file_name: "source.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                size_octets: source_bytes.len() as u64,
                file_reference: source_reference.clone(),
            }],
        )]))),
        attachment_contents: Arc::new(Mutex::new(HashMap::from([(
            source_reference.clone(),
            ActiveSyncAttachmentContent {
                file_reference: source_reference,
                file_name: "source.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                blob_bytes: source_bytes.to_vec(),
            },
        )]))),
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
    append_mapi_utf16_property(&mut property_values, 0x3707_001F, "copied-stream.pdf");
    append_mapi_utf16_property(&mut property_values, 0x370E_001F, "application/pdf");

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
        0x2B, 0x00, 0x03, 0x04, // RopOpenStream, source
    ]);
    rops.extend_from_slice(&0x3701_0102u32.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x23, 0x00, 0x02, 0x05, // RopCreateAttachment
        0x0A, 0x00, 0x05, // RopSetProperties
    ]);
    rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
    rops.extend_from_slice(&[
        0x2B, 0x00, 0x05, 0x06, // RopOpenStream, destination
    ]);
    rops.extend_from_slice(&0x3701_0102u32.to_le_bytes());
    rops.push(1);
    rops.extend_from_slice(&[
        0x3A, 0x00, 0x04, 0x06, // RopCopyToStream
    ]);
    rops.extend_from_slice(&(source_bytes.len() as u64).to_le_bytes());
    rops.extend_from_slice(&[
        0x25, 0x00, 0x02, 0x05, 0x00, // RopSaveChangesAttachment
    ]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(
        &rops,
        &[
            1,
            u32::MAX,
            u32::MAX,
            u32::MAX,
            u32::MAX,
            u32::MAX,
            u32::MAX,
        ],
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
        &[
            0x3A,
            0x04,
            0,
            0,
            0,
            0,
            source_bytes.len() as u8,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            source_bytes.len() as u8,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
        ]
    ));
    assert!(contains_bytes(response_rops, &[0x25, 0x02, 0, 0, 0, 0]));
    let created = created_attachments.lock().unwrap();
    assert_eq!(created.len(), 1);
    assert_eq!(created[0].file_name, "copied-stream.pdf");
    assert_eq!(created[0].blob_bytes, source_bytes);
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
    let folder_change = mapi_mailstore::canonical_folder_change_number(&inbox);
    let mut email = FakeStore::email(message_id, mailbox_id, "inbox", "Cached mode message");
    email.flagged = true;
    let message_change_number = mapi_mailstore::canonical_message_change_number(&email);
    let message_commit_time = mapi_mailstore::filetime_from_rfc3339_utc(&email.received_at);
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
        0x07, 0x00, 0x01, // RopGetPropertiesSpecific on the folder
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());
    rops.extend_from_slice(&5u16.to_le_bytes());
    rops.extend_from_slice(&0x6709_0040u32.to_le_bytes());
    rops.extend_from_slice(&0x670A_0040u32.to_le_bytes());
    rops.extend_from_slice(&0x663E_0003u32.to_le_bytes());
    rops.extend_from_slice(&0x4082_0040u32.to_le_bytes());
    rops.extend_from_slice(&0x65E2_0102u32.to_le_bytes());
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
    rops.extend_from_slice(&6u16.to_le_bytes());
    rops.extend_from_slice(&0x65E0_0102u32.to_le_bytes());
    rops.extend_from_slice(&0x65E1_0102u32.to_le_bytes());
    rops.extend_from_slice(&0x65E2_0102u32.to_le_bytes());
    rops.extend_from_slice(&0x67A4_0014u32.to_le_bytes());
    rops.extend_from_slice(&0x6709_0040u32.to_le_bytes());
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
    let message_uuid = Uuid::parse_str(message_id).unwrap();
    let message_source_key = mapi_mailstore::source_key_for_uuid(&message_uuid);
    let message_change_key = mapi_mailstore::change_key_for_change_number(message_change_number);
    let mut source_key_wire_value = 22u16.to_le_bytes().to_vec();
    source_key_wire_value.extend_from_slice(&message_source_key);
    let mut change_key_wire_value = 22u16.to_le_bytes().to_vec();
    change_key_wire_value.extend_from_slice(&message_change_key);
    assert!(contains_bytes(&response_rops, &source_key_wire_value));
    assert!(contains_bytes(&response_rops, &change_key_wire_value));
    assert!(contains_bytes(
        &response_rops,
        &mapi_mailstore::filetime_from_change_number(folder_change).to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &(folder_change.min(u64::from(u32::MAX)) as u32).to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &message_commit_time.to_le_bytes()
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
        0x01, 0x00, 0x00, 0x00, // content sync
        0x00, 0x00, // RestrictionDataSize
        0x00, 0x00, 0x00, 0x00, // SynchronizationExtraFlags
        0x00, 0x00, // PropertyTagCount
        0x4E, 0x00, 0x02, // RopFastTransferSourceGetBuffer
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
    assert!(!contains_bytes(&response_rops, b"LPE-MAPI-SYNC\0"));
    assert!(contains_bytes(&response_rops, b"Sync manifest message"));
    assert!(!contains_bytes(&response_rops, b"hidden@example.test"));
}

#[tokio::test]
async fn mapi_over_http_content_sync_uses_mailbox_state_membership() {
    let inbox_id = Uuid::parse_str("97979797-9797-4797-9797-979797979797").unwrap();
    let sent_id = Uuid::parse_str("98989898-9898-4898-9898-989898989898").unwrap();
    let mut inbox = FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox");
    inbox.total_emails = 1;
    let sent = FakeStore::mailbox(&sent_id.to_string(), "sent", "Sent");
    let mut email = FakeStore::email(
        "99999999-9999-4999-9999-999999999999",
        &sent_id.to_string(),
        "sent",
        "Inbox membership sync",
    );
    email.mailbox_ids.push(inbox_id);
    email.mailbox_states.push(JmapEmailMailboxState {
        mailbox_id: inbox_id,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        modseq: 42,
        unread: false,
        flagged: false,
        draft: false,
    });
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox, sent])),
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
        0x01, 0x00, 0x00, 0x00, // content sync
        0x00, 0x00, // RestrictionDataSize
        0x00, 0x00, 0x00, 0x00, // SynchronizationExtraFlags
        0x00, 0x00, // PropertyTagCount
        0x4E, 0x00, 0x02, // RopFastTransferSourceGetBuffer
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
    assert!(contains_bytes(&response_rops, b"Inbox membership sync"));
}

#[tokio::test]
async fn mapi_over_http_tell_version_accepts_fast_transfer_sync_context() {
    let message_id = "40404040-4040-4040-4040-404040404041";
    let mailbox_id = "55555555-5555-5555-5555-555555555555";
    let mut inbox = FakeStore::mailbox(mailbox_id, "inbox", "Inbox");
    inbox.total_emails = 1;
    let email = FakeStore::email(message_id, mailbox_id, "inbox", "TellVersion sync message");
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
        0x01, 0x00, 0x00, 0x00, // content sync
        0x00, 0x00, // RestrictionDataSize
        0x00, 0x00, 0x00, 0x00, // SynchronizationExtraFlags
        0x00, 0x00, // PropertyTagCount
        0x86, 0x00, 0x02, // RopTellVersion
    ]);
    rops.extend_from_slice(&[15, 20, 0, 1, 0, 0]);
    rops.extend_from_slice(&[
        0x4E, 0x00, 0x02, // RopFastTransferSourceGetBuffer
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
    assert!(contains_bytes(&response_rops, &[0x86, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, b"TellVersion sync message"));
}

#[tokio::test]
async fn mapi_over_http_sync_configure_separates_content_and_hierarchy_manifests() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let sent_id = "22222222-2222-2222-2222-222222222222";
    let mut inbox = FakeStore::mailbox(inbox_id, "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut sent = FakeStore::mailbox(sent_id, "sent", "Sent");
    sent.total_emails = 1;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox, sent])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                "41414141-4141-4141-4141-414141414141",
                inbox_id,
                "inbox",
                "Inbox scoped sync",
            ),
            FakeStore::email(
                "42424242-4242-4242-4242-424242424242",
                sent_id,
                "sent",
                "Sent scoped sync",
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

    let mut content_rops = Vec::new();
    append_rop_open_folder(&mut content_rops, 0, 1, test_mapi_folder_id(5));
    append_rop_sync_manifest_get_buffer(&mut content_rops, 1, 2, 4096);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let content_request = execute_body(&rop_buffer(&content_rops, &[1, u32::MAX, u32::MAX]));
    let content_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &content_request)
        .await
        .unwrap();
    let content_rops = response_rops_from_execute_response(content_response).await;

    assert_eq!(mapi_sync_manifest_counts(&content_rops), Some((1, 1)));
    assert!(contains_bytes(&content_rops, b"Inbox scoped sync"));
    assert!(!contains_bytes(&content_rops, b"Sent scoped sync"));

    let mut hierarchy_rops = Vec::new();
    append_rop_open_folder(&mut hierarchy_rops, 0, 1, test_mapi_folder_id(1));
    hierarchy_rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x02, 0x00, 0x00, 0x00, // hierarchy sync
        0x00, 0x00, // RestrictionDataSize
        0x01, 0x00, 0x00, 0x00, // SynchronizationExtraFlags, Eid
        0x00, 0x00, // PropertyTagCount
        0x4E, 0x00, 0x02, // RopFastTransferSourceGetBuffer
    ]);
    hierarchy_rops.extend_from_slice(&16384u16.to_le_bytes());
    renew_mapi_request_id(&mut execute_headers);
    let hierarchy_request = execute_body(&rop_buffer(&hierarchy_rops, &[1, u32::MAX, u32::MAX]));
    let hierarchy_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &hierarchy_request)
        .await
        .unwrap();
    let hierarchy_rops = response_rops_from_execute_response(hierarchy_response).await;

    assert_eq!(mapi_sync_manifest_counts(&hierarchy_rops), Some((12, 0)));
    assert!(!contains_bytes(&hierarchy_rops, b"Inbox scoped sync"));
    assert!(!contains_bytes(&hierarchy_rops, b"Sent scoped sync"));
}

#[tokio::test]
async fn mapi_over_http_sync_checkpoint_resumes_incremental_content_with_tombstone() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let unchanged_id = Uuid::parse_str("41414141-4141-4141-4141-414141414141").unwrap();
    let changed_id = Uuid::parse_str("42424242-4242-4242-4242-424242424242").unwrap();
    let deleted_id = Uuid::parse_str("43434343-4343-4343-4343-434343434343").unwrap();
    let mut inbox = FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox");
    inbox.total_emails = 2;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                &unchanged_id.to_string(),
                &inbox_id.to_string(),
                "inbox",
                "Checkpoint unchanged",
            ),
            FakeStore::email(
                &changed_id.to_string(),
                &inbox_id.to_string(),
                "inbox",
                "Checkpoint changed",
            ),
        ])),
        ..Default::default()
    };
    store
        .store_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            Some(inbox_id),
            MapiCheckpointKind::Content,
            10,
            4,
            serde_json::json!({"source": "previous-run"}),
        )
        .await
        .unwrap();
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 12,
        current_modseq: 6,
        changed_message_ids: vec![changed_id],
        deleted_message_ids: vec![deleted_id],
        ..Default::default()
    };

    let service = ExchangeService::new(store.clone());
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_sync_manifest_get_buffer_with_state(&mut rops, 1, 2, 4096, b"client-content-state");
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert_eq!(mapi_sync_manifest_counts(&response_rops), Some((0, 1)));
    assert!(contains_bytes(&response_rops, b"Checkpoint changed"));
    assert!(!contains_bytes(&response_rops, b"Checkpoint unchanged"));
    assert!(contains_bytes(
        &response_rops,
        &0x4013_0003u32.to_le_bytes()
    ));
    let deleted_counter = test_mapi_message_id(&deleted_id.to_string()) >> 16;
    let mut deleted_idset = 1u16.to_le_bytes().to_vec();
    deleted_idset.push(0x52);
    deleted_idset.extend_from_slice(&globcnt_bytes(deleted_counter));
    deleted_idset.extend_from_slice(&globcnt_bytes(deleted_counter));
    deleted_idset.push(0);
    let mut deleted_property = 0x4018_0102u32.to_le_bytes().to_vec();
    deleted_property.extend_from_slice(&(deleted_idset.len() as u32).to_le_bytes());
    deleted_property.extend_from_slice(&deleted_idset);
    assert!(contains_bytes(&response_rops, &deleted_property));

    let checkpoint = store
        .fetch_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            Some(inbox_id),
            MapiCheckpointKind::Content,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(checkpoint.last_change_sequence, 12);
    assert_eq!(checkpoint.last_modseq, 6);

    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 12,
        current_modseq: 6,
        ..Default::default()
    };
    let restarted = ExchangeService::new(store.clone());
    let connect = restarted
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut restarted_headers = mapi_headers("Execute");
    restarted_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let mut restart_rops = Vec::new();
    append_rop_open_folder(&mut restart_rops, 0, 1, test_mapi_folder_id(5));
    append_rop_sync_manifest_get_buffer_with_state(
        &mut restart_rops,
        1,
        2,
        4096,
        b"client-content-state",
    );
    let response = restarted
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &restarted_headers,
            &execute_body(&rop_buffer(&restart_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert_eq!(mapi_sync_manifest_counts(&response_rops), None);
    assert!(!contains_bytes(&response_rops, b"Checkpoint changed"));
    assert!(!contains_bytes(&response_rops, b"LPE-MAPI-SYNC\0"));
    assert!(contains_bytes(
        &response_rops,
        &0x403A_0003u32.to_le_bytes()
    ));
}

#[tokio::test]
async fn mapi_over_http_content_sync_first_baseline_exports_all_current_messages() {
    let inbox_id = Uuid::parse_str("51515151-5151-5151-5151-515151515151").unwrap();
    let first_id = Uuid::parse_str("61616161-6161-6161-6161-616161616161").unwrap();
    let second_id = Uuid::parse_str("62626262-6262-6262-6262-626262626262").unwrap();
    let removed_id = Uuid::parse_str("63636363-6363-6363-6363-636363636363").unwrap();
    let mut inbox = FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox");
    inbox.total_emails = 2;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                &first_id.to_string(),
                &inbox_id.to_string(),
                "inbox",
                "Baseline first",
            ),
            FakeStore::email(
                &second_id.to_string(),
                &inbox_id.to_string(),
                "inbox",
                "Baseline second",
            ),
        ])),
        ..Default::default()
    };
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 55,
        current_modseq: 41,
        changed_message_ids: vec![first_id],
        deleted_message_ids: vec![removed_id],
        ..Default::default()
    };

    let response_rops = content_sync_response_rops(store, 5, &[]).await;

    assert_eq!(
        mapi_sync_manifest_counts(&response_rops).map(|(_, messages)| messages),
        Some(2)
    );
    assert!(contains_bytes(&response_rops, b"Baseline first"));
    assert!(contains_bytes(&response_rops, b"Baseline second"));
    assert!(!contains_bytes(
        &response_rops,
        &META_TAG_IDSET_DELETED.to_le_bytes()
    ));
    assert_content_final_state_includes(&response_rops, &[first_id, second_id], &[41]);
}

#[tokio::test]
async fn mapi_over_http_content_sync_incremental_after_client_state_exports_delta() {
    let inbox_id = Uuid::parse_str("52525252-5252-5252-5252-525252525252").unwrap();
    let unchanged_id = Uuid::parse_str("64646464-6464-6464-6464-646464646464").unwrap();
    let changed_id = Uuid::parse_str("65656565-6565-6565-6565-656565656565").unwrap();
    let mut inbox = FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox");
    inbox.total_emails = 2;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(
                &unchanged_id.to_string(),
                &inbox_id.to_string(),
                "inbox",
                "Incremental unchanged",
            ),
            FakeStore::email(
                &changed_id.to_string(),
                &inbox_id.to_string(),
                "inbox",
                "Incremental changed",
            ),
        ])),
        ..Default::default()
    };
    store
        .store_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            Some(inbox_id),
            MapiCheckpointKind::Content,
            20,
            40,
            serde_json::json!({"source": "previous-run"}),
        )
        .await
        .unwrap();
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 21,
        current_modseq: 41,
        changed_message_ids: vec![changed_id],
        ..Default::default()
    };

    let response_rops = content_sync_response_rops(store, 5, b"client-content-state").await;

    assert_eq!(mapi_sync_manifest_counts(&response_rops), Some((0, 1)));
    assert!(contains_bytes(&response_rops, b"Incremental changed"));
    assert!(!contains_bytes(&response_rops, b"Incremental unchanged"));
    assert_content_final_state_includes(&response_rops, &[unchanged_id, changed_id], &[41]);
}

#[tokio::test]
async fn mapi_over_http_content_sync_move_across_folders_exports_source_tombstone_and_target_change(
) {
    let inbox_id = Uuid::parse_str("53535353-5353-5353-5353-535353535353").unwrap();
    let archive_id = Uuid::parse_str("54545454-5454-5454-5454-545454545454").unwrap();
    let moved_id = Uuid::parse_str("66666666-6666-6666-6666-666666666666").unwrap();
    let inbox = FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox");
    let mut archive = FakeStore::mailbox(&archive_id.to_string(), "archive", "Archive");
    archive.total_emails = 1;
    let moved = FakeStore::email(
        &moved_id.to_string(),
        &archive_id.to_string(),
        "archive",
        "Moved canonical message",
    );
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox, archive])),
        emails: Arc::new(Mutex::new(vec![moved])),
        ..Default::default()
    };
    for mailbox_id in [inbox_id, archive_id] {
        store
            .store_mapi_sync_checkpoint(
                FakeStore::account().account_id,
                Some(mailbox_id),
                MapiCheckpointKind::Content,
                30,
                40,
                serde_json::json!({"source": "previous-run"}),
            )
            .await
            .unwrap();
    }
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 31,
        current_modseq: 41,
        changed_message_ids: vec![moved_id],
        deleted_message_ids: vec![moved_id],
        ..Default::default()
    };

    let source_rops = content_sync_response_rops(store.clone(), 5, b"client-content-state").await;
    let target_rops = content_sync_response_rops(
        store,
        test_mapi_uuid_id(&archive_id) >> 16,
        b"client-content-state",
    )
    .await;

    assert_eq!(mapi_sync_manifest_counts(&source_rops), None);
    assert!(contains_bytes(
        &source_rops,
        &mapi_deleted_message_idset_property(&[moved_id])
    ));
    assert_content_final_state_includes(&source_rops, &[], &[]);
    assert_eq!(mapi_sync_manifest_counts(&target_rops), Some((0, 1)));
    assert!(contains_bytes(&target_rops, b"Moved canonical message"));
    assert_content_final_state_includes(&target_rops, &[moved_id], &[41]);
}

#[tokio::test]
async fn mapi_over_http_content_sync_hard_delete_exports_tombstone_and_empty_final_state() {
    let inbox_id = Uuid::parse_str("56565656-5656-5656-5656-565656565656").unwrap();
    let deleted_id = Uuid::parse_str("67676767-6767-6767-6767-676767676767").unwrap();
    let inbox = FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(Vec::new())),
        ..Default::default()
    };
    store
        .store_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            Some(inbox_id),
            MapiCheckpointKind::Content,
            40,
            40,
            serde_json::json!({"source": "previous-run"}),
        )
        .await
        .unwrap();
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 41,
        current_modseq: 41,
        deleted_message_ids: vec![deleted_id],
        ..Default::default()
    };

    let response_rops = content_sync_response_rops(store, 5, b"client-content-state").await;

    assert_eq!(mapi_sync_manifest_counts(&response_rops), None);
    assert!(contains_bytes(
        &response_rops,
        &mapi_deleted_message_idset_property(&[deleted_id])
    ));
    assert_content_final_state_includes(&response_rops, &[], &[]);
}

#[tokio::test]
async fn mapi_over_http_content_sync_read_flag_update_exports_read_state() {
    let inbox_id = Uuid::parse_str("57575757-5757-5757-5757-575757575757").unwrap();
    let message_id = Uuid::parse_str("68686868-6868-6868-6868-686868686868").unwrap();
    let mut inbox = FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        &message_id.to_string(),
        &inbox_id.to_string(),
        "inbox",
        "Read flag canonical update",
    );
    email.unread = false;
    email.mailbox_states[0].unread = false;
    email.modseq = 47;
    email.mailbox_states[0].modseq = 47;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    store
        .store_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            Some(inbox_id),
            MapiCheckpointKind::Content,
            46,
            46,
            serde_json::json!({"source": "previous-run"}),
        )
        .await
        .unwrap();
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 47,
        current_modseq: 47,
        changed_message_ids: vec![message_id],
        ..Default::default()
    };

    let response_rops = content_sync_response_rops(store, 5, b"client-content-state").await;

    assert_eq!(mapi_sync_manifest_counts(&response_rops), Some((0, 1)));
    assert!(contains_bytes(
        &response_rops,
        &0x402F_0003u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x0000_0001i32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        b"Read flag canonical update"
    ));
    assert_content_final_state_includes(&response_rops, &[message_id], &[47]);
    assert!(contains_bytes(
        &response_rops,
        &mapi_message_cnset_property(META_TAG_CNSET_READ, &[47])
    ));
}

#[tokio::test]
async fn mapi_over_http_content_sync_incremental_does_not_leak_protected_bcc() {
    let inbox_id = Uuid::parse_str("58585858-5858-5858-5858-585858585858").unwrap();
    let message_id = Uuid::parse_str("69696969-6969-6969-6969-696969696969").unwrap();
    let mut inbox = FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        &message_id.to_string(),
        &inbox_id.to_string(),
        "inbox",
        "Protected Bcc sync",
    );
    email.bcc.push(JmapEmailAddress {
        address: "hidden@example.test".to_string(),
        display_name: Some("Hidden Bcc".to_string()),
    });
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    store
        .store_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            Some(inbox_id),
            MapiCheckpointKind::Content,
            50,
            40,
            serde_json::json!({"source": "previous-run"}),
        )
        .await
        .unwrap();
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 51,
        current_modseq: 41,
        changed_message_ids: vec![message_id],
        ..Default::default()
    };

    let response_rops = content_sync_response_rops(store, 5, b"client-content-state").await;

    assert_eq!(mapi_sync_manifest_counts(&response_rops), Some((0, 1)));
    assert!(contains_bytes(&response_rops, b"Protected Bcc sync"));
    assert!(!contains_bytes(&response_rops, b"hidden@example.test"));
    assert!(!contains_bytes(&response_rops, b"Hidden Bcc"));
    assert_content_final_state_includes(&response_rops, &[message_id], &[41]);
}

#[tokio::test]
async fn mapi_over_http_sync_manifest_includes_attachment_change_facts_without_bcc() {
    let message_uuid = Uuid::parse_str("43434343-4343-4343-4343-434343434343").unwrap();
    let attachment_id = Uuid::parse_str("abababab-abab-abab-abab-abababababab").unwrap();
    let mailbox_id = "55555555-5555-5555-5555-555555555555";
    let mut inbox = FakeStore::mailbox(mailbox_id, "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        &message_uuid.to_string(),
        mailbox_id,
        "inbox",
        "Attachment sync message",
    );
    email.has_attachments = true;
    email.bcc.push(JmapEmailAddress {
        address: "hidden@example.test".to_string(),
        display_name: None,
    });
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
                size_octets: 42,
                file_reference: file_reference.clone(),
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

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_sync_manifest_get_buffer(&mut rops, 1, 2, 4096);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert_eq!(mapi_sync_manifest_counts(&response_rops), Some((1, 1)));
    assert!(contains_bytes(&response_rops, b"Attachment sync message"));
    assert!(contains_bytes(&response_rops, b"brief.pdf"));
    assert!(contains_bytes(&response_rops, b"application/pdf"));
    assert!(contains_bytes(&response_rops, file_reference.as_bytes()));
    assert!(!contains_bytes(&response_rops, b"hidden@example.test"));
}

#[tokio::test]
async fn mapi_over_http_sync_manifest_includes_visible_recipient_facts_without_bcc() {
    let message_uuid = Uuid::parse_str("44444444-4444-4444-4444-444444444444").unwrap();
    let mailbox_id = "55555555-5555-5555-5555-555555555555";
    let mut inbox = FakeStore::mailbox(mailbox_id, "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        &message_uuid.to_string(),
        mailbox_id,
        "inbox",
        "Recipient sync message",
    );
    email.to.push(JmapEmailAddress {
        address: "to@example.test".to_string(),
        display_name: Some("Visible To".to_string()),
    });
    email.cc.push(JmapEmailAddress {
        address: "cc@example.test".to_string(),
        display_name: Some("Visible Cc".to_string()),
    });
    email.bcc.push(JmapEmailAddress {
        address: "hidden@example.test".to_string(),
        display_name: Some("Hidden Bcc".to_string()),
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

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_sync_manifest_get_buffer(&mut rops, 1, 2, 4096);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert_eq!(mapi_sync_manifest_counts(&response_rops), Some((1, 1)));
    assert!(contains_bytes(&response_rops, b"Recipient sync message"));
    assert!(contains_bytes(&response_rops, b"to@example.test"));
    assert!(contains_bytes(&response_rops, b"Visible To"));
    assert!(contains_bytes(&response_rops, b"cc@example.test"));
    assert!(contains_bytes(&response_rops, b"Visible Cc"));
    assert!(!contains_bytes(&response_rops, b"hidden@example.test"));
    assert!(!contains_bytes(&response_rops, b"Hidden Bcc"));
}

#[tokio::test]
async fn mapi_over_http_sync_manifest_includes_canonical_read_flag_state() {
    let message_uuid = Uuid::parse_str("45454545-4545-4545-4545-454545454545").unwrap();
    let mailbox_id = "55555555-5555-5555-5555-555555555555";
    let mut inbox = FakeStore::mailbox(mailbox_id, "inbox", "Inbox");
    inbox.total_emails = 1;
    inbox.unread_emails = 1;
    let mut email = FakeStore::email(
        &message_uuid.to_string(),
        mailbox_id,
        "inbox",
        "Read flag sync message",
    );
    email.unread = false;
    email.flagged = true;
    email.has_attachments = true;
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

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_sync_manifest_get_buffer(&mut rops, 1, 2, 4096);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert_eq!(mapi_sync_manifest_counts(&response_rops), Some((1, 1)));
    assert_eq!(
        mapi_sync_manifest_message_state(&response_rops, "Read flag sync message"),
        Some((0x0000_0011, 2))
    );
}

#[tokio::test]
async fn mapi_over_http_sync_manifest_includes_stable_change_key_facts_without_bcc() {
    let message_uuid = Uuid::parse_str("46464646-4646-4646-4646-464646464646").unwrap();
    let mailbox_id = "55555555-5555-5555-5555-555555555555";
    let mut inbox = FakeStore::mailbox(mailbox_id, "inbox", "Inbox");
    inbox.total_emails = 1;
    let mut email = FakeStore::email(
        &message_uuid.to_string(),
        mailbox_id,
        "inbox",
        "Change key sync message",
    );
    email.bcc.push(JmapEmailAddress {
        address: "hidden@example.test".to_string(),
        display_name: Some("Hidden Bcc".to_string()),
    });
    let change_number = mapi_mailstore::canonical_message_change_number(&email);
    let change_key = mapi_mailstore::change_key_for_change_number(change_number);
    let predecessor_change_list = mapi_mailstore::predecessor_change_list(change_number);
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

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    append_rop_sync_manifest_get_buffer(&mut rops, 1, 2, 4096);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert_eq!(mapi_sync_manifest_counts(&response_rops), Some((1, 1)));
    assert!(contains_bytes(&response_rops, b"Change key sync message"));
    assert!(contains_bytes(&response_rops, &change_key));
    assert!(contains_bytes(&response_rops, &predecessor_change_list));
    assert!(!contains_bytes(&response_rops, b"hidden@example.test"));
    assert!(!contains_bytes(&response_rops, b"Hidden Bcc"));
}

#[tokio::test]
async fn mapi_over_http_hierarchy_sync_manifest_includes_folder_change_key_facts() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let mut inbox = FakeStore::mailbox(inbox_id, "inbox", "Inbox");
    inbox.total_emails = 3;
    inbox.unread_emails = 1;
    let change_number = mapi_mailstore::canonical_folder_change_number(&inbox);
    let change_key = mapi_mailstore::change_key_for_change_number(change_number);
    let predecessor_change_list = mapi_mailstore::predecessor_change_list(change_number);
    let email = FakeStore::email(
        "57575757-5757-5757-5757-575757575757",
        inbox_id,
        "inbox",
        "Hierarchy aggregate message",
    );
    let message_change_number = mapi_mailstore::canonical_message_change_number(&email);
    assert_ne!(change_number, message_change_number);
    let local_commit_time_max = mapi_mailstore::filetime_from_change_number(message_change_number);
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

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(4));
    rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x02, 0x00, 0x00, 0x00, // hierarchy sync
        0x00, 0x00, // RestrictionDataSize
        0x01, 0x00, 0x00, 0x00, // SynchronizationExtraFlags, Eid
        0x00, 0x00, // PropertyTagCount
        0x4E, 0x00, 0x02, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&8192u16.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert_eq!(mapi_sync_manifest_counts(&response_rops), Some((11, 0)));
    assert!(contains_bytes(&response_rops, &change_key));
    assert!(contains_bytes(&response_rops, &predecessor_change_list));
    let mut local_commit_time_property = 0x670A_0040u32.to_le_bytes().to_vec();
    local_commit_time_property.extend_from_slice(&(local_commit_time_max as i64).to_le_bytes());
    assert!(contains_bytes(&response_rops, &local_commit_time_property));
    let mut deleted_count_property = 0x670B_0003u32.to_le_bytes().to_vec();
    deleted_count_property.extend_from_slice(&0i32.to_le_bytes());
    assert!(contains_bytes(&response_rops, &deleted_count_property));
    let mut folder_type_property = 0x3601_0003u32.to_le_bytes().to_vec();
    folder_type_property.extend_from_slice(&1i32.to_le_bytes());
    assert!(contains_bytes(&response_rops, &folder_type_property));
    let final_cnset_seen = mapi_last_binary_property(&response_rops, 0x6796_0102).unwrap();
    assert!(contains_bytes(
        final_cnset_seen,
        &globcnt_bytes(change_number)
    ));
    assert!(!contains_bytes(
        final_cnset_seen,
        &globcnt_bytes(message_change_number)
    ));
}

#[tokio::test]
async fn mapi_over_http_outlook_hierarchy_sync_manifest_includes_folders() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(4));
    append_rop_outlook_hierarchy_sync_manifest_get_buffer(&mut rops, 1, 2, 4096);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;
    let get_buffer_response_offset = response_rops
        .windows(6)
        .position(|window| window == [0x4E, 0x02, 0x00, 0x00, 0x00, 0x00])
        .unwrap();
    assert_eq!(
        u16::from_le_bytes(
            response_rops[get_buffer_response_offset + 6..get_buffer_response_offset + 8]
                .try_into()
                .unwrap()
        ),
        0x0003
    );
    assert_eq!(
        u16::from_le_bytes(
            response_rops[get_buffer_response_offset + 8..get_buffer_response_offset + 10]
                .try_into()
                .unwrap()
        ),
        1
    );
    assert_eq!(
        u16::from_le_bytes(
            response_rops[get_buffer_response_offset + 10..get_buffer_response_offset + 12]
                .try_into()
                .unwrap()
        ),
        1
    );
    assert_eq!(response_rops[get_buffer_response_offset + 12], 0);
    let transfer_buffer_size = u16::from_le_bytes(
        response_rops[get_buffer_response_offset + 13..get_buffer_response_offset + 15]
            .try_into()
            .unwrap(),
    ) as usize;
    assert!(transfer_buffer_size > 0);
    assert!(response_rops.len() >= get_buffer_response_offset + 15 + transfer_buffer_size);

    assert!(contains_bytes(
        &response_rops,
        &0x4012_0003u32.to_le_bytes()
    ));
    assert!(contains_bytes(&response_rops, &utf16z("Inbox")));
    assert!(contains_bytes(
        &response_rops,
        &0x6749_0014u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &test_mapi_folder_id(4).to_le_bytes()
    ));
    assert!(!contains_bytes(
        &response_rops,
        &0x6748_0014u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x3008_0040u32.to_le_bytes()
    ));
    let mut empty_local_commit_time_property = 0x670A_0040u32.to_le_bytes().to_vec();
    empty_local_commit_time_property.extend_from_slice(&0i64.to_le_bytes());
    assert!(contains_bytes(
        &response_rops,
        &empty_local_commit_time_property
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x3613_001Fu32.to_le_bytes()
    ));
    assert!(!contains_bytes(
        &response_rops,
        &0x001A_001Fu32.to_le_bytes()
    ));
    let tag_position = |tag: u32| {
        let tag_bytes = tag.to_le_bytes();
        response_rops
            .windows(tag_bytes.len())
            .position(|window| window == tag_bytes)
            .unwrap()
    };
    assert!(
        tag_position(0x65E1_0102) < tag_position(0x65E0_0102)
            && tag_position(0x65E0_0102) < tag_position(0x3008_0040)
            && tag_position(0x3008_0040) < tag_position(0x65E2_0102)
            && tag_position(0x65E2_0102) < tag_position(0x65E3_0102)
            && tag_position(0x65E3_0102) < tag_position(0x3001_001F)
            && tag_position(0x3001_001F) < tag_position(0x6749_0014)
    );
    for tag in [
        0x3601_0003u32,
        0x3602_0003,
        0x3603_0003,
        0x0E08_0003,
        0x0FF4_0003,
        0x3FE0_0102,
        0x3FE1_0102,
        0x0E27_0102,
    ] {
        assert!(!contains_bytes(&response_rops, &tag.to_le_bytes()));
    }
    assert!(contains_bytes(
        &response_rops,
        &0x65E1_0102u32.to_le_bytes()
    ));
    let mut root_child_parent_source_key = 0x65E1_0102u32.to_le_bytes().to_vec();
    root_child_parent_source_key.extend_from_slice(&0u32.to_le_bytes());
    assert!(contains_bytes(
        &response_rops,
        &root_child_parent_source_key
    ));
    let source_key_offset = tag_position(0x65E0_0102);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[source_key_offset + 4..source_key_offset + 8]
                .try_into()
                .unwrap()
        ),
        22
    );
    let change_key_offset = tag_position(0x65E2_0102);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[change_key_offset + 4..change_key_offset + 8]
                .try_into()
                .unwrap()
        ),
        22
    );
    assert!(contains_bytes(&response_rops, &utf16z("IPF.Note")));
}

#[tokio::test]
async fn mapi_over_http_hierarchy_sync_includes_default_ipm_special_folders() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(4));
    append_rop_outlook_hierarchy_sync_manifest_get_buffer(&mut rops, 1, 2, 4096);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert_eq!(mapi_sync_manifest_counts(&response_rops), Some((11, 0)));
    assert!(contains_bytes(&response_rops, &utf16z("Inbox")));
    assert!(contains_bytes(&response_rops, &utf16z("Drafts")));
    assert!(contains_bytes(&response_rops, &utf16z("Outbox")));
    assert!(contains_bytes(&response_rops, &utf16z("Sent Items")));
    assert!(contains_bytes(&response_rops, &utf16z("Deleted Items")));
    assert!(contains_bytes(&response_rops, &utf16z("Contacts")));
    assert!(contains_bytes(&response_rops, &utf16z("Calendar")));
    assert!(contains_bytes(&response_rops, &utf16z("Journal")));
    assert!(contains_bytes(&response_rops, &utf16z("Notes")));
    assert!(contains_bytes(&response_rops, &utf16z("Tasks")));
    assert!(contains_bytes(&response_rops, &utf16z("Reminders")));
    let mut folder_offsets = Vec::new();
    for name in [
        "Inbox",
        "Drafts",
        "Outbox",
        "Sent Items",
        "Deleted Items",
        "Contacts",
        "Calendar",
        "Journal",
        "Notes",
        "Tasks",
        "Reminders",
    ] {
        let name_bytes = utf16z(name);
        folder_offsets.push(
            response_rops
                .windows(name_bytes.len())
                .position(|window| window == name_bytes.as_slice())
                .unwrap(),
        );
    }
    assert!(folder_offsets.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(contains_bytes(&response_rops, &utf16z("IPF.Contact")));
    assert!(contains_bytes(&response_rops, &utf16z("IPF.Appointment")));
    assert!(contains_bytes(&response_rops, &utf16z("IPF.Journal")));
    assert!(contains_bytes(&response_rops, &utf16z("IPF.StickyNote")));
    assert!(contains_bytes(&response_rops, &utf16z("IPF.Task")));
    assert!(!contains_bytes(
        &response_rops,
        &utf16z("Top of Information Store")
    ));
    assert!(contains_bytes(
        &response_rops,
        &mapi_mailstore::source_key_for_store_id(crate::mapi::identity::OUTBOX_FOLDER_ID)
    ));
    assert!(contains_bytes(
        &response_rops,
        &mapi_mailstore::source_key_for_store_id(crate::mapi::identity::DRAFTS_FOLDER_ID)
    ));
    assert!(contains_bytes(
        &response_rops,
        &mapi_mailstore::source_key_for_store_id(crate::mapi::identity::TRASH_FOLDER_ID)
    ));
    assert!(contains_bytes(
        &response_rops,
        &mapi_mailstore::source_key_for_store_id(crate::mapi::identity::CONTACTS_FOLDER_ID)
    ));
    assert!(contains_bytes(
        &response_rops,
        &mapi_mailstore::source_key_for_store_id(crate::mapi::identity::CALENDAR_FOLDER_ID)
    ));
    assert!(contains_bytes(
        &response_rops,
        &mapi_mailstore::source_key_for_store_id(crate::mapi::identity::JOURNAL_FOLDER_ID)
    ));
    assert!(contains_bytes(
        &response_rops,
        &mapi_mailstore::source_key_for_store_id(crate::mapi::identity::NOTES_FOLDER_ID)
    ));
    assert!(contains_bytes(
        &response_rops,
        &mapi_mailstore::source_key_for_store_id(crate::mapi::identity::TASKS_FOLDER_ID)
    ));
    assert!(contains_bytes(
        &response_rops,
        &mapi_mailstore::source_key_for_store_id(crate::mapi::identity::REMINDERS_FOLDER_ID)
    ));
}

#[tokio::test]
async fn mapi_over_http_root_hierarchy_sync_keeps_parent_keys_root_relative() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(1));
    append_rop_outlook_hierarchy_sync_manifest_get_buffer(&mut rops, 1, 2, 4096);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    let decoded =
        strict_hierarchy_sync_transfer_from_response(&response_rops).expect("strict hierarchy ICS");
    let top = decoded
        .folder_changes
        .iter()
        .find(|folder| folder.display_name == "Top of Information Store")
        .expect("IPM subtree folderChange");
    assert!(top.parent_source_key.is_empty());
    for name in [
        "Deferred Action",
        "Spooler Queue",
        "Common Views",
        "Schedule",
        "Search",
        "Views",
        "Shortcuts",
    ] {
        let folder = decoded
            .folder_changes
            .iter()
            .find(|folder| folder.display_name == name)
            .unwrap_or_else(|| panic!("{name} folderChange"));
        assert!(folder.parent_source_key.is_empty());
    }
    let ipm_source_key = mapi_mailstore::source_key_for_store_id(test_mapi_folder_id(4));
    for name in ["Inbox", "Outbox", "Sent Items", "Deleted Items"] {
        let folder = decoded
            .folder_changes
            .iter()
            .find(|folder| folder.display_name == name)
            .unwrap_or_else(|| panic!("{name} folderChange"));
        assert_eq!(folder.parent_source_key, ipm_source_key);
    }
}

#[tokio::test]
async fn mapi_over_http_hierarchy_sync_preserves_nested_folder_parent_keys() {
    let parent_id = Uuid::parse_str("90909090-9090-4090-9090-909090909090").unwrap();
    let child_id = Uuid::parse_str("91919191-9191-4191-9191-919191919191").unwrap();
    let parent = FakeStore::mailbox(&parent_id.to_string(), "custom", "Projects");
    let mut child = FakeStore::mailbox(&child_id.to_string(), "custom", "Archive");
    child.parent_id = Some(parent_id);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![parent.clone(), child.clone()])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(4));
    append_rop_outlook_hierarchy_sync_manifest_get_buffer(&mut rops, 1, 2, 4096);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert_eq!(mapi_sync_manifest_counts(&response_rops), Some((13, 0)));
    assert!(contains_bytes(&response_rops, &utf16z("Projects")));
    assert!(contains_bytes(&response_rops, &utf16z("Archive")));
    let projects_offset = response_rops
        .windows(utf16z("Projects").len())
        .position(|window| window == utf16z("Projects"))
        .unwrap();
    let archive_offset = response_rops
        .windows(utf16z("Archive").len())
        .position(|window| window == utf16z("Archive"))
        .unwrap();
    assert!(projects_offset < archive_offset);
    let mut child_parent_source_key = 0x65E1_0102u32.to_le_bytes().to_vec();
    let parent_source_key = mapi_mailstore::source_key_for_mailbox_folder(&parent);
    child_parent_source_key.extend_from_slice(&(parent_source_key.len() as u32).to_le_bytes());
    child_parent_source_key.extend_from_slice(&parent_source_key);
    assert!(contains_bytes(&response_rops, &child_parent_source_key));

    let parent_folder_id = crate::mapi::identity::mapped_mapi_object_id(&parent_id).unwrap();
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut child_scope_headers = mapi_headers("Execute");
    child_scope_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let mut child_scope_rops = Vec::new();
    append_rop_open_folder(&mut child_scope_rops, 0, 1, parent_folder_id);
    append_rop_outlook_hierarchy_sync_manifest_get_buffer(&mut child_scope_rops, 1, 2, 4096);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &child_scope_headers,
            &execute_body(&rop_buffer(&child_scope_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert_eq!(mapi_sync_manifest_counts(&response_rops), Some((1, 0)));
    assert!(contains_bytes(&response_rops, &utf16z("Archive")));
    assert!(!contains_bytes(&response_rops, &utf16z("Projects")));
    let decoded =
        strict_hierarchy_sync_transfer_from_response(&response_rops).expect("strict hierarchy ICS");
    assert_eq!(decoded.folder_changes.len(), 1);
    assert_eq!(decoded.folder_changes[0].display_name, "Archive");
    assert!(decoded.folder_changes[0].parent_source_key.is_empty());
}

#[tokio::test]
async fn mapi_over_http_hierarchy_sync_fast_transfer_stream_decodes_strictly() {
    let parent_id = Uuid::parse_str("92929292-9292-4292-9292-929292929292").unwrap();
    let child_id = Uuid::parse_str("93939393-9393-4393-9393-939393939393").unwrap();
    let parent = FakeStore::mailbox(&parent_id.to_string(), "custom", "Projects");
    let mut child = FakeStore::mailbox(&child_id.to_string(), "custom", "Archive");
    child.parent_id = Some(parent_id);
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![parent, child])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(4));
    append_rop_outlook_hierarchy_sync_manifest_get_buffer(&mut rops, 1, 2, 4096);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    let decoded =
        strict_hierarchy_sync_transfer_from_response(&response_rops).expect("strict hierarchy ICS");
    assert_eq!(decoded.folder_changes.len(), 13);
    let names = decoded
        .folder_changes
        .iter()
        .map(|folder| folder.display_name.as_str())
        .collect::<Vec<_>>();
    let projects = names
        .iter()
        .position(|name| *name == "Projects")
        .expect("Projects folderChange");
    let archive = names
        .iter()
        .position(|name| *name == "Archive")
        .expect("Archive folderChange");
    assert!(projects < archive);
    assert!(decoded.folder_changes[projects]
        .parent_source_key
        .is_empty());
    assert!(decoded.folder_changes[archive]
        .parent_source_key
        .eq(&decoded.folder_changes[projects].source_key));
    assert!(decoded
        .idset_given
        .starts_with(&mapi_mailstore::STORE_REPLICA_GUID));
    assert!(decoded
        .cnset_seen
        .starts_with(&mapi_mailstore::STORE_REPLICA_GUID));
}

#[tokio::test]
async fn mapi_over_http_hierarchy_sync_advertises_counts_from_snapshot_messages() {
    let inbox_id = Uuid::parse_str("94949494-9494-4494-9494-949494949494").unwrap();
    let sent_id = Uuid::parse_str("96969696-9696-4696-9696-969696969696").unwrap();
    let inbox = FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox");
    let sent = FakeStore::mailbox(&sent_id.to_string(), "sent", "Sent");
    let mut email = FakeStore::email(
        "95959595-9595-4595-9595-959595959595",
        &sent_id.to_string(),
        "sent",
        "Unread hierarchy count",
    );
    email.mailbox_ids.push(inbox_id);
    email.mailbox_states.push(JmapEmailMailboxState {
        mailbox_id: inbox_id,
        role: "inbox".to_string(),
        name: "Inbox".to_string(),
        modseq: 42,
        unread: true,
        flagged: false,
        draft: false,
    });
    let inbox_local_commit_time_max = mapi_mailstore::filetime_from_change_number(
        mapi_mailstore::canonical_message_change_number(&email),
    );
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox, sent])),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(4));
    rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x02, 0x00, 0x00, 0x00, // hierarchy sync
        0x00, 0x00, // RestrictionDataSize
        0x01, 0x00, 0x00, 0x00, // SynchronizationExtraFlags, Eid
        0x00, 0x00, // PropertyTagCount
        0x4E, 0x00, 0x02, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    let decoded =
        strict_hierarchy_sync_transfer_from_response(&response_rops).expect("strict hierarchy ICS");
    let inbox = decoded
        .folder_changes
        .iter()
        .find(|folder| folder.display_name.eq_ignore_ascii_case("inbox"))
        .expect("Inbox folderChange");
    assert_eq!(inbox.content_count, Some(1));
    assert_eq!(inbox.content_unread_count, Some(1));
    assert_eq!(
        inbox.local_commit_time_max,
        Some(inbox_local_commit_time_max)
    );
}

#[tokio::test]
async fn mapi_over_http_hierarchy_sync_manifest_ignores_stale_server_checkpoint() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let mut inbox = FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox");
    inbox.total_emails = 3;
    inbox.unread_emails = 1;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        ..Default::default()
    };
    store
        .store_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            None,
            MapiCheckpointKind::Hierarchy,
            99,
            9,
            serde_json::json!({"source": "emsmdb-ics-download"}),
        )
        .await
        .unwrap();
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 99,
        current_modseq: 9,
        ..Default::default()
    };

    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(4));
    rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x02, 0x00, 0x00, 0x00, // hierarchy sync
        0x00, 0x00, // RestrictionDataSize
        0x01, 0x00, 0x00, 0x00, // SynchronizationExtraFlags, Eid
        0x00, 0x00, // PropertyTagCount
        0x4E, 0x00, 0x02, // RopFastTransferSourceGetBuffer
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert_eq!(mapi_sync_manifest_counts(&response_rops), Some((11, 0)));
    assert!(contains_bytes(&response_rops, &utf16z("Inbox")));
}

#[tokio::test]
async fn mapi_over_http_hierarchy_sync_uses_baseline_for_stale_root_checkpoint_with_client_state() {
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
    store
        .store_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            None,
            MapiCheckpointKind::Hierarchy,
            42,
            7,
            serde_json::json!({
                "source": "emsmdb-ics-download",
                "syncType": 2,
                "syncRootFolderId": test_mapi_folder_id(1),
                "hierarchySyncVersion": 2
            }),
        )
        .await
        .unwrap();
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 42,
        current_modseq: 7,
        ..Default::default()
    };

    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(4));
    append_rop_outlook_hierarchy_sync_manifest_get_buffer_with_state(
        &mut rops,
        1,
        2,
        4096,
        b"client-hierarchy-state",
    );
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert_eq!(mapi_sync_manifest_counts(&response_rops), Some((11, 0)));
    assert!(contains_bytes(&response_rops, &utf16z("Inbox")));
    let decoded =
        strict_hierarchy_sync_transfer_from_response(&response_rops).expect("strict hierarchy ICS");
    assert!(decoded
        .folder_changes
        .iter()
        .any(|folder| folder.display_name == "Inbox"));
}

#[tokio::test]
async fn mapi_over_http_hierarchy_sync_checkpoint_resumes_after_completed_download() {
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let mut inbox = FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox");
    inbox.total_emails = 3;
    inbox.unread_emails = 1;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        ..Default::default()
    };
    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 42,
        current_modseq: 7,
        ..Default::default()
    };

    let service = ExchangeService::new(store.clone());
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(4));
    append_rop_outlook_hierarchy_sync_manifest_get_buffer(&mut rops, 1, 2, 4096);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert_eq!(mapi_sync_manifest_counts(&response_rops), Some((11, 0)));
    assert!(contains_bytes(&response_rops, &utf16z("Inbox")));
    let checkpoint = store
        .fetch_mapi_sync_checkpoint(
            FakeStore::account().account_id,
            None,
            MapiCheckpointKind::Hierarchy,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(checkpoint.last_change_sequence, 42);
    assert_eq!(checkpoint.last_modseq, 7);
    assert_eq!(
        checkpoint
            .cursor_json
            .get("source")
            .and_then(serde_json::Value::as_str),
        Some("emsmdb-ics-download")
    );
    assert_eq!(
        checkpoint
            .cursor_json
            .get("syncRootFolderId")
            .and_then(serde_json::Value::as_u64),
        Some(test_mapi_folder_id(4))
    );
    assert_eq!(
        checkpoint
            .cursor_json
            .get("hierarchySyncVersion")
            .and_then(serde_json::Value::as_u64),
        Some(2)
    );

    *store.mapi_sync_changes.lock().unwrap() = MapiSyncChangeSet {
        current_change_sequence: 42,
        current_modseq: 7,
        ..Default::default()
    };
    let restarted = ExchangeService::new(store.clone());
    let connect = restarted
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut restarted_headers = mapi_headers("Execute");
    restarted_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let mut restart_rops = Vec::new();
    append_rop_open_folder(&mut restart_rops, 0, 1, test_mapi_folder_id(4));
    append_rop_outlook_hierarchy_sync_manifest_get_buffer_with_state(
        &mut restart_rops,
        1,
        2,
        4096,
        b"client-hierarchy-state",
    );
    let response = restarted
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &restarted_headers,
            &execute_body(&rop_buffer(&restart_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert_eq!(mapi_sync_manifest_counts(&response_rops), None);
    assert!(!contains_bytes(&response_rops, &utf16z("Inbox")));
    assert!(contains_bytes(
        &response_rops,
        &0x403A_0003u32.to_le_bytes()
    ));
    assert!(contains_bytes(
        &response_rops,
        &0x4014_0003u32.to_le_bytes()
    ));

    let restarted = ExchangeService::new(store.clone());
    let connect = restarted
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut restarted_headers = mapi_headers("Execute");
    restarted_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );
    let mut restart_rops = Vec::new();
    append_rop_open_folder(&mut restart_rops, 0, 1, test_mapi_folder_id(4));
    append_rop_outlook_hierarchy_sync_manifest_get_buffer_with_state(
        &mut restart_rops,
        1,
        2,
        4096,
        &[],
    );
    let response = restarted
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &restarted_headers,
            &execute_body(&rop_buffer(&restart_rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    let response_rops = response_rops_from_execute_response(response).await;

    assert_eq!(mapi_sync_manifest_counts(&response_rops), Some((11, 0)));
    assert!(contains_bytes(&response_rops, &utf16z("Inbox")));
    assert!(contains_bytes(
        &response_rops,
        &0x403A_0003u32.to_le_bytes()
    ));
}

#[tokio::test]
async fn mapi_over_http_fast_transfer_copy_to_message_returns_canonical_manifest_without_bcc() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let message_id = "43434343-4343-4343-4343-434343434343";
    let mut email = FakeStore::email(message_id, inbox_id, "inbox", "CopyTo message");
    email.body_text = "CopyTo body from canonical mail".to_string();
    email.bcc.push(JmapEmailAddress {
        address: "hidden-copyto@example.test".to_string(),
        display_name: None,
    });
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let folder_id = test_mapi_folder_id(5);
    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    rops.extend_from_slice(&folder_id.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[0x03, 0x00, 0x01, 0x02]);
    rops.extend_from_slice(&1200u16.to_le_bytes());
    rops.extend_from_slice(&folder_id.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&test_mapi_message_id(message_id).to_le_bytes());
    rops.extend_from_slice(&[0x4D, 0x00, 0x02, 0x03]);
    rops.push(0);
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.push(0x01);
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&[0x4E, 0x00, 0x03]);
    rops.extend_from_slice(&4096u16.to_le_bytes());

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x4D, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, b"LPE-MAPI-FASTTRANSFER\0"));
    assert!(contains_bytes(&response_rops, b"CopyTo message"));
    assert!(contains_bytes(
        &response_rops,
        b"CopyTo body from canonical mail"
    ));
    assert!(!contains_bytes(
        &response_rops,
        b"hidden-copyto@example.test"
    ));
}

#[tokio::test]
async fn mapi_over_http_fast_transfer_copy_messages_filters_to_requested_canonical_messages() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let selected_id = "44444444-4444-4444-4444-444444444444";
    let other_id = "45454545-4545-4545-4545-454545454545";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        emails: Arc::new(Mutex::new(vec![
            FakeStore::email(selected_id, inbox_id, "inbox", "Selected FastTransfer"),
            FakeStore::email(other_id, inbox_id, "inbox", "Unrequested FastTransfer"),
        ])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let folder_id = test_mapi_folder_id(5);
    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    rops.extend_from_slice(&folder_id.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[0x4B, 0x00, 0x01, 0x02]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&test_mapi_message_id(selected_id).to_le_bytes());
    rops.push(0);
    rops.push(0x01);
    rops.extend_from_slice(&[0x4E, 0x00, 0x02]);
    rops.extend_from_slice(&4096u16.to_le_bytes());

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x4B, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, b"LPE-MAPI-FASTTRANSFER\0"));
    assert!(contains_bytes(&response_rops, b"Selected FastTransfer"));
    assert!(!contains_bytes(&response_rops, b"Unrequested FastTransfer"));
}

#[tokio::test]
async fn mapi_over_http_fast_transfer_copy_folder_returns_canonical_folder_manifest() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            "46464646-4646-4646-4646-464646464646",
            inbox_id,
            "inbox",
            "Folder FastTransfer",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let folder_id = test_mapi_folder_id(5);
    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    rops.extend_from_slice(&folder_id.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[0x4C, 0x00, 0x01, 0x02]);
    rops.push(0);
    rops.push(0x01);
    rops.extend_from_slice(&[0x4E, 0x00, 0x02]);
    rops.extend_from_slice(&4096u16.to_le_bytes());

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x4C, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, b"LPE-MAPI-FASTTRANSFER\0"));
    assert!(contains_bytes(&response_rops, b"inbox"));
    assert!(contains_bytes(&response_rops, b"Inbox"));
    assert!(contains_bytes(&response_rops, b"Folder FastTransfer"));
}

#[tokio::test]
async fn mapi_over_http_fast_transfer_copy_properties_message_returns_canonical_manifest_without_bcc(
) {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let message_id = "47474747-4747-4747-4747-474747474747";
    let mut email = FakeStore::email(message_id, inbox_id, "inbox", "CopyProperties message");
    email.body_text = "CopyProperties body from canonical mail".to_string();
    email.bcc.push(JmapEmailAddress {
        address: "hidden-copyprops@example.test".to_string(),
        display_name: None,
    });
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        emails: Arc::new(Mutex::new(vec![email])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let folder_id = test_mapi_folder_id(5);
    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    rops.extend_from_slice(&folder_id.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[0x03, 0x00, 0x01, 0x02]);
    rops.extend_from_slice(&1200u16.to_le_bytes());
    rops.extend_from_slice(&folder_id.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&test_mapi_message_id(message_id).to_le_bytes());
    rops.extend_from_slice(&[0x69, 0x00, 0x02, 0x03]);
    rops.push(0);
    rops.push(0);
    rops.push(0x01);
    rops.extend_from_slice(&2u16.to_le_bytes());
    rops.extend_from_slice(&0x0037_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x1000_001Fu32.to_le_bytes());
    rops.extend_from_slice(&[0x4E, 0x00, 0x03]);
    rops.extend_from_slice(&4096u16.to_le_bytes());

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x69, 0x03, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, b"LPE-MAPI-FASTTRANSFER\0"));
    assert!(contains_bytes(&response_rops, b"CopyProperties message"));
    assert!(contains_bytes(
        &response_rops,
        b"CopyProperties body from canonical mail"
    ));
    assert!(!contains_bytes(
        &response_rops,
        b"hidden-copyprops@example.test"
    ));
}

#[tokio::test]
async fn mapi_over_http_fast_transfer_upload_rops_return_rop_specific_protocol_errors() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[0x53, 0x00, 0x01, 0x02, 0x01, 0x00]);
    rops.extend_from_slice(&[0x54, 0x00, 0x01]);
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&[0x86, 0x00, 0x01]);
    rops.extend_from_slice(&[15, 20, 0, 1, 0, 0]);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x53, 0x02, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x54, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x86, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(!contains_bytes(
        &response_rops,
        &[0x00, 0x00, 0x02, 0x01, 0x04, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_per_user_information_rops_return_rop_specific_protocol_errors() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[0x60, 0x00, 0x01]);
    rops.extend_from_slice(&[0x11; 16]);
    rops.extend_from_slice(&[0x61, 0x00, 0x01]);
    rops.extend_from_slice(&[0x22; 24]);
    rops.extend_from_slice(&[0x63, 0x00, 0x01]);
    rops.extend_from_slice(&[0x33; 24]);
    rops.push(0);
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&512u16.to_le_bytes());
    rops.extend_from_slice(&[0x64, 0x00, 0x01]);
    rops.extend_from_slice(&[0x44; 24]);
    rops.push(1);
    rops.extend_from_slice(&1u32.to_le_bytes());
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(b"LPE");

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x60, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x61, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x63, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x64, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(!contains_bytes(
        &response_rops,
        &[0x00, 0x00, 0x02, 0x01, 0x04, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_rule_rops_return_rop_specific_protocol_errors() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[0x3F, 0x00, 0x01, 0x02, 0x00]);
    rops.extend_from_slice(&[0x41, 0x00, 0x01, 0x00]);
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&[0x57, 0x00, 0x01]);
    rops.extend_from_slice(&4u16.to_le_bytes());
    rops.extend_from_slice(b"SRVR");
    rops.extend_from_slice(&6u16.to_le_bytes());
    rops.extend_from_slice(b"CLIENT");
    rops.extend_from_slice(&[0x7B, 0x00, 0x01]);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x3F, 0x02, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x41, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x57, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x7B, 0x01, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
    assert!(!contains_bytes(
        &response_rops,
        &[0x00, 0x00, 0x02, 0x01, 0x04, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_non_empty_modify_rules_is_terminal_without_canonical_side_effects() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        ..Default::default()
    };
    let imported_emails = store.imported_emails.clone();
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    rops.extend_from_slice(&[0x41, 0x00, 0x01, 0x00]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    append_rop_create_message(&mut rops, 1, 2, test_mapi_folder_id(5));

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x41, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(!contains_bytes(
        &response_rops,
        &[0x06, 0x02, 0, 0, 0, 0, 0]
    ));
    assert!(imported_emails.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mapi_over_http_permissions_table_maps_delegate_folder_access() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let delegate_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        mapi_folder_permissions: Arc::new(Mutex::new(vec![
            crate::mapi::permissions::owner_permission(
                Uuid::parse_str(inbox_id).unwrap(),
                &AccountPrincipal {
                    tenant_id: FakeStore::account().tenant_id,
                    account_id: FakeStore::account().account_id,
                    email: FakeStore::account().email,
                    display_name: FakeStore::account().display_name,
                },
            ),
            MapiFolderPermission {
                mailbox_id: Uuid::parse_str(inbox_id).unwrap(),
                member_account_id: Some(delegate_id),
                member_name: "Bob Delegate".to_string(),
                rights: crate::mapi::permissions::rights_from_grant(true, true, false, false),
            },
        ])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[0x3E, 0x00, 0x01, 0x02, 0x00]);
    rops.extend_from_slice(&[0x12, 0x00, 0x02, 0x00]);
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&0x6671_0014u32.to_le_bytes());
    rops.extend_from_slice(&0x6672_001Fu32.to_le_bytes());
    rops.extend_from_slice(&0x6673_0003u32.to_le_bytes());
    rops.extend_from_slice(&[0x15, 0x00, 0x02, 0x00, 0x01]);
    rops.extend_from_slice(&8u16.to_le_bytes());

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x3E, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &utf16z("Bob Delegate")));
    assert!(contains_bytes(
        &response_rops,
        &crate::mapi::permissions::rights_from_grant(true, true, false, false).to_le_bytes()
    ));
}

#[tokio::test]
async fn mapi_over_http_modify_permissions_mutation_is_explicitly_unsupported() {
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
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[0x40, 0x00, 0x01, 0x00]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.push(0x01);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x6673_0003u32.to_le_bytes());
    rops.extend_from_slice(&0x0000_0401i32.to_le_bytes());

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x40, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(!contains_bytes(
        &response_rops,
        &[0x00, 0x00, 0x02, 0x01, 0x04, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_denies_mutation_without_folder_write_permission() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        mapi_folder_permissions: Arc::new(Mutex::new(vec![MapiFolderPermission {
            mailbox_id: Uuid::parse_str(inbox_id).unwrap(),
            member_account_id: Some(account.account_id),
            member_name: account.display_name,
            rights: crate::mapi::permissions::rights_from_grant(true, false, false, false),
        }])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    append_rop_create_message(&mut rops, 1, 2, test_mapi_folder_id(5));

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x06, 0x02, 0x05, 0x00, 0x07, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_denies_contents_table_without_folder_read_permission() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        mapi_folder_permissions: Arc::new(Mutex::new(vec![MapiFolderPermission {
            mailbox_id: Uuid::parse_str(inbox_id).unwrap(),
            member_account_id: Some(account.account_id),
            member_name: account.display_name,
            rights: crate::mapi::permissions::rights_from_grant(false, false, false, false),
        }])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[0x05, 0x00, 0x01, 0x02, 0x00]);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x05, 0x02, 0x05, 0x00, 0x07, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_search_criteria_rops_return_rop_specific_protocol_errors() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    rops.extend_from_slice(&test_mapi_folder_id(11).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[0x30, 0x00, 0x01]);
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.extend_from_slice(&0x0004_0002u32.to_le_bytes());
    rops.extend_from_slice(&[0x31, 0x00, 0x01, 0x01, 0x01, 0x01]);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x30, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x31, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(!contains_bytes(
        &response_rops,
        &[0x00, 0x00, 0x02, 0x01, 0x04, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_register_notification_returns_protocol_success_handles() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[0x29, 0x00, 0x01, 0x02]);
    rops.extend_from_slice(&0x0001u16.to_le_bytes());
    rops.push(1);
    rops.extend_from_slice(&[0x29, 0x00, 0x01, 0x03]);
    rops.extend_from_slice(&0x0401u16.to_le_bytes());
    rops.push(0);
    rops.push(0);
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.extend_from_slice(&0u64.to_le_bytes());

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x29, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x29, 0x03, 0, 0, 0, 0]));
    assert!(!contains_bytes(
        &response_rops,
        &[0x00, 0x00, 0x02, 0x01, 0x04, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_notification_wait_reports_content_event_after_registered_delete() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let message_id = "99999999-9999-9999-9999-999999999999";
    let mut inbox = FakeStore::mailbox(inbox_id, "inbox", "Inbox");
    inbox.total_emails = 1;
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![inbox])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            message_id,
            inbox_id,
            "inbox",
            "Notification target",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[0x29, 0x00, 0x01, 0x02]);
    rops.extend_from_slice(&0x0008u16.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.extend_from_slice(&0u64.to_le_bytes());
    rops.extend_from_slice(&[0x1E, 0x00, 0x01, 0x00, 0x01]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&test_mapi_message_id(message_id).to_le_bytes());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let mut wait_headers = mapi_headers("NotificationWait");
    wait_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &wait_headers, b"")
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[8..12].try_into().unwrap()), 1);
}

#[tokio::test]
async fn mapi_over_http_notification_wait_reports_content_event_after_registered_save() {
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
    let cookie = mapi_cookie_header(&connect);

    let mut property_values = Vec::new();
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "Notification save");
    append_mapi_utf16_property(&mut property_values, 0x1000_001F, "Notification body");

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let mut rops = Vec::new();
    append_rop_open_folder(&mut rops, 0, 1, test_mapi_folder_id(5));
    rops.extend_from_slice(&[0x29, 0x00, 0x01, 0x02]);
    rops.extend_from_slice(&0x0004u16.to_le_bytes());
    rops.push(1);
    append_rop_create_message(&mut rops, 1, 3, test_mapi_folder_id(5));
    append_rop_set_properties(&mut rops, 3, 2, &property_values);
    append_rop_save_changes_message(&mut rops, 3, 3);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(imported_emails.lock().unwrap().len(), 1);

    let mut wait_headers = mapi_headers("NotificationWait");
    wait_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &wait_headers, b"")
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[8..12].try_into().unwrap()), 1);
}

#[tokio::test]
async fn mapi_over_http_notification_wait_polls_canonical_change_cursor() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            "55555555-5555-5555-5555-555555555555",
            "inbox",
            "Inbox",
        )])),
        mapi_notification_cursor: Arc::new(Mutex::new(Some(7))),
        mapi_notification_polls: Arc::new(Mutex::new(vec![MapiNotificationPoll {
            event_pending: true,
            cursor: Some(8),
        }])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[0x29, 0x00, 0x01, 0x02]);
    rops.extend_from_slice(&0x0010u16.to_le_bytes());
    rops.push(1);
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let mut wait_headers = mapi_headers("NotificationWait");
    wait_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &wait_headers, b"")
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[8..12].try_into().unwrap()), 1);
}

#[tokio::test]
async fn mapi_over_http_notification_wait_reports_hierarchy_event_after_registered_create_folder() {
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
    let cookie = mapi_cookie_header(&connect);

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    rops.extend_from_slice(&test_mapi_folder_id(1).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[0x29, 0x00, 0x01, 0x02]);
    rops.extend_from_slice(&0x0104u16.to_le_bytes());
    rops.push(1);
    rops.extend_from_slice(&[0x1C, 0x00, 0x01, 0x03, 0x01, 0x01, 0x00, 0x00]);
    rops.extend_from_slice(&utf16z("MAPI Notifications"));
    rops.extend_from_slice(&utf16z(""));

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(created_mailboxes.lock().unwrap().len(), 1);

    let mut wait_headers = mapi_headers("NotificationWait");
    wait_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &wait_headers, b"")
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[8..12].try_into().unwrap()), 1);
}

#[tokio::test]
async fn mapi_over_http_async_table_control_rops_return_rop_specific_protocol_errors() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[0x05, 0x00, 0x01, 0x02, 0x00]);
    rops.extend_from_slice(&[0x38, 0x00, 0x02]);
    rops.extend_from_slice(&[0x50, 0x00, 0x02, 0x01]);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x05, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &[0x38, 0x02, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x50, 0x02, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(!contains_bytes(
        &response_rops,
        &[0x00, 0x00, 0x02, 0x01, 0x04, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_whole_folder_delete_rops_return_rop_specific_protocol_errors() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
        )])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut rops = vec![0x02, 0x00, 0x00, 0x01];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[0x58, 0x00, 0x01, 0x00, 0x00]);
    rops.extend_from_slice(&[0x92, 0x00, 0x01, 0x00, 0x00]);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1, u32::MAX])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x58, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x92, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(!contains_bytes(
        &response_rops,
        &[0x00, 0x00, 0x02, 0x01, 0x04, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_public_folder_replica_rops_return_rop_specific_protocol_errors() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let mut rops = vec![0x42, 0x00, 0x00];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.extend_from_slice(&[0x45, 0x00, 0x00]);
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.extend_from_slice(&[0x7B, 0x00, 0x00]);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x42, 0x00, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(&response_rops, &[0x45, 0x00, 0, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &[0x7B, 0x00, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
    assert!(!contains_bytes(
        &response_rops,
        &[0x00, 0x00, 0x02, 0x01, 0x04, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_long_term_id_round_trips_canonical_replica_ids() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header(&connect)).unwrap(),
    );

    let object_id = test_mapi_folder_id(5);
    let mut long_term_id = [0; 24];
    long_term_id[..16].copy_from_slice(&mapi_mailstore::STORE_REPLICA_GUID);
    long_term_id[16..22].copy_from_slice(&globcnt_bytes(5));
    let mut invalid_long_term_id = long_term_id;
    invalid_long_term_id[0] ^= 0xFF;

    let mut rops = vec![0x43, 0x00, 0x00];
    rops.extend_from_slice(&object_id.to_le_bytes());
    rops.extend_from_slice(&[0x44, 0x00, 0x00]);
    rops.extend_from_slice(&long_term_id);
    rops.extend_from_slice(&[0x44, 0x00, 0x00]);
    rops.extend_from_slice(&invalid_long_term_id);

    let response = service
        .handle_mapi(
            MapiEndpoint::Emsmdb,
            &execute_headers,
            &execute_body(&rop_buffer(&rops, &[1])),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    let mut long_term_response = vec![0x43, 0x00, 0, 0, 0, 0];
    long_term_response.extend_from_slice(&long_term_id);
    assert!(contains_bytes(&response_rops, &long_term_response));
    let mut object_id_response = vec![0x44, 0x00, 0, 0, 0, 0];
    object_id_response.extend_from_slice(&object_id.to_le_bytes());
    assert!(contains_bytes(&response_rops, &object_id_response));
    assert!(contains_bytes(
        &response_rops,
        &[0x44, 0x00, 0x0F, 0x01, 0x04, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_fast_transfer_get_buffer_resumes_across_execute_requests() {
    let mailbox_id = "55555555-5555-5555-5555-555555555555";
    let mut inbox = FakeStore::mailbox(mailbox_id, "inbox", "Inbox");
    inbox.total_emails = 1;
    let email = FakeStore::email(
        "47474747-4747-4747-4747-474747474747",
        mailbox_id,
        "inbox",
        "Chunked FastTransfer sync message",
    );
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

    let mut first_rops = Vec::new();
    append_rop_open_folder(&mut first_rops, 0, 1, test_mapi_folder_id(5));
    append_rop_sync_manifest_get_buffer(&mut first_rops, 1, 2, 32);
    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let first_request = execute_body(&rop_buffer(&first_rops, &[1, u32::MAX, u32::MAX]));
    let first_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &first_request)
        .await
        .unwrap();

    assert_eq!(first_response.status(), StatusCode::OK);
    let first_response_rops = response_rops_from_execute_response(first_response).await;
    let first_chunks = mapi_fast_transfer_chunks(&first_response_rops);
    assert_eq!(first_chunks.len(), 1);
    assert_eq!(first_chunks[0].0, 0x0001);
    assert_eq!(first_chunks[0].1.len(), 32);

    let mut second_rops = Vec::new();
    second_rops.extend_from_slice(&[0x4E, 0x00, 0x00]);
    second_rops.extend_from_slice(&4096u16.to_le_bytes());
    renew_mapi_request_id(&mut execute_headers);
    let second_request = execute_body(&rop_buffer(&second_rops, &[3]));
    let second_response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &second_request)
        .await
        .unwrap();

    assert_eq!(second_response.status(), StatusCode::OK);
    let second_response_rops = response_rops_from_execute_response(second_response).await;
    let second_chunks = mapi_fast_transfer_chunks(&second_response_rops);
    assert_eq!(second_chunks.len(), 1);
    assert_eq!(second_chunks[0].0, 0x0003);

    let mut transfer = Vec::new();
    transfer.extend_from_slice(&first_chunks[0].1);
    transfer.extend_from_slice(&second_chunks[0].1);
    assert_eq!(mapi_sync_manifest_counts(&transfer), Some((1, 1)));
    assert!(contains_bytes(
        &transfer,
        b"Chunked FastTransfer sync message"
    ));
}

#[tokio::test]
async fn mapi_over_http_get_local_replica_ids_returns_replica_guid() {
    let account = FakeStore::account();
    let store = FakeStore {
        session: Some(account.clone()),
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
    assert_eq!(response_rops[0], 0x7F);
    assert_eq!(response_rops[1], 0x00);
    assert_eq!(
        u32::from_le_bytes(response_rops[2..6].try_into().unwrap()),
        0
    );
    assert_eq!(&response_rops[6..22], &mapi_mailstore::STORE_REPLICA_GUID);
    let (first_global_counter, _) =
        mapi_mailstore::local_replica_id_range(account.account_id, 4, 1);
    assert_eq!(&response_rops[22..28], &globcnt_bytes(first_global_counter));
    assert_eq!(response_rops.len(), 28);
    assert!(response_rops[22..28].iter().any(|byte| *byte != 0));
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
    rops.extend_from_slice(&0u32.to_le_bytes());
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
async fn mapi_over_http_sync_upload_state_accumulates_multiple_streams() {
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

    let first_state = b"client-idset-given";
    let second_state = b"client-cnset-seen";
    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, // RopSynchronizationOpenCollector
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
    ]);
    rops.extend_from_slice(&0x4017_0102u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x76, 0x00, 0x02, // RopSynchronizationUploadStateStreamContinue
    ]);
    rops.extend_from_slice(&(first_state.len() as u32).to_le_bytes());
    rops.extend_from_slice(first_state);
    rops.extend_from_slice(&[
        0x77, 0x00, 0x02, // RopSynchronizationUploadStateStreamEnd
        0x75, 0x00, 0x02, // RopSynchronizationUploadStateStreamBegin
    ]);
    rops.extend_from_slice(&0x6796_0102u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&[
        0x76, 0x00, 0x02, // RopSynchronizationUploadStateStreamContinue
    ]);
    rops.extend_from_slice(&(second_state.len() as u32).to_le_bytes());
    rops.extend_from_slice(second_state);
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
    assert!(contains_bytes(&response_rops, first_state));
    assert!(contains_bytes(&response_rops, second_state));
    let first_offset = response_rops
        .windows(first_state.len())
        .position(|window| window == first_state)
        .unwrap();
    let second_offset = response_rops
        .windows(second_state.len())
        .position(|window| window == second_state)
        .unwrap();
    assert!(first_offset < second_offset);
}

#[tokio::test]
async fn mapi_over_http_set_local_replica_midset_deleted_round_trips_in_transfer_state() {
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

    let deleted_midset = b"deleted-local-replica-midset";
    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x70, 0x00, 0x01, 0x02, // RopSynchronizationConfigure
        0x01, 0x00, 0x00, 0x00, // content sync
        0x00, 0x00, // RestrictionDataSize
        0x00, 0x00, 0x00, 0x00, // SynchronizationExtraFlags
        0x00, 0x00, // PropertyTagCount
        0x93, 0x00, 0x02, // RopSetLocalReplicaMidsetDeleted
    ]);
    rops.extend_from_slice(&(deleted_midset.len() as u16).to_le_bytes());
    rops.extend_from_slice(deleted_midset);
    rops.extend_from_slice(&[
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
    assert!(contains_bytes(&response_rops, &[0x93, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, deleted_midset));
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
async fn mapi_over_http_sync_import_new_message_saves_canonical_email() {
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
    append_mapi_utf16_property(&mut property_values, 0x0037_001F, "ICS imported subject");
    append_mapi_utf16_property(&mut property_values, 0x1000_001F, "ICS imported body");
    append_mapi_utf16_property(
        &mut property_values,
        0x1035_001F,
        "<mapi-ics-import@example.test>",
    );

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x7E, 0x00, 0x01, 0x02, // RopSynchronizationOpenCollector
        0x72, 0x00, 0x02, 0x03, // RopSynchronizationImportMessageChange
    ]);
    rops.extend_from_slice(&0u64.to_le_bytes());
    rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&3u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
    rops.extend_from_slice(&[
        0x0C, 0x00, 0x01, 0x03, 0x00, // RopSaveChangesMessage
    ]);

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
    assert!(contains_bytes(&response_rops, &[0x0C, 0x01, 0, 0, 0, 0]));

    let recorded = imported_emails.lock().unwrap();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].mailbox_id, inbox_id);
    assert_eq!(recorded[0].source, "mapi-save-message");
    assert_eq!(recorded[0].subject, "ICS imported subject");
    assert_eq!(recorded[0].body_text, "ICS imported body");
    assert_eq!(
        recorded[0].internet_message_id.as_deref(),
        Some("<mapi-ics-import@example.test>")
    );
    assert!(recorded[0].bcc.is_empty());
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
        0x02,
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
async fn mapi_over_http_sync_import_soft_delete_moves_to_trash() {
    let message_id = "45454545-4545-4545-4545-454545454545";
    let inbox_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
    let trash_id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![
            FakeStore::mailbox(&inbox_id.to_string(), "inbox", "Inbox"),
            FakeStore::mailbox(&trash_id.to_string(), "trash", "Deleted"),
        ])),
        emails: Arc::new(Mutex::new(vec![FakeStore::email(
            message_id,
            &inbox_id.to_string(),
            "inbox",
            "Soft delete import",
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
        0x74, 0x00, 0x02, // RopSynchronizationImportDeletes
        0x00,
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&test_mapi_message_id(message_id).to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, &[0x74, 0x02, 0, 0, 0, 0, 0]));
    assert_eq!(
        moved_emails.lock().unwrap().as_slice(),
        &[(Uuid::parse_str(message_id).unwrap(), trash_id)]
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
async fn mapi_over_http_folder_set_properties_round_trips_session_values() {
    let inbox = FakeStore::mailbox("55555555-5555-5555-5555-555555555555", "inbox", "Inbox");
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

    let state = [0x11, 0x22, 0x33, 0x44, 0x55];
    let mut property_values = Vec::new();
    append_mapi_binary_property(&mut property_values, 0x36D0_0102, &state);

    let mut rops = vec![
        0x02, 0x00, 0x00, 0x01, // RopOpenFolder
    ];
    rops.extend_from_slice(&folder_id.to_le_bytes());
    rops.push(0);
    rops.extend_from_slice(&[
        0x0A, 0x00, 0x01, // RopSetProperties on opened folder
    ]);
    rops.extend_from_slice(&((property_values.len() + 2) as u16).to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&property_values);
    rops.extend_from_slice(&[
        0x07, 0x00, 0x01, // RopGetPropertiesSpecific on same folder
    ]);
    rops.extend_from_slice(&4096u16.to_le_bytes());
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&0x36D0_0102u32.to_le_bytes());

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(
        &response_rops,
        &[0x0A, 0x01, 0x00, 0x00, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(&response_rops, &state));
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
    let folder_property_count = 18usize;
    assert_eq!(
        u16::from_le_bytes(
            response_rops[props_list_offset + 6..props_list_offset + 8]
                .try_into()
                .unwrap()
        ),
        folder_property_count as u16
    );
    assert!(contains_bytes(response_rops, &0x6748_0014u32.to_le_bytes()));
    assert!(contains_bytes(response_rops, &0x3613_001Fu32.to_le_bytes()));

    let contents_offset = props_list_offset + 8 + folder_property_count * 4;
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
async fn mapi_over_http_table_control_rops_require_table_handles() {
    let inbox_id = "55555555-5555-5555-5555-555555555555";
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: Arc::new(Mutex::new(vec![FakeStore::mailbox(
            inbox_id, "inbox", "Inbox",
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
        0x12, 0x00, 0x01, 0x00, // RopSetColumns on the folder handle.
    ]);
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x16, 0x00, 0x01, // RopGetStatus on the folder handle.
        0x17, 0x00, 0x01, // RopQueryPosition on the folder handle.
        0x15, 0x00, 0x01, 0x00, 0x01, // RopQueryRows on the folder handle.
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x81, 0x00, 0x01, // RopResetTable on the folder handle.
        0x05, 0x00, 0x01, 0x02, 0x00, // RopGetContentsTable
        0x12, 0x00, 0x02, 0x00, // RopSetColumns on the contents table handle.
    ]);
    rops.extend_from_slice(&0u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x16, 0x00, 0x02, // RopGetStatus on the contents table handle.
        0x17, 0x00, 0x02, // RopQueryPosition on the contents table handle.
        0x15, 0x00, 0x02, 0x00, 0x01, // RopQueryRows on the contents table handle.
    ]);
    rops.extend_from_slice(&1u16.to_le_bytes());
    rops.extend_from_slice(&[
        0x81, 0x00, 0x02, // RopResetTable on the contents table handle.
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
        &[0x12, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x16, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x17, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x15, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x81, 0x01, 0x02, 0x01, 0x04, 0x80]
    ));
    assert!(contains_bytes(&response_rops, &[0x12, 0x02, 0, 0, 0, 0]));
    assert!(contains_bytes(&response_rops, &[0x16, 0x02, 0, 0, 0, 0, 0]));
    assert!(contains_bytes(
        &response_rops,
        &[0x17, 0x02, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x15, 0x02, 0, 0, 0, 0, 0x02, 0, 0]
    ));
    assert!(contains_bytes(&response_rops, &[0x81, 0x02, 0, 0, 0, 0]));
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
async fn mapi_over_http_get_receive_folder_uses_message_class_matching() {
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
    rops.extend_from_slice(b"IPM.Note.Custom\0");
    rops.extend_from_slice(&[0x27, 0x00, 0x00]);
    rops.extend_from_slice(b"MY.Class\0");
    rops.extend_from_slice(&[0x27, 0x00, 0x00]);
    rops.extend_from_slice(b".Invalid\0");

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, b"IPM.Note\0"));
    let mut unmatched_response = vec![0x27, 0x00, 0, 0, 0, 0];
    unmatched_response.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    unmatched_response.push(0);
    assert!(contains_bytes(
        &response_rops,
        unmatched_response.as_slice()
    ));
    assert!(contains_bytes(
        &response_rops,
        &[0x27, 0x00, 0x57, 0x00, 0x07, 0x80]
    ));
}

#[tokio::test]
async fn mapi_over_http_execute_returns_transport_folder_without_protocol_outbox_state() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let legacy_dn = b"/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=alice\0";
    let mut rops = vec![0xFE, 0x00, 0x00, 0x01];
    rops.extend_from_slice(&0x0100_0004u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&(legacy_dn.len() as u16).to_le_bytes());
    rops.extend_from_slice(legacy_dn);
    rops.extend_from_slice(&[
        0x6D, 0x00, 0x01, // RopGetTransportFolder against the logon handle.
    ]);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    let transport_offset = response_rops.len() - 14;
    assert_eq!(response_rops[transport_offset], 0x6D);
    assert_eq!(response_rops[transport_offset + 1], 0x01);
    assert_eq!(
        u32::from_le_bytes(
            response_rops[transport_offset + 2..transport_offset + 6]
                .try_into()
                .unwrap()
        ),
        0
    );
    assert_eq!(
        u64::from_le_bytes(
            response_rops[transport_offset + 6..transport_offset + 14]
                .try_into()
                .unwrap()
        ),
        test_mapi_folder_id(6)
    );
}

#[tokio::test]
async fn mapi_over_http_transport_spooler_rops_return_parseable_errors_without_corrupting_batch() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);
    let message_id = test_mapi_message_id("87878787-8787-8787-8787-878787878787");
    let folder_id = test_mapi_folder_id(5);

    let mut rops = Vec::new();
    rops.extend_from_slice(&[0x34, 0x00, 0x00]); // RopAbortSubmit.
    rops.extend_from_slice(&folder_id.to_le_bytes());
    rops.extend_from_slice(&message_id.to_le_bytes());
    rops.extend_from_slice(&[0x47, 0x00, 0x00]); // RopSetSpooler.
    rops.extend_from_slice(&[0x48, 0x00, 0x00]); // RopSpoolerLockMessage.
    rops.extend_from_slice(&message_id.to_le_bytes());
    rops.push(1);
    rops.extend_from_slice(&[0x51, 0x00, 0x00]); // RopTransportNewMail.
    rops.extend_from_slice(&message_id.to_le_bytes());
    rops.extend_from_slice(&folder_id.to_le_bytes());
    rops.extend_from_slice(b"IPM.Note\0");
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&[0x7B, 0x00, 0x00]); // RopGetStoreState proves the batch stayed aligned.

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[1]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    for rop_id in [0x34, 0x47, 0x48, 0x51] {
        assert!(contains_bytes(
            &response_rops,
            &[rop_id, 0x00, 0x02, 0x01, 0x04, 0x80]
        ));
    }
    assert!(contains_bytes(
        &response_rops,
        &[0x7B, 0x00, 0, 0, 0, 0, 0, 0, 0, 0]
    ));
}

#[tokio::test]
async fn mapi_over_http_set_receive_folder_returns_rop_specific_protocol_error() {
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

    let mut rops = vec![0x26, 0x00, 0x00];
    rops.extend_from_slice(&test_mapi_folder_id(5).to_le_bytes());
    rops.extend_from_slice(b"IPM.Note\0");

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

    assert_eq!(response_rops[0], 0x26);
    assert_eq!(response_rops[1], 0x00);
    assert_eq!(
        u32::from_le_bytes(response_rops[2..6].try_into().unwrap()),
        0x8004_0102
    );
}

#[tokio::test]
async fn mapi_over_http_execute_returns_empty_transport_options_data() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let connect = service
        .handle_mapi(MapiEndpoint::Emsmdb, &mapi_headers("Connect"), b"")
        .await
        .unwrap();
    let cookie = mapi_cookie_header(&connect);

    let legacy_dn = b"/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=alice\0";
    let mut rops = vec![0xFE, 0x00, 0x00, 0x01];
    rops.extend_from_slice(&0x0100_0004u32.to_le_bytes());
    rops.extend_from_slice(&0u32.to_le_bytes());
    rops.extend_from_slice(&(legacy_dn.len() as u16).to_le_bytes());
    rops.extend_from_slice(legacy_dn);
    rops.extend_from_slice(&[
        0x49, 0x00, 0x01, // RopGetAddressTypes
        0x6F, 0x00, 0x01, // RopOptionsData
    ]);
    rops.extend_from_slice(b"SMTP\0");
    rops.push(0);

    let mut execute_headers = mapi_headers("Execute");
    execute_headers.insert("cookie", HeaderValue::from_str(&cookie).unwrap());
    let request = execute_body(&rop_buffer(&rops, &[u32::MAX, u32::MAX]));
    let response = service
        .handle_mapi(MapiEndpoint::Emsmdb, &execute_headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let response_rops = response_rops_from_execute_response(response).await;
    assert!(contains_bytes(&response_rops, b"EX\0SMTP\0"));
    assert_eq!(
        &response_rops[response_rops.len() - 11..],
        &[0x6F, 0x01, 0, 0, 0, 0, 1, 0, 0, 0, 0]
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
        .starts_with("MapiContext="));

    let body = response_bytes(response).await;
    assert_eq!(body.len(), 28);
    assert_eq!(&body[0..8], &[0, 0, 0, 0, 0, 0, 0, 0]);
    assert_ne!(&body[8..24], &[0; 16]);
    assert_eq!(body[15] & 0xf0, 0x40);
    assert_eq!(body[16] & 0xc0, 0x80);
}

#[tokio::test]
async fn mapi_over_http_bind_accepts_rca_bare_guid_headers() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let mut headers = mapi_headers("Bind");
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    headers.insert(
        axum::http::header::CONTENT_LENGTH,
        HeaderValue::from_static("45"),
    );
    headers.insert(
        "x-requestid",
        HeaderValue::from_static("8efcc291-b798-442e-b608-bd3f6c67b78b:1"),
    );
    headers.insert(
        "x-clientinfo",
        HeaderValue::from_static("c9a1f6bb-76d3-41a1-8abb-fc60106a4a97:1"),
    );
    let request = [0u8; 45];

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-requesttype").unwrap(), "Bind");
    assert_eq!(
        response.headers().get("x-requestid").unwrap(),
        "8efcc291-b798-442e-b608-bd3f6c67b78b:1"
    );
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
}

#[tokio::test]
async fn mapi_over_http_bind_reestablishes_nspi_session_cookie() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let bind = service
        .handle_mapi(MapiEndpoint::Nspi, &mapi_headers("Bind"), b"")
        .await
        .unwrap();
    let first_cookie = bind
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();

    let mut rebind_headers = mapi_headers("Bind");
    rebind_headers.insert("cookie", HeaderValue::from_str(&first_cookie).unwrap());
    let rebind = service
        .handle_mapi(MapiEndpoint::Nspi, &rebind_headers, b"")
        .await
        .unwrap();

    assert_eq!(rebind.status(), StatusCode::OK);
    assert_eq!(rebind.headers().get("x-requesttype").unwrap(), "Bind");
    assert_eq!(rebind.headers().get("x-responsecode").unwrap(), "0");
    let reconnected_cookie = rebind
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string();
    assert_ne!(reconnected_cookie, first_cookie);

    let mut old_unbind_headers = mapi_headers("Unbind");
    old_unbind_headers.insert("cookie", HeaderValue::from_str(&first_cookie).unwrap());
    let old_unbind = service
        .handle_mapi(MapiEndpoint::Nspi, &old_unbind_headers, b"")
        .await
        .unwrap();
    assert_eq!(old_unbind.headers().get("x-responsecode").unwrap(), "10");

    let mut new_unbind_headers = mapi_headers("Unbind");
    new_unbind_headers.insert(
        "cookie",
        HeaderValue::from_str(&reconnected_cookie).unwrap(),
    );
    let new_unbind = service
        .handle_mapi(MapiEndpoint::Nspi, &new_unbind_headers, b"")
        .await
        .unwrap();
    assert_eq!(new_unbind.headers().get("x-responsecode").unwrap(), "0");
    assert!(new_unbind
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("Max-Age=0"));
}

#[tokio::test]
async fn mapi_over_http_bind_ignores_mismatched_sequence_cookie_on_reconnect() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let bind = service
        .handle_mapi(MapiEndpoint::Nspi, &mapi_headers("Bind"), b"")
        .await
        .unwrap();

    let mut rebind_headers = mapi_headers("Bind");
    rebind_headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header_with_mismatched_sequence(&bind)).unwrap(),
    );
    let rebind = service
        .handle_mapi(MapiEndpoint::Nspi, &rebind_headers, b"")
        .await
        .unwrap();

    assert_eq!(rebind.status(), StatusCode::OK);
    assert_eq!(rebind.headers().get("x-requesttype").unwrap(), "Bind");
    assert_eq!(rebind.headers().get("x-responsecode").unwrap(), "0");
    let set_cookies = rebind
        .headers()
        .get_all("set-cookie")
        .iter()
        .map(|value| value.to_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(set_cookies.len(), 2);
    assert!(set_cookies
        .iter()
        .any(|cookie| cookie.starts_with("MapiContext=")));
    assert!(set_cookies
        .iter()
        .any(|cookie| cookie.starts_with("MapiSequence=")));
}

#[tokio::test]
async fn mapi_over_http_nspi_operation_requires_bound_session_cookie() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &mapi_headers("QueryRows"), &[0; 32])
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("x-requesttype").unwrap(),
        "QueryRows"
    );
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "13");
    let body = String::from_utf8(response_bytes(response).await).unwrap();
    assert!(body.contains("missing MAPI session cookie"));
}

#[tokio::test]
async fn mapi_over_http_nspi_operation_rejects_mismatched_sequence_cookie() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let bind = service
        .handle_mapi(MapiEndpoint::Nspi, &mapi_headers("Bind"), b"")
        .await
        .unwrap();
    let mut headers = mapi_headers("QueryRows");
    headers.insert(
        "cookie",
        HeaderValue::from_str(&mapi_cookie_header_with_mismatched_sequence(&bind)).unwrap(),
    );

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &headers, &[0; 32])
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("x-requesttype").unwrap(),
        "QueryRows"
    );
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "6");
    let body = String::from_utf8(response_bytes(response).await).unwrap();
    assert!(body.contains("invalid MAPI request sequence cookie"));
}

#[tokio::test]
async fn mapi_over_http_returns_nspi_and_mailbox_urls() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let mut headers = nspi_bound_headers(&service, "GetAddressBookUrl").await;
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
    renew_mapi_request_id(&mut headers);
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
    let headers = nspi_bound_headers(&service, "ResolveNames").await;

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &headers, &[0; 103])
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
    let headers = nspi_bound_headers(&service, "ResolveNames").await;

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &headers, &request)
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
    let headers = nspi_bound_headers(&service, "ResolveNames").await;

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &headers, &request)
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
    let headers = nspi_bound_headers(&service, "ResolveNames").await;

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &headers, &request)
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
async fn mapi_over_http_resolve_names_ranks_exact_contact_before_partial_account() {
    let mut partial = FakeStore::account();
    partial.account_id = Uuid::parse_str("dddddddd-dddd-dddd-dddd-dddddddddddd").unwrap();
    partial.email = "bob.alias@example.test".to_string();
    partial.display_name = "Bob Example Alias".to_string();

    let store = FakeStore {
        session: Some(FakeStore::account()),
        directory_accounts: Arc::new(Mutex::new(vec![partial])),
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
    let headers = nspi_bound_headers(&service, "ResolveNames").await;

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &headers, &request)
        .await
        .unwrap();

    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[17..21].try_into().unwrap()), 2);
    assert!(contains_bytes(&body, &utf16z("bob@example.test")));
    assert!(contains_bytes(&body, &utf16z("Bob Contact")));
    assert!(!contains_bytes(&body, &utf16z("bob.alias@example.test")));
}

#[tokio::test]
async fn mapi_over_http_hidden_authenticated_account_is_not_browsed_but_resolves_self() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        omit_principal_from_directory: true,
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let query_headers = nspi_bound_headers(&service, "QueryRows").await;
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &query_headers, &[0; 32])
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert!(!contains_bytes(&body, &utf16z("alice@example.test")));

    let matches_headers = nspi_bound_headers(&service, "GetMatches").await;
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &matches_headers, &[0; 32])
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert!(!contains_bytes(&body, &utf16z("alice@example.test")));

    let request = resolve_names_request("alice@example.test", &[0x3003_001F, 0x3001_001F]);
    let resolve_headers = nspi_bound_headers(&service, "ResolveNames").await;
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &resolve_headers, &request)
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[17..21].try_into().unwrap()), 2);
    assert!(contains_bytes(&body, &utf16z("alice@example.test")));

    let dn_to_mid_request = b"\0\0\0\0\xff\x01\0\0\0/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=alice-example-test\0\0\0\0\0";
    let dn_to_mid_headers = nspi_bound_headers(&service, "DNToMId").await;
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &dn_to_mid_headers, dn_to_mid_request)
        .await
        .unwrap();
    let body = response_bytes(response).await;
    let self_mid = u32::from_le_bytes(body[13..17].try_into().unwrap());
    assert_ne!(self_mid, 0);

    let mut props_request = Vec::new();
    props_request.extend_from_slice(&self_mid.to_le_bytes());
    props_request.extend_from_slice(&0x3003_001Fu32.to_le_bytes());
    props_request.extend_from_slice(&0x3001_001Fu32.to_le_bytes());
    let props_headers = nspi_bound_headers(&service, "GetProps").await;
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &props_headers, &props_request)
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(body[12], 1);
    assert!(contains_bytes(&body, &utf16z("alice@example.test")));
    assert!(contains_bytes(&body, &utf16z("Alice")));

    let outlook_stat_props_request = hex_bytes(
        "00000000ff000000000000000000000000000000000000000000000000b00400000904000009080000ff0100000002016d8c00000000",
    );
    let response = service
        .handle_mapi(
            MapiEndpoint::Nspi,
            &props_headers,
            &outlook_stat_props_request,
        )
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(body[12], 1);
    assert_eq!(u32::from_le_bytes(body[13..17].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[17..21].try_into().unwrap()), 1);
    assert_eq!(
        u32::from_le_bytes(body[21..25].try_into().unwrap()),
        0x8C6D_0102
    );
    assert_eq!(u32::from_le_bytes(body[25..29].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[29..33].try_into().unwrap()), 16);
    assert_eq!(
        &body[33..49],
        FakeStore::account().account_id.as_bytes().as_slice()
    );
    assert!(!contains_bytes(&body, &utf16z("alice@example.test")));

    let proxy_addresses_request = hex_bytes(
        "00000000ff000000000000000012000080000000000000000000000000b00400000904000009080000ff010000001f100f8000000000",
    );
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &props_headers, &proxy_addresses_request)
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(body[12], 1);
    assert_eq!(u32::from_le_bytes(body[13..17].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[17..21].try_into().unwrap()), 1);
    assert_eq!(
        u32::from_le_bytes(body[21..25].try_into().unwrap()),
        0x800F_101F
    );
    assert_eq!(u32::from_le_bytes(body[25..29].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[29..33].try_into().unwrap()), 1);
    assert!(contains_bytes(&body, &utf16z("SMTP:alice@example.test")));
}

#[tokio::test]
async fn mapi_over_http_query_rows_stays_in_authenticated_tenant() {
    let mut same_tenant = FakeStore::account();
    same_tenant.account_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    same_tenant.email = "bob@example.test".to_string();
    same_tenant.display_name = "Bob".to_string();

    let mut other_tenant = FakeStore::account();
    other_tenant.tenant_id = Uuid::from_u128(0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb);
    other_tenant.account_id = Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap();
    other_tenant.email = "mallory@other.test".to_string();
    other_tenant.display_name = "Mallory".to_string();

    let store = FakeStore {
        session: Some(FakeStore::account()),
        directory_accounts: Arc::new(Mutex::new(vec![same_tenant, other_tenant])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let query_headers = nspi_bound_headers(&service, "QueryRows").await;
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &query_headers, &[0; 32])
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_bytes(response).await;
    assert!(contains_bytes(&body, &utf16z("alice@example.test")));
    assert!(contains_bytes(&body, &utf16z("bob@example.test")));
    assert!(!contains_bytes(&body, &utf16z("mallory@other.test")));

    let request = resolve_names_request("mallory@other.test", &[0x3003_001F, 0x3001_001F]);
    let resolve_headers = nspi_bound_headers(&service, "ResolveNames").await;
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &resolve_headers, &request)
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[17..21].try_into().unwrap()), 0);
    assert_eq!(body[21], 0);
    assert!(!contains_bytes(&body, &utf16z("mallory@other.test")));

    let matches_request = resolve_names_request("mallory@other.test", &[0x3003_001F, 0x3001_001F]);
    let matches_headers = nspi_bound_headers(&service, "GetMatches").await;
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &matches_headers, &matches_request)
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(body[9], 0);
    assert!(!contains_bytes(&body, &utf16z("mallory@other.test")));

    let props_headers = nspi_bound_headers(&service, "GetProps").await;
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &props_headers, &matches_request)
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(body[12], 0);
    assert!(!contains_bytes(&body, &utf16z("mallory@other.test")));
}

#[tokio::test]
async fn mapi_over_http_nspi_requested_string8_columns_stay_tenant_scoped() {
    let mut same_tenant = FakeStore::account();
    same_tenant.account_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    same_tenant.email = "bob@example.test".to_string();
    same_tenant.display_name = "Bob".to_string();

    let mut other_tenant = FakeStore::account();
    other_tenant.tenant_id = Uuid::from_u128(0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb);
    other_tenant.account_id = Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap();
    other_tenant.email = "mallory@other.test".to_string();
    other_tenant.display_name = "Mallory".to_string();

    let store = FakeStore {
        session: Some(FakeStore::account()),
        directory_accounts: Arc::new(Mutex::new(vec![same_tenant, other_tenant])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let mut request = Vec::new();
    for tag in [0x3003_001Eu32, 0x3001_001E, 0x3002_001E] {
        request.extend_from_slice(&tag.to_le_bytes());
    }

    let query_headers = nspi_bound_headers(&service, "QueryRows").await;
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &query_headers, &request)
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert!(contains_bytes(&body, &0x3003_001Eu32.to_le_bytes()));
    assert!(contains_bytes(&body, b"alice@example.test\0"));
    assert!(contains_bytes(&body, b"bob@example.test\0"));
    assert!(contains_bytes(&body, b"SMTP\0"));
    assert!(!contains_bytes(&body, &utf16z("bob@example.test")));
    assert!(!contains_bytes(&body, b"mallory@other.test"));

    let props_headers = nspi_bound_headers(&service, "GetProps").await;
    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &props_headers, &request)
        .await
        .unwrap();
    let body = response_bytes(response).await;
    assert_eq!(body[12], 1);
    assert!(contains_bytes(&body, &0x3001_001Eu32.to_le_bytes()));
    assert!(contains_bytes(&body, b"alice@example.test\0"));
    assert!(contains_bytes(&body, b"Alice\0"));
    assert!(!contains_bytes(&body, &utf16z("Alice")));
    assert!(!contains_bytes(&body, b"mallory@other.test"));
}

#[tokio::test]
async fn mapi_over_http_get_matches_uses_complete_utf16_lookup_value() {
    let mut principal = FakeStore::account();
    principal.account_id = Uuid::parse_str("f732c3ed-7780-4011-8c67-36b9215bd913").unwrap();
    principal.email = "test@l-p-e.ch".to_string();
    principal.display_name = "test".to_string();

    let mut same_domain = FakeStore::account();
    same_domain.account_id = Uuid::parse_str("315383c4-0000-0000-0000-000000000000").unwrap();
    same_domain.email = "fabien@l-p-e.ch".to_string();
    same_domain.display_name = "Fabien".to_string();

    let store = FakeStore {
        session: Some(principal.clone()),
        directory_accounts: Arc::new(Mutex::new(vec![same_domain, principal])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let request = hex_bytes(
        "00000000ff000000000000000000000000000000000000000088130000e40400000904000009080000\
         0000000000ff04041f000c36ff1f000c36ff740065007300740040006c002d0070002d0065002e\
         006300680000000088130000ff0f0000001e0001301e00173a1e00083a1e00193a1e00183a\
         1e00fe391e00163a1e00003a1e0002300201ff0f0300fe0f03000039030005390201f60f\
         1e00033000000000",
    );
    let headers = nspi_bound_headers(&service, "GetMatches").await;

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &headers, &request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    assert_eq!(body[8], 0);
    assert_eq!(body[9], 1);
    assert_eq!(u32::from_le_bytes(body[10..14].try_into().unwrap()), 1);
    let matched_id = u32::from_le_bytes(body[14..18].try_into().unwrap());
    assert_ne!(matched_id, 0);
    assert_eq!(matched_id & 0x8000_0000, 0x8000_0000);
    assert_ne!(matched_id, 0xedc3_32f7);
    assert_eq!(body[18], 1);
    assert!(contains_bytes(&body, &utf16z("test@l-p-e.ch")));
    assert!(!contains_bytes(&body, &utf16z("fabien@l-p-e.ch")));
    assert!(!contains_bytes(&body, &utf16z("Fabien")));
}

#[tokio::test]
async fn mapi_over_http_nspi_minimal_ids_use_identity_mapping_not_uuid_prefix() {
    let mut first = FakeStore::account();
    first.account_id = Uuid::parse_str("11111111-1111-0000-0000-000000000001").unwrap();
    first.email = "first@example.test".to_string();
    first.display_name = "First".to_string();

    let mut second = FakeStore::account();
    second.account_id = Uuid::parse_str("11111111-1111-0000-0000-000000000002").unwrap();
    second.email = "second@example.test".to_string();
    second.display_name = "Second".to_string();

    let store = FakeStore {
        session: Some(FakeStore::account()),
        directory_accounts: Arc::new(Mutex::new(vec![first, second])),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let first_request = b"first@example.test\0";
    let first_headers = nspi_bound_headers(&service, "GetMatches").await;
    let first_response = service
        .handle_mapi(MapiEndpoint::Nspi, &first_headers, first_request)
        .await
        .unwrap();
    let first_body = response_bytes(first_response).await;
    let first_id = u32::from_le_bytes(first_body[14..18].try_into().unwrap());

    let second_request = b"second@example.test\0";
    let second_headers = nspi_bound_headers(&service, "GetMatches").await;
    let second_response = service
        .handle_mapi(MapiEndpoint::Nspi, &second_headers, second_request)
        .await
        .unwrap();
    let second_body = response_bytes(second_response).await;
    let second_id = u32::from_le_bytes(second_body[14..18].try_into().unwrap());

    assert_ne!(first_id, second_id);
    assert_ne!(first_id, 0x9111_1111);
    assert_ne!(second_id, 0x9111_1111);
}

#[tokio::test]
async fn mapi_over_http_resolve_names_returns_no_match_for_unknown_name() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let request = resolve_names_request("nobody@example.test", &[0x3003_001F, 0x3001_001F]);
    let headers = nspi_bound_headers(&service, "ResolveNames").await;

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &headers, &request)
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
        let headers = nspi_bound_headers(&service, request_type).await;
        let response = service
            .handle_mapi(MapiEndpoint::Nspi, &headers, &[0; 32])
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
                assert_eq!(body[8], 1, "{request_type}");
                assert_eq!(
                    u32::from_le_bytes(body[9..13].try_into().unwrap()),
                    1,
                    "{request_type}"
                );
                assert_ne!(
                    u32::from_le_bytes(body[13..17].try_into().unwrap()),
                    0,
                    "{request_type}"
                );
                assert_eq!(
                    u32::from_le_bytes(body[17..21].try_into().unwrap()),
                    0,
                    "{request_type}"
                );
            }
            _ => {}
        }
    }
}

#[tokio::test]
async fn mapi_over_http_nspi_mutation_requests_return_parseable_disabled_errors() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    for request_type in ["ModLinkAtt", "ModProps"] {
        let headers = nspi_bound_headers(&service, request_type).await;
        let response = service
            .handle_mapi(MapiEndpoint::Nspi, &headers, &[0; 32])
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
            "16",
            "{request_type}"
        );
        let body = String::from_utf8(response_bytes(response).await).unwrap();
        assert!(body.contains("disabled"), "{request_type}: {body}");
        assert!(
            body.contains("canonical accounts and contacts"),
            "{request_type}: {body}"
        );
    }
}

#[tokio::test]
async fn mapi_over_http_dn_to_mid_resolves_outlook_unprefixed_legacy_dn_to_principal() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let request = b"\0\0\0\0\xff\x01\0\0\0/o=LPE/ou=Exchange Administrative Group/cn=Recipients/cn=alice-example-test\0\0\0\0\0";
    let headers = nspi_bound_headers(&service, "DNToMId").await;

    let response = service
        .handle_mapi(MapiEndpoint::Nspi, &headers, request)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers().get("x-responsecode").unwrap(), "0");
    let body = response_bytes(response).await;
    assert_eq!(u32::from_le_bytes(body[0..4].try_into().unwrap()), 0);
    assert_eq!(u32::from_le_bytes(body[4..8].try_into().unwrap()), 0);
    assert_eq!(body[8], 1);
    assert_eq!(u32::from_le_bytes(body[9..13].try_into().unwrap()), 1);
    let matched_id = u32::from_le_bytes(body[13..17].try_into().unwrap());
    assert_ne!(matched_id, 0);
    assert_eq!(matched_id & 0x8000_0000, 0x8000_0000);
    assert_ne!(matched_id, 0xaaaa_aaaa);
    assert_eq!(u32::from_le_bytes(body[17..21].try_into().unwrap()), 0);
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
async fn rpc_proxy_referral_endpoint_ping_returns_a3_without_synthetic_bind_ack() {
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
    assert_eq!(body.len(), 28);
    assert_eq!(u16::from_le_bytes([body[8], body[9]]), 28);
    assert_eq!(u16::from_le_bytes([body[18], body[19]]), 1);
    assert_eq!(
        u32::from_le_bytes([body[20], body[21], body[22], body[23]]),
        2
    );
}

#[tokio::test]
async fn rpc_proxy_mailstore_endpoint_ping_waits_for_b1_before_bind_ack() {
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
    assert_eq!(body.len(), 28);
    assert_eq!(u16::from_le_bytes([body[8], body[9]]), 28);
}

#[tokio::test]
async fn rpc_proxy_address_book_endpoint_ping_returns_a3_without_synthetic_bind_ack() {
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
    assert_eq!(body[28], 0x05);
    assert_eq!(body[30], 0x14);
    assert_eq!(u16::from_le_bytes([body[36], body[37]]), 44);
}

#[tokio::test]
async fn rpc_proxy_address_book_endpoint_ping_includes_pending_conn_b1_when_in_arrives_first() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let endpoint_query = "mail.conn-b1-before-out.example.test:6004";
    let in_method = Method::from_bytes(b"RPC_IN_DATA").expect("valid RPC method");
    let in_uri: Uri = format!("/rpc/rpcproxy.dll?{endpoint_query}")
        .parse()
        .unwrap();
    let mut headers = bearer_headers();
    headers.insert("user-agent", HeaderValue::from_static("MSRPC"));
    headers.insert("accept", HeaderValue::from_static("application/rpc"));
    let mut conn_b1 = hex_bytes(
        "0500140310000000680000000000000000000600\
         06000000010000000300000076ed340685c5dd390e9a6acbc8cb9951\
         03000000a6c4ac6df261ef9fc3804d0c73a59fff\
         040000000000004005000000e09304000c0000005475b4942dd08746bf4c3d2821816b2c",
    );
    conn_b1[32..48].copy_from_slice(&[0x11; 16]);

    let response = service
        .handle_rpc_proxy_in_data_channel(&in_method, &in_uri, &headers, Body::from(conn_b1))
        .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("x-lpe-rpc-proxy-status"),
        Some(&HeaderValue::from_static("in-channel-open"))
    );

    tokio::task::yield_now().await;

    let out_method = Method::from_bytes(b"RPC_OUT_DATA").expect("valid RPC method");
    let out_uri: Uri = format!("/rpc/rpcproxy.dll?{endpoint_query}")
        .parse()
        .unwrap();
    let connect_body = rpc_proxy_conn_a1_request_body(0x0000_8000);
    let response = service
        .handle_rpc_proxy(&out_method, &out_uri, &headers, &connect_body)
        .await;

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.len(), 72);
    assert_eq!(body[28], 0x05);
    assert_eq!(body[30], 0x14);
    assert_eq!(u16::from_le_bytes([body[36], body[37]]), 44);
    assert_eq!(u16::from_le_bytes([body[46], body[47]]), 3);
    assert_eq!(
        u32::from_le_bytes([body[48], body[49], body[50], body[51]]),
        6
    );
    assert_eq!(
        u32::from_le_bytes([body[60], body[61], body[62], body[63]]),
        0x0001_0000
    );
}

#[tokio::test]
async fn rpc_proxy_address_book_endpoint_ping_suppresses_duplicate_conn_b1_when_out_arrives_first()
{
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let endpoint_query = "mail.conn-b1-after-out.example.test:6004";
    let mut headers = bearer_headers();
    headers.insert("user-agent", HeaderValue::from_static("MSRPC"));
    headers.insert("accept", HeaderValue::from_static("application/rpc"));

    let out_method = Method::from_bytes(b"RPC_OUT_DATA").expect("valid RPC method");
    let out_uri: Uri = format!("/rpc/rpcproxy.dll?{endpoint_query}")
        .parse()
        .unwrap();
    let connect_body = rpc_proxy_conn_a1_request_body(0x0000_8000);
    let response = service
        .handle_rpc_proxy(&out_method, &out_uri, &headers, &connect_body)
        .await;

    let in_method = Method::from_bytes(b"RPC_IN_DATA").expect("valid RPC method");
    let in_uri: Uri = format!("/rpc/rpcproxy.dll?{endpoint_query}")
        .parse()
        .unwrap();
    let mut conn_b1 = hex_bytes(
        "0500140310000000680000000000000000000600\
         06000000010000000300000076ed340685c5dd390e9a6acbc8cb9951\
         03000000a6c4ac6df261ef9fc3804d0c73a59fff\
         040000000000004005000000e09304000c0000005475b4942dd08746bf4c3d2821816b2c",
    );
    conn_b1[32..48].copy_from_slice(&connect_body[32..48]);

    let in_response = service
        .handle_rpc_proxy_in_data_channel(&in_method, &in_uri, &headers, Body::from(conn_b1))
        .await;

    assert_eq!(in_response.status(), StatusCode::OK);
    tokio::task::yield_now().await;

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.len(), 72);
    assert_eq!(u16::from_le_bytes([body[8], body[9]]), 28);
    assert_eq!(u16::from_le_bytes([body[36], body[37]]), 44);
}

#[tokio::test]
async fn rpc_proxy_mailstore_endpoint_ping_orders_pending_conn_b1_before_bind_ack() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);
    let endpoint_query = "mail.conn-b1-before-bind.example.test:6001";
    let in_method = Method::from_bytes(b"RPC_IN_DATA").expect("valid RPC method");
    let in_uri: Uri = format!("/rpc/rpcproxy.dll?{endpoint_query}")
        .parse()
        .unwrap();
    let mut headers = bearer_headers();
    headers.insert("user-agent", HeaderValue::from_static("MSRPC"));
    headers.insert("accept", HeaderValue::from_static("application/rpc"));
    let mut conn_b1 = hex_bytes(
        "0500140310000000680000000000000000000600\
         06000000010000000300000076ed340685c5dd390e9a6acbc8cb9951\
         03000000a6c4ac6df261ef9fc3804d0c73a59fff\
         040000000000004005000000e09304000c0000005475b4942dd08746bf4c3d2821816b2c",
    );
    conn_b1[32..48].copy_from_slice(&[0x11; 16]);

    let response = service
        .handle_rpc_proxy_in_data_channel(&in_method, &in_uri, &headers, Body::from(conn_b1))
        .await;

    assert_eq!(response.status(), StatusCode::OK);
    tokio::task::yield_now().await;

    let out_method = Method::from_bytes(b"RPC_OUT_DATA").expect("valid RPC method");
    let out_uri: Uri = format!("/rpc/rpcproxy.dll?{endpoint_query}")
        .parse()
        .unwrap();
    let connect_body = rpc_proxy_conn_a1_request_body(0x0000_8000);
    let response = service
        .handle_rpc_proxy(&out_method, &out_uri, &headers, &connect_body)
        .await;

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.len(), 184);
    assert_eq!(body[28], 0x05);
    assert_eq!(body[30], 0x14);
    assert_eq!(u16::from_le_bytes([body[36], body[37]]), 44);
    assert_eq!(body[72], 0x05);
    assert_eq!(body[74], 0x0c);
    assert_eq!(u16::from_le_bytes([body[80], body[81]]), 112);
}

#[tokio::test]
async fn rpc_proxy_opens_authenticated_mailstore_in_data_channel_without_waiting_for_body_eof() {
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
fn rpc_proxy_classifies_zero_length_endpoint_in_data_as_echo_probe() {
    let method = Method::from_bytes(b"RPC_IN_DATA").expect("valid RPC method");
    let uri: Uri = "/rpc/rpcproxy.dll?mail.example.test:6001".parse().unwrap();
    let mut headers = HeaderMap::new();
    headers.insert("user-agent", HeaderValue::from_static("MSRPC"));
    headers.insert("accept", HeaderValue::from_static("application/rpc"));
    headers.insert("content-length", HeaderValue::from_static("0"));

    assert!(!is_rpc_proxy_in_data_channel_request(
        &method, &uri, &headers
    ));
}

#[tokio::test]
async fn rpc_proxy_answers_zero_length_endpoint_in_data_echo_probe() {
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
    headers.insert("content-length", HeaderValue::from_static("0"));

    let response = service.handle_rpc_proxy(&method, &uri, &headers, b"").await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("x-lpe-rpc-proxy-status"),
        Some(&HeaderValue::from_static("echo"))
    );
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(body.len(), 20);
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
fn rpc_proxy_in_channel_bind_ack_negotiates_bind_time_features() {
    let bind = hex_bytes(
        "05000b0310000000d000280096000000\
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
    let mut buffer = bind;

    let response =
        rpc_proxy_in_channel_response_for_buffer(&mut buffer).expect("bind ack response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x0c, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        150
    );
    assert_eq!(response[28], 3);
    assert_eq!(u16::from_le_bytes(response[32..34].try_into().unwrap()), 0);
    assert_eq!(u16::from_le_bytes(response[56..58].try_into().unwrap()), 2);
    assert_eq!(u16::from_le_bytes(response[80..82].try_into().unwrap()), 3);
    assert_eq!(u16::from_le_bytes(response[82..84].try_into().unwrap()), 0);
    assert_eq!(&response[84..104], &[0; 20]);
}

#[test]
fn rpc_proxy_referral_endpoint_management_ping_uses_bound_context_before_rfri_heuristic() {
    let endpoint_query = "mail.management.example.test:6002";
    let bind = hex_bytes(
        "05000b0310000000d000280002000000\
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
    let mut buffer = bind;

    let bind_response =
        rpc_proxy_in_channel_response_for_endpoint_query(endpoint_query, &mut buffer)
            .expect("management bind response");

    assert_eq!(bind_response[0..4], [0x05, 0x00, 0x0c, 0x03]);
    assert_eq!(
        u32::from_le_bytes([
            bind_response[12],
            bind_response[13],
            bind_response[14],
            bind_response[15]
        ]),
        2
    );

    let mut auth3 = vec![0u8; 250];
    auth3[0..8].copy_from_slice(&[0x05, 0x00, 0x10, 0x03, 0x10, 0x00, 0x00, 0x00]);
    auth3[8..10].copy_from_slice(&250u16.to_le_bytes());
    auth3[10..12].copy_from_slice(&0x00deu16.to_le_bytes());
    auth3[12..16].copy_from_slice(&2u32.to_le_bytes());
    let management = [
        0x05, 0x00, 0x00, 0x03, 0x10, 0x00, 0x00, 0x00, 0x40, 0x00, 0x10, 0x00, 0x02, 0x00, 0x00,
        0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a, 0x02, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
    ];
    let mut request = auth3;
    request.extend_from_slice(&management);

    let response = rpc_proxy_in_channel_response_for_endpoint_query(endpoint_query, &mut request)
        .expect("management ping response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        2
    );
    assert_eq!(
        u32::from_le_bytes([response[24], response[25], response[26], response[27]]),
        4
    );
    assert_eq!(
        u32::from_le_bytes([response[28], response[29], response[30], response[31]]),
        4
    );
    assert!(!contains_bytes(&response, b"mail.management.example.test"));
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

#[test]
fn rpc_proxy_mailstore_management_stats_accepts_rca_short_stub() {
    let mut buffer = vec![0u8; 626];
    buffer[0..64].copy_from_slice(&[
        0x05, 0x00, 0x00, 0x03, 0x10, 0x00, 0x00, 0x00, 0x40, 0x00, 0x10, 0x00, 0x02, 0x00, 0x00,
        0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a, 0x02, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
    ]);

    let response =
        rpc_proxy_in_channel_response_for_endpoint_query("mail.example.test:6001", &mut buffer)
            .expect("management stats response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        2
    );
    assert_eq!(u32::from_le_bytes(response[24..28].try_into().unwrap()), 4);
    assert_eq!(u32::from_le_bytes(response[28..32].try_into().unwrap()), 4);
    assert_eq!(u32::from_le_bytes(response[48..52].try_into().unwrap()), 0);
    assert_eq!(buffer.len(), 562);
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
        &FakeStore::account().account_id.to_bytes_le()
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
    assert_eq!(u16::from_le_bytes([response[8], response[9]]), 76);
    assert_eq!(u16::from_le_bytes([response[10], response[11]]), 16);
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
    assert_eq!(&response[52..60], &[0x0a, 0x02, 0x00, 0x00, 0, 0, 0, 0]);
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
fn rpc_proxy_address_book_endpoint_resolves_names_on_alternate_context_id() {
    let request = [
        0x05, 0x00, 0x00, 0x03, 0x10, 0x00, 0x00, 0x00, 0xd0, 0x00, 0x10, 0x00, 0x04, 0x00, 0x00,
        0x00, 0x98, 0x00, 0x00, 0x00, 0x01, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x4c, 0x50,
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
        rpc_proxy_in_channel_response_for_endpoint_query("mail.example.test:6004", &mut buffer)
            .expect("resolve names response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        4
    );
    assert!(response
        .windows(b"fabien@l-p-e.ch".len())
        .any(|window| window == b"fabien@l-p-e.ch"));
}

#[tokio::test]
async fn rpc_proxy_address_book_check_name_fallback_answers_framing_mismatch() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let validator = Validator::new(FakeDetector::pdf(), 0.8);
    let principal = test_account_principal();
    let mut buffer = vec![0u8; 626];
    buffer[0..16].copy_from_slice(&[
        0x05, 0x00, 0x00, 0x03, 0x10, 0x00, 0x00, 0x00, 0x40, 0x00, 0x10, 0x00, 0x4d, 0x00, 0x00,
        0x00,
    ]);
    buffer[16..24].copy_from_slice(&[0x10, 0x00, 0x00, 0x00, 0x07, 0x00, 0x63, 0x00]);
    let requested_name: Vec<u8> = "=SMTP:alice@example.test\0"
        .encode_utf16()
        .flat_map(|unit| unit.to_le_bytes())
        .collect();
    buffer[320..320 + requested_name.len()].copy_from_slice(&requested_name);

    let response = rpc_proxy_in_channel_response_for_endpoint_query_with_store(
        &store,
        &validator,
        &principal,
        "mail.example.test:6004",
        &mut buffer,
    )
    .await
    .expect("fallback resolve names response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        77
    );
    assert!(response
        .windows(b"alice@example.test".len())
        .any(|window| window == b"alice@example.test"));
    assert!(buffer.is_empty());
}

#[tokio::test]
async fn rpc_proxy_address_book_auth3_does_not_trigger_check_name_fallback() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let validator = Validator::new(FakeDetector::pdf(), 0.8);
    let principal = test_account_principal();
    let mut buffer = vec![0u8; 250];
    buffer[0..16].copy_from_slice(&[
        0x05, 0x00, 0x10, 0x03, 0x10, 0x00, 0x00, 0x00, 0xfa, 0x00, 0xde, 0x00, 0x7f, 0x00, 0x00,
        0x00,
    ]);
    buffer[16..24].copy_from_slice(&[0xf8, 0x0f, 0xf8, 0x0f, 0x0a, 0x02, 0x00, 0x00]);
    let authenticated_name: Vec<u8> = "test@l-p-e.ch\0"
        .encode_utf16()
        .flat_map(|unit| unit.to_le_bytes())
        .collect();
    buffer[80..80 + authenticated_name.len()].copy_from_slice(&authenticated_name);

    let response = rpc_proxy_in_channel_response_for_endpoint_query_with_store(
        &store,
        &validator,
        &principal,
        "mail.example.test:6004",
        &mut buffer,
    )
    .await;

    assert!(response.is_none());
    assert!(buffer.is_empty());
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

#[tokio::test]
async fn rpc_proxy_address_book_management_stats_accepts_rca_short_stub() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let validator = Validator::new(FakeDetector::pdf(), 0.8);
    let principal = test_account_principal();
    let mut buffer = vec![0u8; 626];
    buffer[0..64].copy_from_slice(&[
        0x05, 0x00, 0x00, 0x03, 0x10, 0x00, 0x00, 0x00, 0x40, 0x00, 0x10, 0x00, 0x7f, 0x00, 0x00,
        0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a, 0x02, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
    ]);

    let response = rpc_proxy_in_channel_response_for_endpoint_query_with_store(
        &store,
        &validator,
        &principal,
        "mail.example.test:6004",
        &mut buffer,
    )
    .await
    .expect("address book management stats response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        127
    );
    assert_eq!(u32::from_le_bytes(response[24..28].try_into().unwrap()), 4);
    assert_eq!(u32::from_le_bytes(response[28..32].try_into().unwrap()), 4);
    assert_eq!(u32::from_le_bytes(response[48..52].try_into().unwrap()), 0);
    assert_eq!(buffer.len(), 562);
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

#[tokio::test]
async fn rpc_proxy_referral_get_fqdn_accepts_rca_short_stub() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let validator = Validator::new(FakeDetector::pdf(), 0.8);
    let principal = test_account_principal();
    let mut buffer = vec![0u8; 626];
    buffer[0..64].copy_from_slice(&[
        0x05, 0x00, 0x00, 0x03, 0x10, 0x00, 0x00, 0x00, 0x40, 0x00, 0x10, 0x00, 0x7f, 0x00, 0x00,
        0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a, 0x02, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00,
    ]);

    let response = rpc_proxy_in_channel_response_for_endpoint_query_with_store(
        &store,
        &validator,
        &principal,
        "mail.example.test:6002",
        &mut buffer,
    )
    .await
    .expect("referral response");

    assert_eq!(response[0..4], [0x05, 0x00, 0x02, 0x03]);
    assert_eq!(
        u32::from_le_bytes([response[12], response[13], response[14], response[15]]),
        127
    );
    assert!(response
        .windows(b"mail.example.test".len())
        .any(|window| window == b"mail.example.test"));
    assert_eq!(buffer.len(), 562);
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
                uid: "cccccccc-cccc-cccc-cccc-cccccccccccc".to_string(),
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
                uid: "ffffffff-ffff-ffff-ffff-ffffffffffff".to_string(),
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
    assert!(!created_events[0]
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
async fn get_item_mime_content_hides_bcc_for_sent_message_default_fetch() {
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
    assert!(!mime.contains("Bcc:"));
    assert!(!mime.contains("hidden@example.test"));
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
            uid: event_id.to_string(),
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
