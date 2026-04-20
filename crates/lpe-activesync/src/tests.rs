use std::sync::{Arc, Mutex};

use argon2::{
    password_hash::{rand_core::OsRng, SaltString},
    Argon2, PasswordHasher,
};
use axum::body::to_bytes;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use lpe_mail_auth::AccountAuthStore;
use lpe_storage::{
    AccountLogin, ActiveSyncAttachment, ActiveSyncAttachmentContent, ActiveSyncItemState,
    ActiveSyncSyncState, AuditEntryInput, AuthenticatedAccount, ClientContact, ClientEvent,
    JmapEmail, JmapEmailAddress, JmapEmailQuery, JmapMailbox, MailboxAccountAccess,
    SavedDraftMessage, StoredAccountAppPassword, SubmitMessageInput, SubmittedMessage,
    UpsertClientContactInput, UpsertClientEventInput,
};
use uuid::Uuid;

use crate::{
    service::ActiveSyncService,
    store::{ActiveSyncStore, StoreFuture},
    types::ActiveSyncQuery,
    wbxml::{decode_wbxml, encode_wbxml, WbxmlNode},
};

#[derive(Clone, Default)]
struct FakeStore {
    session: Option<AuthenticatedAccount>,
    login: Option<AccountLogin>,
    mailboxes: Vec<JmapMailbox>,
    accessible_mailbox_accounts: Vec<MailboxAccountAccess>,
    emails: Arc<Mutex<Vec<JmapEmail>>>,
    contacts: Arc<Mutex<Vec<ClientContact>>>,
    events: Arc<Mutex<Vec<ClientEvent>>>,
    attachments: Arc<Mutex<std::collections::HashMap<Uuid, Vec<ActiveSyncAttachment>>>>,
    attachment_contents: Arc<Mutex<std::collections::HashMap<String, ActiveSyncAttachmentContent>>>,
    saved_drafts: Arc<Mutex<Vec<SubmitMessageInput>>>,
    submitted_messages: Arc<Mutex<Vec<SubmitMessageInput>>>,
    sync_states: Arc<Mutex<std::collections::HashMap<String, ActiveSyncSyncState>>>,
    full_email_fetches: Arc<Mutex<u32>>,
}

impl FakeStore {
    fn account() -> AuthenticatedAccount {
        AuthenticatedAccount {
            tenant_id: "tenant-a".to_string(),
            account_id: Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap(),
            email: "alice@example.test".to_string(),
            display_name: "Alice".to_string(),
            expires_at: "2026-04-18T10:00:00Z".to_string(),
        }
    }

    fn password_hash() -> String {
        Argon2::default()
            .hash_password(b"secret", &SaltString::generate(&mut OsRng))
            .unwrap()
            .to_string()
    }

    fn login() -> AccountLogin {
        AccountLogin {
            tenant_id: "tenant-a".to_string(),
            account_id: Self::account().account_id,
            email: Self::account().email,
            display_name: Self::account().display_name,
            password_hash: Self::password_hash(),
            status: "active".to_string(),
        }
    }

    fn draft_mailbox() -> JmapMailbox {
        JmapMailbox {
            id: Uuid::parse_str("dddddddd-dddd-dddd-dddd-dddddddddddd").unwrap(),
            role: "drafts".to_string(),
            name: "Drafts".to_string(),
            sort_order: 10,
            total_emails: 1,
            unread_emails: 0,
        }
    }

    fn inbox_mailbox() -> JmapMailbox {
        JmapMailbox {
            id: Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap(),
            role: "inbox".to_string(),
            name: "Inbox".to_string(),
            sort_order: 1,
            total_emails: 1,
            unread_emails: 1,
        }
    }

    fn sent_mailbox() -> JmapMailbox {
        JmapMailbox {
            id: Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap(),
            role: "sent".to_string(),
            name: "Sent".to_string(),
            sort_order: 2,
            total_emails: 1,
            unread_emails: 0,
        }
    }

    fn inbox_email(id: &str, mailbox_id: Uuid, role: &str, subject: &str) -> JmapEmail {
        JmapEmail {
            id: Uuid::parse_str(id).unwrap(),
            thread_id: Uuid::new_v4(),
            mailbox_id,
            mailbox_role: role.to_string(),
            mailbox_name: role.to_string(),
            received_at: "2026-04-18T20:00:00Z".to_string(),
            sent_at: Some("2026-04-18T20:00:00Z".to_string()),
            from_address: "bob@example.test".to_string(),
            from_display: Some("Bob".to_string()),
            sender_address: None,
            sender_display: None,
            sender_authorization_kind: "self".to_string(),
            submitted_by_account_id: Self::account().account_id,
            to: vec![JmapEmailAddress {
                address: "alice@example.test".to_string(),
                display_name: Some("Alice".to_string()),
            }],
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: subject.to_string(),
            preview: subject.to_string(),
            body_text: format!("Body {subject}"),
            body_html_sanitized: None,
            unread: true,
            flagged: false,
            has_attachments: false,
            size_octets: 32,
            internet_message_id: None,
            mime_blob_ref: None,
            delivery_status: "received".to_string(),
        }
    }

    fn mailbox_access() -> MailboxAccountAccess {
        let account = Self::account();
        MailboxAccountAccess {
            account_id: account.account_id,
            email: account.email,
            display_name: account.display_name,
            is_owned: true,
            may_read: true,
            may_write: true,
            may_send_as: true,
            may_send_on_behalf: true,
        }
    }

    fn shared_mailbox_access(may_send_as: bool, may_send_on_behalf: bool) -> MailboxAccountAccess {
        MailboxAccountAccess {
            account_id: Uuid::parse_str("bbbbbbbb-1111-2222-3333-444444444444").unwrap(),
            email: "shared@example.test".to_string(),
            display_name: "Shared Mailbox".to_string(),
            is_owned: false,
            may_read: true,
            may_write: true,
            may_send_as,
            may_send_on_behalf,
        }
    }
}

impl AccountAuthStore for FakeStore {
    fn fetch_account_session<'a>(
        &'a self,
        token: &'a str,
    ) -> StoreFuture<'a, Option<AuthenticatedAccount>> {
        let session = if token == "token" {
            self.session.clone()
        } else {
            None
        };
        Box::pin(async move { Ok(session) })
    }

    fn fetch_account_login<'a>(&'a self, _email: &'a str) -> StoreFuture<'a, Option<AccountLogin>> {
        let login = self.login.clone();
        Box::pin(async move { Ok(login) })
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
        _entry: AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move { Ok(()) })
    }
}

impl ActiveSyncStore for FakeStore {
    fn fetch_accessible_mailbox_accounts<'a>(
        &'a self,
        _principal_account_id: Uuid,
    ) -> StoreFuture<'a, Vec<MailboxAccountAccess>> {
        let accesses = if self.accessible_mailbox_accounts.is_empty() {
            vec![Self::mailbox_access()]
        } else {
            self.accessible_mailbox_accounts.clone()
        };
        Box::pin(async move { Ok(accesses) })
    }

    fn fetch_jmap_mailboxes<'a>(&'a self, _account_id: Uuid) -> StoreFuture<'a, Vec<JmapMailbox>> {
        let mailboxes = self.mailboxes.clone();
        Box::pin(async move { Ok(mailboxes) })
    }

    fn query_jmap_email_ids<'a>(
        &'a self,
        _account_id: Uuid,
        mailbox_id: Option<Uuid>,
        search_text: Option<&'a str>,
        position: u64,
        limit: u64,
    ) -> StoreFuture<'a, JmapEmailQuery> {
        let search_text = search_text.map(|value| value.to_ascii_lowercase());
        let filtered = self
            .emails
            .lock()
            .unwrap()
            .iter()
            .filter(|email| mailbox_id.is_none() || Some(email.mailbox_id) == mailbox_id)
            .filter(|email| match search_text.as_ref() {
                None => true,
                Some(needle) => {
                    email.subject.to_ascii_lowercase().contains(needle)
                        || email.body_text.to_ascii_lowercase().contains(needle)
                        || email.preview.to_ascii_lowercase().contains(needle)
                }
            })
            .map(|email| email.id)
            .collect::<Vec<_>>();
        let total = filtered.len() as u64;
        let ids = filtered
            .into_iter()
            .skip(position as usize)
            .take(limit as usize)
            .collect::<Vec<_>>();
        Box::pin(async move { Ok(JmapEmailQuery { total, ids }) })
    }

    fn fetch_jmap_emails<'a>(
        &'a self,
        _account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<JmapEmail>> {
        *self.full_email_fetches.lock().unwrap() += 1;
        let emails = self
            .emails
            .lock()
            .unwrap()
            .iter()
            .filter(|email| ids.contains(&email.id))
            .cloned()
            .collect::<Vec<_>>();
        Box::pin(async move { Ok(emails) })
    }

    fn fetch_latest_activesync_sync_state<'a>(
        &'a self,
        account_id: Uuid,
        device_id: &'a str,
        collection_id: &'a str,
    ) -> StoreFuture<'a, Option<ActiveSyncSyncState>> {
        let prefix = format!("{account_id}:{device_id}:{collection_id}:");
        let state = self
            .sync_states
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| key.starts_with(&prefix))
            .max_by(|(left, _), (right, _)| left.cmp(right))
            .map(|(_, value)| value.clone());
        Box::pin(async move { Ok(state) })
    }

    fn fetch_jmap_draft<'a>(
        &'a self,
        _account_id: Uuid,
        id: Uuid,
    ) -> StoreFuture<'a, Option<JmapEmail>> {
        let email = self
            .emails
            .lock()
            .unwrap()
            .iter()
            .find(|email| email.id == id)
            .cloned();
        Box::pin(async move { Ok(email) })
    }

    fn fetch_activesync_message_attachments<'a>(
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

    fn fetch_activesync_attachment_content<'a>(
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

    fn fetch_activesync_email_states<'a>(
        &'a self,
        _account_id: Uuid,
        mailbox_id: Uuid,
        position: u64,
        limit: u64,
    ) -> StoreFuture<'a, Vec<ActiveSyncItemState>> {
        let states = self
            .emails
            .lock()
            .unwrap()
            .iter()
            .filter(|email| email.mailbox_id == mailbox_id)
            .map(|email| ActiveSyncItemState {
                id: email.id,
                fingerprint: format!(
                    "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
                    email.subject,
                    email.preview,
                    email.body_text,
                    email
                        .sent_at
                        .clone()
                        .unwrap_or_else(|| email.received_at.clone()),
                    if email.unread { "1" } else { "0" },
                    if email.flagged { "1" } else { "0" },
                    email.from_display.clone().unwrap_or_default(),
                    email.from_address,
                    email
                        .to
                        .iter()
                        .map(|recipient| format!(
                            "{}:{}",
                            recipient.address.to_lowercase(),
                            recipient.display_name.clone().unwrap_or_default()
                        ))
                        .collect::<Vec<_>>()
                        .join(","),
                    email
                        .cc
                        .iter()
                        .map(|recipient| format!(
                            "{}:{}",
                            recipient.address.to_lowercase(),
                            recipient.display_name.clone().unwrap_or_default()
                        ))
                        .collect::<Vec<_>>()
                        .join(","),
                    email.delivery_status,
                ),
            })
            .collect::<Vec<_>>();
        let paged = states
            .into_iter()
            .skip(position as usize)
            .take(limit as usize)
            .collect::<Vec<_>>();
        Box::pin(async move { Ok(paged) })
    }

    fn fetch_activesync_email_states_by_ids<'a>(
        &'a self,
        _account_id: Uuid,
        mailbox_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<ActiveSyncItemState>> {
        let states = self
            .emails
            .lock()
            .unwrap()
            .iter()
            .filter(|email| email.mailbox_id == mailbox_id && ids.contains(&email.id))
            .map(|email| ActiveSyncItemState {
                id: email.id,
                fingerprint: format!(
                    "{}|{}|{}|{}|{}|{}|{}|{}|{}|{}|{}",
                    email.subject,
                    email.preview,
                    email.body_text,
                    email
                        .sent_at
                        .clone()
                        .unwrap_or_else(|| email.received_at.clone()),
                    if email.unread { "1" } else { "0" },
                    if email.flagged { "1" } else { "0" },
                    email.from_display.clone().unwrap_or_default(),
                    email.from_address,
                    email
                        .to
                        .iter()
                        .map(|recipient| format!(
                            "{}:{}",
                            recipient.address.to_lowercase(),
                            recipient.display_name.clone().unwrap_or_default()
                        ))
                        .collect::<Vec<_>>()
                        .join(","),
                    email
                        .cc
                        .iter()
                        .map(|recipient| format!(
                            "{}:{}",
                            recipient.address.to_lowercase(),
                            recipient.display_name.clone().unwrap_or_default()
                        ))
                        .collect::<Vec<_>>()
                        .join(","),
                    email.delivery_status,
                ),
            })
            .collect::<Vec<_>>();
        Box::pin(async move { Ok(states) })
    }

    fn save_draft_message<'a>(
        &'a self,
        input: SubmitMessageInput,
        _audit: AuditEntryInput,
    ) -> StoreFuture<'a, SavedDraftMessage> {
        self.saved_drafts.lock().unwrap().push(input.clone());
        Box::pin(async move {
            Ok(SavedDraftMessage {
                message_id: input.draft_message_id.unwrap_or_else(|| {
                    Uuid::parse_str("10101010-1010-1010-1010-101010101010").unwrap()
                }),
                account_id: input.account_id,
                submitted_by_account_id: input.submitted_by_account_id,
                draft_mailbox_id: FakeStore::draft_mailbox().id,
                delivery_status: "draft".to_string(),
            })
        })
    }

    fn delete_draft_message<'a>(
        &'a self,
        _account_id: Uuid,
        _message_id: Uuid,
        _audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        Box::pin(async move { Ok(()) })
    }

    fn submit_message<'a>(
        &'a self,
        input: SubmitMessageInput,
        _audit: AuditEntryInput,
    ) -> StoreFuture<'a, SubmittedMessage> {
        self.submitted_messages.lock().unwrap().push(input.clone());
        Box::pin(async move {
            Ok(SubmittedMessage {
                message_id: Uuid::new_v4(),
                thread_id: Uuid::new_v4(),
                account_id: input.account_id,
                submitted_by_account_id: input.submitted_by_account_id,
                sent_mailbox_id: Uuid::new_v4(),
                outbound_queue_id: Uuid::new_v4(),
                delivery_status: "queued".to_string(),
            })
        })
    }

    fn fetch_client_contacts<'a>(
        &'a self,
        _account_id: Uuid,
    ) -> StoreFuture<'a, Vec<ClientContact>> {
        let contacts = self.contacts.lock().unwrap().clone();
        Box::pin(async move { Ok(contacts) })
    }

    fn fetch_client_contacts_by_ids<'a>(
        &'a self,
        _account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<ClientContact>> {
        let contacts = self
            .contacts
            .lock()
            .unwrap()
            .iter()
            .filter(|contact| ids.contains(&contact.id))
            .cloned()
            .collect::<Vec<_>>();
        Box::pin(async move { Ok(contacts) })
    }

    fn upsert_client_contact<'a>(
        &'a self,
        input: UpsertClientContactInput,
    ) -> StoreFuture<'a, ClientContact> {
        let contact = ClientContact {
            id: input.id.unwrap_or_else(Uuid::new_v4),
            name: input.name,
            role: input.role,
            email: input.email,
            phone: input.phone,
            team: input.team,
            notes: input.notes,
        };
        let mut contacts = self.contacts.lock().unwrap();
        if let Some(existing) = contacts.iter_mut().find(|entry| entry.id == contact.id) {
            *existing = contact.clone();
        } else {
            contacts.push(contact.clone());
        }
        Box::pin(async move { Ok(contact) })
    }

    fn delete_client_contact<'a>(
        &'a self,
        _account_id: Uuid,
        contact_id: Uuid,
    ) -> StoreFuture<'a, ()> {
        self.contacts
            .lock()
            .unwrap()
            .retain(|contact| contact.id != contact_id);
        Box::pin(async move { Ok(()) })
    }

    fn fetch_client_events<'a>(&'a self, _account_id: Uuid) -> StoreFuture<'a, Vec<ClientEvent>> {
        let events = self.events.lock().unwrap().clone();
        Box::pin(async move { Ok(events) })
    }

    fn fetch_client_events_by_ids<'a>(
        &'a self,
        _account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<ClientEvent>> {
        let events = self
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| ids.contains(&event.id))
            .cloned()
            .collect::<Vec<_>>();
        Box::pin(async move { Ok(events) })
    }

    fn upsert_client_event<'a>(
        &'a self,
        input: UpsertClientEventInput,
    ) -> StoreFuture<'a, ClientEvent> {
        let event = ClientEvent {
            id: input.id.unwrap_or_else(Uuid::new_v4),
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
        let mut events = self.events.lock().unwrap();
        if let Some(existing) = events.iter_mut().find(|entry| entry.id == event.id) {
            *existing = event.clone();
        } else {
            events.push(event.clone());
        }
        Box::pin(async move { Ok(event) })
    }

    fn delete_client_event<'a>(&'a self, _account_id: Uuid, event_id: Uuid) -> StoreFuture<'a, ()> {
        self.events
            .lock()
            .unwrap()
            .retain(|event| event.id != event_id);
        Box::pin(async move { Ok(()) })
    }

    fn fetch_activesync_contact_states<'a>(
        &'a self,
        _account_id: Uuid,
    ) -> StoreFuture<'a, Vec<ActiveSyncItemState>> {
        let states = self
            .contacts
            .lock()
            .unwrap()
            .iter()
            .map(|contact| ActiveSyncItemState {
                id: contact.id,
                fingerprint: format!(
                    "{}|{}|{}|{}|{}|{}",
                    contact.name,
                    contact.role,
                    contact.email,
                    contact.phone,
                    contact.team,
                    contact.notes
                ),
            })
            .collect::<Vec<_>>();
        Box::pin(async move { Ok(states) })
    }

    fn fetch_activesync_contact_states_by_ids<'a>(
        &'a self,
        _account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<ActiveSyncItemState>> {
        let states = self
            .contacts
            .lock()
            .unwrap()
            .iter()
            .filter(|contact| ids.contains(&contact.id))
            .map(|contact| ActiveSyncItemState {
                id: contact.id,
                fingerprint: format!(
                    "{}|{}|{}|{}|{}|{}",
                    contact.name,
                    contact.role,
                    contact.email,
                    contact.phone,
                    contact.team,
                    contact.notes
                ),
            })
            .collect::<Vec<_>>();
        Box::pin(async move { Ok(states) })
    }

    fn fetch_activesync_event_states<'a>(
        &'a self,
        _account_id: Uuid,
    ) -> StoreFuture<'a, Vec<ActiveSyncItemState>> {
        let states = self
            .events
            .lock()
            .unwrap()
            .iter()
            .map(|event| ActiveSyncItemState {
                id: event.id,
                fingerprint: format!(
                    "{}|{}|{}|{}|{}|{}",
                    event.date,
                    event.time,
                    event.title,
                    event.location,
                    event.attendees,
                    event.notes
                ),
            })
            .collect::<Vec<_>>();
        Box::pin(async move { Ok(states) })
    }

    fn fetch_activesync_event_states_by_ids<'a>(
        &'a self,
        _account_id: Uuid,
        ids: &'a [Uuid],
    ) -> StoreFuture<'a, Vec<ActiveSyncItemState>> {
        let states = self
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| ids.contains(&event.id))
            .map(|event| ActiveSyncItemState {
                id: event.id,
                fingerprint: format!(
                    "{}|{}|{}|{}|{}|{}",
                    event.date,
                    event.time,
                    event.title,
                    event.location,
                    event.attendees,
                    event.notes
                ),
            })
            .collect::<Vec<_>>();
        Box::pin(async move { Ok(states) })
    }

    fn store_activesync_sync_state<'a>(
        &'a self,
        account_id: Uuid,
        device_id: &'a str,
        collection_id: &'a str,
        sync_key: &'a str,
        snapshot_json: String,
    ) -> StoreFuture<'a, ()> {
        let key = format!("{account_id}:{device_id}:{collection_id}:{sync_key}");
        self.sync_states.lock().unwrap().insert(
            key,
            ActiveSyncSyncState {
                sync_key: sync_key.to_string(),
                snapshot_json,
            },
        );
        Box::pin(async move { Ok(()) })
    }

    fn fetch_activesync_sync_state<'a>(
        &'a self,
        account_id: Uuid,
        device_id: &'a str,
        collection_id: &'a str,
        sync_key: &'a str,
    ) -> StoreFuture<'a, Option<ActiveSyncSyncState>> {
        let key = format!("{account_id}:{device_id}:{collection_id}:{sync_key}");
        let state = self.sync_states.lock().unwrap().get(&key).cloned();
        Box::pin(async move { Ok(state) })
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

fn basic_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_static("Basic YWxpY2VAZXhhbXBsZS50ZXN0OnNlY3JldA=="),
    );
    headers
}

fn mime_headers() -> HeaderMap {
    let mut headers = bearer_headers();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("message/rfc822"),
    );
    headers
}

async fn decode_response_body(response: axum::response::Response) -> WbxmlNode {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    decode_wbxml(&bytes).unwrap()
}

fn collection_sync_key(sync: &WbxmlNode, collection_id: &str) -> String {
    sync.child("Collections")
        .unwrap()
        .children_named("Collection")
        .into_iter()
        .find(|collection| {
            collection
                .child("CollectionId")
                .map(|node| node.text_value() == collection_id)
                .unwrap_or(false)
        })
        .and_then(|collection| collection.child("SyncKey"))
        .map(|node| node.text_value().to_string())
        .unwrap()
}

#[test]
fn wbxml_roundtrip_preserves_tokens_and_text() {
    let mut root = WbxmlNode::new(7, "FolderSync");
    root.push(WbxmlNode::with_text(7, "SyncKey", "1"));
    root.push(WbxmlNode::with_text(0, "WindowSize", "10"));
    let bytes = encode_wbxml(&root);
    let decoded = decode_wbxml(&bytes).unwrap();

    assert_eq!(decoded.name, "FolderSync");
    assert_eq!(decoded.child("SyncKey").unwrap().text_value(), "1");
    assert_eq!(decoded.child("WindowSize").unwrap().text_value(), "10");
}

#[tokio::test]
async fn folder_sync_returns_mail_and_collaboration_collections() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: vec![
            FakeStore::inbox_mailbox(),
            FakeStore::draft_mailbox(),
            FakeStore::sent_mailbox(),
        ],
        ..Default::default()
    };
    let service = ActiveSyncService::new(store);
    let request = encode_wbxml(&{
        let mut node = WbxmlNode::new(7, "FolderSync");
        node.push(WbxmlNode::with_text(7, "SyncKey", "0"));
        node
    });

    let response = service
        .handle_request(
            ActiveSyncQuery {
                cmd: Some("FolderSync".to_string()),
                user: Some("alice@example.test".to_string()),
                device_id: Some("dev1".to_string()),
                _device_type: Some("phone".to_string()),
            },
            &bearer_headers(),
            &request,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn sync_add_command_saves_draft_through_canonical_storage() {
    let draft_mailbox = FakeStore::draft_mailbox();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: vec![draft_mailbox.clone()],
        ..Default::default()
    };
    let service = ActiveSyncService::new(store.clone());

    let request = encode_wbxml(&{
        let mut sync = WbxmlNode::new(0, "Sync");
        let mut collections = WbxmlNode::new(0, "Collections");
        let mut collection = WbxmlNode::new(0, "Collection");
        collection.push(WbxmlNode::with_text(0, "SyncKey", "0"));
        collection.push(WbxmlNode::with_text(
            0,
            "CollectionId",
            draft_mailbox.id.to_string(),
        ));
        let mut commands = WbxmlNode::new(0, "Commands");
        let mut add = WbxmlNode::new(0, "Add");
        add.push(WbxmlNode::with_text(0, "ClientId", "c1"));
        let mut app_data = WbxmlNode::new(0, "ApplicationData");
        app_data.push(WbxmlNode::with_text(2, "To", "bob@example.test"));
        app_data.push(WbxmlNode::with_text(2, "Subject", "Draft"));
        let mut body = WbxmlNode::new(17, "Body");
        body.push(WbxmlNode::with_text(17, "Data", "Draft body"));
        app_data.push(body);
        add.push(app_data);
        commands.push(add);
        collection.push(commands);
        collections.push(collection);
        sync.push(collections);
        sync
    });

    service
        .handle_request(
            ActiveSyncQuery {
                cmd: Some("Sync".to_string()),
                user: Some("alice@example.test".to_string()),
                device_id: Some("dev1".to_string()),
                _device_type: Some("phone".to_string()),
            },
            &bearer_headers(),
            &request,
        )
        .await
        .unwrap();

    let saved = store.saved_drafts.lock().unwrap();
    assert_eq!(saved.len(), 1);
    assert_eq!(saved[0].subject, "Draft");
    assert_eq!(saved[0].to[0].address, "bob@example.test");
}

#[tokio::test]
async fn sync_handles_multiple_collections_and_common_optional_tokens() {
    let inbox = FakeStore::inbox_mailbox();
    let sent = FakeStore::sent_mailbox();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: vec![inbox.clone(), sent.clone()],
        emails: Arc::new(Mutex::new(vec![
            FakeStore::inbox_email(
                "11111111-1111-1111-1111-111111111111",
                inbox.id,
                "inbox",
                "One",
            ),
            FakeStore::inbox_email(
                "22222222-2222-2222-2222-222222222222",
                sent.id,
                "sent",
                "Two",
            ),
        ])),
        ..Default::default()
    };
    let service = ActiveSyncService::new(store);

    let request = encode_wbxml(&{
        let mut sync = WbxmlNode::new(0, "Sync");
        sync.push(WbxmlNode::with_text(0, "WindowSize", "32"));
        let mut collections = WbxmlNode::new(0, "Collections");
        for mailbox in [&inbox, &sent] {
            let mut collection = WbxmlNode::new(0, "Collection");
            collection.push(WbxmlNode::with_text(0, "SyncKey", "0"));
            collection.push(WbxmlNode::with_text(
                0,
                "CollectionId",
                mailbox.id.to_string(),
            ));
            collection.push(WbxmlNode::with_text(0, "GetChanges", "1"));
            collection.push(WbxmlNode::with_text(0, "DeletesAsMoves", "0"));
            let mut options = WbxmlNode::new(0, "Options");
            let mut body_preference = WbxmlNode::new(17, "BodyPreference");
            body_preference.push(WbxmlNode::with_text(17, "Type", "1"));
            options.push(body_preference);
            collection.push(options);
            collections.push(collection);
        }
        sync.push(collections);
        sync
    });

    let response = service
        .handle_request(
            ActiveSyncQuery {
                cmd: Some("Sync".to_string()),
                user: Some("alice@example.test".to_string()),
                device_id: Some("dev1".to_string()),
                _device_type: Some("phone".to_string()),
            },
            &bearer_headers(),
            &request,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn sync_key_zero_primes_then_returns_paged_more_available_changes() {
    let inbox = FakeStore::inbox_mailbox();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: vec![inbox.clone()],
        emails: Arc::new(Mutex::new(vec![
            FakeStore::inbox_email(
                "11111111-1111-1111-1111-111111111111",
                inbox.id,
                "inbox",
                "One",
            ),
            FakeStore::inbox_email(
                "22222222-2222-2222-2222-222222222222",
                inbox.id,
                "inbox",
                "Two",
            ),
            FakeStore::inbox_email(
                "33333333-3333-3333-3333-333333333333",
                inbox.id,
                "inbox",
                "Three",
            ),
        ])),
        ..Default::default()
    };
    let service = ActiveSyncService::new(store);

    let priming_request = encode_wbxml(&{
        let mut sync = WbxmlNode::new(0, "Sync");
        let mut collections = WbxmlNode::new(0, "Collections");
        let mut collection = WbxmlNode::new(0, "Collection");
        collection.push(WbxmlNode::with_text(0, "SyncKey", "0"));
        collection.push(WbxmlNode::with_text(
            0,
            "CollectionId",
            inbox.id.to_string(),
        ));
        collection.push(WbxmlNode::with_text(0, "WindowSize", "2"));
        collections.push(collection);
        sync.push(collections);
        sync
    });

    let priming_response = service
        .handle_request(
            ActiveSyncQuery {
                cmd: Some("Sync".to_string()),
                user: Some("alice@example.test".to_string()),
                device_id: Some("dev1".to_string()),
                _device_type: Some("phone".to_string()),
            },
            &bearer_headers(),
            &priming_request,
        )
        .await
        .unwrap();
    let priming_sync = decode_response_body(priming_response).await;
    let priming_collection = priming_sync
        .child("Collections")
        .unwrap()
        .child("Collection")
        .unwrap();
    assert!(priming_collection.child("Commands").is_none());
    assert!(priming_collection.child("MoreAvailable").is_none());

    let first_key = collection_sync_key(&priming_sync, &inbox.id.to_string());
    let first_page_request = encode_wbxml(&{
        let mut sync = WbxmlNode::new(0, "Sync");
        let mut collections = WbxmlNode::new(0, "Collections");
        let mut collection = WbxmlNode::new(0, "Collection");
        collection.push(WbxmlNode::with_text(0, "SyncKey", &first_key));
        collection.push(WbxmlNode::with_text(
            0,
            "CollectionId",
            inbox.id.to_string(),
        ));
        collection.push(WbxmlNode::with_text(0, "WindowSize", "2"));
        collections.push(collection);
        sync.push(collections);
        sync
    });

    let first_page_response = service
        .handle_request(
            ActiveSyncQuery {
                cmd: Some("Sync".to_string()),
                user: Some("alice@example.test".to_string()),
                device_id: Some("dev1".to_string()),
                _device_type: Some("phone".to_string()),
            },
            &bearer_headers(),
            &first_page_request,
        )
        .await
        .unwrap();
    let first_page_sync = decode_response_body(first_page_response).await;
    let first_page_collection = first_page_sync
        .child("Collections")
        .unwrap()
        .child("Collection")
        .unwrap();
    let first_commands = first_page_collection.child("Commands").unwrap();
    assert_eq!(first_commands.children.len(), 2);
    assert!(first_page_collection.child("MoreAvailable").is_some());

    let second_key = collection_sync_key(&first_page_sync, &inbox.id.to_string());
    let second_page_request = encode_wbxml(&{
        let mut sync = WbxmlNode::new(0, "Sync");
        let mut collections = WbxmlNode::new(0, "Collections");
        let mut collection = WbxmlNode::new(0, "Collection");
        collection.push(WbxmlNode::with_text(0, "SyncKey", &second_key));
        collection.push(WbxmlNode::with_text(
            0,
            "CollectionId",
            inbox.id.to_string(),
        ));
        collection.push(WbxmlNode::with_text(0, "WindowSize", "2"));
        collections.push(collection);
        sync.push(collections);
        sync
    });

    let second_page_response = service
        .handle_request(
            ActiveSyncQuery {
                cmd: Some("Sync".to_string()),
                user: Some("alice@example.test".to_string()),
                device_id: Some("dev1".to_string()),
                _device_type: Some("phone".to_string()),
            },
            &bearer_headers(),
            &second_page_request,
        )
        .await
        .unwrap();
    let second_page_sync = decode_response_body(second_page_response).await;
    let second_page_collection = second_page_sync
        .child("Collections")
        .unwrap()
        .child("Collection")
        .unwrap();
    let second_commands = second_page_collection.child("Commands").unwrap();
    assert_eq!(second_commands.children.len(), 1);
    assert!(second_page_collection.child("MoreAvailable").is_none());

    let stable_key = collection_sync_key(&second_page_sync, &inbox.id.to_string());
    let stable_request = encode_wbxml(&{
        let mut sync = WbxmlNode::new(0, "Sync");
        let mut collections = WbxmlNode::new(0, "Collections");
        let mut collection = WbxmlNode::new(0, "Collection");
        collection.push(WbxmlNode::with_text(0, "SyncKey", &stable_key));
        collection.push(WbxmlNode::with_text(
            0,
            "CollectionId",
            inbox.id.to_string(),
        ));
        collection.push(WbxmlNode::with_text(0, "WindowSize", "2"));
        collections.push(collection);
        sync.push(collections);
        sync
    });

    let stable_response = service
        .handle_request(
            ActiveSyncQuery {
                cmd: Some("Sync".to_string()),
                user: Some("alice@example.test".to_string()),
                device_id: Some("dev1".to_string()),
                _device_type: Some("phone".to_string()),
            },
            &bearer_headers(),
            &stable_request,
        )
        .await
        .unwrap();
    let stable_sync = decode_response_body(stable_response).await;
    let stable_collection = stable_sync
        .child("Collections")
        .unwrap()
        .child("Collection")
        .unwrap();
    assert!(stable_collection.child("Commands").is_none());
    assert!(stable_collection.child("MoreAvailable").is_none());
}

#[tokio::test]
async fn stable_sync_does_not_reload_full_email_payloads_without_changes() {
    let inbox = FakeStore::inbox_mailbox();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: vec![inbox.clone()],
        emails: Arc::new(Mutex::new(vec![FakeStore::inbox_email(
            "11111111-1111-1111-1111-111111111111",
            inbox.id,
            "inbox",
            "One",
        )])),
        ..Default::default()
    };
    let service = ActiveSyncService::new(store.clone());

    let priming_request = encode_wbxml(&{
        let mut sync = WbxmlNode::new(0, "Sync");
        let mut collections = WbxmlNode::new(0, "Collections");
        let mut collection = WbxmlNode::new(0, "Collection");
        collection.push(WbxmlNode::with_text(0, "SyncKey", "0"));
        collection.push(WbxmlNode::with_text(
            0,
            "CollectionId",
            inbox.id.to_string(),
        ));
        collections.push(collection);
        sync.push(collections);
        sync
    });

    let priming_response = service
        .handle_request(
            ActiveSyncQuery {
                cmd: Some("Sync".to_string()),
                user: Some("alice@example.test".to_string()),
                device_id: Some("dev1".to_string()),
                _device_type: Some("phone".to_string()),
            },
            &bearer_headers(),
            &priming_request,
        )
        .await
        .unwrap();
    let first_key = collection_sync_key(
        &decode_response_body(priming_response).await,
        &inbox.id.to_string(),
    );

    let page_request = encode_wbxml(&{
        let mut sync = WbxmlNode::new(0, "Sync");
        let mut collections = WbxmlNode::new(0, "Collections");
        let mut collection = WbxmlNode::new(0, "Collection");
        collection.push(WbxmlNode::with_text(0, "SyncKey", &first_key));
        collection.push(WbxmlNode::with_text(
            0,
            "CollectionId",
            inbox.id.to_string(),
        ));
        collections.push(collection);
        sync.push(collections);
        sync
    });

    let page_response = service
        .handle_request(
            ActiveSyncQuery {
                cmd: Some("Sync".to_string()),
                user: Some("alice@example.test".to_string()),
                device_id: Some("dev1".to_string()),
                _device_type: Some("phone".to_string()),
            },
            &bearer_headers(),
            &page_request,
        )
        .await
        .unwrap();
    let stable_key = collection_sync_key(
        &decode_response_body(page_response).await,
        &inbox.id.to_string(),
    );
    *store.full_email_fetches.lock().unwrap() = 0;

    let stable_request = encode_wbxml(&{
        let mut sync = WbxmlNode::new(0, "Sync");
        let mut collections = WbxmlNode::new(0, "Collections");
        let mut collection = WbxmlNode::new(0, "Collection");
        collection.push(WbxmlNode::with_text(0, "SyncKey", &stable_key));
        collection.push(WbxmlNode::with_text(
            0,
            "CollectionId",
            inbox.id.to_string(),
        ));
        collections.push(collection);
        sync.push(collections);
        sync
    });

    let stable_response = service
        .handle_request(
            ActiveSyncQuery {
                cmd: Some("Sync".to_string()),
                user: Some("alice@example.test".to_string()),
                device_id: Some("dev1".to_string()),
                _device_type: Some("phone".to_string()),
            },
            &bearer_headers(),
            &stable_request,
        )
        .await
        .unwrap();
    let stable_sync = decode_response_body(stable_response).await;
    let stable_collection = stable_sync
        .child("Collections")
        .unwrap()
        .child("Collection")
        .unwrap();
    assert!(stable_collection.child("Commands").is_none());
    assert_eq!(*store.full_email_fetches.lock().unwrap(), 0);
}

#[tokio::test]
async fn sync_key_stays_usable_for_new_changes_after_a_stable_round() {
    let inbox = FakeStore::inbox_mailbox();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: vec![inbox.clone()],
        emails: Arc::new(Mutex::new(vec![FakeStore::inbox_email(
            "11111111-1111-1111-1111-111111111111",
            inbox.id,
            "inbox",
            "One",
        )])),
        ..Default::default()
    };
    let service = ActiveSyncService::new(store.clone());

    let first_request = encode_wbxml(&{
        let mut sync = WbxmlNode::new(0, "Sync");
        let mut collections = WbxmlNode::new(0, "Collections");
        let mut collection = WbxmlNode::new(0, "Collection");
        collection.push(WbxmlNode::with_text(0, "SyncKey", "0"));
        collection.push(WbxmlNode::with_text(
            0,
            "CollectionId",
            inbox.id.to_string(),
        ));
        collections.push(collection);
        sync.push(collections);
        sync
    });
    let first_response = service
        .handle_request(
            ActiveSyncQuery {
                cmd: Some("Sync".to_string()),
                user: Some("alice@example.test".to_string()),
                device_id: Some("dev1".to_string()),
                _device_type: Some("phone".to_string()),
            },
            &bearer_headers(),
            &first_request,
        )
        .await
        .unwrap();
    let primed_key = collection_sync_key(
        &decode_response_body(first_response).await,
        &inbox.id.to_string(),
    );

    let second_request = encode_wbxml(&{
        let mut sync = WbxmlNode::new(0, "Sync");
        let mut collections = WbxmlNode::new(0, "Collections");
        let mut collection = WbxmlNode::new(0, "Collection");
        collection.push(WbxmlNode::with_text(0, "SyncKey", &primed_key));
        collection.push(WbxmlNode::with_text(
            0,
            "CollectionId",
            inbox.id.to_string(),
        ));
        collections.push(collection);
        sync.push(collections);
        sync
    });
    let second_response = service
        .handle_request(
            ActiveSyncQuery {
                cmd: Some("Sync".to_string()),
                user: Some("alice@example.test".to_string()),
                device_id: Some("dev1".to_string()),
                _device_type: Some("phone".to_string()),
            },
            &bearer_headers(),
            &second_request,
        )
        .await
        .unwrap();
    let stable_key = collection_sync_key(
        &decode_response_body(second_response).await,
        &inbox.id.to_string(),
    );

    store.emails.lock().unwrap().push(FakeStore::inbox_email(
        "22222222-2222-2222-2222-222222222222",
        inbox.id,
        "inbox",
        "Two",
    ));

    let delta_request = encode_wbxml(&{
        let mut sync = WbxmlNode::new(0, "Sync");
        let mut collections = WbxmlNode::new(0, "Collections");
        let mut collection = WbxmlNode::new(0, "Collection");
        collection.push(WbxmlNode::with_text(0, "SyncKey", &stable_key));
        collection.push(WbxmlNode::with_text(
            0,
            "CollectionId",
            inbox.id.to_string(),
        ));
        collection.push(WbxmlNode::with_text(0, "WindowSize", "1"));
        collections.push(collection);
        sync.push(collections);
        sync
    });

    let delta_response = service
        .handle_request(
            ActiveSyncQuery {
                cmd: Some("Sync".to_string()),
                user: Some("alice@example.test".to_string()),
                device_id: Some("dev1".to_string()),
                _device_type: Some("phone".to_string()),
            },
            &bearer_headers(),
            &delta_request,
        )
        .await
        .unwrap();
    let delta_sync = decode_response_body(delta_response).await;
    let delta_collection = delta_sync
        .child("Collections")
        .unwrap()
        .child("Collection")
        .unwrap();
    assert_eq!(
        delta_collection.child("Commands").unwrap().children.len(),
        1
    );
    assert!(delta_collection.child("MoreAvailable").is_none());
}

#[tokio::test]
async fn send_mail_uses_canonical_submission_model() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ActiveSyncService::new(store.clone());

    service
        .handle_request(
            ActiveSyncQuery {
                cmd: Some("SendMail".to_string()),
                user: Some("alice@example.test".to_string()),
                device_id: Some("dev1".to_string()),
                _device_type: Some("phone".to_string()),
            },
            &mime_headers(),
            b"To: Bob <bob@example.test>\r\nSubject: Hello\r\n\r\nBody",
        )
        .await
        .unwrap();

    let submitted = store.submitted_messages.lock().unwrap();
    assert_eq!(submitted.len(), 1);
    assert_eq!(submitted[0].source, "activesync-sendmail");
    assert_eq!(submitted[0].subject, "Hello");
    assert_eq!(submitted[0].to[0].address, "bob@example.test");
}

#[tokio::test]
async fn send_mail_uses_on_behalf_sender_for_delegated_mailbox() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        accessible_mailbox_accounts: vec![
            FakeStore::mailbox_access(),
            FakeStore::shared_mailbox_access(false, true),
        ],
        ..Default::default()
    };
    let service = ActiveSyncService::new(store.clone());

    service
        .handle_request(
            ActiveSyncQuery {
                cmd: Some("SendMail".to_string()),
                user: Some("alice@example.test".to_string()),
                device_id: Some("dev1".to_string()),
                _device_type: Some("phone".to_string()),
            },
            &mime_headers(),
            b"From: Shared Mailbox <shared@example.test>\r\nTo: Bob <bob@example.test>\r\nSubject: Delegated\r\n\r\nBody",
        )
        .await
        .unwrap();

    let submitted = store.submitted_messages.lock().unwrap();
    assert_eq!(
        submitted[0].account_id,
        FakeStore::shared_mailbox_access(false, true).account_id
    );
    assert_eq!(
        submitted[0].submitted_by_account_id,
        FakeStore::account().account_id
    );
    assert_eq!(submitted[0].from_address, "shared@example.test");
    assert_eq!(
        submitted[0].sender_address.as_deref(),
        Some("alice@example.test")
    );
}

#[tokio::test]
async fn send_mail_rejects_inaccessible_shared_mailbox_address() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        accessible_mailbox_accounts: vec![FakeStore::mailbox_access()],
        ..Default::default()
    };
    let service = ActiveSyncService::new(store);

    let result = service
        .handle_request(
            ActiveSyncQuery {
                cmd: Some("SendMail".to_string()),
                user: Some("alice@example.test".to_string()),
                device_id: Some("dev1".to_string()),
                _device_type: Some("phone".to_string()),
            },
            &mime_headers(),
            b"From: Shared Mailbox <shared@example.test>\r\nTo: Bob <bob@example.test>\r\nSubject: Nope\r\n\r\nBody",
        )
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn send_mail_decodes_multipart_and_encoded_headers() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ActiveSyncService::new(store.clone());

    let message = concat!(
        "From: =?UTF-8?Q?Alice_Doe?= <alice@example.test>\r\n",
        "To: \"Bob, Example\" <bob@example.test>\r\n",
        "Subject: =?UTF-8?Q?Bonjour_=C3=A9quipe?=\r\n",
        "Content-Type: multipart/alternative; boundary=\"b1\"\r\n",
        "\r\n",
        "--b1\r\n",
        "Content-Type: text/plain; charset=utf-8\r\n",
        "Content-Transfer-Encoding: quoted-printable\r\n",
        "\r\n",
        "Ligne=20un=0ALigne=20deux\r\n",
        "--b1--\r\n"
    );

    service
        .handle_request(
            ActiveSyncQuery {
                cmd: Some("SendMail".to_string()),
                user: Some("alice@example.test".to_string()),
                device_id: Some("dev1".to_string()),
                _device_type: Some("phone".to_string()),
            },
            &mime_headers(),
            message.as_bytes(),
        )
        .await
        .unwrap();

    let submitted = store.submitted_messages.lock().unwrap();
    assert_eq!(submitted.len(), 1);
    assert_eq!(submitted[0].subject, "Bonjour équipe");
    assert_eq!(submitted[0].body_text, "Ligne un\nLigne deux");
    assert_eq!(
        submitted[0].to[0].display_name.as_deref(),
        Some("Bob, Example")
    );
    assert_eq!(submitted[0].from_address, "alice@example.test");
}

#[tokio::test]
async fn basic_authentication_is_accepted() {
    let store = FakeStore {
        login: Some(FakeStore::login()),
        mailboxes: vec![FakeStore::inbox_mailbox()],
        ..Default::default()
    };
    let service = ActiveSyncService::new(store);
    let request = encode_wbxml(&{
        let mut node = WbxmlNode::new(7, "FolderSync");
        node.push(WbxmlNode::with_text(7, "SyncKey", "0"));
        node
    });

    let response = service
        .handle_request(
            ActiveSyncQuery {
                cmd: Some("FolderSync".to_string()),
                user: Some("alice@example.test".to_string()),
                device_id: Some("dev1".to_string()),
                _device_type: Some("phone".to_string()),
            },
            &basic_headers(),
            &request,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn mime_parser_extracts_attachments_for_sendmail_submission() {
    let message = concat!(
        "From: Alice <alice@example.test>\r\n",
        "To: Bob <bob@example.test>\r\n",
        "Subject: Attachment test\r\n",
        "Content-Type: multipart/mixed; boundary=\"mix\"\r\n",
        "\r\n",
        "--mix\r\n",
        "Content-Type: text/plain; charset=utf-8\r\n",
        "\r\n",
        "Hello\r\n",
        "--mix\r\n",
        "Content-Type: text/plain\r\n",
        "Content-Disposition: attachment; filename=\"note.txt\"\r\n",
        "\r\n",
        "Attachment body\r\n",
        "--mix--\r\n"
    );

    let parsed = crate::message::parse_mime_message(message.as_bytes()).unwrap();
    assert_eq!(parsed.attachments.len(), 1);
    assert_eq!(parsed.attachments[0].file_name, "note.txt");
}

#[tokio::test]
async fn item_operations_fetch_returns_attachment_bytes() {
    let inbox = FakeStore::inbox_mailbox();
    let message_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
    let attachment_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
    let file_reference = format!("attachment:{message_id}:{attachment_id}");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: vec![inbox.clone()],
        emails: Arc::new(Mutex::new(vec![FakeStore::inbox_email(
            "11111111-1111-1111-1111-111111111111",
            inbox.id,
            "inbox",
            "One",
        )])),
        attachments: Arc::new(Mutex::new(std::collections::HashMap::from([(
            message_id,
            vec![ActiveSyncAttachment {
                id: attachment_id,
                message_id,
                file_name: "note.txt".to_string(),
                media_type: "text/plain".to_string(),
                size_octets: 15,
                file_reference: file_reference.clone(),
            }],
        )]))),
        attachment_contents: Arc::new(Mutex::new(std::collections::HashMap::from([(
            file_reference.clone(),
            ActiveSyncAttachmentContent {
                file_reference: file_reference.clone(),
                file_name: "note.txt".to_string(),
                media_type: "text/plain".to_string(),
                blob_bytes: b"attachment body".to_vec(),
            },
        )]))),
        ..Default::default()
    };
    let service = ActiveSyncService::new(store);
    let request = encode_wbxml(&{
        let mut root = WbxmlNode::new(20, "ItemOperations");
        let mut fetch = WbxmlNode::new(20, "Fetch");
        fetch.push(WbxmlNode::with_text(17, "FileReference", &file_reference));
        root.push(fetch);
        root
    });

    let response = service
        .handle_request(
            ActiveSyncQuery {
                cmd: Some("ItemOperations".to_string()),
                user: Some("alice@example.test".to_string()),
                device_id: Some("dev1".to_string()),
                _device_type: Some("phone".to_string()),
            },
            &bearer_headers(),
            &request,
        )
        .await
        .unwrap();
    let body = decode_response_body(response).await;
    let data = body
        .child("Response")
        .unwrap()
        .child("Fetch")
        .unwrap()
        .child("Properties")
        .unwrap()
        .child("Data")
        .unwrap()
        .opaque
        .clone()
        .unwrap();

    assert_eq!(data, b"attachment body".to_vec());
}

#[tokio::test]
async fn search_queries_canonical_mail_projection() {
    let inbox = FakeStore::inbox_mailbox();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: vec![inbox.clone()],
        emails: Arc::new(Mutex::new(vec![
            FakeStore::inbox_email(
                "11111111-1111-1111-1111-111111111111",
                inbox.id,
                "inbox",
                "Quarterly budget",
            ),
            FakeStore::inbox_email(
                "22222222-2222-2222-2222-222222222222",
                inbox.id,
                "inbox",
                "Travel",
            ),
        ])),
        ..Default::default()
    };
    let service = ActiveSyncService::new(store);
    let request = encode_wbxml(&{
        let mut root = WbxmlNode::new(15, "Search");
        let mut store = WbxmlNode::new(15, "Store");
        store.push(WbxmlNode::with_text(15, "Name", "Mailbox"));
        let mut query = WbxmlNode::new(15, "Query");
        query.push(WbxmlNode::with_text(15, "FreeText", "budget"));
        store.push(query);
        root.push(store);
        root
    });

    let response = service
        .handle_request(
            ActiveSyncQuery {
                cmd: Some("Search".to_string()),
                user: Some("alice@example.test".to_string()),
                device_id: Some("dev1".to_string()),
                _device_type: Some("phone".to_string()),
            },
            &bearer_headers(),
            &request,
        )
        .await
        .unwrap();
    let body = decode_response_body(response).await;
    let result = body
        .child("Response")
        .unwrap()
        .child("Store")
        .unwrap()
        .child("Result")
        .unwrap();
    assert_eq!(
        result
            .child("Properties")
            .unwrap()
            .child("ApplicationData")
            .unwrap()
            .child("Subject")
            .unwrap()
            .text_value(),
        "Quarterly budget"
    );
}

#[tokio::test]
async fn ping_reports_changed_collections_after_sync_state_exists() {
    let inbox = FakeStore::inbox_mailbox();
    let emails = Arc::new(Mutex::new(vec![FakeStore::inbox_email(
        "11111111-1111-1111-1111-111111111111",
        inbox.id,
        "inbox",
        "One",
    )]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: vec![inbox.clone()],
        emails: emails.clone(),
        ..Default::default()
    };
    let service = ActiveSyncService::new(store);

    let sync_request = encode_wbxml(&{
        let mut sync = WbxmlNode::new(0, "Sync");
        let mut collections = WbxmlNode::new(0, "Collections");
        let mut collection = WbxmlNode::new(0, "Collection");
        collection.push(WbxmlNode::with_text(0, "SyncKey", "0"));
        collection.push(WbxmlNode::with_text(
            0,
            "CollectionId",
            inbox.id.to_string(),
        ));
        collections.push(collection);
        sync.push(collections);
        sync
    });
    service
        .handle_request(
            ActiveSyncQuery {
                cmd: Some("Sync".to_string()),
                user: Some("alice@example.test".to_string()),
                device_id: Some("dev1".to_string()),
                _device_type: Some("phone".to_string()),
            },
            &bearer_headers(),
            &sync_request,
        )
        .await
        .unwrap();

    emails.lock().unwrap().push(FakeStore::inbox_email(
        "22222222-2222-2222-2222-222222222222",
        inbox.id,
        "inbox",
        "Two",
    ));

    let ping_request = encode_wbxml(&{
        let mut ping = WbxmlNode::new(13, "Ping");
        ping.push(WbxmlNode::with_text(13, "HeartbeatInterval", "60"));
        let mut folders = WbxmlNode::new(13, "Folders");
        let mut folder = WbxmlNode::new(13, "Folder");
        folder.push(WbxmlNode::with_text(13, "Id", inbox.id.to_string()));
        folder.push(WbxmlNode::with_text(13, "Class", "Email"));
        folders.push(folder);
        ping.push(folders);
        ping
    });
    let response = service
        .handle_request(
            ActiveSyncQuery {
                cmd: Some("Ping".to_string()),
                user: Some("alice@example.test".to_string()),
                device_id: Some("dev1".to_string()),
                _device_type: Some("phone".to_string()),
            },
            &bearer_headers(),
            &ping_request,
        )
        .await
        .unwrap();
    let body = decode_response_body(response).await;
    assert_eq!(body.child("Status").unwrap().text_value(), "2");
}

#[tokio::test]
async fn smart_forward_reuses_source_message_and_attachments() {
    let inbox = FakeStore::inbox_mailbox();
    let source_message_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
    let attachment_id = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
    let file_reference = format!("attachment:{source_message_id}:{attachment_id}");
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: vec![inbox.clone()],
        emails: Arc::new(Mutex::new(vec![FakeStore::inbox_email(
            "11111111-1111-1111-1111-111111111111",
            inbox.id,
            "inbox",
            "Source subject",
        )])),
        attachments: Arc::new(Mutex::new(std::collections::HashMap::from([(
            source_message_id,
            vec![ActiveSyncAttachment {
                id: attachment_id,
                message_id: source_message_id,
                file_name: "report.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                size_octets: 7,
                file_reference: file_reference.clone(),
            }],
        )]))),
        attachment_contents: Arc::new(Mutex::new(std::collections::HashMap::from([(
            file_reference.clone(),
            ActiveSyncAttachmentContent {
                file_reference,
                file_name: "report.pdf".to_string(),
                media_type: "application/pdf".to_string(),
                blob_bytes: b"pdfdata".to_vec(),
            },
        )]))),
        ..Default::default()
    };
    let service = ActiveSyncService::new(store.clone());
    let request = encode_wbxml(&{
        let mut root = WbxmlNode::new(21, "SmartForward");
        let mut source = WbxmlNode::new(21, "Source");
        source.push(WbxmlNode::with_text(
            21,
            "ItemId",
            source_message_id.to_string(),
        ));
        root.push(source);
        root.push(WbxmlNode::with_text(
            21,
            "Mime",
            concat!(
                "From: Alice <alice@example.test>\r\n",
                "To: target@example.test\r\n",
                "Subject: \r\n",
                "\r\n",
                "Please see below."
            ),
        ));
        root
    });

    service
        .handle_request(
            ActiveSyncQuery {
                cmd: Some("SmartForward".to_string()),
                user: Some("alice@example.test".to_string()),
                device_id: Some("dev1".to_string()),
                _device_type: Some("phone".to_string()),
            },
            &bearer_headers(),
            &request,
        )
        .await
        .unwrap();

    let submitted = store.submitted_messages.lock().unwrap();
    assert_eq!(submitted[0].attachments.len(), 1);
    assert!(submitted[0].body_text.contains("Forwarded message"));
    assert_eq!(submitted[0].subject, "Fwd: Source subject");
}

#[tokio::test]
async fn sync_contact_and_calendar_mutations_update_canonical_models() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contacts: Arc::new(Mutex::new(Vec::new())),
        events: Arc::new(Mutex::new(Vec::new())),
        ..Default::default()
    };
    let service = ActiveSyncService::new(store.clone());

    let contact_request = encode_wbxml(&{
        let mut sync = WbxmlNode::new(0, "Sync");
        let mut collections = WbxmlNode::new(0, "Collections");
        let mut collection = WbxmlNode::new(0, "Collection");
        collection.push(WbxmlNode::with_text(0, "SyncKey", "0"));
        collection.push(WbxmlNode::with_text(0, "CollectionId", "contacts"));
        let mut commands = WbxmlNode::new(0, "Commands");
        let mut add = WbxmlNode::new(0, "Add");
        add.push(WbxmlNode::with_text(0, "ClientId", "c1"));
        let mut app = WbxmlNode::new(0, "ApplicationData");
        app.push(WbxmlNode::with_text(1, "FileAs", "Bob Example"));
        app.push(WbxmlNode::with_text(1, "Email1Address", "bob@example.test"));
        commands.push({
            add.push(app);
            add
        });
        collection.push(commands);
        collections.push(collection);
        sync.push(collections);
        sync
    });
    service
        .handle_request(
            ActiveSyncQuery {
                cmd: Some("Sync".to_string()),
                user: Some("alice@example.test".to_string()),
                device_id: Some("dev1".to_string()),
                _device_type: Some("phone".to_string()),
            },
            &bearer_headers(),
            &contact_request,
        )
        .await
        .unwrap();

    let event_request = encode_wbxml(&{
        let mut sync = WbxmlNode::new(0, "Sync");
        let mut collections = WbxmlNode::new(0, "Collections");
        let mut collection = WbxmlNode::new(0, "Collection");
        collection.push(WbxmlNode::with_text(0, "SyncKey", "0"));
        collection.push(WbxmlNode::with_text(0, "CollectionId", "calendar"));
        let mut commands = WbxmlNode::new(0, "Commands");
        let mut add = WbxmlNode::new(0, "Add");
        add.push(WbxmlNode::with_text(0, "ClientId", "e1"));
        let mut app = WbxmlNode::new(0, "ApplicationData");
        app.push(WbxmlNode::with_text(4, "Subject", "Standup"));
        app.push(WbxmlNode::with_text(4, "StartTime", "20260419T090000Z"));
        app.push(WbxmlNode::with_text(4, "EndTime", "20260419T093000Z"));
        app.push(WbxmlNode::with_text(4, "Location", "Room 1"));
        commands.push({
            add.push(app);
            add
        });
        collection.push(commands);
        collections.push(collection);
        sync.push(collections);
        sync
    });
    service
        .handle_request(
            ActiveSyncQuery {
                cmd: Some("Sync".to_string()),
                user: Some("alice@example.test".to_string()),
                device_id: Some("dev1".to_string()),
                _device_type: Some("phone".to_string()),
            },
            &bearer_headers(),
            &event_request,
        )
        .await
        .unwrap();

    assert_eq!(store.contacts.lock().unwrap()[0].email, "bob@example.test");
    assert_eq!(store.events.lock().unwrap()[0].duration_minutes, 30);
}
