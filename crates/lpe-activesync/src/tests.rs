use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Instant,
};

use anyhow::anyhow;
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
    JmapEmail, JmapEmailAddress, JmapEmailMailboxState, JmapEmailQuery, JmapMailbox,
    JmapUploadBlob, MailboxAccountAccess, SavedDraftMessage, StoredAccountAppPassword,
    SubmitMessageInput, SubmittedMessage, UpsertClientContactInput, UpsertClientEventInput,
};
use uuid::Uuid;

use crate::{
    app::options_response_for_store,
    response::error_response,
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
    mailboxes_by_account: HashMap<Uuid, Vec<JmapMailbox>>,
    accessible_mailbox_accounts: Vec<MailboxAccountAccess>,
    emails: Arc<Mutex<Vec<JmapEmail>>>,
    contacts: Arc<Mutex<Vec<ClientContact>>>,
    events: Arc<Mutex<Vec<ClientEvent>>>,
    attachments: Arc<Mutex<std::collections::HashMap<Uuid, Vec<ActiveSyncAttachment>>>>,
    attachment_contents: Arc<Mutex<std::collections::HashMap<String, ActiveSyncAttachmentContent>>>,
    raw_message_blobs: Arc<Mutex<std::collections::HashMap<Uuid, Vec<u8>>>>,
    saved_drafts: Arc<Mutex<Vec<SubmitMessageInput>>>,
    submitted_messages: Arc<Mutex<Vec<SubmitMessageInput>>>,
    deleted_drafts: Arc<Mutex<Vec<Uuid>>>,
    sync_states: Arc<Mutex<std::collections::HashMap<String, ActiveSyncSyncState>>>,
    sync_state_order: Arc<Mutex<Vec<String>>>,
    expired_sync_states: Arc<Mutex<HashSet<String>>>,
    full_email_fetches: Arc<Mutex<u32>>,
}

impl FakeStore {
    fn tenant_id() -> Uuid {
        Uuid::parse_str("11111111-aaaa-aaaa-aaaa-111111111111").unwrap()
    }

    fn account() -> AuthenticatedAccount {
        AuthenticatedAccount {
            tenant_id: Self::tenant_id(),
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
            tenant_id: Self::tenant_id(),
            account_id: Self::account().account_id,
            email: Self::account().email,
            display_name: Self::account().display_name,
            password_hash: Self::password_hash(),
            status: "active".to_string(),
        }
    }

    fn draft_mailbox() -> JmapMailbox {
        Self::mailbox(
            "dddddddd-dddd-dddd-dddd-dddddddddddd",
            "drafts",
            "Drafts",
            10,
            None,
        )
    }

    fn inbox_mailbox() -> JmapMailbox {
        Self::mailbox(
            "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            "inbox",
            "Inbox",
            1,
            None,
        )
    }

    fn sent_mailbox() -> JmapMailbox {
        Self::mailbox(
            "cccccccc-cccc-cccc-cccc-cccccccccccc",
            "sent",
            "Sent",
            2,
            None,
        )
    }

    fn mailbox(
        id: &str,
        role: &str,
        name: &str,
        sort_order: i32,
        parent_id: Option<Uuid>,
    ) -> JmapMailbox {
        JmapMailbox {
            id: Uuid::parse_str(id).unwrap(),
            parent_id,
            role: role.to_string(),
            name: name.to_string(),
            sort_order,
            modseq: 1,
            total_emails: 0,
            unread_emails: 0,
            is_subscribed: true,
        }
    }

    fn inbox_email(id: &str, mailbox_id: Uuid, role: &str, subject: &str) -> JmapEmail {
        JmapEmail {
            id: Uuid::parse_str(id).unwrap(),
            thread_id: Uuid::new_v4(),
            mailbox_ids: vec![mailbox_id],
            mailbox_states: vec![JmapEmailMailboxState {
                mailbox_id,
                role: role.to_string(),
                name: role.to_string(),
                modseq: 1,
                unread: true,
                flagged: false,
                draft: role == "drafts",
            }],
            mailbox_id,
            mailbox_role: role.to_string(),
            mailbox_name: role.to_string(),
            modseq: 1,
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
            tenant_id: account.tenant_id,
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
            tenant_id: Self::tenant_id(),
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
        _tenant_id: &'a Uuid,
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

    fn fetch_jmap_mailboxes<'a>(&'a self, account_id: Uuid) -> StoreFuture<'a, Vec<JmapMailbox>> {
        let mailboxes = self
            .mailboxes_by_account
            .get(&account_id)
            .cloned()
            .unwrap_or_else(|| self.mailboxes.clone());
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
        let states = self.sync_states.lock().unwrap();
        let expired = self.expired_sync_states.lock().unwrap();
        let state = self
            .sync_state_order
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find(|key| key.starts_with(&prefix))
            .filter(|key| !expired.contains(*key))
            .and_then(|key| states.get(key))
            .cloned();
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

    fn fetch_jmap_message_blob<'a>(
        &'a self,
        account_id: Uuid,
        message_id: Uuid,
    ) -> StoreFuture<'a, Option<JmapUploadBlob>> {
        let blob_bytes = self
            .raw_message_blobs
            .lock()
            .unwrap()
            .get(&message_id)
            .cloned();
        Box::pin(async move {
            Ok(blob_bytes.map(|blob_bytes| JmapUploadBlob {
                id: message_id,
                account_id,
                media_type: "message/rfc822".to_string(),
                octet_size: blob_bytes.len() as u64,
                blob_bytes,
            }))
        })
    }

    fn move_jmap_email_from_mailbox<'a>(
        &'a self,
        _account_id: Uuid,
        source_mailbox_id: Uuid,
        message_id: Uuid,
        target_mailbox_id: Uuid,
        _audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail> {
        let mut emails = self.emails.lock().unwrap();
        let email = emails
            .iter_mut()
            .find(|email| email.id == message_id && email.mailbox_id == source_mailbox_id)
            .map(|email| {
                email.mailbox_id = target_mailbox_id;
                email.mailbox_ids = vec![target_mailbox_id];
                for state in &mut email.mailbox_states {
                    state.mailbox_id = target_mailbox_id;
                }
                email.clone()
            });
        Box::pin(async move { email.ok_or_else(|| anyhow!("message not found")) })
    }

    fn delete_jmap_email_from_mailbox<'a>(
        &'a self,
        _account_id: Uuid,
        mailbox_id: Uuid,
        message_id: Uuid,
        _audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        self.emails
            .lock()
            .unwrap()
            .retain(|email| !(email.id == message_id && email.mailbox_id == mailbox_id));
        Box::pin(async move { Ok(()) })
    }

    fn update_jmap_email_flags<'a>(
        &'a self,
        _account_id: Uuid,
        message_id: Uuid,
        unread: Option<bool>,
        flagged: Option<bool>,
        _audit: AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail> {
        let mut emails = self.emails.lock().unwrap();
        let email = emails
            .iter_mut()
            .find(|email| email.id == message_id)
            .map(|email| {
                if let Some(unread) = unread {
                    email.unread = unread;
                    for state in &mut email.mailbox_states {
                        state.unread = unread;
                    }
                }
                if let Some(flagged) = flagged {
                    email.flagged = flagged;
                    for state in &mut email.mailbox_states {
                        state.flagged = flagged;
                    }
                }
                email.clone()
            });
        Box::pin(async move { email.ok_or_else(|| anyhow!("message not found")) })
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
        message_id: Uuid,
        _audit: AuditEntryInput,
    ) -> StoreFuture<'a, ()> {
        self.deleted_drafts.lock().unwrap().push(message_id);
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
        let event_id = input.id.unwrap_or_else(Uuid::new_v4);
        let event = ClientEvent {
            id: event_id,
            uid: if input.uid.trim().is_empty() {
                event_id.to_string()
            } else {
                input.uid
            },
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
        let mut states = self.sync_states.lock().unwrap();
        let mut order = self.sync_state_order.lock().unwrap();
        self.expired_sync_states.lock().unwrap().remove(&key);
        if !states.contains_key(&key) {
            order.push(key.clone());
        }
        states.insert(
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
        let state = if self.expired_sync_states.lock().unwrap().contains(&key) {
            None
        } else {
            self.sync_states.lock().unwrap().get(&key).cloned()
        };
        Box::pin(async move { Ok(state) })
    }

    fn cleanup_expired_activesync_sync_cursors<'a>(
        &'a self,
        account_id: Uuid,
        device_id: &'a str,
    ) -> StoreFuture<'a, ()> {
        let prefix = format!("{account_id}:{device_id}:");
        let mut states = self.sync_states.lock().unwrap();
        let mut order = self.sync_state_order.lock().unwrap();
        let expired = self.expired_sync_states.lock().unwrap();
        states.retain(|key, _| !key.starts_with(&prefix) || !expired.contains(key));
        order.retain(|key| !key.starts_with(&prefix) || !expired.contains(key));
        Box::pin(async move { Ok(()) })
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

#[tokio::test]
async fn options_challenges_anonymous_requests() {
    let store = FakeStore::default();
    let response = options_response_for_store(
        &store,
        &ActiveSyncQuery {
            cmd: None,
            user: Some("alice@example.test".to_string()),
            device_id: Some("dev1".to_string()),
            _device_type: Some("phone".to_string()),
        },
        &HeaderMap::new(),
    )
    .await;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response
            .headers()
            .get(axum::http::header::WWW_AUTHENTICATE)
            .and_then(|value| value.to_str().ok()),
        Some("Basic realm=\"LPE ActiveSync\"")
    );
    assert_eq!(
        response
            .headers()
            .get("ms-server-activesync")
            .and_then(|value| value.to_str().ok()),
        Some("16.1")
    );
    assert_eq!(
        response
            .headers()
            .get("ms-asprotocolversions")
            .and_then(|value| value.to_str().ok()),
        Some("16.1")
    );
    assert_eq!(
        response
            .headers()
            .get("ms-asprotocolcommands")
            .and_then(|value| value.to_str().ok()),
        Some(
            "FolderSync,GetItemEstimate,ItemOperations,MoveItems,Ping,Provision,Search,SendMail,SmartForward,SmartReply,Sync"
        )
    );
}

#[tokio::test]
async fn options_returns_capabilities_after_authentication() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let response = options_response_for_store(
        &store,
        &ActiveSyncQuery {
            cmd: None,
            user: Some("alice@example.test".to_string()),
            device_id: Some("dev1".to_string()),
            _device_type: Some("phone".to_string()),
        },
        &bearer_headers(),
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("ms-server-activesync")
            .and_then(|value| value.to_str().ok()),
        Some("16.1")
    );
    assert_eq!(
        response
            .headers()
            .get("ms-asprotocolversions")
            .and_then(|value| value.to_str().ok()),
        Some("16.1")
    );
    assert_eq!(
        response
            .headers()
            .get("ms-asprotocolcommands")
            .and_then(|value| value.to_str().ok()),
        Some(
            "FolderSync,GetItemEstimate,ItemOperations,MoveItems,Ping,Provision,Search,SendMail,SmartForward,SmartReply,Sync"
        )
    );
}

#[test]
fn post_authentication_errors_return_http_challenge() {
    let response = error_response(anyhow!("missing account authentication"));

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response
            .headers()
            .get(axum::http::header::WWW_AUTHENTICATE)
            .and_then(|value| value.to_str().ok()),
        Some("Basic realm=\"LPE ActiveSync\"")
    );
}

#[tokio::test]
async fn provision_returns_policy_key_and_lightweight_policy_document() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ActiveSyncService::new(store);
    let request = encode_wbxml(&{
        let mut root = WbxmlNode::new(14, "Provision");
        let mut device_information = WbxmlNode::new(18, "DeviceInformation");
        device_information.push(WbxmlNode::with_text(18, "Set", "1"));
        root.push(device_information);
        let mut policies = WbxmlNode::new(14, "Policies");
        let mut policy = WbxmlNode::new(14, "Policy");
        policy.push(WbxmlNode::with_text(
            14,
            "PolicyType",
            "MS-EAS-Provisioning-WBXML",
        ));
        policies.push(policy);
        root.push(policies);
        root
    });

    let response = service
        .handle_request(
            ActiveSyncQuery {
                cmd: Some("Provision".to_string()),
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
    let policy = body.child("Policies").unwrap().child("Policy").unwrap();

    assert_eq!(body.child("Status").unwrap().text_value(), "1");
    assert_eq!(
        body.child("DeviceInformation")
            .unwrap()
            .child("Status")
            .unwrap()
            .text_value(),
        "1"
    );
    assert_eq!(
        policy.child("PolicyType").unwrap().text_value(),
        "MS-EAS-Provisioning-WBXML"
    );
    assert_eq!(policy.child("Status").unwrap().text_value(), "1");
    assert!(!policy.child("PolicyKey").unwrap().text_value().is_empty());
    assert_eq!(
        policy
            .child("Data")
            .unwrap()
            .child("EASProvisionDoc")
            .unwrap()
            .child("AttachmentsEnabled")
            .unwrap()
            .text_value(),
        "1"
    );
    assert_eq!(
        policy
            .child("Data")
            .unwrap()
            .child("EASProvisionDoc")
            .unwrap()
            .child("DevicePasswordEnabled")
            .unwrap()
            .text_value(),
        "0"
    );
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

fn active_sync_query(cmd: &str, device_id: &str) -> ActiveSyncQuery {
    ActiveSyncQuery {
        cmd: Some(cmd.to_string()),
        user: Some("alice@example.test".to_string()),
        device_id: Some(device_id.to_string()),
        _device_type: Some("phone".to_string()),
    }
}

async fn sync_collection(
    service: &ActiveSyncService<FakeStore>,
    collection_id: &str,
    sync_key: &str,
    device_id: &str,
) -> WbxmlNode {
    let request = encode_wbxml(&{
        let mut sync = WbxmlNode::new(0, "Sync");
        let mut collections = WbxmlNode::new(0, "Collections");
        let mut collection = WbxmlNode::new(0, "Collection");
        collection.push(WbxmlNode::with_text(0, "SyncKey", sync_key));
        collection.push(WbxmlNode::with_text(0, "CollectionId", collection_id));
        collections.push(collection);
        sync.push(collections);
        sync
    });
    let response = service
        .handle_request(
            active_sync_query("Sync", device_id),
            &bearer_headers(),
            &request,
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    decode_response_body(response).await
}

fn only_sync_collection<'a>(sync: &'a WbxmlNode, collection_id: &str) -> &'a WbxmlNode {
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
        .unwrap()
}

async fn folder_sync(
    service: &ActiveSyncService<FakeStore>,
    sync_key: &str,
    device_id: &str,
) -> WbxmlNode {
    let request = encode_wbxml(&{
        let mut node = WbxmlNode::new(7, "FolderSync");
        node.push(WbxmlNode::with_text(7, "SyncKey", sync_key));
        node
    });
    let response = service
        .handle_request(
            ActiveSyncQuery {
                cmd: Some("FolderSync".to_string()),
                user: Some("alice@example.test".to_string()),
                device_id: Some(device_id.to_string()),
                _device_type: Some("phone".to_string()),
            },
            &bearer_headers(),
            &request,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    decode_response_body(response).await
}

fn ping_request(heartbeat: Option<&str>, folders: &[(&str, &str)]) -> Vec<u8> {
    encode_wbxml(&{
        let mut ping = WbxmlNode::new(13, "Ping");
        if let Some(heartbeat) = heartbeat {
            ping.push(WbxmlNode::with_text(13, "HeartbeatInterval", heartbeat));
        }
        if !folders.is_empty() {
            let mut folders_node = WbxmlNode::new(13, "Folders");
            for (id, class_name) in folders {
                let mut folder = WbxmlNode::new(13, "Folder");
                folder.push(WbxmlNode::with_text(13, "Id", *id));
                folder.push(WbxmlNode::with_text(13, "Class", *class_name));
                folders_node.push(folder);
            }
            ping.push(folders_node);
        }
        ping
    })
}

async fn ping(service: &ActiveSyncService<FakeStore>, device_id: &str, body: &[u8]) -> WbxmlNode {
    let response = service
        .handle_request(
            active_sync_query("Ping", device_id),
            &bearer_headers(),
            body,
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    decode_response_body(response).await
}

fn folder_add<'a>(changes: &'a WbxmlNode, server_id: &str) -> &'a WbxmlNode {
    changes
        .children_named("Add")
        .into_iter()
        .find(|change| change.child("ServerId").unwrap().text_value() == server_id)
        .unwrap()
}

async fn handle_sync_node(service: &ActiveSyncService<FakeStore>, node: WbxmlNode) -> WbxmlNode {
    let response = service
        .handle_request(
            ActiveSyncQuery {
                cmd: Some("Sync".to_string()),
                user: Some("alice@example.test".to_string()),
                device_id: Some("dev1".to_string()),
                _device_type: Some("phone".to_string()),
            },
            &bearer_headers(),
            &encode_wbxml(&node),
        )
        .await
        .unwrap();
    decode_response_body(response).await
}

fn one_collection_sync(collection_id: &str, sync_key: &str) -> WbxmlNode {
    let mut sync = WbxmlNode::new(0, "Sync");
    let mut collections = WbxmlNode::new(0, "Collections");
    let mut collection = WbxmlNode::new(0, "Collection");
    collection.push(WbxmlNode::with_text(0, "SyncKey", sync_key));
    collection.push(WbxmlNode::with_text(0, "CollectionId", collection_id));
    collections.push(collection);
    sync.push(collections);
    sync
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

#[test]
fn wbxml_roundtrip_preserves_get_item_estimate_tokens() {
    let mut root = WbxmlNode::new(6, "GetItemEstimate");
    let mut collections = WbxmlNode::new(6, "Collections");
    let mut collection = WbxmlNode::new(6, "Collection");
    collection.push(WbxmlNode::with_text(0, "SyncKey", "key1"));
    collection.push(WbxmlNode::with_text(
        6,
        "CollectionId",
        "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
    ));
    collections.push(collection);
    root.push(collections);

    let decoded = decode_wbxml(&encode_wbxml(&root)).unwrap();
    let decoded_collection = decoded
        .child("Collections")
        .unwrap()
        .child("Collection")
        .unwrap();

    assert_eq!(decoded.name, "GetItemEstimate");
    assert_eq!(
        decoded_collection.child("SyncKey").unwrap().text_value(),
        "key1"
    );
    assert_eq!(
        decoded_collection
            .child("CollectionId")
            .unwrap()
            .text_value(),
        "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    );
}

#[tokio::test]
async fn move_items_moves_message_between_canonical_mail_folders() {
    let inbox = FakeStore::inbox_mailbox();
    let archive = FakeStore::mailbox(
        "99999999-9999-9999-9999-999999999999",
        "archive",
        "Archive",
        20,
        None,
    );
    let message_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: vec![inbox.clone(), archive.clone()],
        emails: Arc::new(Mutex::new(vec![FakeStore::inbox_email(
            "11111111-1111-1111-1111-111111111111",
            inbox.id,
            "inbox",
            "Move me",
        )])),
        ..Default::default()
    };
    let service = ActiveSyncService::new(store.clone());

    let inbox_id = inbox.id.to_string();
    let archive_id = archive.id.to_string();
    let first_inbox_key = collection_sync_key(
        &handle_sync_node(&service, one_collection_sync(&inbox_id, "0")).await,
        &inbox_id,
    );
    let inbox_key = collection_sync_key(
        &handle_sync_node(&service, one_collection_sync(&inbox_id, &first_inbox_key)).await,
        &inbox_id,
    );
    let archive_key = collection_sync_key(
        &handle_sync_node(&service, one_collection_sync(&archive_id, "0")).await,
        &archive_id,
    );

    let response = service
        .handle_request(
            ActiveSyncQuery {
                cmd: Some("MoveItems".to_string()),
                user: Some("alice@example.test".to_string()),
                device_id: Some("dev1".to_string()),
                _device_type: Some("phone".to_string()),
            },
            &bearer_headers(),
            &encode_wbxml(&{
                let mut root = WbxmlNode::new(5, "MoveItems");
                let mut move_node = WbxmlNode::new(5, "Move");
                move_node.push(WbxmlNode::with_text(5, "SrcMsgId", message_id.to_string()));
                move_node.push(WbxmlNode::with_text(5, "SrcFldId", &inbox_id));
                move_node.push(WbxmlNode::with_text(5, "DstFldId", &archive_id));
                root.push(move_node);
                root
            }),
        )
        .await
        .unwrap();
    let body = decode_response_body(response).await;
    assert_eq!(
        body.child("Response")
            .unwrap()
            .child("Status")
            .unwrap()
            .text_value(),
        "3"
    );
    assert_eq!(store.emails.lock().unwrap()[0].mailbox_id, archive.id);

    let inbox_delta = handle_sync_node(&service, one_collection_sync(&inbox_id, &inbox_key)).await;
    assert_eq!(
        only_sync_collection(&inbox_delta, &inbox_id)
            .child("Commands")
            .unwrap()
            .child("Delete")
            .unwrap()
            .child("ServerId")
            .unwrap()
            .text_value(),
        message_id.to_string()
    );
    let archive_delta =
        handle_sync_node(&service, one_collection_sync(&archive_id, &archive_key)).await;
    assert_eq!(
        only_sync_collection(&archive_delta, &archive_id)
            .child("Commands")
            .unwrap()
            .child("Add")
            .unwrap()
            .child("ServerId")
            .unwrap()
            .text_value(),
        message_id.to_string()
    );
}

#[tokio::test]
async fn sync_delete_moves_message_to_trash_by_default() {
    let inbox = FakeStore::inbox_mailbox();
    let trash = FakeStore::mailbox(
        "77777777-7777-7777-7777-777777777777",
        "trash",
        "Trash",
        30,
        None,
    );
    let message_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: vec![inbox.clone(), trash.clone()],
        emails: Arc::new(Mutex::new(vec![FakeStore::inbox_email(
            "11111111-1111-1111-1111-111111111111",
            inbox.id,
            "inbox",
            "Delete me",
        )])),
        ..Default::default()
    };
    let service = ActiveSyncService::new(store.clone());
    let inbox_id = inbox.id.to_string();
    let trash_id = trash.id.to_string();
    let first_inbox_key = collection_sync_key(
        &handle_sync_node(&service, one_collection_sync(&inbox_id, "0")).await,
        &inbox_id,
    );
    let inbox_key = collection_sync_key(
        &handle_sync_node(&service, one_collection_sync(&inbox_id, &first_inbox_key)).await,
        &inbox_id,
    );
    let trash_key = collection_sync_key(
        &handle_sync_node(&service, one_collection_sync(&trash_id, "0")).await,
        &trash_id,
    );

    let mut request = one_collection_sync(&inbox_id, &inbox_key);
    let mut commands = WbxmlNode::new(0, "Commands");
    let mut delete = WbxmlNode::new(0, "Delete");
    delete.push(WbxmlNode::with_text(0, "ServerId", message_id.to_string()));
    commands.push(delete);
    request.children[0].children[0].push(commands);

    let delete_response = handle_sync_node(&service, request).await;
    assert_eq!(store.emails.lock().unwrap()[0].mailbox_id, trash.id);
    assert!(only_sync_collection(&delete_response, &inbox_id)
        .child("Commands")
        .unwrap()
        .child("Delete")
        .is_some());
    let trash_delta = handle_sync_node(&service, one_collection_sync(&trash_id, &trash_key)).await;
    assert!(only_sync_collection(&trash_delta, &trash_id)
        .child("Commands")
        .unwrap()
        .child("Add")
        .is_some());
}

#[tokio::test]
async fn sync_change_updates_read_state_and_round_trips() {
    let inbox = FakeStore::inbox_mailbox();
    let message_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: vec![inbox.clone()],
        emails: Arc::new(Mutex::new(vec![FakeStore::inbox_email(
            "11111111-1111-1111-1111-111111111111",
            inbox.id,
            "inbox",
            "Read me",
        )])),
        ..Default::default()
    };
    let service = ActiveSyncService::new(store.clone());
    let inbox_id = inbox.id.to_string();
    let first_inbox_key = collection_sync_key(
        &handle_sync_node(&service, one_collection_sync(&inbox_id, "0")).await,
        &inbox_id,
    );
    let inbox_key = collection_sync_key(
        &handle_sync_node(&service, one_collection_sync(&inbox_id, &first_inbox_key)).await,
        &inbox_id,
    );

    let mut request = one_collection_sync(&inbox_id, &inbox_key);
    let mut commands = WbxmlNode::new(0, "Commands");
    let mut change = WbxmlNode::new(0, "Change");
    change.push(WbxmlNode::with_text(0, "ServerId", message_id.to_string()));
    let mut app_data = WbxmlNode::new(0, "ApplicationData");
    app_data.push(WbxmlNode::with_text(2, "Read", "1"));
    change.push(app_data);
    commands.push(change);
    request.children[0].children[0].push(commands);

    let response = handle_sync_node(&service, request).await;
    assert!(!store.emails.lock().unwrap()[0].unread);
    let app_data = only_sync_collection(&response, &inbox_id)
        .child("Commands")
        .unwrap()
        .child("Change")
        .unwrap()
        .child("ApplicationData")
        .unwrap();
    assert_eq!(app_data.child("Read").unwrap().text_value(), "1");
}

#[tokio::test]
async fn sync_respects_body_preference_for_html_text_and_mime() {
    let inbox = FakeStore::inbox_mailbox();
    let message_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
    let mut email = FakeStore::inbox_email(
        "11111111-1111-1111-1111-111111111111",
        inbox.id,
        "inbox",
        "Body",
    );
    email.body_text = "plain text body".to_string();
    email.body_html_sanitized = Some("<p>html body</p>".to_string());
    let raw = b"Subject: Body\r\n\r\nplain text body".to_vec();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: vec![inbox.clone()],
        emails: Arc::new(Mutex::new(vec![email])),
        raw_message_blobs: Arc::new(Mutex::new(HashMap::from([(message_id, raw.clone())]))),
        ..Default::default()
    };
    let service = ActiveSyncService::new(store.clone());
    let inbox_id = inbox.id.to_string();
    let first_key = collection_sync_key(
        &handle_sync_node(&service, one_collection_sync(&inbox_id, "0")).await,
        &inbox_id,
    );

    let mut html_request = one_collection_sync(&inbox_id, &first_key);
    let mut options = WbxmlNode::new(0, "Options");
    let mut body_preference = WbxmlNode::new(17, "BodyPreference");
    body_preference.push(WbxmlNode::with_text(17, "Type", "2"));
    options.push(body_preference);
    html_request.children[0].children[0].push(options);
    let html_response = handle_sync_node(&service, html_request).await;
    let body = only_sync_collection(&html_response, &inbox_id)
        .child("Commands")
        .unwrap()
        .child("Add")
        .unwrap()
        .child("ApplicationData")
        .unwrap()
        .child("Body")
        .unwrap();
    assert_eq!(body.child("Type").unwrap().text_value(), "2");
    assert_eq!(body.child("Data").unwrap().text_value(), "<p>html body</p>");

    let second_key = only_sync_collection(&html_response, &inbox_id)
        .child("SyncKey")
        .unwrap()
        .text_value()
        .to_string();
    store.emails.lock().unwrap()[0].subject = "Body text changed".to_string();
    let mut text_request = one_collection_sync(&inbox_id, &second_key);
    let mut options = WbxmlNode::new(0, "Options");
    let mut body_preference = WbxmlNode::new(17, "BodyPreference");
    body_preference.push(WbxmlNode::with_text(17, "Type", "1"));
    body_preference.push(WbxmlNode::with_text(17, "TruncationSize", "5"));
    options.push(body_preference);
    text_request.children[0].children[0].push(options);
    let text_response = handle_sync_node(&service, text_request).await;
    let body = only_sync_collection(&text_response, &inbox_id)
        .child("Commands")
        .unwrap()
        .child("Change")
        .unwrap()
        .child("ApplicationData")
        .unwrap()
        .child("Body")
        .unwrap();
    assert_eq!(body.child("Type").unwrap().text_value(), "1");
    assert_eq!(body.child("Data").unwrap().text_value(), "plain");
    assert_eq!(body.child("Truncated").unwrap().text_value(), "1");

    let third_key = only_sync_collection(&text_response, &inbox_id)
        .child("SyncKey")
        .unwrap()
        .text_value()
        .to_string();
    store.emails.lock().unwrap()[0].subject = "Body mime changed".to_string();
    let mut mime_request = one_collection_sync(&inbox_id, &third_key);
    let mut options = WbxmlNode::new(0, "Options");
    let mut body_preference = WbxmlNode::new(17, "BodyPreference");
    body_preference.push(WbxmlNode::with_text(17, "Type", "4"));
    body_preference.push(WbxmlNode::with_text(17, "TruncationSize", "10"));
    options.push(body_preference);
    mime_request.children[0].children[0].push(options);
    let mime_response = handle_sync_node(&service, mime_request).await;
    let body = only_sync_collection(&mime_response, &inbox_id)
        .child("Commands")
        .unwrap()
        .child("Change")
        .unwrap()
        .child("ApplicationData")
        .unwrap()
        .child("Body")
        .unwrap();
    assert_eq!(body.child("Type").unwrap().text_value(), "4");
    assert_eq!(
        body.child("Data").unwrap().opaque.as_deref(),
        Some(&raw[..10])
    );
    assert_eq!(body.child("Truncated").unwrap().text_value(), "1");
}

#[tokio::test]
async fn folder_sync_returns_mail_and_collaboration_collections() {
    let inbox = FakeStore::inbox_mailbox();
    let drafts = FakeStore::draft_mailbox();
    let sent = FakeStore::sent_mailbox();
    let trash = FakeStore::mailbox(
        "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee",
        "trash",
        "Trash",
        30,
        None,
    );
    let junk = FakeStore::mailbox(
        "ffffffff-ffff-ffff-ffff-ffffffffffff",
        "junk",
        "Junk",
        40,
        None,
    );
    let archive = FakeStore::mailbox(
        "12121212-1212-4212-9212-121212121212",
        "archive",
        "Archive",
        50,
        None,
    );
    let custom = FakeStore::mailbox(
        "34343434-3434-4434-9434-343434343434",
        "custom",
        "Projects",
        60,
        None,
    );
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: vec![
            inbox.clone(),
            drafts.clone(),
            sent.clone(),
            trash.clone(),
            junk.clone(),
            archive.clone(),
            custom.clone(),
        ],
        ..Default::default()
    };
    let service = ActiveSyncService::new(store);

    let first_body = folder_sync(&service, "0", "dev1").await;
    let changes = first_body.child("Changes").unwrap();
    assert_eq!(changes.child("Count").unwrap().text_value(), "9");

    for (mailbox, display_name, folder_type) in [
        (&inbox, "Inbox", "2"),
        (&drafts, "Drafts", "3"),
        (&sent, "Sent", "5"),
        (&trash, "Trash", "4"),
        (&junk, "Junk", "12"),
        (&archive, "Archive", "12"),
        (&custom, "Projects", "12"),
    ] {
        let add = folder_add(changes, &mailbox.id.to_string());
        assert_eq!(add.child("ParentId").unwrap().text_value(), "0");
        assert_eq!(add.child("DisplayName").unwrap().text_value(), display_name);
        assert_eq!(add.child("Type").unwrap().text_value(), folder_type);
    }

    let contacts = folder_add(changes, "contacts");
    assert_eq!(contacts.child("ParentId").unwrap().text_value(), "0");
    assert_eq!(
        contacts.child("DisplayName").unwrap().text_value(),
        "Contacts"
    );
    assert_eq!(contacts.child("Type").unwrap().text_value(), "9");

    let calendar = folder_add(changes, "calendar");
    assert_eq!(calendar.child("ParentId").unwrap().text_value(), "0");
    assert_eq!(
        calendar.child("DisplayName").unwrap().text_value(),
        "Calendar"
    );
    assert_eq!(calendar.child("Type").unwrap().text_value(), "8");
    assert!(!changes.children_named("Add").iter().any(|change| {
        change.child("Type").unwrap().text_value() == "7"
            || change.child("Type").unwrap().text_value() == "15"
    }));

    let stable_body = folder_sync(
        &service,
        first_body.child("SyncKey").unwrap().text_value(),
        "dev1",
    )
    .await;
    assert_eq!(
        stable_body
            .child("Changes")
            .unwrap()
            .child("Count")
            .unwrap()
            .text_value(),
        "0"
    );
}

#[tokio::test]
async fn folder_sync_preserves_nested_mailbox_parent_ids() {
    let parent = FakeStore::mailbox(
        "45454545-4545-4545-8545-454545454545",
        "custom",
        "Projects",
        60,
        None,
    );
    let child = FakeStore::mailbox(
        "56565656-5656-4656-9656-565656565656",
        "custom",
        "Alpha",
        61,
        Some(parent.id),
    );
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: vec![parent.clone(), child.clone()],
        ..Default::default()
    };
    let service = ActiveSyncService::new(store);

    let body = folder_sync(&service, "0", "dev-nested").await;
    let changes = body.child("Changes").unwrap();
    assert_eq!(
        folder_add(changes, &parent.id.to_string())
            .child("ParentId")
            .unwrap()
            .text_value(),
        "0"
    );
    let child_add = folder_add(changes, &child.id.to_string());
    assert_eq!(
        child_add.child("ParentId").unwrap().text_value(),
        parent.id.to_string()
    );
    assert_eq!(
        child_add.child("DisplayName").unwrap().text_value(),
        "Alpha"
    );
}

#[tokio::test]
async fn folder_sync_projects_shared_mailbox_folders_with_hierarchy() {
    let account = FakeStore::account();
    let own_inbox = FakeStore::inbox_mailbox();
    let shared_access = FakeStore::shared_mailbox_access(false, true);
    let shared_parent = FakeStore::mailbox(
        "67676767-6767-4767-9767-676767676767",
        "custom",
        "Projects",
        10,
        None,
    );
    let shared_child = FakeStore::mailbox(
        "78787878-7878-4878-9878-787878787878",
        "archive",
        "Closed",
        11,
        Some(shared_parent.id),
    );
    let store = FakeStore {
        session: Some(account.clone()),
        accessible_mailbox_accounts: vec![FakeStore::mailbox_access(), shared_access.clone()],
        mailboxes_by_account: HashMap::from([
            (account.account_id, vec![own_inbox]),
            (
                shared_access.account_id,
                vec![shared_parent.clone(), shared_child.clone()],
            ),
        ]),
        ..Default::default()
    };
    let service = ActiveSyncService::new(store);

    let body = folder_sync(&service, "0", "dev-shared").await;
    let changes = body.child("Changes").unwrap();
    let shared_parent_add = folder_add(changes, &shared_parent.id.to_string());
    assert_eq!(
        shared_parent_add.child("DisplayName").unwrap().text_value(),
        "shared@example.test / Projects"
    );
    assert_eq!(
        shared_parent_add.child("ParentId").unwrap().text_value(),
        "0"
    );

    let shared_child_add = folder_add(changes, &shared_child.id.to_string());
    assert_eq!(
        shared_child_add.child("ParentId").unwrap().text_value(),
        shared_parent.id.to_string()
    );
    assert_eq!(
        shared_child_add.child("DisplayName").unwrap().text_value(),
        "Closed"
    );
    assert_eq!(shared_child_add.child("Type").unwrap().text_value(), "12");
}

#[tokio::test]
async fn stale_folder_sync_key_is_rejected_after_completed_round() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: vec![FakeStore::inbox_mailbox()],
        ..Default::default()
    };
    let service = ActiveSyncService::new(store);

    let primed = folder_sync(&service, "0", "dev-stale-folder").await;
    let primed_key = primed.child("SyncKey").unwrap().text_value().to_string();
    let stable = folder_sync(&service, &primed_key, "dev-stale-folder").await;
    assert_ne!(
        primed_key,
        stable.child("SyncKey").unwrap().text_value().to_string()
    );

    let stale = folder_sync(&service, &primed_key, "dev-stale-folder").await;
    assert_eq!(stale.child("Status").unwrap().text_value(), "9");
    assert!(stale.child("SyncKey").is_none());
    assert!(stale.child("Changes").is_none());
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
async fn get_item_estimate_returns_pending_sync_count() {
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
    let sync_key = collection_sync_key(&priming_sync, &inbox.id.to_string());

    let estimate_request = encode_wbxml(&{
        let mut root = WbxmlNode::new(6, "GetItemEstimate");
        let mut collections = WbxmlNode::new(6, "Collections");
        let mut collection = WbxmlNode::new(6, "Collection");
        collection.push(WbxmlNode::with_text(0, "SyncKey", sync_key));
        collection.push(WbxmlNode::with_text(
            6,
            "CollectionId",
            inbox.id.to_string(),
        ));
        collections.push(collection);
        root.push(collections);
        root
    });

    let estimate_response = service
        .handle_request(
            ActiveSyncQuery {
                cmd: Some("GetItemEstimate".to_string()),
                user: Some("alice@example.test".to_string()),
                device_id: Some("dev1".to_string()),
                _device_type: Some("phone".to_string()),
            },
            &bearer_headers(),
            &estimate_request,
        )
        .await
        .unwrap();
    assert_eq!(estimate_response.status(), StatusCode::OK);

    let estimate = decode_response_body(estimate_response).await;
    let response = estimate.child("Response").unwrap();
    let response_collection = response.child("Collection").unwrap();
    assert_eq!(estimate.child("Status").unwrap().text_value(), "1");
    assert_eq!(response.child("Status").unwrap().text_value(), "1");
    assert_eq!(
        response_collection
            .child("CollectionId")
            .unwrap()
            .text_value(),
        inbox.id.to_string()
    );
    assert_eq!(
        response_collection.child("Estimate").unwrap().text_value(),
        "3"
    );
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
async fn stale_sync_key_is_rejected_after_a_completed_round() {
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
    let primed_key = collection_sync_key(
        &decode_response_body(priming_response).await,
        &inbox.id.to_string(),
    );

    let continuation_request = encode_wbxml(&{
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
    let continuation_response = service
        .handle_request(
            ActiveSyncQuery {
                cmd: Some("Sync".to_string()),
                user: Some("alice@example.test".to_string()),
                device_id: Some("dev1".to_string()),
                _device_type: Some("phone".to_string()),
            },
            &bearer_headers(),
            &continuation_request,
        )
        .await
        .unwrap();
    let stable_key = collection_sync_key(
        &decode_response_body(continuation_response).await,
        &inbox.id.to_string(),
    );
    assert_ne!(primed_key, stable_key);

    let stale_request = encode_wbxml(&{
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
    let stale_response = service
        .handle_request(
            ActiveSyncQuery {
                cmd: Some("Sync".to_string()),
                user: Some("alice@example.test".to_string()),
                device_id: Some("dev1".to_string()),
                _device_type: Some("phone".to_string()),
            },
            &bearer_headers(),
            &stale_request,
        )
        .await
        .unwrap();
    let stale_sync = decode_response_body(stale_response).await;
    let stale_collection = stale_sync
        .child("Collections")
        .unwrap()
        .child("Collection")
        .unwrap();
    assert_eq!(stale_collection.child("Status").unwrap().text_value(), "3");
    assert!(stale_collection.child("SyncKey").is_none());
}

#[tokio::test]
async fn restart_safe_no_change_sync_keeps_persisted_key_usable() {
    let inbox = FakeStore::inbox_mailbox();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: vec![inbox.clone()],
        ..Default::default()
    };
    let first_service = ActiveSyncService::new(store.clone());

    let priming_sync =
        sync_collection(&first_service, &inbox.id.to_string(), "0", "dev-restart").await;
    let primed_key = collection_sync_key(&priming_sync, &inbox.id.to_string());

    let restarted_service = ActiveSyncService::new(store);
    let stable_sync = sync_collection(
        &restarted_service,
        &inbox.id.to_string(),
        &primed_key,
        "dev-restart",
    )
    .await;
    let collection = only_sync_collection(&stable_sync, &inbox.id.to_string());

    assert_eq!(collection.child("Status").unwrap().text_value(), "1");
    assert!(collection.child("SyncKey").is_some());
    assert!(collection.child("Commands").is_none());
}

#[tokio::test]
async fn unknown_sync_key_is_rejected_with_invalid_sync_key_status() {
    let inbox = FakeStore::inbox_mailbox();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: vec![inbox.clone()],
        ..Default::default()
    };
    let service = ActiveSyncService::new(store);

    let sync = sync_collection(
        &service,
        &inbox.id.to_string(),
        "unknown-sync-key",
        "dev-unknown",
    )
    .await;
    let collection = only_sync_collection(&sync, &inbox.id.to_string());

    assert_eq!(collection.child("Status").unwrap().text_value(), "3");
    assert!(collection.child("SyncKey").is_none());
}

#[tokio::test]
async fn expired_sync_key_is_cleaned_up_and_rejected() {
    let inbox = FakeStore::inbox_mailbox();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: vec![inbox.clone()],
        ..Default::default()
    };
    let service = ActiveSyncService::new(store.clone());
    let collection_id = inbox.id.to_string();

    let priming_sync = sync_collection(&service, &collection_id, "0", "dev-expired").await;
    let primed_key = collection_sync_key(&priming_sync, &collection_id);
    store.expired_sync_states.lock().unwrap().insert(format!(
        "{}:{}:{}:{}",
        FakeStore::account().account_id,
        "dev-expired",
        collection_id,
        primed_key
    ));

    let expired_sync = sync_collection(&service, &collection_id, &primed_key, "dev-expired").await;
    let collection = only_sync_collection(&expired_sync, &collection_id);

    assert_eq!(collection.child("Status").unwrap().text_value(), "3");
    assert!(collection.child("SyncKey").is_none());
    assert!(store.sync_states.lock().unwrap().is_empty());
}

#[tokio::test]
async fn superseded_incomplete_sync_key_is_rejected() {
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
        ])),
        ..Default::default()
    };
    let service = ActiveSyncService::new(store);
    let collection_id = inbox.id.to_string();

    let priming_sync = sync_collection(&service, &collection_id, "0", "dev-superseded").await;
    let primed_key = collection_sync_key(&priming_sync, &collection_id);
    let page_request = encode_wbxml(&{
        let mut sync = WbxmlNode::new(0, "Sync");
        let mut collections = WbxmlNode::new(0, "Collections");
        let mut collection = WbxmlNode::new(0, "Collection");
        collection.push(WbxmlNode::with_text(0, "SyncKey", &primed_key));
        collection.push(WbxmlNode::with_text(0, "CollectionId", &collection_id));
        collection.push(WbxmlNode::with_text(0, "WindowSize", "1"));
        collections.push(collection);
        sync.push(collections);
        sync
    });
    let page_response = service
        .handle_request(
            ActiveSyncQuery {
                cmd: Some("Sync".to_string()),
                user: Some("alice@example.test".to_string()),
                device_id: Some("dev-superseded".to_string()),
                _device_type: Some("phone".to_string()),
            },
            &bearer_headers(),
            &page_request,
        )
        .await
        .unwrap();
    let page_sync = decode_response_body(page_response).await;
    assert!(only_sync_collection(&page_sync, &collection_id)
        .child("MoreAvailable")
        .is_some());

    let superseded_sync =
        sync_collection(&service, &collection_id, &primed_key, "dev-superseded").await;
    let collection = only_sync_collection(&superseded_sync, &collection_id);

    assert_eq!(collection.child("Status").unwrap().text_value(), "3");
    assert!(collection.child("SyncKey").is_none());
}

#[tokio::test]
async fn hierarchy_change_after_existing_sync_returns_folder_sync_required() {
    let inbox = FakeStore::inbox_mailbox();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: vec![inbox.clone()],
        ..Default::default()
    };
    let service = ActiveSyncService::new(store.clone());
    let collection_id = inbox.id.to_string();

    let folder_state = folder_sync(&service, "0", "dev-hierarchy").await;
    let folder_key = folder_state
        .child("SyncKey")
        .unwrap()
        .text_value()
        .to_string();
    let content_state = sync_collection(&service, &collection_id, "0", "dev-hierarchy").await;
    let content_key = collection_sync_key(&content_state, &collection_id);

    let mut changed_store = store.clone();
    changed_store.mailboxes.push(FakeStore::mailbox(
        "34343434-3434-4434-9434-343434343434",
        "custom",
        "Projects",
        60,
        None,
    ));
    let changed_service = ActiveSyncService::new(changed_store);

    let stale_hierarchy_sync = sync_collection(
        &changed_service,
        &collection_id,
        &content_key,
        "dev-hierarchy",
    )
    .await;
    let collection = only_sync_collection(&stale_hierarchy_sync, &collection_id);
    assert_eq!(collection.child("Status").unwrap().text_value(), "12");
    assert!(collection.child("SyncKey").is_none());

    let refreshed_folder_state = folder_sync(&changed_service, &folder_key, "dev-hierarchy").await;
    assert_eq!(
        refreshed_folder_state
            .child("Changes")
            .unwrap()
            .child("Count")
            .unwrap()
            .text_value(),
        "1"
    );
    let advanced_sync = sync_collection(
        &changed_service,
        &collection_id,
        &content_key,
        "dev-hierarchy",
    )
    .await;
    let advanced_collection = only_sync_collection(&advanced_sync, &collection_id);
    assert_eq!(
        advanced_collection.child("Status").unwrap().text_value(),
        "1"
    );
    assert!(advanced_collection.child("SyncKey").is_some());
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
    assert_eq!(submitted[0].draft_message_id, None);
    assert_eq!(submitted[0].subject, "Hello");
    assert_eq!(submitted[0].to[0].address, "bob@example.test");
    assert!(store.saved_drafts.lock().unwrap().is_empty());
    assert!(store.deleted_drafts.lock().unwrap().is_empty());
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
    let mut first = FakeStore::inbox_email(
        "11111111-1111-1111-1111-111111111111",
        inbox.id,
        "inbox",
        "Quarterly budget",
    );
    first.bcc = vec![JmapEmailAddress {
        address: "hidden@example.test".to_string(),
        display_name: Some("Hidden".to_string()),
    }];
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: vec![inbox.clone()],
        emails: Arc::new(Mutex::new(vec![
            first,
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
    assert!(result
        .child("Properties")
        .unwrap()
        .child("ApplicationData")
        .unwrap()
        .child("Bcc")
        .is_none());
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
async fn ping_reconnects_after_service_restart_using_persisted_sync_state() {
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
    let service = ActiveSyncService::new(store.clone());

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

    let full_ping_request = ping_request(Some("60"), &[(&inbox.id.to_string(), "Email")]);
    let primed_ping = ping(&service, "dev1", &full_ping_request).await;
    assert_eq!(primed_ping.child("Status").unwrap().text_value(), "1");

    let restarted_service = ActiveSyncService::new(store);
    emails.lock().unwrap().push(FakeStore::inbox_email(
        "22222222-2222-2222-2222-222222222222",
        inbox.id,
        "inbox",
        "Two",
    ));

    let body = ping(&restarted_service, "dev1", &[]).await;
    assert_eq!(body.child("Status").unwrap().text_value(), "2");
    let changed = body
        .child("Folders")
        .unwrap()
        .children_named("Folder")
        .into_iter()
        .map(|folder| folder.text_value().to_string())
        .collect::<Vec<_>>();
    assert_eq!(changed, vec![inbox.id.to_string()]);
}

#[tokio::test]
async fn ping_rejects_unsynchronized_folders() {
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
    let service = ActiveSyncService::new(store);

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
    assert_eq!(body.child("Status").unwrap().text_value(), "3");
    assert!(body.child("Folders").is_none());
}

#[tokio::test]
async fn ping_empty_request_without_cached_parameters_returns_missing_parameters() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: vec![FakeStore::inbox_mailbox()],
        ..Default::default()
    };
    let service = ActiveSyncService::new(store);

    let body = ping(&service, "dev-empty", &[]).await;
    assert_eq!(body.child("Status").unwrap().text_value(), "3");
}

#[tokio::test]
async fn ping_invalid_folder_id_requires_folder_sync() {
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
    let service = ActiveSyncService::new(store);
    sync_collection(&service, &inbox.id.to_string(), "0", "dev-invalid").await;

    let request = ping_request(
        Some("60"),
        &[("99999999-9999-9999-9999-999999999999", "Email")],
    );
    let body = ping(&service, "dev-invalid", &request).await;
    assert_eq!(body.child("Status").unwrap().text_value(), "7");
}

#[tokio::test]
async fn ping_no_changes_returns_no_change_status() {
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
    let service = ActiveSyncService::new(store);
    sync_collection(&service, &inbox.id.to_string(), "0", "dev-no-change").await;

    let request = ping_request(Some("60"), &[(&inbox.id.to_string(), "Email")]);
    let body = ping(&service, "dev-no-change", &request).await;
    assert_eq!(body.child("Status").unwrap().text_value(), "1");
    assert!(body.child("Folders").is_none());
}

#[tokio::test]
async fn ping_reports_changed_folder_ids_as_folder_values() {
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
    sync_collection(&service, &inbox.id.to_string(), "0", "dev-changed").await;
    emails.lock().unwrap().push(FakeStore::inbox_email(
        "22222222-2222-2222-2222-222222222222",
        inbox.id,
        "inbox",
        "Two",
    ));

    let request = ping_request(Some("60"), &[(&inbox.id.to_string(), "Email")]);
    let body = ping(&service, "dev-changed", &request).await;
    assert_eq!(body.child("Status").unwrap().text_value(), "2");
    let folder = body
        .child("Folders")
        .unwrap()
        .children_named("Folder")
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(folder.text_value(), inbox.id.to_string());
    assert!(folder.child("Id").is_none());
}

#[tokio::test]
async fn ping_detects_changes_across_multiple_monitored_collections() {
    let inbox = FakeStore::inbox_mailbox();
    let sent = FakeStore::sent_mailbox();
    let emails = Arc::new(Mutex::new(vec![FakeStore::inbox_email(
        "11111111-1111-1111-1111-111111111111",
        inbox.id,
        "inbox",
        "One",
    )]));
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: vec![inbox.clone(), sent.clone()],
        emails: emails.clone(),
        ..Default::default()
    };
    let service = ActiveSyncService::new(store);
    sync_collection(&service, &inbox.id.to_string(), "0", "dev-multi").await;
    sync_collection(&service, &sent.id.to_string(), "0", "dev-multi").await;
    emails.lock().unwrap().push(FakeStore::inbox_email(
        "22222222-2222-2222-2222-222222222222",
        sent.id,
        "sent",
        "Sent copy",
    ));

    let request = ping_request(
        Some("60"),
        &[
            (&inbox.id.to_string(), "Email"),
            (&sent.id.to_string(), "Email"),
        ],
    );
    let body = ping(&service, "dev-multi", &request).await;
    let changed = body
        .child("Folders")
        .unwrap()
        .children_named("Folder")
        .into_iter()
        .map(|folder| folder.text_value().to_string())
        .collect::<Vec<_>>();
    assert_eq!(body.child("Status").unwrap().text_value(), "2");
    assert_eq!(changed, vec![sent.id.to_string()]);
}

#[tokio::test]
async fn ping_heartbeat_outside_supported_range_returns_limit() {
    let inbox = FakeStore::inbox_mailbox();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: vec![inbox.clone()],
        ..Default::default()
    };
    let service = ActiveSyncService::new(store);

    let too_low = ping_request(Some("30"), &[(&inbox.id.to_string(), "Email")]);
    let too_low_body = ping(&service, "dev-heartbeat", &too_low).await;
    assert_eq!(too_low_body.child("Status").unwrap().text_value(), "5");
    assert_eq!(
        too_low_body
            .child("HeartbeatInterval")
            .unwrap()
            .text_value(),
        "60"
    );

    let too_high = ping_request(Some("4000"), &[(&inbox.id.to_string(), "Email")]);
    let too_high_body = ping(&service, "dev-heartbeat", &too_high).await;
    assert_eq!(too_high_body.child("Status").unwrap().text_value(), "5");
    assert_eq!(
        too_high_body
            .child("HeartbeatInterval")
            .unwrap()
            .text_value(),
        "3540"
    );
}

#[tokio::test]
async fn ping_too_many_monitored_folders_returns_max_folders() {
    let inbox = FakeStore::inbox_mailbox();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: vec![inbox.clone()],
        ..Default::default()
    };
    let service = ActiveSyncService::new(store);
    let folders = vec![(inbox.id.to_string(), "Email".to_string()); 201];
    let folder_refs = folders
        .iter()
        .map(|(id, class_name)| (id.as_str(), class_name.as_str()))
        .collect::<Vec<_>>();

    let request = ping_request(Some("60"), &folder_refs);
    let body = ping(&service, "dev-max-folders", &request).await;
    assert_eq!(body.child("Status").unwrap().text_value(), "6");
    assert_eq!(body.child("MaxFolders").unwrap().text_value(), "200");
}

#[tokio::test]
async fn ping_surfaces_hierarchy_change_as_folder_sync_required() {
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
    sync_collection(&service, &inbox.id.to_string(), "0", "dev-hierarchy-ping").await;

    let archive = FakeStore::mailbox(
        "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee",
        "archive",
        "Archive",
        20,
        None,
    );
    let mut changed_store = store.clone();
    changed_store.mailboxes.push(archive);
    let changed_service = ActiveSyncService::new(changed_store);

    let request = ping_request(Some("60"), &[(&inbox.id.to_string(), "Email")]);
    let body = ping(&changed_service, "dev-hierarchy-ping", &request).await;
    assert_eq!(body.child("Status").unwrap().text_value(), "7");
}

#[tokio::test]
async fn smart_reply_uses_source_recipients_and_canonical_submission() {
    let inbox = FakeStore::inbox_mailbox();
    let source_message_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
    let store = FakeStore {
        session: Some(FakeStore::account()),
        mailboxes: vec![inbox.clone()],
        emails: Arc::new(Mutex::new(vec![FakeStore::inbox_email(
            "11111111-1111-1111-1111-111111111111",
            inbox.id,
            "inbox",
            "Source subject",
        )])),
        ..Default::default()
    };
    let service = ActiveSyncService::new(store.clone());
    let request = encode_wbxml(&{
        let mut root = WbxmlNode::new(21, "SmartReply");
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
                "Subject: \r\n",
                "\r\n",
                "Thanks for the update."
            ),
        ));
        root
    });

    let response = service
        .handle_request(
            ActiveSyncQuery {
                cmd: Some("SmartReply".to_string()),
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
    assert_eq!(body.child("Status").unwrap().text_value(), "1");
    let submitted = store.submitted_messages.lock().unwrap();
    assert_eq!(submitted[0].source, "activesync-smartreply");
    assert_eq!(submitted[0].to[0].address, "bob@example.test");
    assert_eq!(submitted[0].subject, "Re: Source subject");
    assert!(submitted[0].body_text.contains("Original message"));
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

#[tokio::test]
#[ignore = "benchmark"]
async fn benchmark_sync_refresh_and_submission_paths() {
    fn query(cmd: &str) -> ActiveSyncQuery {
        ActiveSyncQuery {
            cmd: Some(cmd.to_string()),
            user: Some("alice@example.test".to_string()),
            device_id: Some("bench-device".to_string()),
            _device_type: Some("phone".to_string()),
        }
    }

    let store = FakeStore {
        session: Some(FakeStore::account()),
        login: Some(FakeStore::login()),
        mailboxes: vec![
            FakeStore::inbox_mailbox(),
            FakeStore::draft_mailbox(),
            FakeStore::sent_mailbox(),
        ],
        emails: Arc::new(Mutex::new(
            (0..512)
                .map(|index| {
                    FakeStore::inbox_email(
                        &format!("00000000-0000-0000-0000-{:012x}", index + 1),
                        FakeStore::inbox_mailbox().id,
                        "inbox",
                        &format!("Message {index:04}"),
                    )
                })
                .collect(),
        )),
        ..Default::default()
    };
    let service = ActiveSyncService::new(store.clone());

    let folder_sync_request = encode_wbxml(&{
        let mut node = WbxmlNode::new(7, "FolderSync");
        node.push(WbxmlNode::with_text(7, "SyncKey", "0"));
        node
    });

    let folder_sync_start = Instant::now();
    for _ in 0..100 {
        service
            .handle_request(query("FolderSync"), &basic_headers(), &folder_sync_request)
            .await
            .unwrap();
    }
    let folder_sync_elapsed = folder_sync_start.elapsed();

    let sync_request = encode_wbxml(&{
        let mut sync = WbxmlNode::new(0, "Sync");
        let mut collections = WbxmlNode::new(0, "Collections");
        let mut collection = WbxmlNode::new(0, "Collection");
        collection.push(WbxmlNode::with_text(0, "SyncKey", "0"));
        collection.push(WbxmlNode::with_text(
            0,
            "CollectionId",
            &FakeStore::inbox_mailbox().id.to_string(),
        ));
        collection.push(WbxmlNode::with_text(0, "WindowSize", "128"));
        collections.push(collection);
        sync.push(collections);
        sync
    });
    let sync_start = Instant::now();
    for _ in 0..40 {
        service
            .handle_request(query("Sync"), &basic_headers(), &sync_request)
            .await
            .unwrap();
    }
    let sync_elapsed = sync_start.elapsed();

    let ping_request = encode_wbxml(&{
        let mut ping = WbxmlNode::new(13, "Ping");
        ping.push(WbxmlNode::with_text(13, "HeartbeatInterval", "60"));
        let mut folders = WbxmlNode::new(13, "Folders");
        let mut folder = WbxmlNode::new(13, "Folder");
        folder.push(WbxmlNode::with_text(
            13,
            "Id",
            &FakeStore::inbox_mailbox().id.to_string(),
        ));
        folders.push(folder);
        ping.push(folders);
        ping
    });
    let ping_start = Instant::now();
    for _ in 0..80 {
        service
            .handle_request(query("Ping"), &basic_headers(), &ping_request)
            .await
            .unwrap();
    }
    let ping_elapsed = ping_start.elapsed();

    let send_mail_request = concat!(
        "From: Alice <alice@example.test>\r\n",
        "To: Bob <bob@example.test>\r\n",
        "Subject: Benchmark\r\n",
        "\r\n",
        "Benchmark body\r\n"
    )
    .as_bytes()
    .to_vec();
    let send_mail_start = Instant::now();
    for _ in 0..60 {
        service
            .handle_request(query("SendMail"), &mime_headers(), &send_mail_request)
            .await
            .unwrap();
    }
    let send_mail_elapsed = send_mail_start.elapsed();

    println!(
        "BENCH lpe-activesync foldersync total={:?} avg_per_iter_us={} mailboxes={}",
        folder_sync_elapsed,
        folder_sync_elapsed.as_micros() / 100,
        3
    );
    println!(
        "BENCH lpe-activesync sync_refresh total={:?} avg_per_iter_us={} emails={} window_size=128 full_email_fetches={}",
        sync_elapsed,
        sync_elapsed.as_micros() / 40,
        512,
        *store.full_email_fetches.lock().unwrap()
    );
    println!(
        "BENCH lpe-activesync ping total={:?} avg_per_iter_us={} monitored_folders=1",
        ping_elapsed,
        ping_elapsed.as_micros() / 80
    );
    println!(
        "BENCH lpe-activesync sendmail total={:?} avg_per_iter_us={} submissions={}",
        send_mail_elapsed,
        send_mail_elapsed.as_micros() / 60,
        store.submitted_messages.lock().unwrap().len()
    );
}
