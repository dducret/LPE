use axum::body::to_bytes;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use lpe_mail_auth::{AccountAuthStore, StoreFuture};
use lpe_storage::{
    AccessibleContact, AccessibleEvent, AccountLogin, AuthenticatedAccount,
    CollaborationCollection, CollaborationRights, JmapEmail, JmapEmailAddress, JmapEmailQuery,
    JmapImportedEmailInput, JmapMailbox, JmapMailboxCreateInput, SavedDraftMessage,
    StoredAccountAppPassword, SubmitMessageInput, SubmittedMessage, UpsertClientContactInput,
    UpsertClientEventInput,
};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

use crate::{
    mapi::MapiEndpoint,
    service::{error_response, ExchangeService},
    store::ExchangeStore,
};

#[derive(Clone, Default)]
struct FakeStore {
    session: Option<AuthenticatedAccount>,
    contact_collections: Arc<Mutex<Vec<CollaborationCollection>>>,
    calendar_collections: Arc<Mutex<Vec<CollaborationCollection>>>,
    contacts: Arc<Mutex<Vec<AccessibleContact>>>,
    deleted_contacts: Arc<Mutex<Vec<Uuid>>>,
    events: Arc<Mutex<Vec<AccessibleEvent>>>,
    deleted_events: Arc<Mutex<Vec<Uuid>>>,
    saved_drafts: Arc<Mutex<Vec<SubmitMessageInput>>>,
    imported_emails: Arc<Mutex<Vec<JmapImportedEmailInput>>>,
    emails: Arc<Mutex<Vec<JmapEmail>>>,
    submitted_messages: Arc<Mutex<Vec<SubmitMessageInput>>>,
    deleted_emails: Arc<Mutex<Vec<Uuid>>>,
    moved_emails: Arc<Mutex<Vec<(Uuid, Uuid)>>>,
    mailboxes: Arc<Mutex<Vec<JmapMailbox>>>,
    created_mailboxes: Arc<Mutex<Vec<JmapMailboxCreateInput>>>,
    destroyed_mailboxes: Arc<Mutex<Vec<Uuid>>>,
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
        self.contacts.lock().unwrap().push(contact.clone());
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
        self.events.lock().unwrap().push(event.clone());
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

    fn import_jmap_email<'a>(
        &'a self,
        input: JmapImportedEmailInput,
        _audit: lpe_storage::AuditEntryInput,
    ) -> StoreFuture<'a, JmapEmail> {
        self.imported_emails.lock().unwrap().push(input.clone());
        let email = FakeStore::email(
            "99999999-9999-9999-9999-999999999999",
            &input.mailbox_id.to_string(),
            "custom",
            &input.subject,
        );
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
        self.submitted_messages.lock().unwrap().push(input);
        Box::pin(async move {
            Ok(SubmittedMessage {
                message_id: Uuid::parse_str("ffffffff-ffff-ffff-ffff-ffffffffffff").unwrap(),
                thread_id: Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap(),
                account_id: Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap(),
                submitted_by_account_id: Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
                    .unwrap(),
                sent_mailbox_id: Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap(),
                outbound_queue_id: Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap(),
                delivery_status: "queued".to_string(),
            })
        })
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

fn mapi_headers(request_type: &str) -> HeaderMap {
    let mut headers = bearer_headers();
    headers.insert(
        "x-requesttype",
        HeaderValue::from_str(request_type).unwrap(),
    );
    headers.insert("x-requestid", HeaderValue::from_static("request-1"));
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

async fn response_text(response: axum::response::Response) -> String {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

async fn response_bytes(response: axum::response::Response) -> Vec<u8> {
    to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec()
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
    assert!(response
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .starts_with("lpe_mapi_emsmdb="));

    let body = response_bytes(response).await;
    assert_eq!(&body[0..8], &[0, 0, 0, 0, 0, 0, 0, 0]);
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
    assert_eq!(response_rop[6] & 0x01, 0x01);
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
async fn resolve_names_returns_ews_no_results_error() {
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
    assert!(body.contains("<m:ResolveNamesResponseMessage ResponseClass=\"Error\">"));
    assert!(body.contains("<m:ResponseCode>ErrorNameResolutionNoResults</m:ResponseCode>"));
    assert!(body.contains("<t:ServerVersionInfo"));
}

#[tokio::test]
async fn get_user_availability_returns_ews_not_available_error() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    let response = service
        .handle(
            &bearer_headers(),
            br#"<s:Envelope><s:Body><m:GetUserAvailabilityRequest /></s:Body></s:Envelope>"#,
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response_text(response).await;
    assert!(body.contains("<m:GetUserAvailabilityResponse>"));
    assert!(body.contains("<m:ResponseCode>ErrorFreeBusyGenerationFailed</m:ResponseCode>"));
    assert!(body.contains("<t:ServerVersionInfo"));
}

#[tokio::test]
async fn write_operations_return_ews_unsupported_errors() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
    let service = ExchangeService::new(store);

    for operation in ["UpdateItem"] {
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
async fn delete_item_rejects_unsupported_item_ids() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        ..Default::default()
    };
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
        "GetUserOofSettings",
        "GetRoomLists",
        "FindPeople",
        "ExpandDL",
        "Subscribe",
        "GetDelegate",
        "GetUserConfiguration",
        "GetSharingMetadata",
        "GetSharingFolder",
        "GetAttachment",
        "Unsubscribe",
        "GetEvents",
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
async fn request_suffixed_operations_use_canonical_response_names() {
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
    assert!(body.contains("<m:ResponseCode>ErrorInvalidOperation</m:ResponseCode>"));
    assert!(body.contains("<t:ServerVersionInfo"));
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
    assert!(body.contains(
        "<m:SyncState>contacts:default:bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb</m:SyncState>"
    ));

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
    assert!(body.contains("<m:SyncState>contacts:default:0</m:SyncState>"));
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
                      <t:RequiredAttendees>
                        <t:Attendee><t:Mailbox><t:Name>Bob</t:Name><t:EmailAddress>bob@example.test</t:EmailAddress></t:Mailbox></t:Attendee>
                      </t:RequiredAttendees>
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
    assert!(body.contains(
        "<m:SyncState>calendar:default:cccccccc-cccc-cccc-cccc-cccccccccccc</m:SyncState>"
    ));

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
    assert!(body.contains("<m:SyncState>calendar:default:0</m:SyncState>"));
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
