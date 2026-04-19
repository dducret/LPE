use std::sync::{Arc, Mutex};

use argon2::{
    password_hash::{rand_core::OsRng, SaltString},
    Argon2, PasswordHasher,
};
use axum::body::to_bytes;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use lpe_mail_auth::AccountAuthStore;
use lpe_storage::{
    AccountLogin, ActiveSyncSyncState, AuditEntryInput, AuthenticatedAccount, ClientContact,
    ClientEvent, JmapEmail, JmapEmailAddress, JmapEmailQuery, JmapMailbox, SavedDraftMessage,
    SubmitMessageInput, SubmittedMessage,
};
use serde_json::Value;
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
    emails: Vec<JmapEmail>,
    contacts: Vec<ClientContact>,
    events: Vec<ClientEvent>,
    saved_drafts: Arc<Mutex<Vec<SubmitMessageInput>>>,
    submitted_messages: Arc<Mutex<Vec<SubmitMessageInput>>>,
    sync_states: Arc<Mutex<std::collections::HashMap<String, ActiveSyncSyncState>>>,
}

impl FakeStore {
    fn account() -> AuthenticatedAccount {
        AuthenticatedAccount {
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
            delivery_status: "received".to_string(),
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
}

impl ActiveSyncStore for FakeStore {
    fn fetch_jmap_mailboxes<'a>(&'a self, _account_id: Uuid) -> StoreFuture<'a, Vec<JmapMailbox>> {
        let mailboxes = self.mailboxes.clone();
        Box::pin(async move { Ok(mailboxes) })
    }

    fn query_jmap_email_ids<'a>(
        &'a self,
        _account_id: Uuid,
        mailbox_id: Option<Uuid>,
        position: u64,
        limit: u64,
    ) -> StoreFuture<'a, JmapEmailQuery> {
        let filtered = self
            .emails
            .iter()
            .filter(|email| mailbox_id.is_none() || Some(email.mailbox_id) == mailbox_id)
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
        let emails = self
            .emails
            .iter()
            .filter(|email| ids.contains(&email.id))
            .cloned()
            .collect::<Vec<_>>();
        Box::pin(async move { Ok(emails) })
    }

    fn fetch_jmap_draft<'a>(
        &'a self,
        _account_id: Uuid,
        id: Uuid,
    ) -> StoreFuture<'a, Option<JmapEmail>> {
        let email = self.emails.iter().find(|email| email.id == id).cloned();
        Box::pin(async move { Ok(email) })
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
        let contacts = self.contacts.clone();
        Box::pin(async move { Ok(contacts) })
    }

    fn fetch_client_events<'a>(&'a self, _account_id: Uuid) -> StoreFuture<'a, Vec<ClientEvent>> {
        let events = self.events.clone();
        Box::pin(async move { Ok(events) })
    }

    fn store_activesync_sync_state<'a>(
        &'a self,
        account_id: Uuid,
        device_id: &'a str,
        collection_id: &'a str,
        sync_key: &'a str,
        snapshot: Value,
    ) -> StoreFuture<'a, ()> {
        let key = format!("{account_id}:{device_id}:{collection_id}:{sync_key}");
        self.sync_states.lock().unwrap().insert(
            key,
            ActiveSyncSyncState {
                sync_key: sync_key.to_string(),
                snapshot,
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
        emails: vec![
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
        ],
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
        emails: vec![
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
        ],
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
