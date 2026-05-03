use axum::body::to_bytes;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use lpe_mail_auth::{AccountAuthStore, StoreFuture};
use lpe_storage::{
    AccessibleContact, AccessibleEvent, AccountLogin, AuthenticatedAccount,
    CollaborationCollection, CollaborationRights, StoredAccountAppPassword,
};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

use crate::{service::ExchangeService, store::ExchangeStore};

#[derive(Clone, Default)]
struct FakeStore {
    session: Option<AuthenticatedAccount>,
    contact_collections: Arc<Mutex<Vec<CollaborationCollection>>>,
    calendar_collections: Arc<Mutex<Vec<CollaborationCollection>>>,
    contacts: Arc<Mutex<Vec<AccessibleContact>>>,
    events: Arc<Mutex<Vec<AccessibleEvent>>>,
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
}

fn bearer_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        HeaderValue::from_static("Bearer token"),
    );
    headers
}

async fn response_text(response: axum::response::Response) -> String {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[tokio::test]
async fn find_folder_lists_contact_and_calendar_folders() {
    let store = FakeStore {
        session: Some(FakeStore::account()),
        contact_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "contacts", "contacts", "Contacts",
        )])),
        calendar_collections: Arc::new(Mutex::new(vec![FakeStore::collection(
            "calendar", "calendar", "Calendar",
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
    assert!(body.contains("<m:FindFolderResponse>"));
    assert!(body.contains("<t:FolderClass>IPF.Contacts</t:FolderClass>"));
    assert!(body.contains("<t:FolderClass>IPF.Calendar</t:FolderClass>"));
}

#[tokio::test]
async fn sync_folder_items_returns_contacts_from_canonical_store() {
    let contact_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
    let collection = FakeStore::collection("contacts", "contacts", "Contacts");
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
async fn find_item_returns_calendar_items_from_canonical_store() {
    let event_id = Uuid::parse_str("cccccccc-cccc-cccc-cccc-cccccccccccc").unwrap();
    let collection = FakeStore::collection("calendar", "calendar", "Calendar");
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
