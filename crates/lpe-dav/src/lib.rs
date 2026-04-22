mod paths;
mod parse;
mod preconditions;
mod propfind;
mod report;
mod responses;
mod service;
mod serialize;
mod store;

pub use crate::service::router;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::paths::{DEFAULT_COLLECTION_ID, TASK_COLLECTION_PREFIX};
    use crate::store::DavStore;
    use axum::body::to_bytes;
    use axum::http::HeaderValue;
    use lpe_mail_auth::AccountAuthStore;
    use lpe_storage::{
        AccessibleContact, AccessibleEvent, AccountLogin, AuthenticatedAccount, ClientContact,
        ClientEvent, CollaborationCollection, CollaborationRights,
    };
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

    fn task_collection_path(collection_id: &str) -> String {
        format!("/dav/calendars/me/{TASK_COLLECTION_PREFIX}{collection_id}/")
    }

    fn task_resource_path(collection_id: &str, task_id: Uuid) -> String {
        format!("{}{}.ics", task_collection_path(collection_id), task_id)
    }

    #[derive(Clone, Default)]
    struct FakeStore {
        session: Option<AuthenticatedAccount>,
        login: Option<AccountLogin>,
        contacts: Arc<Mutex<Vec<ClientContact>>>,
        events: Arc<Mutex<Vec<ClientEvent>>>,
        task_collections: Arc<Mutex<Vec<CollaborationCollection>>>,
        tasks: Arc<Mutex<Vec<DavTask>>>,
    }

    impl FakeStore {
        fn full_rights() -> CollaborationRights {
            CollaborationRights {
                may_read: true,
                may_write: true,
                may_delete: true,
                may_share: true,
            }
        }

        fn read_only_rights() -> CollaborationRights {
            CollaborationRights {
                may_read: true,
                may_write: false,
                may_delete: false,
                may_share: false,
            }
        }

        fn contact_collection() -> CollaborationCollection {
            let account = Self::account();
            CollaborationCollection {
                id: DEFAULT_COLLECTION_ID.to_string(),
                kind: "contacts".to_string(),
                owner_account_id: account.account_id,
                owner_email: account.email.clone(),
                owner_display_name: account.display_name.clone(),
                display_name: "Contacts".to_string(),
                is_owned: true,
                rights: Self::full_rights(),
            }
        }

        fn calendar_collection() -> CollaborationCollection {
            let account = Self::account();
            CollaborationCollection {
                id: DEFAULT_COLLECTION_ID.to_string(),
                kind: "calendar".to_string(),
                owner_account_id: account.account_id,
                owner_email: account.email.clone(),
                owner_display_name: account.display_name.clone(),
                display_name: "Calendar".to_string(),
                is_owned: true,
                rights: Self::full_rights(),
            }
        }

        fn task_collection() -> CollaborationCollection {
            let account = Self::account();
            let task_list_id = Uuid::nil().to_string();
            CollaborationCollection {
                id: task_list_id,
                kind: "tasks".to_string(),
                owner_account_id: account.account_id,
                owner_email: account.email.clone(),
                owner_display_name: account.display_name.clone(),
                display_name: "Tasks".to_string(),
                is_owned: true,
                rights: Self::full_rights(),
            }
        }

        fn shared_read_only_task_collection() -> CollaborationCollection {
            let owner_account_id = Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap();
            CollaborationCollection {
                id: Uuid::parse_str("90909090-9090-9090-9090-909090909090")
                    .unwrap()
                    .to_string(),
                kind: "tasks".to_string(),
                owner_account_id,
                owner_email: "owner@example.test".to_string(),
                owner_display_name: "Owner Example".to_string(),
                display_name: "Shared Ops".to_string(),
                is_owned: false,
                rights: Self::read_only_rights(),
            }
        }

        fn accessible_contact(contact: ClientContact) -> AccessibleContact {
            let account = Self::account();
            AccessibleContact {
                id: contact.id,
                collection_id: DEFAULT_COLLECTION_ID.to_string(),
                owner_account_id: account.account_id,
                owner_email: account.email.clone(),
                owner_display_name: account.display_name.clone(),
                rights: Self::full_rights(),
                name: contact.name,
                role: contact.role,
                email: contact.email,
                phone: contact.phone,
                team: contact.team,
                notes: contact.notes,
            }
        }

        fn accessible_event(event: ClientEvent) -> AccessibleEvent {
            let account = Self::account();
            AccessibleEvent {
                id: event.id,
                collection_id: DEFAULT_COLLECTION_ID.to_string(),
                owner_account_id: account.account_id,
                owner_email: account.email.clone(),
                owner_display_name: account.display_name.clone(),
                rights: Self::full_rights(),
                date: event.date,
                time: event.time,
                time_zone: event.time_zone,
                duration_minutes: event.duration_minutes,
                recurrence_rule: event.recurrence_rule,
                title: event.title,
                location: event.location,
                attendees: event.attendees,
                attendees_json: event.attendees_json,
                notes: event.notes,
            }
        }

        fn task(id: Uuid, title: &str) -> DavTask {
            let account = Self::account();
            DavTask {
                id,
                task_list_id: Uuid::nil(),
                collection_id: Uuid::nil().to_string(),
                owner_account_id: account.account_id,
                owner_email: account.email,
                owner_display_name: "Alice".to_string(),
                rights: Self::full_rights(),
                task_list_name: "Tasks".to_string(),
                title: title.to_string(),
                description: String::new(),
                status: "needs-action".to_string(),
                due_at: None,
                completed_at: None,
                sort_order: 0,
                updated_at: "2026-04-20T09:00:00Z".to_string(),
            }
        }

        fn account() -> AuthenticatedAccount {
            AuthenticatedAccount {
                tenant_id: "tenant-a".to_string(),
                account_id: Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap(),
                email: "alice@example.test".to_string(),
                display_name: "Alice".to_string(),
                expires_at: "2026-04-19T09:00:00Z".to_string(),
            }
        }
    }

    impl AccountAuthStore for FakeStore {
        fn fetch_account_session<'a>(
            &'a self,
            token: &'a str,
        ) -> lpe_mail_auth::StoreFuture<'a, Option<AuthenticatedAccount>> {
            let session = if token == "token" {
                self.session.clone()
            } else {
                None
            };
            Box::pin(async move { Ok(session) })
        }

        fn fetch_account_login<'a>(
            &'a self,
            _email: &'a str,
        ) -> lpe_mail_auth::StoreFuture<'a, Option<AccountLogin>> {
            let login = self.login.clone();
            Box::pin(async move { Ok(login) })
        }

        fn fetch_active_account_app_passwords<'a>(
            &'a self,
            _email: &'a str,
        ) -> lpe_mail_auth::StoreFuture<'a, Vec<lpe_storage::StoredAccountAppPassword>> {
            Box::pin(async move { Ok(Vec::new()) })
        }

        fn touch_account_app_password<'a>(
            &'a self,
            _email: &'a str,
            _app_password_id: Uuid,
        ) -> lpe_mail_auth::StoreFuture<'a, ()> {
            Box::pin(async move { Ok(()) })
        }

        fn append_audit_event<'a>(
            &'a self,
            _tenant_id: &'a str,
            _entry: lpe_storage::AuditEntryInput,
        ) -> lpe_mail_auth::StoreFuture<'a, ()> {
            Box::pin(async move { Ok(()) })
        }
    }

    impl DavStore for FakeStore {
        fn fetch_accessible_contact_collections<'a>(
            &'a self,
            _principal_account_id: Uuid,
        ) -> lpe_mail_auth::StoreFuture<'a, Vec<CollaborationCollection>> {
            Box::pin(async move { Ok(vec![Self::contact_collection()]) })
        }

        fn fetch_accessible_calendar_collections<'a>(
            &'a self,
            _principal_account_id: Uuid,
        ) -> lpe_mail_auth::StoreFuture<'a, Vec<CollaborationCollection>> {
            let task_collections = self.task_collections.lock().unwrap().clone();
            Box::pin(async move {
                let mut collections = vec![Self::calendar_collection()];
                if task_collections.is_empty() {
                    collections.push(Self::task_collection());
                } else {
                    collections.extend(task_collections);
                }
                Ok(collections)
            })
        }

        fn fetch_accessible_task_collections<'a>(
            &'a self,
            _principal_account_id: Uuid,
        ) -> lpe_mail_auth::StoreFuture<'a, Vec<CollaborationCollection>> {
            let task_collections = self.task_collections.lock().unwrap().clone();
            Box::pin(async move {
                if task_collections.is_empty() {
                    Ok(vec![Self::task_collection()])
                } else {
                    Ok(task_collections)
                }
            })
        }

        fn fetch_accessible_contacts<'a>(
            &'a self,
            _principal_account_id: Uuid,
        ) -> lpe_mail_auth::StoreFuture<'a, Vec<AccessibleContact>> {
            let contacts = self
                .contacts
                .lock()
                .unwrap()
                .clone()
                .into_iter()
                .map(Self::accessible_contact)
                .collect();
            Box::pin(async move { Ok(contacts) })
        }

        fn fetch_accessible_contacts_in_collection<'a>(
            &'a self,
            principal_account_id: Uuid,
            _collection_id: &'a str,
        ) -> lpe_mail_auth::StoreFuture<'a, Vec<AccessibleContact>> {
            self.fetch_accessible_contacts(principal_account_id)
        }

        fn fetch_accessible_events<'a>(
            &'a self,
            _principal_account_id: Uuid,
        ) -> lpe_mail_auth::StoreFuture<'a, Vec<AccessibleEvent>> {
            let events = self
                .events
                .lock()
                .unwrap()
                .clone()
                .into_iter()
                .map(Self::accessible_event)
                .collect();
            Box::pin(async move { Ok(events) })
        }

        fn fetch_accessible_events_in_collection<'a>(
            &'a self,
            principal_account_id: Uuid,
            _collection_id: &'a str,
        ) -> lpe_mail_auth::StoreFuture<'a, Vec<AccessibleEvent>> {
            self.fetch_accessible_events(principal_account_id)
        }

        fn fetch_dav_tasks<'a>(
            &'a self,
            _principal_account_id: Uuid,
        ) -> lpe_mail_auth::StoreFuture<'a, Vec<DavTask>> {
            let tasks = self.tasks.lock().unwrap().clone();
            Box::pin(async move { Ok(tasks) })
        }

        fn fetch_dav_tasks_by_ids<'a>(
            &'a self,
            _principal_account_id: Uuid,
            ids: &'a [Uuid],
        ) -> lpe_mail_auth::StoreFuture<'a, Vec<DavTask>> {
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

        fn create_accessible_contact<'a>(
            &'a self,
            _principal_account_id: Uuid,
            _collection_id: Option<&'a str>,
            input: UpsertClientContactInput,
        ) -> lpe_mail_auth::StoreFuture<'a, AccessibleContact> {
            let mut contacts = self.contacts.lock().unwrap();
            let contact = ClientContact {
                id: input.id.unwrap(),
                name: input.name,
                role: input.role,
                email: input.email,
                phone: input.phone,
                team: input.team,
                notes: input.notes,
            };
            contacts.retain(|entry| entry.id != contact.id);
            contacts.push(contact.clone());
            Box::pin(async move { Ok(Self::accessible_contact(contact)) })
        }

        fn create_accessible_event<'a>(
            &'a self,
            _principal_account_id: Uuid,
            _collection_id: Option<&'a str>,
            input: UpsertClientEventInput,
        ) -> lpe_mail_auth::StoreFuture<'a, AccessibleEvent> {
            let mut events = self.events.lock().unwrap();
            let event = ClientEvent {
                id: input.id.unwrap(),
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
            events.retain(|entry| entry.id != event.id);
            events.push(event.clone());
            Box::pin(async move { Ok(Self::accessible_event(event)) })
        }

        fn update_accessible_contact<'a>(
            &'a self,
            principal_account_id: Uuid,
            contact_id: Uuid,
            mut input: UpsertClientContactInput,
        ) -> lpe_mail_auth::StoreFuture<'a, AccessibleContact> {
            input.id = Some(contact_id);
            self.create_accessible_contact(principal_account_id, Some(DEFAULT_COLLECTION_ID), input)
        }

        fn update_accessible_event<'a>(
            &'a self,
            principal_account_id: Uuid,
            event_id: Uuid,
            mut input: UpsertClientEventInput,
        ) -> lpe_mail_auth::StoreFuture<'a, AccessibleEvent> {
            input.id = Some(event_id);
            self.create_accessible_event(principal_account_id, Some(DEFAULT_COLLECTION_ID), input)
        }

        fn upsert_dav_task<'a>(
            &'a self,
            input: UpsertClientTaskInput,
        ) -> lpe_mail_auth::StoreFuture<'a, DavTask> {
            let task_collections = self.task_collections.lock().unwrap().clone();
            let mut tasks = self.tasks.lock().unwrap();
            let task_id = input.id.unwrap();
            let task_list_id = input.task_list_id.unwrap_or(Uuid::nil());
            let collection = if task_collections.is_empty() {
                Ok(Self::task_collection())
            } else {
                task_collections
                    .into_iter()
                    .find(|collection| collection.id == task_list_id.to_string())
                    .ok_or_else(|| anyhow!("task list not found"))
            };
            let existing = tasks.iter().find(|entry| entry.id == task_id).cloned();
            let result = match (collection, existing) {
                (Ok(_), Some(existing)) if !existing.rights.may_write => {
                    Err(anyhow!("write access is not granted on this task"))
                }
                (Ok(collection), None) if !collection.rights.may_write => {
                    Err(anyhow!("write access is not granted on this task list"))
                }
                (Ok(collection), existing) => {
                    let task = DavTask {
                        id: task_id,
                        task_list_id,
                        collection_id: task_list_id.to_string(),
                        owner_account_id: collection.owner_account_id,
                        owner_email: collection.owner_email,
                        owner_display_name: collection.owner_display_name,
                        rights: collection.rights,
                        task_list_name: collection.display_name,
                        title: input.title,
                        description: input.description,
                        status: if input.status.trim().is_empty() {
                            "needs-action".to_string()
                        } else {
                            input.status
                        },
                        due_at: input.due_at,
                        completed_at: match input.completed_at {
                            Some(value) if !value.trim().is_empty() => Some(value),
                            _ => None,
                        },
                        sort_order: input.sort_order,
                        updated_at: "2026-04-20T09:00:00Z".to_string(),
                    };
                    tasks.retain(|entry| entry.id != task.id);
                    if let Some(existing) = existing {
                        tasks.retain(|entry| entry.id != existing.id);
                    }
                    tasks.push(task.clone());
                    Ok(task)
                }
                (Err(error), _) => Err(error),
            };
            Box::pin(async move { result })
        }

        fn delete_accessible_contact<'a>(
            &'a self,
            _principal_account_id: Uuid,
            contact_id: Uuid,
        ) -> lpe_mail_auth::StoreFuture<'a, ()> {
            self.contacts
                .lock()
                .unwrap()
                .retain(|entry| entry.id != contact_id);
            Box::pin(async move { Ok(()) })
        }

        fn delete_accessible_event<'a>(
            &'a self,
            _principal_account_id: Uuid,
            event_id: Uuid,
        ) -> lpe_mail_auth::StoreFuture<'a, ()> {
            self.events
                .lock()
                .unwrap()
                .retain(|entry| entry.id != event_id);
            Box::pin(async move { Ok(()) })
        }

        fn delete_dav_task<'a>(
            &'a self,
            _principal_account_id: Uuid,
            task_id: Uuid,
        ) -> lpe_mail_auth::StoreFuture<'a, ()> {
            let mut tasks = self.tasks.lock().unwrap();
            let index = tasks
                .iter()
                .position(|entry| entry.id == task_id)
                .ok_or_else(|| anyhow!("task not found"));
            let result = match index {
                Ok(index) if !tasks[index].rights.may_delete => {
                    Err(anyhow!("delete access is not granted on this task"))
                }
                Ok(index) => {
                    tasks.remove(index);
                    Ok(())
                }
                Err(error) => Err(error),
            };
            Box::pin(async move { result })
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

    async fn response_text(response: Response) -> String {
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn propfind_lists_contact_resources() {
        let contact_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
        let store = FakeStore {
            session: Some(FakeStore::account()),
            contacts: Arc::new(Mutex::new(vec![ClientContact {
                id: contact_id,
                name: "Bob".to_string(),
                role: "Sales".to_string(),
                email: "bob@example.test".to_string(),
                phone: "+331234".to_string(),
                team: "North".to_string(),
                notes: "VIP".to_string(),
            }])),
            ..Default::default()
        };
        let service = DavService::new(store);
        let mut headers = bearer_headers();
        headers.insert("depth", HeaderValue::from_static("1"));

        let response = service
            .handle(
                &Method::from_bytes(b"PROPFIND").unwrap(),
                &Uri::from_static(ADDRESSBOOK_COLLECTION_PATH),
                &headers,
                &[],
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::from_u16(207).unwrap());
        let body = response_text(response).await;
        assert!(body.contains(&contact_href(DEFAULT_COLLECTION_ID, contact_id)));
        assert!(body.contains("text/vcard"));
    }

    #[tokio::test]
    async fn get_returns_ical_for_existing_event() {
        let event_id = Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();
        let store = FakeStore {
            session: Some(FakeStore::account()),
            events: Arc::new(Mutex::new(vec![ClientEvent {
                id: event_id,
                date: "2026-04-20".to_string(),
                time: "09:30".to_string(),
                time_zone: "".to_string(),
                duration_minutes: 0,
                recurrence_rule: "".to_string(),
                title: "Standup".to_string(),
                location: "Room A".to_string(),
                attendees: "alice@example.test".to_string(),
                attendees_json: "[]".to_string(),
                notes: "Daily".to_string(),
            }])),
            ..Default::default()
        };
        let service = DavService::new(store);

        let response = service
            .handle(
                &Method::GET,
                &Uri::from_static(
                    "/dav/calendars/me/default/22222222-2222-2222-2222-222222222222.ics",
                ),
                &bearer_headers(),
                &[],
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_text(response).await;
        assert!(body.contains("BEGIN:VEVENT"));
        assert!(body.contains("SUMMARY:Standup"));
    }

    #[tokio::test]
    async fn put_upserts_contact_from_vcard() {
        let contact_id = Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap();
        let store = FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        };
        let service = DavService::new(store.clone());

        let response = service
            .handle(
                &Method::PUT,
                &Uri::from_static("/dav/addressbooks/me/default/33333333-3333-3333-3333-333333333333.vcf"),
                &bearer_headers(),
                b"BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Carol\r\nEMAIL:carol@example.test\r\nTITLE:Ops\r\nEND:VCARD",
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let contacts = store.contacts.lock().unwrap();
        assert_eq!(contacts.len(), 1);
        assert_eq!(contacts[0].id, contact_id);
        assert_eq!(contacts[0].email, "carol@example.test");
    }

    #[tokio::test]
    async fn delete_removes_event() {
        let event_id = Uuid::parse_str("44444444-4444-4444-4444-444444444444").unwrap();
        let store = FakeStore {
            session: Some(FakeStore::account()),
            events: Arc::new(Mutex::new(vec![ClientEvent {
                id: event_id,
                date: "2026-04-21".to_string(),
                time: "11:00".to_string(),
                time_zone: "".to_string(),
                duration_minutes: 0,
                recurrence_rule: "".to_string(),
                title: "Review".to_string(),
                location: "".to_string(),
                attendees: "".to_string(),
                attendees_json: "[]".to_string(),
                notes: "".to_string(),
            }])),
            ..Default::default()
        };
        let service = DavService::new(store.clone());

        let response = service
            .handle(
                &Method::DELETE,
                &Uri::from_static(
                    "/dav/calendars/me/default/44444444-4444-4444-4444-444444444444.ics",
                ),
                &bearer_headers(),
                &[],
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(store.events.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn get_returns_not_modified_when_if_none_match_matches() {
        let event_id = Uuid::parse_str("55555555-5555-5555-5555-555555555555").unwrap();
        let event = ClientEvent {
            id: event_id,
            date: "2026-04-22".to_string(),
            time: "14:00".to_string(),
            time_zone: "Europe/Berlin".to_string(),
            duration_minutes: 45,
            recurrence_rule: "FREQ=WEEKLY;BYDAY=WE".to_string(),
            title: "Planning".to_string(),
            location: "Room B".to_string(),
            attendees: "Alice".to_string(),
            attendees_json: serialize_calendar_participants_metadata(
                &CalendarParticipantsMetadata {
                    organizer: Some(CalendarOrganizerMetadata {
                        email: "organizer@example.test".to_string(),
                        common_name: "Organizer".to_string(),
                    }),
                    attendees: vec![CalendarParticipantMetadata {
                        email: "alice@example.test".to_string(),
                        common_name: "Alice".to_string(),
                        role: "REQ-PARTICIPANT".to_string(),
                        partstat: "accepted".to_string(),
                        rsvp: true,
                    }],
                },
            ),
            notes: "Weekly planning".to_string(),
        };
        let store = FakeStore {
            session: Some(FakeStore::account()),
            events: Arc::new(Mutex::new(vec![event.clone()])),
            ..Default::default()
        };
        let service = DavService::new(store);
        let mut headers = bearer_headers();
        headers.insert(
            "if-none-match",
            HeaderValue::from_str(&etag_for_event(&FakeStore::accessible_event(event.clone())))
                .unwrap(),
        );

        let response = service
            .handle(
                &Method::GET,
                &Uri::from_static(
                    "/dav/calendars/me/default/55555555-5555-5555-5555-555555555555.ics",
                ),
                &headers,
                &[],
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
    }

    #[tokio::test]
    async fn report_filters_collection_by_text_and_href() {
        let first_id = Uuid::parse_str("66666666-6666-6666-6666-666666666666").unwrap();
        let second_id = Uuid::parse_str("77777777-7777-7777-7777-777777777777").unwrap();
        let store = FakeStore {
            session: Some(FakeStore::account()),
            contacts: Arc::new(Mutex::new(vec![
                ClientContact {
                    id: first_id,
                    name: "Bob Example".to_string(),
                    role: "Sales".to_string(),
                    email: "bob@example.test".to_string(),
                    phone: "".to_string(),
                    team: "".to_string(),
                    notes: "".to_string(),
                },
                ClientContact {
                    id: second_id,
                    name: "Carol Example".to_string(),
                    role: "Ops".to_string(),
                    email: "carol@example.test".to_string(),
                    phone: "".to_string(),
                    team: "".to_string(),
                    notes: "".to_string(),
                },
            ])),
            ..Default::default()
        };
        let service = DavService::new(store);
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"utf-8\"?>\
<card:addressbook-query xmlns:d=\"DAV:\" xmlns:card=\"urn:ietf:params:xml:ns:carddav\">\
<d:prop><d:getetag/><card:address-data/></d:prop>\
<card:filter><card:prop-filter name=\"FN\"><card:text-match>bob</card:text-match></card:prop-filter></card:filter>\
<d:href>{}</d:href>\
</card:addressbook-query>",
            contact_href(DEFAULT_COLLECTION_ID, first_id)
        );

        let response = service
            .handle(
                &Method::from_bytes(b"REPORT").unwrap(),
                &Uri::from_static(ADDRESSBOOK_COLLECTION_PATH),
                &bearer_headers(),
                body.as_bytes(),
            )
            .await
            .unwrap();

        let payload = response_text(response).await;
        assert!(payload.contains(&contact_href(DEFAULT_COLLECTION_ID, first_id)));
        assert!(!payload.contains(&contact_href(DEFAULT_COLLECTION_ID, second_id)));
    }

    #[tokio::test]
    async fn put_rejects_stale_if_match() {
        let contact_id = Uuid::parse_str("88888888-8888-8888-8888-888888888888").unwrap();
        let store = FakeStore {
            session: Some(FakeStore::account()),
            contacts: Arc::new(Mutex::new(vec![ClientContact {
                id: contact_id,
                name: "Dora".to_string(),
                role: "".to_string(),
                email: "dora@example.test".to_string(),
                phone: "".to_string(),
                team: "".to_string(),
                notes: "".to_string(),
            }])),
            ..Default::default()
        };
        let service = DavService::new(store);
        let mut headers = bearer_headers();
        headers.insert("if-match", HeaderValue::from_static("\"stale\""));

        let error = service
            .handle(
                &Method::PUT,
                &Uri::from_static("/dav/addressbooks/me/default/88888888-8888-8888-8888-888888888888.vcf"),
                &headers,
                b"BEGIN:VCARD\r\nVERSION:3.0\r\nFN:Dora Updated\r\nEMAIL:dora@example.test\r\nEND:VCARD",
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("precondition failed"));
    }

    #[tokio::test]
    async fn put_parses_structured_calendar_metadata() {
        let event_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").unwrap();
        let store = FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        };
        let service = DavService::new(store.clone());

        let response = service
            .handle(
                &Method::PUT,
                &Uri::from_static("/dav/calendars/me/default/99999999-9999-9999-9999-999999999999.ics"),
                &bearer_headers(),
                b"BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VEVENT\r\nUID:99999999-9999-9999-9999-999999999999\r\nDTSTART;TZID=Europe/Berlin:20260423T103000\r\nDURATION:PT45M\r\nRRULE:FREQ=WEEKLY;BYDAY=TH\r\nSUMMARY:Interop review\r\nORGANIZER;CN=Owner Example:mailto:owner@example.test\r\nATTENDEE;CN=Alice Example;ROLE=REQ-PARTICIPANT;PARTSTAT=ACCEPTED;RSVP=TRUE:mailto:alice@example.test\r\nDESCRIPTION:Calendar interop\r\nEND:VEVENT\r\nEND:VCALENDAR",
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let events = store.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id, event_id);
        assert_eq!(events[0].time_zone, "Europe/Berlin");
        assert_eq!(events[0].duration_minutes, 45);
        assert_eq!(events[0].recurrence_rule, "FREQ=WEEKLY;BYDAY=TH");
        assert_eq!(events[0].attendees, "Alice Example");
        assert!(events[0].attendees_json.contains("\"organizer\""));
        assert!(events[0].attendees_json.contains("owner@example.test"));
        assert!(events[0].attendees_json.contains("alice@example.test"));
    }

    #[tokio::test]
    async fn get_serializes_organizer_and_participant_status() {
        let event_id = Uuid::parse_str("abababab-abab-abab-abab-abababababab").unwrap();
        let store = FakeStore {
            session: Some(FakeStore::account()),
            events: Arc::new(Mutex::new(vec![ClientEvent {
                id: event_id,
                date: "2026-04-24".to_string(),
                time: "15:00".to_string(),
                time_zone: "Europe/Berlin".to_string(),
                duration_minutes: 30,
                recurrence_rule: "".to_string(),
                title: "Status review".to_string(),
                location: "".to_string(),
                attendees: "Bob".to_string(),
                attendees_json: serialize_calendar_participants_metadata(
                    &CalendarParticipantsMetadata {
                        organizer: Some(CalendarOrganizerMetadata {
                            email: "owner@example.test".to_string(),
                            common_name: "Owner Example".to_string(),
                        }),
                        attendees: vec![CalendarParticipantMetadata {
                            email: "bob@example.test".to_string(),
                            common_name: "Bob".to_string(),
                            role: "REQ-PARTICIPANT".to_string(),
                            partstat: "declined".to_string(),
                            rsvp: true,
                        }],
                    },
                ),
                notes: "".to_string(),
            }])),
            ..Default::default()
        };
        let service = DavService::new(store);

        let response = service
            .handle(
                &Method::GET,
                &Uri::from_static(
                    "/dav/calendars/me/default/abababab-abab-abab-abab-abababababab.ics",
                ),
                &bearer_headers(),
                &[],
            )
            .await
            .unwrap();

        let body = response_text(response).await;
        assert!(body.contains("ORGANIZER;CN=Owner Example:mailto:owner@example.test"));
        assert!(
            body.contains(
                "ATTENDEE;CN=Bob;ROLE=REQ-PARTICIPANT;PARTSTAT=DECLINED;RSVP=TRUE:mailto:bob@example.test"
            )
        );
    }

    #[tokio::test]
    async fn propfind_lists_task_collection_and_resources() {
        let task_id = Uuid::parse_str("12121212-1212-1212-1212-121212121212").unwrap();
        let account = FakeStore::account();
        let collection_id = Uuid::nil().to_string();
        let store = FakeStore {
            session: Some(account.clone()),
            tasks: Arc::new(Mutex::new(vec![DavTask {
                id: task_id,
                task_list_id: Uuid::nil(),
                collection_id: collection_id.clone(),
                owner_account_id: account.account_id,
                owner_email: account.email,
                owner_display_name: account.display_name,
                rights: FakeStore::full_rights(),
                task_list_name: "Tasks".to_string(),
                title: "Prepare launch".to_string(),
                description: "Review the last checklist".to_string(),
                status: "in-progress".to_string(),
                due_at: Some("2026-04-25T08:30:00Z".to_string()),
                completed_at: None,
                sort_order: 7,
                updated_at: "2026-04-20T09:00:00Z".to_string(),
            }])),
            ..Default::default()
        };
        let service = DavService::new(store);
        let mut headers = bearer_headers();
        headers.insert("depth", HeaderValue::from_static("1"));

        let response = service
            .handle(
                &Method::from_bytes(b"PROPFIND").unwrap(),
                &Uri::from_maybe_shared(task_collection_path(&collection_id)).unwrap(),
                &headers,
                &[],
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::from_u16(207).unwrap());
        let body = response_text(response).await;
        assert!(body.contains(&task_collection_path(&collection_id)));
        assert!(body.contains(&task_href(&collection_id, task_id)));
        assert!(body.contains("VTODO"));
    }

    #[tokio::test]
    async fn propfind_lists_shared_task_collection_with_canonical_name() {
        let shared_collection = FakeStore::shared_read_only_task_collection();
        let task_id = Uuid::parse_str("16161616-1616-1616-1616-161616161616").unwrap();
        let store = FakeStore {
            session: Some(FakeStore::account()),
            task_collections: Arc::new(Mutex::new(vec![shared_collection.clone()])),
            tasks: Arc::new(Mutex::new(vec![DavTask {
                id: task_id,
                task_list_id: Uuid::parse_str(&shared_collection.id).unwrap(),
                collection_id: shared_collection.id.clone(),
                owner_account_id: shared_collection.owner_account_id,
                owner_email: shared_collection.owner_email.clone(),
                owner_display_name: shared_collection.owner_display_name.clone(),
                rights: shared_collection.rights.clone(),
                task_list_name: shared_collection.display_name.clone(),
                title: "Shared task".to_string(),
                description: String::new(),
                status: "needs-action".to_string(),
                due_at: None,
                completed_at: None,
                sort_order: 0,
                updated_at: "2026-04-20T09:00:00Z".to_string(),
            }])),
            ..Default::default()
        };
        let service = DavService::new(store);
        let mut headers = bearer_headers();
        headers.insert("depth", HeaderValue::from_static("1"));

        let response = service
            .handle(
                &Method::from_bytes(b"PROPFIND").unwrap(),
                &Uri::from_static(CALENDAR_HOME_PATH),
                &headers,
                &[],
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::from_u16(207).unwrap());
        let body = response_text(response).await;
        assert!(body.contains(&task_collection_path(&shared_collection.id)));
        assert!(body.contains("<d:displayname>Shared Ops</d:displayname>"));
        assert!(!body.contains("Owner Example / Shared Ops"));
    }

    #[tokio::test]
    async fn get_returns_vtodo_for_existing_task() {
        let task_id = Uuid::parse_str("13131313-1313-1313-1313-131313131313").unwrap();
        let account = FakeStore::account();
        let collection_id = Uuid::nil().to_string();
        let store = FakeStore {
            session: Some(account.clone()),
            tasks: Arc::new(Mutex::new(vec![DavTask {
                id: task_id,
                task_list_id: Uuid::nil(),
                collection_id: collection_id.clone(),
                owner_account_id: account.account_id,
                owner_email: account.email,
                owner_display_name: account.display_name,
                rights: FakeStore::full_rights(),
                task_list_name: "Tasks".to_string(),
                title: "File quarterly report".to_string(),
                description: "Publish before the board review".to_string(),
                status: "completed".to_string(),
                due_at: Some("2026-04-30T10:00:00Z".to_string()),
                completed_at: Some("2026-04-28T16:45:00Z".to_string()),
                sort_order: 3,
                updated_at: "2026-04-20T09:00:00Z".to_string(),
            }])),
            ..Default::default()
        };
        let service = DavService::new(store);

        let response = service
            .handle(
                &Method::GET,
                &Uri::from_maybe_shared(task_resource_path(&collection_id, task_id)).unwrap(),
                &bearer_headers(),
                &[],
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_text(response).await;
        assert!(body.contains("BEGIN:VTODO"));
        assert!(body.contains("SUMMARY:File quarterly report"));
        assert!(body.contains("STATUS:COMPLETED"));
        assert!(body.contains("X-LPE-SORT-ORDER:3"));
    }

    #[tokio::test]
    async fn put_upserts_task_from_vtodo() {
        let task_id = Uuid::parse_str("14141414-1414-1414-1414-141414141414").unwrap();
        let collection_id = Uuid::nil().to_string();
        let store = FakeStore {
            session: Some(FakeStore::account()),
            ..Default::default()
        };
        let service = DavService::new(store.clone());

        let response = service
            .handle(
                &Method::PUT,
                &Uri::from_maybe_shared(task_resource_path(&collection_id, task_id)).unwrap(),
                &bearer_headers(),
                b"BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VTODO\r\nUID:14141414-1414-1414-1414-141414141414\r\nSUMMARY:Prepare migration\r\nDESCRIPTION:Freeze tenant changes before the window\r\nSTATUS:IN-PROCESS\r\nDUE:20260501T083000Z\r\nX-LPE-SORT-ORDER:5\r\nEND:VTODO\r\nEND:VCALENDAR",
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CREATED);
        let tasks = store.tasks.lock().unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].id, task_id);
        assert_eq!(tasks[0].collection_id, collection_id);
        assert_eq!(tasks[0].status, "in-progress");
        assert_eq!(tasks[0].due_at.as_deref(), Some("2026-05-01T08:30:00Z"));
        assert_eq!(tasks[0].sort_order, 5);
    }

    #[tokio::test]
    async fn delete_removes_task() {
        let task_id = Uuid::parse_str("15151515-1515-1515-1515-151515151515").unwrap();
        let collection_id = Uuid::nil().to_string();
        let store = FakeStore {
            session: Some(FakeStore::account()),
            tasks: Arc::new(Mutex::new(vec![FakeStore::task(
                task_id,
                "Retire old export",
            )])),
            ..Default::default()
        };
        let service = DavService::new(store.clone());

        let response = service
            .handle(
                &Method::DELETE,
                &Uri::from_maybe_shared(task_resource_path(&collection_id, task_id)).unwrap(),
                &bearer_headers(),
                &[],
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(store.tasks.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn put_returns_forbidden_for_read_only_shared_task_collection() {
        let shared_collection = FakeStore::shared_read_only_task_collection();
        let task_id = Uuid::parse_str("17171717-1717-1717-1717-171717171717").unwrap();
        let store = FakeStore {
            session: Some(FakeStore::account()),
            task_collections: Arc::new(Mutex::new(vec![shared_collection.clone()])),
            ..Default::default()
        };
        let service = DavService::new(store);

        let response = service
            .handle(
                &Method::PUT,
                &Uri::from_maybe_shared(task_resource_path(&shared_collection.id, task_id)).unwrap(),
                &bearer_headers(),
                b"BEGIN:VCALENDAR\r\nVERSION:2.0\r\nBEGIN:VTODO\r\nUID:17171717-1717-1717-1717-171717171717\r\nSUMMARY:Blocked update\r\nEND:VTODO\r\nEND:VCALENDAR",
            )
            .await;
        let response = error_response(response.unwrap_err());

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn delete_returns_forbidden_for_read_only_shared_task() {
        let shared_collection = FakeStore::shared_read_only_task_collection();
        let task_id = Uuid::parse_str("18181818-1818-1818-1818-181818181818").unwrap();
        let store = FakeStore {
            session: Some(FakeStore::account()),
            task_collections: Arc::new(Mutex::new(vec![shared_collection.clone()])),
            tasks: Arc::new(Mutex::new(vec![DavTask {
                id: task_id,
                task_list_id: Uuid::parse_str(&shared_collection.id).unwrap(),
                collection_id: shared_collection.id.clone(),
                owner_account_id: shared_collection.owner_account_id,
                owner_email: shared_collection.owner_email.clone(),
                owner_display_name: shared_collection.owner_display_name.clone(),
                rights: shared_collection.rights.clone(),
                task_list_name: shared_collection.display_name.clone(),
                title: "Read only".to_string(),
                description: String::new(),
                status: "needs-action".to_string(),
                due_at: None,
                completed_at: None,
                sort_order: 0,
                updated_at: "2026-04-20T09:00:00Z".to_string(),
            }])),
            ..Default::default()
        };
        let service = DavService::new(store);

        let response = service
            .handle(
                &Method::DELETE,
                &Uri::from_maybe_shared(task_resource_path(&shared_collection.id, task_id))
                    .unwrap(),
                &bearer_headers(),
                &[],
            )
            .await;
        let response = error_response(response.unwrap_err());

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }
}
