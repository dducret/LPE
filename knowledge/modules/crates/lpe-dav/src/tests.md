---
type: Rust Module
title: tests
resource: crates/lpe-dav/src/tests.rs#L1-L1514
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/crate-paths-contact-href-etag-for-event-event-href-task-href-addressbook-collection-path-calendar-home-path-default-collection-id-task-collection-prefix
  - external/crate-responses-error-response
  - external/crate-service-davservice
  - external/crate-store-davstore
  - external/anyhow-anyhow
  - external/axum-body-to-bytes
  - external/axum-http-headermap-headervalue-method-statuscode-uri
  - external/axum-response-response
  - external/lpe-mail-auth-accountauthstore
  - external/lpe-storage-serialize-calendar-participants-metadata-accessiblecontact-accessibleevent-accountlogin-authenticatedaccount-calendarorganizermetadata-calendarparticipantmetadata-calendarparticipantsmetadata-collaborationcollection-collaborationrights-davtask-upsertclientcontactinput-upsertclienteventinput-upsertclienttaskinput
  - external/std-sync-arc-mutex
  - external/uuid-uuid
  member_of:
  - packages/crates/lpe-dav
---

# Contains

- [task_collection_path](../../../../functions/crates/lpe-dav/src/tests/task_collection_path.md)
- [task_resource_path](../../../../functions/crates/lpe-dav/src/tests/task_resource_path.md)
- [FakeStore](../../../../classes/crates/lpe-dav/src/tests/FakeStore.md)
- [tenant_id](../../../../functions/crates/lpe-dav/src/tests/FakeStore/tenant_id.md)
- [full_rights](../../../../functions/crates/lpe-dav/src/tests/FakeStore/full_rights.md)
- [read_only_rights](../../../../functions/crates/lpe-dav/src/tests/FakeStore/read_only_rights.md)
- [account](../../../../functions/crates/lpe-dav/src/tests/FakeStore/account.md)
- [owned_collection](../../../../functions/crates/lpe-dav/src/tests/FakeStore/owned_collection.md)
- [contact_collection](../../../../functions/crates/lpe-dav/src/tests/FakeStore/contact_collection.md)
- [calendar_collection](../../../../functions/crates/lpe-dav/src/tests/FakeStore/calendar_collection.md)
- [task_collection](../../../../functions/crates/lpe-dav/src/tests/FakeStore/task_collection.md)
- [shared_collection](../../../../functions/crates/lpe-dav/src/tests/FakeStore/shared_collection.md)
- [shared_read_only_contact_collection](../../../../functions/crates/lpe-dav/src/tests/FakeStore/shared_read_only_contact_collection.md)
- [shared_writable_calendar_collection](../../../../functions/crates/lpe-dav/src/tests/FakeStore/shared_writable_calendar_collection.md)
- [shared_read_only_calendar_collection](../../../../functions/crates/lpe-dav/src/tests/FakeStore/shared_read_only_calendar_collection.md)
- [shared_read_only_task_collection](../../../../functions/crates/lpe-dav/src/tests/FakeStore/shared_read_only_task_collection.md)
- [accessible_contact](../../../../functions/crates/lpe-dav/src/tests/FakeStore/accessible_contact.md)
- [accessible_event](../../../../functions/crates/lpe-dav/src/tests/FakeStore/accessible_event.md)
- [task](../../../../functions/crates/lpe-dav/src/tests/FakeStore/task.md)
- [fetch_account_session](../../../../functions/crates/lpe-dav/src/tests/FakeStore/accountauthstore/fetch_account_session.md)
- [fetch_account_login](../../../../functions/crates/lpe-dav/src/tests/FakeStore/accountauthstore/fetch_account_login.md)
- [fetch_active_account_app_passwords](../../../../functions/crates/lpe-dav/src/tests/FakeStore/accountauthstore/fetch_active_account_app_passwords.md)
- [touch_account_app_password](../../../../functions/crates/lpe-dav/src/tests/FakeStore/accountauthstore/touch_account_app_password.md)
- [append_audit_event](../../../../functions/crates/lpe-dav/src/tests/FakeStore/accountauthstore/append_audit_event.md)
- [fetch_accessible_contact_collections](../../../../functions/crates/lpe-dav/src/tests/FakeStore/davstore/fetch_accessible_contact_collections.md)
- [fetch_accessible_calendar_collections](../../../../functions/crates/lpe-dav/src/tests/FakeStore/davstore/fetch_accessible_calendar_collections.md)
- [fetch_accessible_task_collections](../../../../functions/crates/lpe-dav/src/tests/FakeStore/davstore/fetch_accessible_task_collections.md)
- [fetch_accessible_contacts](../../../../functions/crates/lpe-dav/src/tests/FakeStore/davstore/fetch_accessible_contacts.md)
- [fetch_accessible_contacts_in_collection](../../../../functions/crates/lpe-dav/src/tests/FakeStore/davstore/fetch_accessible_contacts_in_collection.md)
- [fetch_accessible_events](../../../../functions/crates/lpe-dav/src/tests/FakeStore/davstore/fetch_accessible_events.md)
- [fetch_accessible_events_in_collection](../../../../functions/crates/lpe-dav/src/tests/FakeStore/davstore/fetch_accessible_events_in_collection.md)
- [fetch_dav_tasks](../../../../functions/crates/lpe-dav/src/tests/FakeStore/davstore/fetch_dav_tasks.md)
- [fetch_dav_tasks_by_ids](../../../../functions/crates/lpe-dav/src/tests/FakeStore/davstore/fetch_dav_tasks_by_ids.md)
- [create_accessible_contact](../../../../functions/crates/lpe-dav/src/tests/FakeStore/davstore/create_accessible_contact.md)
- [create_accessible_event](../../../../functions/crates/lpe-dav/src/tests/FakeStore/davstore/create_accessible_event.md)
- [update_accessible_contact](../../../../functions/crates/lpe-dav/src/tests/FakeStore/davstore/update_accessible_contact.md)
- [update_accessible_event](../../../../functions/crates/lpe-dav/src/tests/FakeStore/davstore/update_accessible_event.md)
- [upsert_dav_task](../../../../functions/crates/lpe-dav/src/tests/FakeStore/davstore/upsert_dav_task.md)
- [delete_accessible_contact](../../../../functions/crates/lpe-dav/src/tests/FakeStore/davstore/delete_accessible_contact.md)
- [delete_accessible_event](../../../../functions/crates/lpe-dav/src/tests/FakeStore/davstore/delete_accessible_event.md)
- [delete_dav_task](../../../../functions/crates/lpe-dav/src/tests/FakeStore/davstore/delete_dav_task.md)
- [bearer_headers](../../../../functions/crates/lpe-dav/src/tests/bearer_headers.md)
- [response_text](../../../../functions/crates/lpe-dav/src/tests/response_text.md)
- [propfind_lists_contact_resources](../../../../functions/crates/lpe-dav/src/tests/propfind_lists_contact_resources.md)
- [get_returns_ical_for_existing_event](../../../../functions/crates/lpe-dav/src/tests/get_returns_ical_for_existing_event.md)
- [put_upserts_contact_from_vcard](../../../../functions/crates/lpe-dav/src/tests/put_upserts_contact_from_vcard.md)
- [delete_removes_event](../../../../functions/crates/lpe-dav/src/tests/delete_removes_event.md)
- [get_returns_not_modified_when_if_none_match_matches](../../../../functions/crates/lpe-dav/src/tests/get_returns_not_modified_when_if_none_match_matches.md)
- [report_filters_collection_by_text_and_href](../../../../functions/crates/lpe-dav/src/tests/report_filters_collection_by_text_and_href.md)
- [put_rejects_stale_if_match](../../../../functions/crates/lpe-dav/src/tests/put_rejects_stale_if_match.md)
- [put_parses_structured_calendar_metadata](../../../../functions/crates/lpe-dav/src/tests/put_parses_structured_calendar_metadata.md)
- [get_serializes_organizer_and_participant_status](../../../../functions/crates/lpe-dav/src/tests/get_serializes_organizer_and_participant_status.md)
- [propfind_lists_task_collection_and_resources](../../../../functions/crates/lpe-dav/src/tests/propfind_lists_task_collection_and_resources.md)
- [propfind_lists_shared_task_collection_with_canonical_name](../../../../functions/crates/lpe-dav/src/tests/propfind_lists_shared_task_collection_with_canonical_name.md)
- [get_returns_vtodo_for_existing_task](../../../../functions/crates/lpe-dav/src/tests/get_returns_vtodo_for_existing_task.md)
- [put_upserts_task_from_vtodo](../../../../functions/crates/lpe-dav/src/tests/put_upserts_task_from_vtodo.md)
- [delete_removes_task](../../../../functions/crates/lpe-dav/src/tests/delete_removes_task.md)
- [put_returns_forbidden_for_read_only_shared_task_collection](../../../../functions/crates/lpe-dav/src/tests/put_returns_forbidden_for_read_only_shared_task_collection.md)
- [propfind_lists_shared_contact_collection_with_read_only_privileges](../../../../functions/crates/lpe-dav/src/tests/propfind_lists_shared_contact_collection_with_read_only_privileges.md)
- [report_filters_shared_contact_collection_by_shared_href](../../../../functions/crates/lpe-dav/src/tests/report_filters_shared_contact_collection_by_shared_href.md)
- [report_filters_shared_calendar_collection_by_shared_href](../../../../functions/crates/lpe-dav/src/tests/report_filters_shared_calendar_collection_by_shared_href.md)
- [put_returns_forbidden_for_read_only_shared_calendar_collection](../../../../functions/crates/lpe-dav/src/tests/put_returns_forbidden_for_read_only_shared_calendar_collection.md)
- [delete_returns_forbidden_for_read_only_shared_task](../../../../functions/crates/lpe-dav/src/tests/delete_returns_forbidden_for_read_only_shared_task.md)

# Imports

- `crate::paths::{
    contact_href, etag_for_event, event_href, task_href, ADDRESSBOOK_COLLECTION_PATH,
    CALENDAR_HOME_PATH, DEFAULT_COLLECTION_ID, TASK_COLLECTION_PREFIX,
}`
- `crate::responses::error_response`
- `crate::service::DavService`
- `crate::store::DavStore`
- `anyhow::anyhow`
- `axum::body::to_bytes`
- `axum::http::{HeaderMap, HeaderValue, Method, StatusCode, Uri}`
- `axum::response::Response`
- `lpe_mail_auth::AccountAuthStore`
- `lpe_storage::{
    serialize_calendar_participants_metadata, AccessibleContact, AccessibleEvent, AccountLogin,
    AuthenticatedAccount, CalendarOrganizerMetadata, CalendarParticipantMetadata,
    CalendarParticipantsMetadata, CollaborationCollection, CollaborationRights, DavTask,
    UpsertClientContactInput, UpsertClientEventInput, UpsertClientTaskInput,
}`
- `std::sync::{Arc, Mutex}`
- `uuid::Uuid`

# Member of

- [lpe-dav](../../../../packages/crates/lpe-dav.md)