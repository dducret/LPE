---
type: Rust Module
title: service
resource: crates/lpe-dav/src/service.rs#L1-L463
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/anyhow-anyhow-bail-result
  - external/axum-body-bytes-extract-state-http-headermap-method-uri-response-response-routing-any-router
  - external/lpe-mail-auth-authenticate-account-accountprincipal
  - external/lpe-storage-accessiblecontact-accessibleevent-davtask-storage
  - external/uuid-uuid
  - external/crate-parse-parse-ical-parse-vcard-parse-vtodo-paths-collection-id-from-contact-path-collection-id-from-event-path-etag-etag-for-contact-etag-for-event-etag-for-task-normalized-path-resource-id-for-contact-path-resource-id-for-event-path-resource-id-for-task-path-task-collection-id-from-path-addressbook-collection-path-addressbook-home-path-calendar-collection-path-calendar-home-path-principal-path-root-path-preconditions-check-delete-preconditions-check-write-preconditions-precondition-not-modified-propfind-addressbook-collection-entry-calendar-collection-entry-collection-home-entry-collection-resourcetype-contact-report-entry-contact-resource-entry-event-report-entry-event-resource-entry-principal-propfind-entry-root-propfind-entry-task-collection-entry-task-report-entry-task-resource-entry-report-contact-matches-report-event-matches-report-parse-report-filter-task-matches-report-responses-error-response-multistatus-response-options-response-redirect-response-status-only-status-with-etag-text-response-serialize-serialize-ical-serialize-vcard-serialize-vtodo-store-davstore
  member_of:
  - packages/crates/lpe-dav
---

# Contains

- [router](../../../../functions/crates/lpe-dav/src/service/router.md)
- [DavService](../../../../classes/crates/lpe-dav/src/service/DavService.md)
- [new](../../../../functions/crates/lpe-dav/src/service/DavService/new.md)
- [carddav_redirect](../../../../functions/crates/lpe-dav/src/service/carddav_redirect.md)
- [caldav_redirect](../../../../functions/crates/lpe-dav/src/service/caldav_redirect.md)
- [dav_handler](../../../../functions/crates/lpe-dav/src/service/dav_handler.md)
- [handle](../../../../functions/crates/lpe-dav/src/service/DavService/handle.md)
- [handle_propfind](../../../../functions/crates/lpe-dav/src/service/DavService/handle_propfind.md)
- [handle_report](../../../../functions/crates/lpe-dav/src/service/DavService/handle_report.md)
- [handle_get](../../../../functions/crates/lpe-dav/src/service/DavService/handle_get.md)
- [handle_put](../../../../functions/crates/lpe-dav/src/service/DavService/handle_put.md)
- [handle_delete](../../../../functions/crates/lpe-dav/src/service/DavService/handle_delete.md)
- [contact_for_path](../../../../functions/crates/lpe-dav/src/service/DavService/contact_for_path.md)
- [event_for_path](../../../../functions/crates/lpe-dav/src/service/DavService/event_for_path.md)
- [task_for_path](../../../../functions/crates/lpe-dav/src/service/DavService/task_for_path.md)

# Imports

- `anyhow::{anyhow, bail, Result}`
- `axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, Method, Uri},
    response::Response,
    routing::any,
    Router,
}`
- `lpe_mail_auth::{authenticate_account, AccountPrincipal}`
- `lpe_storage::{AccessibleContact, AccessibleEvent, DavTask, Storage}`
- `uuid::Uuid`
- `crate::{
    parse::{parse_ical, parse_vcard, parse_vtodo},
    paths::{
        collection_id_from_contact_path, collection_id_from_event_path, etag, etag_for_contact,
        etag_for_event, etag_for_task, normalized_path, resource_id_for_contact_path,
        resource_id_for_event_path, resource_id_for_task_path, task_collection_id_from_path,
        ADDRESSBOOK_COLLECTION_PATH, ADDRESSBOOK_HOME_PATH, CALENDAR_COLLECTION_PATH,
        CALENDAR_HOME_PATH, PRINCIPAL_PATH, ROOT_PATH,
    },
    preconditions::{
        check_delete_preconditions, check_write_preconditions, precondition_not_modified,
    },
    propfind::{
        addressbook_collection_entry, calendar_collection_entry, collection_home_entry,
        collection_resourcetype, contact_report_entry, contact_resource_entry, event_report_entry,
        event_resource_entry, principal_propfind_entry, root_propfind_entry, task_collection_entry,
        task_report_entry, task_resource_entry,
    },
    report::{
        contact_matches_report, event_matches_report, parse_report_filter, task_matches_report,
    },
    responses::{
        error_response, multistatus_response, options_response, redirect_response, status_only,
        status_with_etag, text_response,
    },
    serialize::{serialize_ical, serialize_vcard, serialize_vtodo},
    store::DavStore,
}`

# Member of

- [lpe-dav](../../../../packages/crates/lpe-dav.md)