---
type: Rust Module
title: propfind
resource: crates/lpe-dav/src/propfind.rs#L1-L269
generated:
  by: okf-rs/0.3.0
relationships:
  imports:
  - external/lpe-storage-accessiblecontact-accessibleevent-collaborationcollection-collaborationrights-davtask
  - external/crate-paths-contact-collection-href-contact-href-etag-event-collection-href-event-href-task-collection-href-task-href-addressbook-home-path-calendar-home-path-principal-path-root-path-responses-response-entry-serialize-serialize-ical-serialize-vcard-serialize-vtodo-xml-escape
  member_of:
  - packages/crates/lpe-dav
---

# Contains

- [root_propfind_entry](../../../../functions/crates/lpe-dav/src/propfind/root_propfind_entry.md)
- [principal_propfind_entry](../../../../functions/crates/lpe-dav/src/propfind/principal_propfind_entry.md)
- [addressbook_collection_entry](../../../../functions/crates/lpe-dav/src/propfind/addressbook_collection_entry.md)
- [task_collection_entry](../../../../functions/crates/lpe-dav/src/propfind/task_collection_entry.md)
- [calendar_collection_entry](../../../../functions/crates/lpe-dav/src/propfind/calendar_collection_entry.md)
- [collection_home_entry](../../../../functions/crates/lpe-dav/src/propfind/collection_home_entry.md)
- [contact_resource_entry](../../../../functions/crates/lpe-dav/src/propfind/contact_resource_entry.md)
- [event_resource_entry](../../../../functions/crates/lpe-dav/src/propfind/event_resource_entry.md)
- [task_resource_entry](../../../../functions/crates/lpe-dav/src/propfind/task_resource_entry.md)
- [contact_report_entry](../../../../functions/crates/lpe-dav/src/propfind/contact_report_entry.md)
- [task_report_entry](../../../../functions/crates/lpe-dav/src/propfind/task_report_entry.md)
- [event_report_entry](../../../../functions/crates/lpe-dav/src/propfind/event_report_entry.md)
- [collection_props](../../../../functions/crates/lpe-dav/src/propfind/collection_props.md)
- [collection_metadata](../../../../functions/crates/lpe-dav/src/propfind/collection_metadata.md)
- [current_user_privilege_set](../../../../functions/crates/lpe-dav/src/propfind/current_user_privilege_set.md)
- [collection_resourcetype](../../../../functions/crates/lpe-dav/src/propfind/collection_resourcetype.md)

# Imports

- `lpe_storage::{
    AccessibleContact, AccessibleEvent, CollaborationCollection, CollaborationRights, DavTask,
}`
- `crate::{
    paths::{
        contact_collection_href, contact_href, etag, event_collection_href, event_href,
        task_collection_href, task_href, ADDRESSBOOK_HOME_PATH, CALENDAR_HOME_PATH, PRINCIPAL_PATH,
        ROOT_PATH,
    },
    responses::response_entry,
    serialize::{serialize_ical, serialize_vcard, serialize_vtodo, xml_escape},
}`

# Member of

- [lpe-dav](../../../../packages/crates/lpe-dav.md)