---
type: Rust Function
title: response_entry
resource: crates/lpe-dav/src/responses.rs#L4-L6
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-dav/src/propfind/root_propfind_entry
  - functions/crates/lpe-dav/src/propfind/principal_propfind_entry
  - functions/crates/lpe-dav/src/propfind/addressbook_collection_entry
  - functions/crates/lpe-dav/src/propfind/task_collection_entry
  - functions/crates/lpe-dav/src/propfind/calendar_collection_entry
  - functions/crates/lpe-dav/src/propfind/collection_home_entry
  - functions/crates/lpe-dav/src/propfind/contact_resource_entry
  - functions/crates/lpe-dav/src/propfind/event_resource_entry
  - functions/crates/lpe-dav/src/propfind/task_resource_entry
  - functions/crates/lpe-dav/src/propfind/contact_report_entry
  - functions/crates/lpe-dav/src/propfind/task_report_entry
  - functions/crates/lpe-dav/src/propfind/event_report_entry
---

# Signature

`pub(crate) fn response_entry(href: &str, propstat: String) -> String`

# Called by

- [root_propfind_entry](../../../../../functions/crates/lpe-dav/src/propfind/root_propfind_entry.md)
- [principal_propfind_entry](../../../../../functions/crates/lpe-dav/src/propfind/principal_propfind_entry.md)
- [addressbook_collection_entry](../../../../../functions/crates/lpe-dav/src/propfind/addressbook_collection_entry.md)
- [task_collection_entry](../../../../../functions/crates/lpe-dav/src/propfind/task_collection_entry.md)
- [calendar_collection_entry](../../../../../functions/crates/lpe-dav/src/propfind/calendar_collection_entry.md)
- [collection_home_entry](../../../../../functions/crates/lpe-dav/src/propfind/collection_home_entry.md)
- [contact_resource_entry](../../../../../functions/crates/lpe-dav/src/propfind/contact_resource_entry.md)
- [event_resource_entry](../../../../../functions/crates/lpe-dav/src/propfind/event_resource_entry.md)
- [task_resource_entry](../../../../../functions/crates/lpe-dav/src/propfind/task_resource_entry.md)
- [contact_report_entry](../../../../../functions/crates/lpe-dav/src/propfind/contact_report_entry.md)
- [task_report_entry](../../../../../functions/crates/lpe-dav/src/propfind/task_report_entry.md)
- [event_report_entry](../../../../../functions/crates/lpe-dav/src/propfind/event_report_entry.md)