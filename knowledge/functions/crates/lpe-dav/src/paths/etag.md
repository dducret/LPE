---
type: Rust Function
title: etag
resource: crates/lpe-dav/src/paths.rs#L123-L127
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-dav/src/paths/etag_for_contact
  - functions/crates/lpe-dav/src/paths/etag_for_event
  - functions/crates/lpe-dav/src/paths/etag_for_task
  - functions/crates/lpe-dav/src/propfind/contact_resource_entry
  - functions/crates/lpe-dav/src/propfind/event_resource_entry
  - functions/crates/lpe-dav/src/propfind/task_resource_entry
  - functions/crates/lpe-dav/src/service/DavService/handle_get
---

# Signature

`pub(crate) fn etag(value: &str) -> String`

# Called by

- [etag_for_contact](../../../../../functions/crates/lpe-dav/src/paths/etag_for_contact.md)
- [etag_for_event](../../../../../functions/crates/lpe-dav/src/paths/etag_for_event.md)
- [etag_for_task](../../../../../functions/crates/lpe-dav/src/paths/etag_for_task.md)
- [contact_resource_entry](../../../../../functions/crates/lpe-dav/src/propfind/contact_resource_entry.md)
- [event_resource_entry](../../../../../functions/crates/lpe-dav/src/propfind/event_resource_entry.md)
- [task_resource_entry](../../../../../functions/crates/lpe-dav/src/propfind/task_resource_entry.md)
- [handle_get](../../../../../functions/crates/lpe-dav/src/service/DavService/handle_get.md)