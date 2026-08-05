---
type: Rust Function
title: collection_metadata
resource: crates/lpe-dav/src/propfind.rs#L231-L243
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-dav/src/propfind/addressbook_collection_entry
  - functions/crates/lpe-dav/src/propfind/task_collection_entry
  - functions/crates/lpe-dav/src/propfind/calendar_collection_entry
  - functions/crates/lpe-dav/src/propfind/contact_resource_entry
  - functions/crates/lpe-dav/src/propfind/event_resource_entry
  - functions/crates/lpe-dav/src/propfind/task_resource_entry
---

# Signature

`fn collection_metadata( owner_email: &str, rights: &CollaborationRights, is_collection: bool, extra: &str, ) -> String`

# Called by

- [addressbook_collection_entry](../../../../../functions/crates/lpe-dav/src/propfind/addressbook_collection_entry.md)
- [task_collection_entry](../../../../../functions/crates/lpe-dav/src/propfind/task_collection_entry.md)
- [calendar_collection_entry](../../../../../functions/crates/lpe-dav/src/propfind/calendar_collection_entry.md)
- [contact_resource_entry](../../../../../functions/crates/lpe-dav/src/propfind/contact_resource_entry.md)
- [event_resource_entry](../../../../../functions/crates/lpe-dav/src/propfind/event_resource_entry.md)
- [task_resource_entry](../../../../../functions/crates/lpe-dav/src/propfind/task_resource_entry.md)