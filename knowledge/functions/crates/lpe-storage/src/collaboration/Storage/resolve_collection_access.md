---
type: Rust Method
title: resolve_collection_access
resource: crates/lpe-storage/src/collaboration.rs#L1178-L1192
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_collections
  called_by:
  - functions/crates/lpe-storage/src/collaboration/Storage/create_accessible_calendar_collection
  - functions/crates/lpe-storage/src/collaboration/Storage/update_accessible_calendar_collection
  - functions/crates/lpe-storage/src/collaboration/Storage/delete_accessible_calendar_collection
  - functions/crates/lpe-storage/src/collaboration/Storage/create_accessible_contact
  - functions/crates/lpe-storage/src/collaboration/Storage/create_accessible_event
  - functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_contacts_internal
  - functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_events_internal
---

# Signature

`async fn resolve_collection_access( &self, principal_account_id: Uuid, kind: CollaborationResourceKind, collection_id: &str, ) -> Result<CollaborationCollection>`

# Calls

- [fetch_accessible_collections](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_collections.md)

# Called by

- [create_accessible_calendar_collection](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/create_accessible_calendar_collection.md)
- [update_accessible_calendar_collection](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/update_accessible_calendar_collection.md)
- [delete_accessible_calendar_collection](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/delete_accessible_calendar_collection.md)
- [create_accessible_contact](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/create_accessible_contact.md)
- [create_accessible_event](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/create_accessible_event.md)
- [fetch_accessible_contacts_internal](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_contacts_internal.md)
- [fetch_accessible_events_internal](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_events_internal.md)