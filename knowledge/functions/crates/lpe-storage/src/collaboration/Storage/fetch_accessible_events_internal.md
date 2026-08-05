---
type: Rust Method
title: fetch_accessible_events_internal
resource: crates/lpe-storage/src/collaboration.rs#L1338-L1478
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/collaboration/Storage/resolve_collection_access
  - functions/crates/lpe-storage/src/collaboration/types/calendar_collection_id_for_event
  called_by:
  - functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_events
  - functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_events_by_ids
  - functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_events_in_collection
  - functions/crates/lpe-storage/src/collaboration/deleted_events/Storage/fetch_accessible_deleted_events
---

# Signature

`async fn fetch_accessible_events_internal( &self, principal_account_id: Uuid, collection_id: Option<&str>, ids: Option<&[Uuid]>, lifecycle_state: &str, ) -> Result<Vec<AccessibleEvent>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [resolve_collection_access](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/resolve_collection_access.md)
- [calendar_collection_id_for_event](../../../../../../functions/crates/lpe-storage/src/collaboration/types/calendar_collection_id_for_event.md)

# Called by

- [fetch_accessible_events](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_events.md)
- [fetch_accessible_events_by_ids](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_events_by_ids.md)
- [fetch_accessible_events_in_collection](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_events_in_collection.md)
- [fetch_accessible_deleted_events](../../../../../../functions/crates/lpe-storage/src/collaboration/deleted_events/Storage/fetch_accessible_deleted_events.md)