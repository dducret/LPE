---
type: Rust Method
title: ensure_default_calendar_in_tx
resource: crates/lpe-storage/src/collaboration.rs#L1521-L1542
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/collaboration/Storage/delete_accessible_calendar_collection
  - functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_collections
  - functions/crates/lpe-storage/src/collaboration/grants/Storage/upsert_collaboration_grant
  - functions/crates/lpe-storage/src/mapi_events/Storage/create_mapi_event
  - functions/crates/lpe-storage/src/workspace/Storage/upsert_client_event_in_calendar
---

# Signature

`pub(crate) async fn ensure_default_calendar_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, owner_account_id: Uuid, ) -> Result<Uuid>`

# Called by

- [delete_accessible_calendar_collection](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/delete_accessible_calendar_collection.md)
- [fetch_accessible_collections](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/fetch_accessible_collections.md)
- [upsert_collaboration_grant](../../../../../../functions/crates/lpe-storage/src/collaboration/grants/Storage/upsert_collaboration_grant.md)
- [create_mapi_event](../../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/create_mapi_event.md)
- [upsert_client_event_in_calendar](../../../../../../functions/crates/lpe-storage/src/workspace/Storage/upsert_client_event_in_calendar.md)