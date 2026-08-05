---
type: Rust Method
title: advance_calendar_event_version_in_tx
resource: crates/lpe-storage/src/mapi_events.rs#L701-L720
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/mapi_events/Storage/advance_mapi_event_version_for_lifecycle_in_tx
  called_by:
  - functions/crates/lpe-storage/src/attachments/Storage/add_calendar_event_attachment
  - functions/crates/lpe-storage/src/attachments/Storage/delete_calendar_event_attachment
  - functions/crates/lpe-storage/src/collaboration/Storage/update_accessible_event_reminder
  - functions/crates/lpe-storage/src/mapi_events/Storage/move_calendar_events_to_collection_in_tx
  - functions/crates/lpe-storage/src/workspace/Storage/upsert_client_event_in_calendar
---

# Signature

`pub(crate) async fn advance_calendar_event_version_in_tx( &self, tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, owner_account_id: Uuid, event_id: Uuid, modseq: i64, ) -> Result<Vec<EventIdentityVersion>>`

# Calls

- [advance_mapi_event_version_for_lifecycle_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/advance_mapi_event_version_for_lifecycle_in_tx.md)

# Called by

- [add_calendar_event_attachment](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/add_calendar_event_attachment.md)
- [delete_calendar_event_attachment](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/delete_calendar_event_attachment.md)
- [update_accessible_event_reminder](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/update_accessible_event_reminder.md)
- [move_calendar_events_to_collection_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/move_calendar_events_to_collection_in_tx.md)
- [upsert_client_event_in_calendar](../../../../../../functions/crates/lpe-storage/src/workspace/Storage/upsert_client_event_in_calendar.md)