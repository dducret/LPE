---
type: Rust Method
title: apply_mapi_event_attachment_changes_in_tx
resource: crates/lpe-storage/src/attachments.rs#L202-L258
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/attachments/Storage/insert_calendar_event_attachment_in_tx
  - functions/crates/lpe-storage/src/attachments/replace_attachment_custom_properties_in_tx
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/attachments/Storage/delete_calendar_event_attachment_in_tx
  - functions/crates/lpe-storage/src/attachments/Storage/fetch_calendar_event_attachments_in_tx
  called_by:
  - functions/crates/lpe-storage/src/mapi_events/Storage/create_mapi_event
  - functions/crates/lpe-storage/src/mapi_events/Storage/commit_mapi_event_update
---

# Signature

`pub(crate) async fn apply_mapi_event_attachment_changes_in_tx( &self, tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, owner_account_id: Uuid, calendar_id: Uuid, event_id: Uuid, changes: &MapiEventAttachmentChanges, ) -> Result<Vec<CalendarEventAttachment>>`

# Calls

- [insert_calendar_event_attachment_in_tx](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/insert_calendar_event_attachment_in_tx.md)
- [replace_attachment_custom_properties_in_tx](../../../../../../functions/crates/lpe-storage/src/attachments/replace_attachment_custom_properties_in_tx.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [delete_calendar_event_attachment_in_tx](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/delete_calendar_event_attachment_in_tx.md)
- [fetch_calendar_event_attachments_in_tx](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/fetch_calendar_event_attachments_in_tx.md)

# Called by

- [create_mapi_event](../../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/create_mapi_event.md)
- [commit_mapi_event_update](../../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/commit_mapi_event_update.md)