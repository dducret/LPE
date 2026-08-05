---
type: Rust Method
title: emit_collaboration_change
resource: crates/lpe-storage/src/change.rs#L292-L374
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/change/Storage/emit_canonical_change
  called_by:
  - functions/crates/lpe-storage/src/attachments/Storage/add_calendar_event_attachment
  - functions/crates/lpe-storage/src/attachments/Storage/delete_calendar_event_attachment
  - functions/crates/lpe-storage/src/collaboration/Storage/create_accessible_calendar_collection
  - functions/crates/lpe-storage/src/collaboration/Storage/update_accessible_calendar_collection
  - functions/crates/lpe-storage/src/collaboration/Storage/delete_accessible_calendar_collection
  - functions/crates/lpe-storage/src/collaboration/Storage/update_accessible_event_reminder
  - functions/crates/lpe-storage/src/collaboration/deleted_events/Storage/move_accessible_event_to_deleted_items
  - functions/crates/lpe-storage/src/mapi_contacts/Storage/create_mapi_contact
  - functions/crates/lpe-storage/src/mapi_contacts/record_contact_change_in_tx
  - functions/crates/lpe-storage/src/mapi_events/Storage/create_mapi_event
  - functions/crates/lpe-storage/src/mapi_events/Storage/commit_mapi_event_update
  - functions/crates/lpe-storage/src/message_ops/Storage/delete_client_contact
  - functions/crates/lpe-storage/src/tasks/Storage/update_accessible_task_reminder
  - functions/crates/lpe-storage/src/workspace/Storage/upsert_client_contact_in_book_role
  - functions/crates/lpe-storage/src/workspace/Storage/upsert_client_event_in_calendar
---

# Signature

`pub(crate) async fn emit_collaboration_change( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, category: CanonicalChangeCategory, owner_account_id: Uuid, ) -> Result<()>`

# Calls

- [emit_canonical_change](../../../../../../functions/crates/lpe-storage/src/change/Storage/emit_canonical_change.md)

# Called by

- [add_calendar_event_attachment](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/add_calendar_event_attachment.md)
- [delete_calendar_event_attachment](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/delete_calendar_event_attachment.md)
- [create_accessible_calendar_collection](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/create_accessible_calendar_collection.md)
- [update_accessible_calendar_collection](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/update_accessible_calendar_collection.md)
- [delete_accessible_calendar_collection](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/delete_accessible_calendar_collection.md)
- [update_accessible_event_reminder](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/update_accessible_event_reminder.md)
- [move_accessible_event_to_deleted_items](../../../../../../functions/crates/lpe-storage/src/collaboration/deleted_events/Storage/move_accessible_event_to_deleted_items.md)
- [create_mapi_contact](../../../../../../functions/crates/lpe-storage/src/mapi_contacts/Storage/create_mapi_contact.md)
- [record_contact_change_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_contacts/record_contact_change_in_tx.md)
- [create_mapi_event](../../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/create_mapi_event.md)
- [commit_mapi_event_update](../../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/commit_mapi_event_update.md)
- [delete_client_contact](../../../../../../functions/crates/lpe-storage/src/message_ops/Storage/delete_client_contact.md)
- [update_accessible_task_reminder](../../../../../../functions/crates/lpe-storage/src/tasks/Storage/update_accessible_task_reminder.md)
- [upsert_client_contact_in_book_role](../../../../../../functions/crates/lpe-storage/src/workspace/Storage/upsert_client_contact_in_book_role.md)
- [upsert_client_event_in_calendar](../../../../../../functions/crates/lpe-storage/src/workspace/Storage/upsert_client_event_in_calendar.md)