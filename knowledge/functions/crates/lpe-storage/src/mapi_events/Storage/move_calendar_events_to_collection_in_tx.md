---
type: Rust Method
title: move_calendar_events_to_collection_in_tx
resource: crates/lpe-storage/src/mapi_events.rs#L123-L219
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx
  - functions/crates/lpe-storage/src/mapi_events/Storage/advance_calendar_event_version_in_tx
  - functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx
  called_by:
  - functions/crates/lpe-storage/src/collaboration/Storage/delete_accessible_calendar_collection
---

# Signature

`pub(crate) async fn move_calendar_events_to_collection_in_tx( &self, tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, owner_account_id: Uuid, source_calendar_id: Uuid, destination_calendar_id: Uuid, ) -> Result<()>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [allocate_account_modseq_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/allocate_account_modseq_in_tx.md)
- [advance_calendar_event_version_in_tx](../../../../../../functions/crates/lpe-storage/src/mapi_events/Storage/advance_calendar_event_version_in_tx.md)
- [insert_mail_change_log_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/insert_mail_change_log_in_tx.md)

# Called by

- [delete_accessible_calendar_collection](../../../../../../functions/crates/lpe-storage/src/collaboration/Storage/delete_accessible_calendar_collection.md)