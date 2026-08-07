---
type: Rust Method
title: fetch_calendar_event_attachments_in_tx
resource: crates/lpe-storage/src/attachments.rs#L166-L200
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  called_by:
  - functions/crates/lpe-storage/src/attachments/Storage/apply_mapi_event_attachment_changes_in_tx
---

# Signature

`pub(crate) async fn fetch_calendar_event_attachments_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, owner_account_id: Uuid, event_id: Uuid, ) -> Result<Vec<CalendarEventAttachment>>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)

# Called by

- [apply_mapi_event_attachment_changes_in_tx](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/apply_mapi_event_attachment_changes_in_tx.md)