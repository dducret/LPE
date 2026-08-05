---
type: Rust Method
title: delete_calendar_event_attachment_in_tx
resource: crates/lpe-storage/src/attachments.rs#L110-L137
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/attachments/Storage/apply_mapi_event_attachment_changes_in_tx
---

# Signature

`pub(crate) async fn delete_calendar_event_attachment_in_tx( tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, owner_account_id: Uuid, event_id: Uuid, attachment_id: Uuid, ) -> Result<()>`

# Called by

- [apply_mapi_event_attachment_changes_in_tx](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/apply_mapi_event_attachment_changes_in_tx.md)