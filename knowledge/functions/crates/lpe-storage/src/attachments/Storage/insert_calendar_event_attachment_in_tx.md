---
type: Rust Method
title: insert_calendar_event_attachment_in_tx
resource: crates/lpe-storage/src/attachments.rs#L50-L108
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/load_account_domain_id_in_tx
  - functions/crates/lpe-storage/src/attachments/Storage/store_attachment_blob_in_tx
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/attachments/attachment_disposition
  - functions/crates/lpe-storage/src/attachments/normalize_attachment_content_id
  - functions/crates/lpe-storage/src/attachments/calendar_attachment_file_reference
  called_by:
  - functions/crates/lpe-storage/src/attachments/Storage/apply_mapi_event_attachment_changes_in_tx
---

# Signature

`pub(crate) async fn insert_calendar_event_attachment_in_tx( &self, tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, owner_account_id: Uuid, calendar_id: Uuid, event_id: Uuid, ordinal: i32, attachment: &AttachmentUploadInput, ) -> Result<CalendarEventAttachment>`

# Calls

- [load_account_domain_id_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/load_account_domain_id_in_tx.md)
- [store_attachment_blob_in_tx](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/store_attachment_blob_in_tx.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [attachment_disposition](../../../../../../functions/crates/lpe-storage/src/attachments/attachment_disposition.md)
- [normalize_attachment_content_id](../../../../../../functions/crates/lpe-storage/src/attachments/normalize_attachment_content_id.md)
- [calendar_attachment_file_reference](../../../../../../functions/crates/lpe-storage/src/attachments/calendar_attachment_file_reference.md)

# Called by

- [apply_mapi_event_attachment_changes_in_tx](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/apply_mapi_event_attachment_changes_in_tx.md)