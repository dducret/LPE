---
type: Rust Method
title: load_account_domain_id_in_tx
resource: crates/lpe-storage/src/shared.rs#L396-L415
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/attachments/Storage/insert_calendar_event_attachment_in_tx
  - functions/crates/lpe-storage/src/attachments/Storage/ingest_message_attachments_in_tx
  - functions/crates/lpe-storage/src/attachments/Storage/add_calendar_event_attachment
  - functions/crates/lpe-storage/src/inbound/Storage/store_inbound_message_in_tx
  - functions/crates/lpe-storage/src/message_ops/Storage/import_jmap_email
  - functions/crates/lpe-storage/src/pst/Storage/persist_pst_imported_message_in_tx
  - functions/crates/lpe-storage/src/submission/Storage/save_draft_message
  - functions/crates/lpe-storage/src/submission/Storage/submit_message
---

# Signature

`pub(crate) async fn load_account_domain_id_in_tx( &self, tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, account_id: Uuid, ) -> Result<Uuid>`

# Called by

- [insert_calendar_event_attachment_in_tx](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/insert_calendar_event_attachment_in_tx.md)
- [ingest_message_attachments_in_tx](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/ingest_message_attachments_in_tx.md)
- [add_calendar_event_attachment](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/add_calendar_event_attachment.md)
- [store_inbound_message_in_tx](../../../../../../functions/crates/lpe-storage/src/inbound/Storage/store_inbound_message_in_tx.md)
- [import_jmap_email](../../../../../../functions/crates/lpe-storage/src/message_ops/Storage/import_jmap_email.md)
- [persist_pst_imported_message_in_tx](../../../../../../functions/crates/lpe-storage/src/pst/Storage/persist_pst_imported_message_in_tx.md)
- [save_draft_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/save_draft_message.md)
- [submit_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/submit_message.md)