---
type: Rust Method
title: ingest_message_attachments_in_tx
resource: crates/lpe-storage/src/attachments.rs#L233-L329
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/load_account_domain_id_in_tx
  - functions/crates/lpe-storage/src/attachments/normalize_attachment_content_id
  - functions/crates/lpe-storage/src/attachments/Storage/store_attachment_blob_in_tx
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/attachments/attachment_disposition
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-storage/src/attachments/Storage/add_message_attachment
  - functions/crates/lpe-storage/src/blob_store/tests/insert_logical_message_with_attachment
  - functions/crates/lpe-storage/src/blob_store/tests/attachment_content_fetch_reads_through_blob_store_boundary
  - functions/crates/lpe-storage/src/inbound/Storage/store_inbound_message_in_tx
  - functions/crates/lpe-storage/src/message_ops/Storage/import_jmap_email
  - functions/crates/lpe-storage/src/pst/Storage/persist_pst_imported_message_in_tx
  - functions/crates/lpe-storage/src/pst/insert_message_with_attachment
  - functions/crates/lpe-storage/src/submission/Storage/save_draft_message
  - functions/crates/lpe-storage/src/submission/Storage/submit_message
---

# Signature

`pub(crate) async fn ingest_message_attachments_in_tx( &self, tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, account_id: Uuid, message_id: Uuid, attachments: &[AttachmentUploadInput], ) -> Result<Vec<Uuid>>`

# Calls

- [load_account_domain_id_in_tx](../../../../../../functions/crates/lpe-storage/src/shared/Storage/load_account_domain_id_in_tx.md)
- [normalize_attachment_content_id](../../../../../../functions/crates/lpe-storage/src/attachments/normalize_attachment_content_id.md)
- [store_attachment_blob_in_tx](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/store_attachment_blob_in_tx.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [attachment_disposition](../../../../../../functions/crates/lpe-storage/src/attachments/attachment_disposition.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [add_message_attachment](../../../../../../functions/crates/lpe-storage/src/attachments/Storage/add_message_attachment.md)
- [insert_logical_message_with_attachment](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/insert_logical_message_with_attachment.md)
- [attachment_content_fetch_reads_through_blob_store_boundary](../../../../../../functions/crates/lpe-storage/src/blob_store/tests/attachment_content_fetch_reads_through_blob_store_boundary.md)
- [store_inbound_message_in_tx](../../../../../../functions/crates/lpe-storage/src/inbound/Storage/store_inbound_message_in_tx.md)
- [import_jmap_email](../../../../../../functions/crates/lpe-storage/src/message_ops/Storage/import_jmap_email.md)
- [persist_pst_imported_message_in_tx](../../../../../../functions/crates/lpe-storage/src/pst/Storage/persist_pst_imported_message_in_tx.md)
- [insert_message_with_attachment](../../../../../../functions/crates/lpe-storage/src/pst/insert_message_with_attachment.md)
- [save_draft_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/save_draft_message.md)
- [submit_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/submit_message.md)