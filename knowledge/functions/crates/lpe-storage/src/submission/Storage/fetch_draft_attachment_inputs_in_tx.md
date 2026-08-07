---
type: Rust Method
title: fetch_draft_attachment_inputs_in_tx
resource: crates/lpe-storage/src/submission.rs#L60-L114
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/read_durable_blob
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-storage/src/submission/Storage/submit_message
---

# Signature

`async fn fetch_draft_attachment_inputs_in_tx( &self, tx: &mut sqlx::Transaction<'_, Postgres>, tenant_id: &Uuid, account_id: Uuid, draft_message_id: Uuid, ) -> Result<Vec<AttachmentUploadInput>>`

# Calls

- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [read_durable_blob](../../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/read_durable_blob.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [submit_message](../../../../../../functions/crates/lpe-storage/src/submission/Storage/submit_message.md)