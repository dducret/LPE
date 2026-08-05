---
type: Rust Method
title: fetch_activesync_message_attachments
resource: crates/lpe-storage/src/activesync.rs#L512-L574
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/stat_durable_blob
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`pub async fn fetch_activesync_message_attachments( &self, account_id: Uuid, message_id: Uuid, ) -> Result<Vec<ActiveSyncAttachment>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [stat_durable_blob](../../../../../../functions/crates/lpe-storage/src/blob_store/io/PostgresBlobStore/stat_durable_blob.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)