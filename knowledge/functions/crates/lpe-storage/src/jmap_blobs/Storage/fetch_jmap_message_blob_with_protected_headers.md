---
type: Rust Method
title: fetch_jmap_message_blob_with_protected_headers
resource: crates/lpe-storage/src/jmap_blobs.rs#L173-L219
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/tests/query
---

# Signature

`pub async fn fetch_jmap_message_blob_with_protected_headers( &self, account_id: Uuid, message_id: Uuid, ) -> Result<Option<JmapUploadBlob>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)