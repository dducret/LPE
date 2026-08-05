---
type: Rust Method
title: fetch_jmap_message_blob
resource: crates/lpe-storage/src/jmap_blobs.rs#L124-L171
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-storage/src/jmap_blobs/strip_protected_bcc_headers
---

# Signature

`pub async fn fetch_jmap_message_blob( &self, account_id: Uuid, message_id: Uuid, ) -> Result<Option<JmapUploadBlob>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [strip_protected_bcc_headers](../../../../../../functions/crates/lpe-storage/src/jmap_blobs/strip_protected_bcc_headers.md)