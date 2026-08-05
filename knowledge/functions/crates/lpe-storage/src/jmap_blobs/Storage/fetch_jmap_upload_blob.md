---
type: Rust Method
title: fetch_jmap_upload_blob
resource: crates/lpe-storage/src/jmap_blobs.rs#L262-L291
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
---

# Signature

`pub async fn fetch_jmap_upload_blob( &self, account_id: Uuid, blob_id: Uuid, ) -> Result<Option<JmapUploadBlob>>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)