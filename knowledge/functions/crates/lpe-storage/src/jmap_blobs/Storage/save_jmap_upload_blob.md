---
type: Rust Method
title: save_jmap_upload_blob
resource: crates/lpe-storage/src/jmap_blobs.rs#L221-L260
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id
  - functions/crates/lpe-storage/src/shared/Storage/ensure_account_exists
  - functions/crates/lpe-activesync/src/tests/query
  - functions/crates/lpe-domain/src/crypto/sha256_hex
  - functions/tools/rca_outlook_connectivity_check/execute
---

# Signature

`pub async fn save_jmap_upload_blob( &self, account_id: Uuid, media_type: &str, blob_bytes: &[u8], ) -> Result<JmapUploadBlob>`

# Calls

- [tenant_id_for_account_id](../../../../../../functions/crates/lpe-storage/src/shared/Storage/tenant_id_for_account_id.md)
- [ensure_account_exists](../../../../../../functions/crates/lpe-storage/src/shared/Storage/ensure_account_exists.md)
- [query](../../../../../../functions/crates/lpe-activesync/src/tests/query.md)
- [sha256_hex](../../../../../../functions/crates/lpe-domain/src/crypto/sha256_hex.md)
- [execute](../../../../../../functions/tools/rca_outlook_connectivity_check/execute.md)