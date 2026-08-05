---
type: Rust Method
title: save_jmap_upload_blob
resource: crates/lpe-jmap/src/tests.rs#L1300-L1322
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`async fn save_jmap_upload_blob( &self, account_id: Uuid, media_type: &str, blob_bytes: &[u8], ) -> Result<JmapUploadBlob>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)