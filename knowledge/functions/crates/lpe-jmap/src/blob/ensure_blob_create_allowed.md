---
type: Rust Function
title: ensure_blob_create_allowed
resource: crates/lpe-jmap/src/blob.rs#L393-L399
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_upload
  - functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_copy
---

# Signature

`fn ensure_blob_create_allowed(access: &MailboxAccountAccess) -> Result<()>`

# Called by

- [handle_blob_upload](../../../../../functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_upload.md)
- [handle_blob_copy](../../../../../functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_copy.md)