---
type: Rust Function
title: supports_attachment_text_extraction
resource: crates/lpe-storage/src/attachments.rs#L1141-L1152
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-storage/src/attachments/Storage/store_attachment_blob_in_tx
---

# Signature

`pub(crate) fn supports_attachment_text_extraction(media_type: &str, file_name: &str) -> bool`

# Called by

- [store_attachment_blob_in_tx](../../../../../functions/crates/lpe-storage/src/attachments/Storage/store_attachment_blob_in_tx.md)