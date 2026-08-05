---
type: Rust Function
title: blob_get_object
resource: crates/lpe-jmap/src/blob.rs#L449-L514
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  - functions/crates/lpe-jmap/src/blob/readable_blob_range
  called_by:
  - functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_get
---

# Signature

`fn blob_get_object( id: &str, bytes: &[u8], properties: &[String], offset: u64, length: Option<u64>, ) -> Result<Value>`

# Calls

- [try_from](../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)
- [readable_blob_range](../../../../../functions/crates/lpe-jmap/src/blob/readable_blob_range.md)

# Called by

- [handle_blob_get](../../../../../functions/crates/lpe-jmap/src/blob/JmapService/handle_blob_get.md)