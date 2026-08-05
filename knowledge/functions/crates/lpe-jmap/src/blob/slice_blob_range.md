---
type: Rust Function
title: slice_blob_range
resource: crates/lpe-jmap/src/blob.rs#L430-L447
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  called_by:
  - functions/crates/lpe-jmap/src/blob/JmapService/resolve_upload_source
---

# Signature

`fn slice_blob_range(bytes: &[u8], offset: u64, length: Option<u64>) -> Result<Vec<u8>>`

# Calls

- [try_from](../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)

# Called by

- [resolve_upload_source](../../../../../functions/crates/lpe-jmap/src/blob/JmapService/resolve_upload_source.md)