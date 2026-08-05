---
type: Rust Function
title: readable_blob_range
resource: crates/lpe-jmap/src/blob.rs#L516-L528
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  called_by:
  - functions/crates/lpe-jmap/src/blob/blob_get_object
---

# Signature

`fn readable_blob_range(bytes: &[u8], offset: usize, length: Option<u64>) -> Result<(&[u8], bool)>`

# Calls

- [try_from](../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)

# Called by

- [blob_get_object](../../../../../functions/crates/lpe-jmap/src/blob/blob_get_object.md)