---
type: Rust Function
title: strict_decode_utf16z
resource: crates/lpe-exchange/src/tests/mod.rs#L13318-L13327
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/tests/strict_record_folder_property
  - functions/crates/lpe-exchange/src/tests/read_rop_utf16z
  - functions/crates/lpe-exchange/src/tests/strict_decode_content_sync_stream
  - functions/crates/lpe-exchange/src/tests/strict_record_content_body_property
---

# Signature

`fn strict_decode_utf16z(bytes: &[u8]) -> Result<String, String>`

# Called by

- [strict_record_folder_property](../../../../../functions/crates/lpe-exchange/src/tests/strict_record_folder_property.md)
- [read_rop_utf16z](../../../../../functions/crates/lpe-exchange/src/tests/read_rop_utf16z.md)
- [strict_decode_content_sync_stream](../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_content_sync_stream.md)
- [strict_record_content_body_property](../../../../../functions/crates/lpe-exchange/src/tests/strict_record_content_body_property.md)