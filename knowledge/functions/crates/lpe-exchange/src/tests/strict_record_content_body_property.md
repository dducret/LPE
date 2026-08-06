---
type: Rust Function
title: strict_record_content_body_property
resource: crates/lpe-exchange/src/tests/mod.rs#L14276-L14319
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/strict_decode_u32_property
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/tests/strict_decode_utf16z
  - functions/crates/lpe-exchange/src/tests/strict_decode_string8z
  - functions/crates/lpe-exchange/src/tests/strict_decode_i32_property
  called_by:
  - functions/crates/lpe-exchange/src/tests/strict_decode_content_sync_stream
---

# Signature

`fn strict_record_content_body_property( message: &mut StrictContentMessageBuilder, property: StrictFastTransferProperty, ) -> Result<(), String>`

# Calls

- [strict_decode_u32_property](../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_u32_property.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [strict_decode_utf16z](../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_utf16z.md)
- [strict_decode_string8z](../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_string8z.md)
- [strict_decode_i32_property](../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_i32_property.md)

# Called by

- [strict_decode_content_sync_stream](../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_content_sync_stream.md)