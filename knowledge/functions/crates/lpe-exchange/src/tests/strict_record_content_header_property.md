---
type: Rust Function
title: strict_record_content_header_property
resource: crates/lpe-exchange/src/tests/mod.rs#L14194-L14233
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/tests/strict_decode_object_id_property
  - functions/crates/lpe-exchange/src/tests/strict_decode_change_number_property
  - functions/crates/lpe-exchange/src/tests/strict_decode_i32_property
  - functions/crates/lpe-exchange/src/tests/strict_decode_u64_property
  called_by:
  - functions/crates/lpe-exchange/src/tests/strict_decode_content_sync_stream
---

# Signature

`fn strict_record_content_header_property( message: &mut StrictContentMessageBuilder, property: StrictFastTransferProperty, ) -> Result<(), String>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [strict_decode_object_id_property](../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_object_id_property.md)
- [strict_decode_change_number_property](../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_change_number_property.md)
- [strict_decode_i32_property](../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_i32_property.md)
- [strict_decode_u64_property](../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_u64_property.md)

# Called by

- [strict_decode_content_sync_stream](../../../../../functions/crates/lpe-exchange/src/tests/strict_decode_content_sync_stream.md)