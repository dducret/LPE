---
type: Rust Function
title: write_typed_string_reduced_unicode_when_lossless
resource: crates/lpe-exchange/src/mapi/rop/buffer.rs#L140-L155
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_typed_string
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_message_response_with_named_properties
---

# Signature

`pub(in crate::mapi) fn write_typed_string_reduced_unicode_when_lossless( body: &mut Vec<u8>, value: &str, )`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [write_typed_string](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_typed_string.md)

# Called by

- [rop_open_message_response_with_named_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_message_response_with_named_properties.md)