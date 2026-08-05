---
type: Rust Function
title: write_typed_string
resource: crates/lpe-exchange/src/mapi/rop/buffer.rs#L131-L138
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_typed_string_reduced_unicode_when_lossless
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_message_response_with_named_properties
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_message_response_with_recipients
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_embedded_message_response
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_reload_cached_information_response
---

# Signature

`pub(in crate::mapi) fn write_typed_string(body: &mut Vec<u8>, value: &str)`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [write_utf16z](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z.md)

# Called by

- [write_typed_string_reduced_unicode_when_lossless](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_typed_string_reduced_unicode_when_lossless.md)
- [rop_open_message_response_with_named_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_message_response_with_named_properties.md)
- [rop_open_message_response_with_recipients](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_message_response_with_recipients.md)
- [rop_open_embedded_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_embedded_message_response.md)
- [rop_reload_cached_information_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_reload_cached_information_response.md)