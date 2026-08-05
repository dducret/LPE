---
type: Rust Function
title: rop_open_embedded_message_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L91-L108
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_typed_string
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_open_embedded_message_response
  - functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_open_embedded_message_response_includes_message_id
---

# Signature

`pub(in crate::mapi) fn rop_open_embedded_message_response( request: &RopRequest, message_id: u64, subject: &str, recipient_count: usize, ) -> Vec<u8>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [write_typed_string](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_typed_string.md)

# Called by

- [append_open_embedded_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/append_open_embedded_message_response.md)
- [microsoft_open_embedded_message_response_includes_message_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_open_embedded_message_response_includes_message_id.md)