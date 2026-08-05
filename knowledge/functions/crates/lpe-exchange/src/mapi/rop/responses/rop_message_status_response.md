---
type: Rust Function
title: rop_message_status_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L110-L121
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_message_status_response
  - functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_get_message_status_response_uses_set_status_opcode
---

# Signature

`pub(in crate::mapi) fn rop_message_status_response( request: &RopRequest, old_status: u32, ) -> Vec<u8>`

# Called by

- [append_message_status_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_message_status_response.md)
- [microsoft_get_message_status_response_uses_set_status_opcode](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/microsoft_get_message_status_response_uses_set_status_opcode.md)