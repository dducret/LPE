---
type: Rust Function
title: unsupported_rop_response
resource: crates/lpe-exchange/src/mapi/rop/errors.rs#L21-L32
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_to_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_properties_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/unsupported/unsupported_known_rop_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/unsupported/unsupported_unknown_rop_response
---

# Signature

`pub(in crate::mapi) fn unsupported_rop_response(rop_id: u8, handle_index: u8) -> Vec<u8>`

# Calls

- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)

# Called by

- [append_delete_messages_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_delete_messages_response.md)
- [append_copy_to_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_to_response.md)
- [append_copy_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_properties_response.md)
- [unsupported_known_rop_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/unsupported/unsupported_known_rop_response.md)
- [unsupported_unknown_rop_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/unsupported/unsupported_unknown_rop_response.md)