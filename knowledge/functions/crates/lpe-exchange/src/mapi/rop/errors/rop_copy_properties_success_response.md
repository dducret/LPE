---
type: Rust Function
title: rop_copy_properties_success_response
resource: crates/lpe-exchange/src/mapi/rop/errors.rs#L48-L53
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_properties_response
---

# Signature

`pub(in crate::mapi) fn rop_copy_properties_success_response(request: &RopRequest) -> Vec<u8>`

# Calls

- [write_u16](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16.md)

# Called by

- [append_copy_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_properties_response.md)