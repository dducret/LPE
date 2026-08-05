---
type: Rust Function
title: rop_copy_properties_null_destination_response
resource: crates/lpe-exchange/src/mapi/rop/errors.rs#L42-L46
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_property_copy_null_destination_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_properties_response
---

# Signature

`pub(in crate::mapi) fn rop_copy_properties_null_destination_response( request: &RopRequest, ) -> Vec<u8>`

# Calls

- [rop_property_copy_null_destination_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_property_copy_null_destination_response.md)

# Called by

- [append_copy_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_properties_response.md)