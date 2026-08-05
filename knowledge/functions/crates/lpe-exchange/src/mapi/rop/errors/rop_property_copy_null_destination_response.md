---
type: Rust Function
title: rop_property_copy_null_destination_response
resource: crates/lpe-exchange/src/mapi/rop/errors.rs#L55-L63
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/output_handle_index
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_copy_to_null_destination_response
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_copy_properties_null_destination_response
---

# Signature

`fn rop_property_copy_null_destination_response(rop_id: u8, request: &RopRequest) -> Vec<u8>`

# Calls

- [output_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/output_handle_index.md)

# Called by

- [rop_copy_to_null_destination_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_copy_to_null_destination_response.md)
- [rop_copy_properties_null_destination_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_copy_properties_null_destination_response.md)