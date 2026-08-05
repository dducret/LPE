---
type: Rust Function
title: append_get_properties_list_response
resource: crates/lpe-exchange/src/mapi/dispatch/properties.rs#L496-L506
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_properties_list_response
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_dispatch/append_property_dispatch_response
---

# Signature

`pub(super) fn append_get_properties_list_response( session: &MapiSession, handle_slots: &[u32], request: &RopRequest, responses: &mut Vec<u8>, )`

# Calls

- [rop_get_properties_list_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_properties_list_response.md)
- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)

# Called by

- [append_property_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_dispatch/append_property_dispatch_response.md)