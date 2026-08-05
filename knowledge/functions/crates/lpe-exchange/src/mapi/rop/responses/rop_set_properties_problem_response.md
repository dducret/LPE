---
type: Rust Function
title: rop_set_properties_problem_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L406-L419
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_properties_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_delete_properties_response
---

# Signature

`pub(in crate::mapi) fn rop_set_properties_problem_response( request: &RopRequest, problems: &[(usize, u32, u32)], ) -> Vec<u8>`

# Called by

- [append_copy_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_properties_response.md)
- [append_set_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response.md)
- [append_delete_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_delete_properties_response.md)