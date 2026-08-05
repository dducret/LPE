---
type: Rust Function
title: rop_set_properties_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L399-L404
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_to_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/first_post_hierarchy_probe_summary_identifies_set_properties_shapes
---

# Signature

`pub(in crate::mapi) fn rop_set_properties_response(request: &RopRequest) -> Vec<u8>`

# Called by

- [append_copy_to_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_copy_to_response.md)
- [append_set_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response.md)
- [first_post_hierarchy_probe_summary_identifies_set_properties_shapes](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/first_post_hierarchy_probe_summary_identifies_set_properties_shapes.md)