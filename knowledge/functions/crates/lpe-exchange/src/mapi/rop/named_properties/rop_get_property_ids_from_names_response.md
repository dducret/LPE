---
type: Rust Function
title: rop_get_property_ids_from_names_response
resource: crates/lpe-exchange/src/mapi/rop/named_properties.rs#L5-L16
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_uses_full_truncated_request_ids
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_keeps_get_property_ids_frame_boundary
---

# Signature

`pub(in crate::mapi) fn rop_get_property_ids_from_names_response( request: &RopRequest, property_ids: &[u16], ) -> Vec<u8>`

# Called by

- [append_get_property_ids_from_names_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/named_properties/append_get_property_ids_from_names_response.md)
- [execute_rop_response_summary_uses_full_truncated_request_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_uses_full_truncated_request_ids.md)
- [execute_rop_response_summary_keeps_get_property_ids_frame_boundary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_keeps_get_property_ids_frame_boundary.md)