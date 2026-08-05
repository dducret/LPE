---
type: Rust Function
title: rop_open_folder_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L25-L31
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_decodes_ids_and_return_codes
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_uses_output_handle_for_open_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_keeps_get_address_types_frame_boundary
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_keeps_get_property_ids_frame_boundary
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/first_post_hierarchy_probe_summary_identifies_open_folder_and_getprops_shapes
  - functions/crates/lpe-exchange/src/mapi/rop/tests/backoff_response_matches_microsoft_targeted_rop_example
---

# Signature

`pub(in crate::mapi) fn rop_open_folder_response(request: &RopRequest, is_ghosted: bool) -> Vec<u8>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_open_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response.md)
- [execute_rop_debug_summary_decodes_ids_and_return_codes](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_decodes_ids_and_return_codes.md)
- [execute_rop_debug_summary_uses_output_handle_for_open_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_uses_output_handle_for_open_folder_response.md)
- [execute_rop_response_summary_keeps_get_address_types_frame_boundary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_keeps_get_address_types_frame_boundary.md)
- [execute_rop_response_summary_keeps_get_property_ids_frame_boundary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_keeps_get_property_ids_frame_boundary.md)
- [first_post_hierarchy_probe_summary_identifies_open_folder_and_getprops_shapes](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/first_post_hierarchy_probe_summary_identifies_open_folder_and_getprops_shapes.md)
- [backoff_response_matches_microsoft_targeted_rop_example](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/backoff_response_matches_microsoft_targeted_rop_example.md)