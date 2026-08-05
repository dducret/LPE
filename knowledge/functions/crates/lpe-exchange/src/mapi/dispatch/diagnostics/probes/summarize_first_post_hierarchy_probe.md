---
type: Rust Function
title: summarize_first_post_hierarchy_probe
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes.rs#L4-L160
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/remaining
  - functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/typed
  - functions/crates/lpe-exchange/src/mapi/rop/parse/TypedRopRequest/rop_id
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_tags
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/set_properties_probe_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/rop_has_no_response
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_open_folder_probe_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_get_properties_probe_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_set_properties_probe_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/response_rop_frame_end
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/read_response_error_code
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute/log_execute_rop_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/first_post_hierarchy_probe_summary_identifies_open_folder_and_getprops_shapes
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/post_hierarchy_probe_summary_marks_default_folder_entry_id_getprops
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/first_post_hierarchy_probe_summary_identifies_set_properties_shapes
---

# Signature

`pub(in crate::mapi::dispatch) fn summarize_first_post_hierarchy_probe( request_rop_buffer: &[u8], response_rop_buffer: &[u8], ) -> FirstPostHierarchyProbeDebugSummary`

# Calls

- [remaining](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/remaining.md)
- [read_rop_request](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request.md)
- [typed](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/typed.md)
- [rop_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/TypedRopRequest/rop_id.md)
- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [input_handle_index](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index.md)
- [property_tags](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/property_tags.md)
- [set_properties_probe_request](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/set_properties_probe_request.md)
- [rop_has_no_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/rop_has_no_response.md)
- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [position](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)
- [summarize_open_folder_probe_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_open_folder_probe_response.md)
- [summarize_get_properties_probe_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_get_properties_probe_response.md)
- [summarize_set_properties_probe_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/probes/summarize_set_properties_probe_response.md)
- [response_rop_frame_end](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/response_rop_frame_end.md)
- [read_response_error_code](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/read_response_error_code.md)

# Called by

- [log_execute_rop_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/execute/log_execute_rop_debug.md)
- [first_post_hierarchy_probe_summary_identifies_open_folder_and_getprops_shapes](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/first_post_hierarchy_probe_summary_identifies_open_folder_and_getprops_shapes.md)
- [post_hierarchy_probe_summary_marks_default_folder_entry_id_getprops](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/post_hierarchy_probe_summary_marks_default_folder_entry_id_getprops.md)
- [first_post_hierarchy_probe_summary_identifies_set_properties_shapes](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/folders/first_post_hierarchy_probe_summary_identifies_set_properties_shapes.md)