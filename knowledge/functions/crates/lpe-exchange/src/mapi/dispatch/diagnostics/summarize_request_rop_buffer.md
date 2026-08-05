---
type: Rust Function
title: summarize_request_rop_buffer
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L388-L457
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/is_rpc_header_ext_rop_buffer
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_handle_table
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/remaining
  - functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/typed
  - functions/crates/lpe-exchange/src/mapi/rop/parse/TypedRopRequest/rop_id
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/rop_has_no_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/transport/headers/hex_preview
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/rop_ids_csv
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/rop_names_csv
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_non_release_request_rops
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_request_rop_raw_frames
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_decodes_ids_and_return_codes
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_skips_false_getprops_inside_findrow_payload
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_uses_output_handle_for_open_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_uses_output_handle_for_open_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_distinguishes_truncated_release_prefix
  - functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_uses_full_truncated_request_ids
  - functions/crates/lpe-exchange/src/mapi/transport/execute_response_trace_metadata
  - functions/crates/lpe-exchange/src/mapi/transport/execute_request_trace_metadata
---

# Signature

`pub(in crate::mapi) fn summarize_request_rop_buffer(rop_buffer: &[u8]) -> RopRequestDebugSummary`

# Calls

- [is_rpc_header_ext_rop_buffer](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/is_rpc_header_ext_rop_buffer.md)
- [summarize_handle_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_handle_table.md)
- [remaining](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/remaining.md)
- [read_rop_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request.md)
- [typed](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/typed.md)
- [rop_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/TypedRopRequest/rop_id.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [rop_has_no_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/rop_has_no_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [position](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [hex_preview](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/headers/hex_preview.md)
- [rop_ids_csv](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/rop_ids_csv.md)
- [rop_names_csv](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/rop_names_csv.md)
- [summarize_non_release_request_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_non_release_request_rops.md)
- [summarize_request_rop_raw_frames](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_request_rop_raw_frames.md)

# Called by

- [execute_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)
- [execute_rop_debug_summary_decodes_ids_and_return_codes](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_decodes_ids_and_return_codes.md)
- [execute_rop_debug_summary_skips_false_getprops_inside_findrow_payload](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_skips_false_getprops_inside_findrow_payload.md)
- [execute_rop_debug_summary_uses_output_handle_for_open_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_uses_output_handle_for_open_folder_response.md)
- [execute_rop_debug_summary_uses_output_handle_for_open_stream_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_uses_output_handle_for_open_stream_response.md)
- [execute_rop_debug_summary_distinguishes_truncated_release_prefix](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_debug_summary_distinguishes_truncated_release_prefix.md)
- [execute_rop_response_summary_uses_full_truncated_request_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tests/execute/execute_rop_response_summary_uses_full_truncated_request_ids.md)
- [execute_response_trace_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/execute_response_trace_metadata.md)
- [execute_request_trace_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/execute_request_trace_metadata.md)