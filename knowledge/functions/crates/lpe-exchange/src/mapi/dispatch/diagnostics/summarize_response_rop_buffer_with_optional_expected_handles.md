---
type: Rust Function
title: summarize_response_rop_buffer_with_optional_expected_handles
resource: crates/lpe-exchange/src/mapi/dispatch/diagnostics.rs#L478-L556
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/is_rpc_header_ext_rop_buffer
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/rop_buffer_layout_name
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/rop_buffer_size_word
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_handle_table
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/rop_has_no_response
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/next_response_rop_start_from
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/read_response_error_code
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/response_rop_frame_end
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_frame
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/rop_ids_csv
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/rop_names_csv
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer_with_expected_handles
---

# Signature

`fn summarize_response_rop_buffer_with_optional_expected_handles( rop_buffer: &[u8], request_rop_ids: &[u8], expected_response_handle_indexes: Option<&[Option<u8>]>, ) -> RopResponseDebugSummary`

# Calls

- [is_rpc_header_ext_rop_buffer](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/is_rpc_header_ext_rop_buffer.md)
- [rop_buffer_layout_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/rop_buffer_layout_name.md)
- [rop_buffer_size_word](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/rop_buffer_size_word.md)
- [summarize_handle_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_handle_table.md)
- [rop_has_no_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/rop_has_no_response.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [next_response_rop_start_from](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/next_response_rop_start_from.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [read_response_error_code](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/read_response_error_code.md)
- [response_rop_frame_end](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/response_rop_frame_end.md)
- [summarize_response_rop_frame](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_frame.md)
- [rop_ids_csv](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/rop_ids_csv.md)
- [rop_names_csv](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/rop_names_csv.md)

# Called by

- [summarize_response_rop_buffer](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer.md)
- [summarize_response_rop_buffer_with_expected_handles](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer_with_expected_handles.md)