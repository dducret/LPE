---
type: Rust Function
title: execute_rop_debug_summary_uses_output_handle_for_open_stream_response
resource: crates/lpe-exchange/src/mapi/dispatch/tests/execute.rs#L707-L750
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_buffer_with_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_request_rop_buffer
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_message_response
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_stream_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer_with_expected_handles
---

# Signature

`fn execute_rop_debug_summary_uses_output_handle_for_open_stream_response()`

# Calls

- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [rop_buffer_with_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_buffer_with_response.md)
- [summarize_request_rop_buffer](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_request_rop_buffer.md)
- [rop_open_message_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_message_response.md)
- [rop_open_stream_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_open_stream_response.md)
- [summarize_response_rop_buffer_with_expected_handles](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer_with_expected_handles.md)