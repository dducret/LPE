---
type: Rust Function
title: execute_rop_response_summary_does_not_treat_find_row_payload_as_next_rop
resource: crates/lpe-exchange/src/mapi/dispatch/tests/execute.rs#L1055-L1094
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_contents_table_response
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_set_columns_response
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/rpc_header_ext_rop_buffer
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_buffer_with_response_spec
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer
---

# Signature

`fn execute_rop_response_summary_does_not_treat_find_row_payload_as_next_rop()`

# Calls

- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [rop_get_contents_table_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_contents_table_response.md)
- [rop_set_columns_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_set_columns_response.md)
- [rpc_header_ext_rop_buffer](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/rpc_header_ext_rop_buffer.md)
- [rop_buffer_with_response_spec](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_buffer_with_response_spec.md)
- [summarize_response_rop_buffer](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer.md)