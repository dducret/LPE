---
type: Rust Function
title: execute_rop_response_summary_uses_full_truncated_request_ids
resource: crates/lpe-exchange/src/mapi/dispatch/tests/execute.rs#L729-L761
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_buffer_with_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_request_rop_buffer
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/rpc_header_ext_rop_buffer
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_buffer_with_response_spec
  - functions/crates/lpe-exchange/src/mapi/rop/named_properties/rop_get_property_ids_from_names_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer
---

# Signature

`fn execute_rop_response_summary_uses_full_truncated_request_ids()`

# Calls

- [push](../../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [rop_buffer_with_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_buffer_with_response.md)
- [summarize_request_rop_buffer](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_request_rop_buffer.md)
- [rpc_header_ext_rop_buffer](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/rpc_header_ext_rop_buffer.md)
- [rop_buffer_with_response_spec](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_buffer_with_response_spec.md)
- [rop_get_property_ids_from_names_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/named_properties/rop_get_property_ids_from_names_response.md)
- [summarize_response_rop_buffer](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_response_rop_buffer.md)