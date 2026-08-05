---
type: Rust Method
title: read_bytes
resource: crates/lpe-exchange/src/mapi/rop/buffer.rs#L40-L51
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_logon_response_rop
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/read_u64
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/read_guid_le
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/parse_execute_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/execute_success_rop_buffer
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/read_fast_transfer_property_value
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/read_fast_transfer_variable_bytes
  - functions/crates/lpe-exchange/src/mapi/nspi/parse_resolve_names_columns
  - functions/crates/lpe-exchange/src/mapi/nspi/parse_resolve_names_values
  - functions/crates/lpe-exchange/src/mapi/nspi/parse_nspi_get_prop_list_request
  - functions/crates/lpe-exchange/src/mapi/nspi/dn_to_mid/parse_dn_to_mid_names
  - functions/crates/lpe-exchange/src/mapi/nspi/property_values/parse_nspi_get_props_request
  - functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u32
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_i32
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_i64
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u16
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_read_state_changes
  - functions/crates/lpe-exchange/src/mapi/rop/parse/read_nonempty_u32_prefixed_bytes
  - functions/crates/lpe-exchange/src/mapi/rop/parse/parse_named_property
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/RopRequest/modify_recipients
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_wrapped_pending_recipient_row
  - functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request_with_logon_id
  - functions/crates/lpe-exchange/src/mapi/transport/diagnostics/summarize_connect_body
---

# Signature

`pub(in crate::mapi) fn read_bytes(&mut self, len: usize) -> Result<&'a [u8]>`

# Calls

- [get](../../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [summarize_logon_response_rop](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_logon_response_rop.md)
- [read_u64](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/read_u64.md)
- [read_guid_le](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/read_guid_le.md)
- [parse_execute_request](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/parse_execute_request.md)
- [execute_success_rop_buffer](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/execute_success_rop_buffer.md)
- [read_fast_transfer_property_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/read_fast_transfer_property_value.md)
- [read_fast_transfer_variable_bytes](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/read_fast_transfer_variable_bytes.md)
- [parse_resolve_names_columns](../../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/parse_resolve_names_columns.md)
- [parse_resolve_names_values](../../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/parse_resolve_names_values.md)
- [parse_nspi_get_prop_list_request](../../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/parse_nspi_get_prop_list_request.md)
- [parse_dn_to_mid_names](../../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/dn_to_mid/parse_dn_to_mid_names.md)
- [parse_nspi_get_props_request](../../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/property_values/parse_nspi_get_props_request.md)
- [parse_mapi_property_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value.md)
- [read_u32](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u32.md)
- [read_i32](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_i32.md)
- [read_i64](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_i64.md)
- [read_u16](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u16.md)
- [read_u8](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_u8.md)
- [import_read_state_changes](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_read_state_changes.md)
- [read_nonempty_u32_prefixed_bytes](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/read_nonempty_u32_prefixed_bytes.md)
- [parse_named_property](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/parse_named_property.md)
- [modify_recipients](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/RopRequest/modify_recipients.md)
- [parse_wrapped_pending_recipient_row](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_wrapped_pending_recipient_row.md)
- [read_rop_request_with_logon_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request_with_logon_id.md)
- [summarize_connect_body](../../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/diagnostics/summarize_connect_body.md)