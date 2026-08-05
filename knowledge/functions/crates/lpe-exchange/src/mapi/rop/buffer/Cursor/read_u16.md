---
type: Rust Method
title: read_u16
resource: crates/lpe-exchange/src/mapi/rop/buffer.rs#L30-L33
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_logon_response_rop
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/set_properties_problem_details_for_debug
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/read_fast_transfer_property_value
  - functions/crates/lpe-exchange/src/mapi/nspi/parse_resolve_names_values
  - functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_utf16z
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_property_values
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_hierarchy_values
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_delete_source_keys
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_read_state_changes
  - functions/crates/lpe-exchange/src/mapi/rop/property_rows/parse_modify_rows
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/RopRequest/modify_recipients
  - functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_wrapped_pending_recipient_row
  - functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request_with_logon_id
  - functions/crates/lpe-exchange/src/mapi/rop/restrictions/parse_mapi_restriction_from
---

# Signature

`pub(in crate::mapi) fn read_u16(&mut self) -> Result<u16>`

# Calls

- [read_bytes](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_bytes.md)

# Called by

- [summarize_logon_response_rop](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_logon_response_rop.md)
- [set_properties_problem_details_for_debug](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/post_hierarchy/set_properties_problem_details_for_debug.md)
- [read_fast_transfer_property_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/read_fast_transfer_property_value.md)
- [parse_resolve_names_values](../../../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/parse_resolve_names_values.md)
- [parse_mapi_property_value](../../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/parse_mapi_property_value.md)
- [read_utf16z](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/read_utf16z.md)
- [import_property_values](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_property_values.md)
- [import_hierarchy_values](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_hierarchy_values.md)
- [import_delete_source_keys](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_delete_source_keys.md)
- [import_read_state_changes](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_read_state_changes.md)
- [parse_modify_rows](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/property_rows/parse_modify_rows.md)
- [modify_recipients](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/RopRequest/modify_recipients.md)
- [parse_wrapped_pending_recipient_row](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/recipients/parse_wrapped_pending_recipient_row.md)
- [read_rop_request_with_logon_id](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request_with_logon_id.md)
- [parse_mapi_restriction_from](../../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/restrictions/parse_mapi_restriction_from.md)