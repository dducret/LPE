---
type: Rust Function
title: rop_get_collapse_state_response
resource: crates/lpe-exchange/src/mapi/tables/collapse.rs#L131-L173
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u64
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/collapse_state_row_id
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/collapse_state_row_instance_number
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_collapse_state_success_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/get_collapse_state_response
  - functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_table_bookmark_and_collapse_rops_require_set_columns
---

# Signature

`pub(in crate::mapi) fn rop_get_collapse_state_response( request: &RopRequest, object: Option<&MapiObject>, ) -> Vec<u8>`

# Calls

- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [write_u64](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u64.md)
- [collapse_state_row_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/collapse_state_row_id.md)
- [collapse_state_row_instance_number](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/collapse_state_row_instance_number.md)
- [write_u16](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16.md)
- [rop_get_collapse_state_success_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_collapse_state_success_response.md)

# Called by

- [get_collapse_state_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/get_collapse_state_response.md)
- [microsoft_table_bookmark_and_collapse_rops_require_set_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_table_bookmark_and_collapse_rops_require_set_columns.md)