---
type: Rust Function
title: rop_set_collapse_state_response
resource: crates/lpe-exchange/src/mapi/tables/collapse.rs#L175-L236
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/tables/state/table_columns_are_available
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/collapse_state
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/tables/collapse/read_u64_from
  - functions/crates/lpe-exchange/src/mapi/tables/collapse/read_u32_from
  - functions/crates/lpe-exchange/src/mapi/tables/collapse/read_u16_from
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_set_collapse_state_success_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/set_collapse_state_response
  - functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_table_bookmark_and_collapse_rops_require_set_columns
---

# Signature

`pub(in crate::mapi) fn rop_set_collapse_state_response( request: &RopRequest, object: Option<&mut MapiObject>, ) -> Vec<u8>`

# Calls

- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [table_columns_are_available](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/state/table_columns_are_available.md)
- [collapse_state](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/collapse_state.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [read_u64_from](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collapse/read_u64_from.md)
- [read_u32_from](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collapse/read_u32_from.md)
- [read_u16_from](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collapse/read_u16_from.md)
- [rop_set_collapse_state_success_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_set_collapse_state_success_response.md)

# Called by

- [set_collapse_state_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/set_collapse_state_response.md)
- [microsoft_table_bookmark_and_collapse_rops_require_set_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_table_bookmark_and_collapse_rops_require_set_columns.md)