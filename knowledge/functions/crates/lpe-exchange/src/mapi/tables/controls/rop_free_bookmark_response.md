---
type: Rust Function
title: rop_free_bookmark_response
resource: crates/lpe-exchange/src/mapi/tables/controls.rs#L295-L313
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  - functions/crates/lpe-exchange/src/mapi/tables/state/table_columns_are_available
  - functions/crates/lpe-exchange/src/mapi/tables/state/table_bookmark_state_mut
  - functions/LPE-CT/web/app/smoke/test/MockClassList/remove
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/bookmark
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/free_bookmark_response
  - functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_table_bookmark_and_collapse_rops_require_set_columns
---

# Signature

`pub(in crate::mapi) fn rop_free_bookmark_response( request: &RopRequest, object: Option<&mut MapiObject>, ) -> Vec<u8>`

# Calls

- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)
- [table_columns_are_available](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/state/table_columns_are_available.md)
- [table_bookmark_state_mut](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/state/table_bookmark_state_mut.md)
- [remove](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockClassList/remove.md)
- [bookmark](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/bookmark.md)

# Called by

- [free_bookmark_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/free_bookmark_response.md)
- [microsoft_table_bookmark_and_collapse_rops_require_set_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_table_bookmark_and_collapse_rops_require_set_columns.md)