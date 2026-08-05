---
type: Rust Function
title: table_columns_are_available
resource: crates/lpe-exchange/src/mapi/tables/state.rs#L14-L53
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/state/table_sort_is_invalid
  - functions/crates/lpe-exchange/src/mapi/tables/state/table_restriction_is_invalid
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/collapse/rop_set_collapse_state_response
  - functions/crates/lpe-exchange/src/mapi/tables/controls/rop_seek_row_bookmark_response
  - functions/crates/lpe-exchange/src/mapi/tables/controls/rop_free_bookmark_response
---

# Signature

`pub(super) fn table_columns_are_available(object: &MapiObject) -> bool`

# Calls

- [table_sort_is_invalid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/state/table_sort_is_invalid.md)
- [table_restriction_is_invalid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/state/table_restriction_is_invalid.md)

# Called by

- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [rop_set_collapse_state_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collapse/rop_set_collapse_state_response.md)
- [rop_seek_row_bookmark_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/rop_seek_row_bookmark_response.md)
- [rop_free_bookmark_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/rop_free_bookmark_response.md)