---
type: Rust Function
title: table_position_mut
resource: crates/lpe-exchange/src/mapi/tables/state.rs#L72-L81
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/controls/rop_seek_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/controls/rop_seek_row_fractional_response
---

# Signature

`pub(in crate::mapi) fn table_position_mut(object: &mut MapiObject) -> Option<&mut usize>`

# Called by

- [rop_seek_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/rop_seek_row_response.md)
- [rop_seek_row_fractional_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/controls/rop_seek_row_fractional_response.md)