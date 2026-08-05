---
type: Rust Function
title: find_row
resource: crates/lpe-exchange/src/mapi/tables/find.rs#L14-L39
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/find_origin
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/find_backward
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
---

# Signature

`pub(in crate::mapi) fn find_row<'a, T>( rows: &'a [&'a T], current_position: usize, request: &RopRequest, matches: impl Fn(&T) -> bool, ) -> Option<(usize, &'a T)>`

# Calls

- [find_origin](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/find_origin.md)
- [find_backward](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/find_backward.md)

# Called by

- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)