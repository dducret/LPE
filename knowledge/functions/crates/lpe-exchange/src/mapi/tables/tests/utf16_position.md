---
type: Rust Function
title: utf16_position
resource: crates/lpe-exchange/src/mapi/tables/tests.rs#L8736-L8742
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  called_by:
  - functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_query_rows_uses_wlink_sort_order
---

# Signature

`fn utf16_position(response: &[u8], value: &str) -> Option<usize>`

# Calls

- [write_utf16z](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_utf16z.md)
- [position](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)

# Called by

- [common_views_query_rows_uses_wlink_sort_order](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/common_views_query_rows_uses_wlink_sort_order.md)