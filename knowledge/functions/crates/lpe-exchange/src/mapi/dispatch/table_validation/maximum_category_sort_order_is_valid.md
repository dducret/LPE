---
type: Rust Function
title: maximum_category_sort_order_is_valid
resource: crates/lpe-exchange/src/mapi/dispatch/table_validation.rs#L74-L88
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-core/src/sieve/Parser/next
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/sort_table_request_is_valid
---

# Signature

`fn maximum_category_sort_order_is_valid( sort_orders: &[MapiSortOrder], category_count: u16, ) -> bool`

# Calls

- [next](../../../../../../../functions/crates/lpe-core/src/sieve/Parser/next.md)

# Called by

- [sort_table_request_is_valid](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/sort_table_request_is_valid.md)