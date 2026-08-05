---
type: Rust Function
title: sort_table_request_is_valid
resource: crates/lpe-exchange/src/mapi/dispatch/table_validation.rs#L30-L62
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/sort_category_count
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/sort_expanded_count
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/sort_orders
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/maximum_category_sort_order_is_valid
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_sort_table_response
---

# Signature

`pub(in crate::mapi::dispatch) fn sort_table_request_is_valid(request: &RopRequest) -> bool`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [sort_category_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/sort_category_count.md)
- [sort_expanded_count](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/sort_expanded_count.md)
- [sort_orders](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/sort_orders.md)
- [maximum_category_sort_order_is_valid](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_validation/maximum_category_sort_order_is_valid.md)

# Called by

- [append_sort_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_sort_table_response.md)