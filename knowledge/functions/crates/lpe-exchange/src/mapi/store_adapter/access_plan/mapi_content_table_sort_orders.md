---
type: Rust Function
title: mapi_content_table_sort_orders
resource: crates/lpe-exchange/src/mapi/store_adapter/access_plan.rs#L809-L837
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulate_table_access
---

# Signature

`fn mapi_content_table_sort_orders( sort_orders: &[MapiSortOrder], ) -> Option<Vec<MapiContentTableSort>>`

# Called by

- [simulate_table_access](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulate_table_access.md)