---
type: Rust Function
title: content_query_ranges_can_merge
resource: crates/lpe-exchange/src/mapi/store_adapter/access_plan.rs#L770-L785
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/add_content_query
---

# Signature

`fn content_query_ranges_can_merge( left_offset: usize, left_limit: usize, right_offset: usize, right_limit: usize, ) -> bool`

# Called by

- [add_content_query](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/add_content_query.md)