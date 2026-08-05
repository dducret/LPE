---
type: Rust Function
title: simulate_allocate_handle
resource: crates/lpe-exchange/src/mapi/store_adapter/access_plan.rs#L700-L716
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulate_table_access
---

# Signature

`fn simulate_allocate_handle( handles: &mut HashMap<u32, MapiObject>, next_handle: &mut u32, output_handle_index: Option<u8>, object: MapiObject, ) -> u32`

# Called by

- [simulate_table_access](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulate_table_access.md)