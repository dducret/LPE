---
type: Rust Function
title: log_mapi_store_load_step
resource: crates/lpe-exchange/src/mapi/store_adapter.rs#L957-L979
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan
---

# Signature

`fn log_mapi_store_load_step( account_id: Uuid, plan: &MapiAccessPlan, step: &'static str, item_count: usize, )`

# Called by

- [load_mapi_store_for_access_plan](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/load_mapi_store_for_access_plan.md)