---
type: Rust Function
title: push_unique
resource: crates/lpe-exchange/src/mapi/store_adapter/access_plan.rs#L985-L989
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/extend_access_plan_for_request
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/hierarchy_sync_selective_fallback_plan
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/add_object_ids_for_handle
---

# Signature

`fn push_unique(values: &mut Vec<u64>, value: u64)`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [extend_access_plan_for_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/extend_access_plan_for_request.md)
- [hierarchy_sync_selective_fallback_plan](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/hierarchy_sync_selective_fallback_plan.md)
- [add_object_ids_for_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/add_object_ids_for_handle.md)