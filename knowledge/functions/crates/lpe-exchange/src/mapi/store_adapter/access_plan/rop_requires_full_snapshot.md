---
type: Rust Function
title: rop_requires_full_snapshot
resource: crates/lpe-exchange/src/mapi/store_adapter/access_plan.rs#L246-L264
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/plan_mapi_store_access
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/hierarchy_sync_selective_fallback_plan
---

# Signature

`pub(in crate::mapi) fn rop_requires_full_snapshot(rop_id: u8) -> bool`

# Called by

- [plan_mapi_store_access](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/plan_mapi_store_access.md)
- [hierarchy_sync_selective_fallback_plan](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/hierarchy_sync_selective_fallback_plan.md)