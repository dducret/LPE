---
type: Rust Method
title: sync_type
resource: crates/lpe-exchange/src/mapi/rop/parse/sync_request_options.rs#L5-L7
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/hierarchy_sync_selective_fallback_plan
---

# Signature

`pub(in crate::mapi) fn sync_type(&self) -> u8`

# Called by

- [append_synchronization_configure_response](../../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_configure/append_synchronization_configure_response.md)
- [hierarchy_sync_selective_fallback_plan](../../../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/hierarchy_sync_selective_fallback_plan.md)