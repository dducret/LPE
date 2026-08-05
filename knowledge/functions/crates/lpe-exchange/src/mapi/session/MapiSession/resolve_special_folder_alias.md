---
type: Rust Method
title: resolve_special_folder_alias
resource: crates/lpe-exchange/src/mapi/session.rs#L1011-L1016
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/rop_buffer_is_store_independent_special_folder_getprops_probe
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/extend_access_plan_for_request
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulate_table_access
---

# Signature

`pub(in crate::mapi) fn resolve_special_folder_alias(&self, folder_id: u64) -> u64`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [rop_buffer_is_store_independent_special_folder_getprops_probe](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/rop_buffer_is_store_independent_special_folder_getprops_probe.md)
- [append_open_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_open/append_open_folder_response.md)
- [append_synchronization_import_hierarchy_change_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import_hierarchy/append_synchronization_import_hierarchy_change_response.md)
- [extend_access_plan_for_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/extend_access_plan_for_request.md)
- [simulate_table_access](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulate_table_access.md)