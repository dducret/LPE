---
type: Rust Function
title: read_handle_table
resource: crates/lpe-exchange/src/mapi/session.rs#L1521-L1529
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_handle_table
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/rop_buffer_is_store_independent_special_folder_getprops_probe
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/parse_execute_rop_dispatch_input
  - functions/crates/lpe-exchange/src/mapi/rop/tests/invalid_input_handle_index_serializes_common_rop_error
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/plan_mapi_store_access
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/hierarchy_sync_selective_fallback_plan
---

# Signature

`pub(in crate::mapi) fn read_handle_table(handle_table: &[u8]) -> Result<Vec<u32>>`

# Called by

- [summarize_handle_table](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/diagnostics/summarize_handle_table.md)
- [rop_buffer_is_store_independent_special_folder_getprops_probe](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/rop_buffer_is_store_independent_special_folder_getprops_probe.md)
- [parse_execute_rop_dispatch_input](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/parse_execute_rop_dispatch_input.md)
- [invalid_input_handle_index_serializes_common_rop_error](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/invalid_input_handle_index_serializes_common_rop_error.md)
- [plan_mapi_store_access](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/plan_mapi_store_access.md)
- [hierarchy_sync_selective_fallback_plan](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/hierarchy_sync_selective_fallback_plan.md)