---
type: Rust Function
title: extend_access_plan_for_request
resource: crates/lpe-exchange/src/mapi/store_adapter/access_plan.rs#L75-L144
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/push_unique
  - functions/crates/lpe-exchange/src/mapi/session/MapiSession/resolve_special_folder_alias
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/status_message_id
  - functions/crates/lpe-exchange/src/mapi/rop/object_ids/RopRequest/long_term_source_object_id
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/message_ids
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/move_copy_message_ids
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/fast_transfer_message_ids
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_delete_message_ids
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_read_state_changes
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_message_id
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_move
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/delete_folder_id
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/move_copy_target_handle
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/add_object_ids_for_handle
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulate_table_access
  called_by:
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/plan_mapi_store_access
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/hierarchy_sync_selective_fallback_plan
---

# Signature

`fn extend_access_plan_for_request( plan: &mut MapiAccessPlan, session: &MapiSession, simulated_handles: &mut HashMap<u32, MapiObject>, simulated_next_handle: &mut u32, handle_slots: &mut Vec<u32>, request: &RopRequest, )`

# Calls

- [push_unique](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/push_unique.md)
- [resolve_special_folder_alias](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/MapiSession/resolve_special_folder_alias.md)
- [status_message_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/status_message_id.md)
- [long_term_source_object_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/object_ids/RopRequest/long_term_source_object_id.md)
- [message_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/message_ids.md)
- [move_copy_message_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/move_copy_message_ids.md)
- [fast_transfer_message_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/fast_transfer_message_ids.md)
- [import_delete_message_ids](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_delete_message_ids.md)
- [import_read_state_changes](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_read_state_changes.md)
- [import_message_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_message_id.md)
- [import_move](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/import_move.md)
- [delete_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/delete_folder_id.md)
- [move_copy_target_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/move_copy_target_handle.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [add_object_ids_for_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/add_object_ids_for_handle.md)
- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [simulate_table_access](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulate_table_access.md)

# Called by

- [plan_mapi_store_access](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/plan_mapi_store_access.md)
- [hierarchy_sync_selective_fallback_plan](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/hierarchy_sync_selective_fallback_plan.md)