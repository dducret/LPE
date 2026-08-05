---
type: Rust Function
title: hierarchy_sync_selective_fallback_plan
resource: crates/lpe-exchange/src/mapi/store_adapter/access_plan.rs#L146-L201
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/read_handle_table
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/remaining
  - functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request
  - functions/crates/lpe-exchange/src/mapi/rop/parse/sync_request_options/RopRequest/sync_type
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/rop_requires_full_snapshot
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/rop_uses_session_state_only
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/extend_access_plan_for_request
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/push_unique
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_response
---

# Signature

`pub(in crate::mapi) fn hierarchy_sync_selective_fallback_plan( session: &MapiSession, rop_buffer: &[u8], ) -> Option<MapiAccessPlan>`

# Calls

- [read_handle_table](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/read_handle_table.md)
- [remaining](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/remaining.md)
- [read_rop_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/request_reader/read_rop_request.md)
- [sync_type](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/sync_request_options/RopRequest/sync_type.md)
- [input_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index.md)
- [rop_requires_full_snapshot](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/rop_requires_full_snapshot.md)
- [rop_uses_session_state_only](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/rop_uses_session_state_only.md)
- [extend_access_plan_for_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/extend_access_plan_for_request.md)
- [push_unique](../../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/push_unique.md)

# Called by

- [execute_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_response.md)