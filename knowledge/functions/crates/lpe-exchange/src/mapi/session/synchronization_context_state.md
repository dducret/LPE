---
type: Rust Function
title: synchronization_context_state
resource: crates/lpe-exchange/src/mapi/session.rs#L1286-L1352
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_synchronization_get_transfer_state_response
  - functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulate_table_access
---

# Signature

`pub(in crate::mapi) fn synchronization_context_state( object: Option<&MapiObject>, ) -> Option<( u64, Option<Uuid>, MapiCheckpointKind, u64, u64, bool, &'static str, u8, Vec<u8>, )>`

# Called by

- [append_synchronization_get_transfer_state_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/sync_import/append_synchronization_get_transfer_state_response.md)
- [simulate_table_access](../../../../../../functions/crates/lpe-exchange/src/mapi/store_adapter/access_plan/simulate_table_access.md)