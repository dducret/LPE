---
type: Rust Function
title: restore_save_changes_response_handle
resource: crates/lpe-exchange/src/mapi/dispatch/messages.rs#L282-L304
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/restore_save_changes_containing_folder_response_handle
  - functions/crates/lpe-exchange/src/mapi/session/set_handle_slot
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
---

# Signature

`pub(super) fn restore_save_changes_response_handle( session: &MapiSession, handle_slots: &mut Vec<u32>, request: &RopRequest, target: SaveChangesResponseHandleTarget, ) -> Option<u32>`

# Calls

- [restore_save_changes_containing_folder_response_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/restore_save_changes_containing_folder_response_handle.md)
- [set_handle_slot](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/set_handle_slot.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)

# Called by

- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)