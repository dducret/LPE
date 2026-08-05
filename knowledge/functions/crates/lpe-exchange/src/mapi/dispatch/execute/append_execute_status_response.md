---
type: Rust Function
title: append_execute_status_response
resource: crates/lpe-exchange/src/mapi/dispatch/execute.rs#L458-L486
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/append_abort_response
  - functions/crates/lpe-exchange/src/mapi/session/input_object
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/append_progress_response
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/crates/lpe-exchange/src/mapi/session/input_object_mut
  - functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/deactivate_table_notifications
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/append_reset_table_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_status_or_bookmark_dispatch_response
---

# Signature

`pub(super) fn append_execute_status_response( session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, responses: &mut Vec<u8>, )`

# Calls

- [append_abort_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/append_abort_response.md)
- [input_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object.md)
- [append_progress_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/append_progress_response.md)
- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [input_object_mut](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_object_mut.md)
- [deactivate_table_notifications](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/table_notifications/MapiSession/deactivate_table_notifications.md)
- [append_reset_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/append_reset_table_response.md)

# Called by

- [append_status_or_bookmark_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_status_or_bookmark_dispatch_response.md)