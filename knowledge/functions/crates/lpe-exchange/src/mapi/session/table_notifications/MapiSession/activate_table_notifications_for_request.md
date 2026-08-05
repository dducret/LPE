---
type: Rust Method
title: activate_table_notifications_for_request
resource: crates/lpe-exchange/src/mapi/session/table_notifications.rs#L24-L59
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_table_control_dispatch_response
---

# Signature

`pub(in crate::mapi) fn activate_table_notifications_for_request( &mut self, handle_slots: &[u32], request: &RopRequest, )`

# Calls

- [input_handle](../../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)

# Called by

- [append_table_control_dispatch_response](../../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_table_control_dispatch_response.md)