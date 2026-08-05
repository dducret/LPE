---
type: Rust Function
title: normal_inbox_table_lifecycle_details
resource: crates/lpe-exchange/src/mapi/dispatch/table_lifecycle.rs#L29-L56
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_table_control_dispatch_response
---

# Signature

`pub(super) fn normal_inbox_table_lifecycle_details( handle_slots: &[u32], request: &RopRequest, object: Option<&MapiObject>, ) -> Option<(Option<u32>, String)>`

# Calls

- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)

# Called by

- [append_table_control_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_table_control_dispatch_response.md)