---
type: Rust Function
title: append_store_state_response
resource: crates/lpe-exchange/src/mapi/dispatch/logon.rs#L241-L248
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  - functions/crates/lpe-exchange/src/mapi/dispatch/logon/store_state_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_status_or_bookmark_dispatch_response
---

# Signature

`pub(super) fn append_store_state_response( handle_slots: &[u32], request: &RopRequest, responses: &mut Vec<u8>, )`

# Calls

- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)
- [store_state_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/logon/store_state_response.md)

# Called by

- [append_status_or_bookmark_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_controls/append_status_or_bookmark_dispatch_response.md)