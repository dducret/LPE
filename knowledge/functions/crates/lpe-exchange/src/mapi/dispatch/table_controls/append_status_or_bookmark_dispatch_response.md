---
type: Rust Function
title: append_status_or_bookmark_dispatch_response
resource: crates/lpe-exchange/src/mapi/dispatch/table_controls.rs#L30-L48
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/logon/append_store_state_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/append_execute_status_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_free_bookmark_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops
---

# Signature

`pub(super) fn append_status_or_bookmark_dispatch_response( session: &mut MapiSession, handle_slots: &[u32], request: &RopRequest, responses: &mut Vec<u8>, )`

# Calls

- [append_store_state_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/logon/append_store_state_response.md)
- [append_execute_status_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/append_execute_status_response.md)
- [append_free_bookmark_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/append_free_bookmark_response.md)

# Called by

- [execute_rops](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute_rops.md)