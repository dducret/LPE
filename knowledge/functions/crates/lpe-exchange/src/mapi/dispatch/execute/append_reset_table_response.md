---
type: Rust Function
title: append_reset_table_response
resource: crates/lpe-exchange/src/mapi/dispatch/execute.rs#L450-L456
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/reset_table_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/append_execute_status_response
---

# Signature

`pub(super) fn append_reset_table_response( request: &RopRequest, reset_succeeded: bool, responses: &mut Vec<u8>, )`

# Calls

- [reset_table_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/reset_table_response.md)

# Called by

- [append_execute_status_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/append_execute_status_response.md)