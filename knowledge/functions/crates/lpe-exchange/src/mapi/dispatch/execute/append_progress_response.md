---
type: Rust Function
title: append_progress_response
resource: crates/lpe-exchange/src/mapi/dispatch/execute.rs#L434-L440
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/progress_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/append_execute_status_response
---

# Signature

`pub(super) fn append_progress_response( request: &RopRequest, input_object: Option<&MapiObject>, responses: &mut Vec<u8>, )`

# Calls

- [progress_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/progress_response.md)

# Called by

- [append_execute_status_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/append_execute_status_response.md)