---
type: Rust Function
title: append_abort_response
resource: crates/lpe-exchange/src/mapi/dispatch/execute.rs#L409-L415
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/abort_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/execute/append_execute_status_response
---

# Signature

`pub(super) fn append_abort_response( request: &RopRequest, input_object: Option<&MapiObject>, responses: &mut Vec<u8>, )`

# Calls

- [abort_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/abort_response.md)

# Called by

- [append_execute_status_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/execute/append_execute_status_response.md)