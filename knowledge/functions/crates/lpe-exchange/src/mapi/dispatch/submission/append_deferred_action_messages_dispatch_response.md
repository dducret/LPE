---
type: Rust Function
title: append_deferred_action_messages_dispatch_response
resource: crates/lpe-exchange/src/mapi/dispatch/submission.rs#L335-L345
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_deferred_action_messages_response
  - functions/crates/lpe-exchange/src/mapi/session/input_handle
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_submission_dispatch_response
---

# Signature

`pub(super) fn append_deferred_action_messages_dispatch_response( handle_slots: &[u32], request: &RopRequest, responses: &mut Vec<u8>, )`

# Calls

- [append_deferred_action_messages_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_deferred_action_messages_response.md)
- [input_handle](../../../../../../../functions/crates/lpe-exchange/src/mapi/session/input_handle.md)

# Called by

- [append_submission_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_submission_dispatch_response.md)