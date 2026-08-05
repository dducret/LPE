---
type: Rust Function
title: append_deferred_action_messages_response
resource: crates/lpe-exchange/src/mapi/dispatch/submission.rs#L324-L333
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/deferred_action_messages_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_deferred_action_messages_dispatch_response
---

# Signature

`pub(super) fn append_deferred_action_messages_response( request: &RopRequest, has_input_handle: bool, responses: &mut Vec<u8>, )`

# Calls

- [deferred_action_messages_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/deferred_action_messages_response.md)

# Called by

- [append_deferred_action_messages_dispatch_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_deferred_action_messages_dispatch_response.md)