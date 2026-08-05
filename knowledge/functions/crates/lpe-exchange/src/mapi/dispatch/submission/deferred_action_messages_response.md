---
type: Rust Function
title: deferred_action_messages_response
resource: crates/lpe-exchange/src/mapi/dispatch/submission.rs#L293-L302
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_deferred_action_messages_response
---

# Signature

`pub(super) fn deferred_action_messages_response( request: &RopRequest, has_input_handle: bool, ) -> Vec<u8>`

# Calls

- [rop_error_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [response_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/response_handle_index.md)

# Called by

- [append_deferred_action_messages_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_deferred_action_messages_response.md)