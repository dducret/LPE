---
type: Rust Function
title: submit_success_response
resource: crates/lpe-exchange/src/mapi/dispatch/submission.rs#L88-L94
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_transport_send_success_response
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_simple_success_response
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_submit_message_response
---

# Signature

`pub(super) fn submit_success_response(request: &RopRequest) -> Vec<u8>`

# Calls

- [rop_transport_send_success_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_transport_send_success_response.md)
- [rop_simple_success_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_simple_success_response.md)

# Called by

- [append_submit_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/append_submit_message_response.md)