---
type: Rust Function
title: rop_transport_send_success_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L288-L293
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/submission/submit_success_response
---

# Signature

`pub(in crate::mapi) fn rop_transport_send_success_response(request: &RopRequest) -> Vec<u8>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [submit_success_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/submission/submit_success_response.md)