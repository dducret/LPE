---
type: Rust Function
title: rop_set_read_flags_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L494-L502
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_read_flags_response
---

# Signature

`pub(in crate::mapi) fn rop_set_read_flags_response( request: &RopRequest, partial_completion: bool, ) -> Vec<u8>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_set_read_flags_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_read_flags_response.md)