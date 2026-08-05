---
type: Rust Function
title: rop_create_message_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L392-L397
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_create_message_response
---

# Signature

`pub(in crate::mapi) fn rop_create_message_response(request: &RopRequest) -> Vec<u8>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_create_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_create_message_response.md)