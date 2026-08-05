---
type: Rust Function
title: rop_set_message_read_flag_response
resource: crates/lpe-exchange/src/mapi/properties.rs#L155-L163
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_message_read_flag_response
---

# Signature

`pub(in crate::mapi) fn rop_set_message_read_flag_response( request: &RopRequest, read_status_changed: bool, ) -> Vec<u8>`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_set_message_read_flag_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_set_message_read_flag_response.md)