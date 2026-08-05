---
type: Rust Function
title: rop_save_changes_message_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L483-L492
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_save_changes_message_response
---

# Signature

`pub(in crate::mapi) fn rop_save_changes_message_response( request: &RopRequest, message_id: u64, ) -> Vec<u8>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [input_handle_index](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index.md)

# Called by

- [append_save_changes_message_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/messages/append_save_changes_message_response.md)