---
type: Rust Function
title: rop_get_receive_folder_response
resource: crates/lpe-exchange/src/mapi/rop/receive_folders.rs#L9-L20
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_get_receive_folder_response
---

# Signature

`pub(in crate::mapi) fn rop_get_receive_folder_response( request: &RopRequest, folder_id: u64, response_message_class: &str, ) -> Vec<u8>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_get_receive_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_get_receive_folder_response.md)