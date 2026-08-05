---
type: Rust Function
title: rop_create_folder_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L123-L136
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_create/append_create_folder_response
  - functions/crates/lpe-exchange/src/mapi/rop/tests/create_folder_private_response_stops_after_non_existing_flag
---

# Signature

`pub(in crate::mapi) fn rop_create_folder_response( request: &RopRequest, folder_id: u64, existing: bool, ) -> Vec<u8>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_create_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_create/append_create_folder_response.md)
- [create_folder_private_response_stops_after_non_existing_flag](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/create_folder_private_response_stops_after_non_existing_flag.md)