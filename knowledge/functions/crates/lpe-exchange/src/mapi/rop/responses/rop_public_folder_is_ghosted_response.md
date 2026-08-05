---
type: Rust Function
title: rop_public_folder_is_ghosted_response
resource: crates/lpe-exchange/src/mapi/rop/responses.rs#L587-L599
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_public_folder_is_ghosted_response
---

# Signature

`pub(in crate::mapi) fn rop_public_folder_is_ghosted_response( request: &RopRequest, is_ghosted: bool, ) -> Vec<u8>`

# Calls

- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [write_u16](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/write_u16.md)

# Called by

- [append_public_folder_is_ghosted_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/append_public_folder_is_ghosted_response.md)