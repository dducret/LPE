---
type: Rust Function
title: valid_receive_folder_message_class
resource: crates/lpe-exchange/src/mapi/rop/receive_folders.rs#L22-L31
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_set_receive_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_get_receive_folder_response
---

# Signature

`pub(in crate::mapi) fn valid_receive_folder_message_class(message_class: &str) -> bool`

# Called by

- [append_set_receive_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_set_receive_folder_response.md)
- [append_get_receive_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_get_receive_folder_response.md)