---
type: Rust Function
title: explicit_receive_folder_message_class
resource: crates/lpe-exchange/src/mapi/rop/receive_folders.rs#L84-L86
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/receive_folders/receive_folder_entry_for_message_class
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_get_receive_folder_response
---

# Signature

`pub(in crate::mapi) fn explicit_receive_folder_message_class(message_class: &str) -> &'static str`

# Calls

- [receive_folder_entry_for_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/receive_folders/receive_folder_entry_for_message_class.md)

# Called by

- [append_get_receive_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_get_receive_folder_response.md)