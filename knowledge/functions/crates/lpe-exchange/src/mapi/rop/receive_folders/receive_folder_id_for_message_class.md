---
type: Rust Function
title: receive_folder_id_for_message_class
resource: crates/lpe-exchange/src/mapi/rop/receive_folders.rs#L88-L90
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/receive_folders/receive_folder_entry_for_message_class
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_set_receive_folder_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_get_receive_folder_response
---

# Signature

`pub(in crate::mapi) fn receive_folder_id_for_message_class(message_class: &str) -> u64`

# Calls

- [receive_folder_entry_for_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/receive_folders/receive_folder_entry_for_message_class.md)

# Called by

- [append_set_receive_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_set_receive_folder_response.md)
- [append_get_receive_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/append_get_receive_folder_response.md)