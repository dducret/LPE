---
type: Rust Function
title: receive_folder_entry_for_message_class
resource: crates/lpe-exchange/src/mapi/rop/receive_folders.rs#L72-L82
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/receive_folders/receive_folder_entry_matches
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/receive_folders/explicit_receive_folder_message_class
  - functions/crates/lpe-exchange/src/mapi/rop/receive_folders/receive_folder_id_for_message_class
---

# Signature

`fn receive_folder_entry_for_message_class(message_class: &str) -> ReceiveFolderEntry`

# Calls

- [receive_folder_entry_matches](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/receive_folders/receive_folder_entry_matches.md)

# Called by

- [explicit_receive_folder_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/receive_folders/explicit_receive_folder_message_class.md)
- [receive_folder_id_for_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/receive_folders/receive_folder_id_for_message_class.md)