---
type: Rust Function
title: receive_folder_entry_matches
resource: crates/lpe-exchange/src/mapi/rop/receive_folders.rs#L58-L70
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/receive_folders/receive_folder_entry_for_message_class
---

# Signature

`fn receive_folder_entry_matches(entry: ReceiveFolderEntry, message_class: &str) -> bool`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [receive_folder_entry_for_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/receive_folders/receive_folder_entry_for_message_class.md)