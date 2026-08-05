---
type: Rust Function
title: raw_folder_entry_id_from_object_id
resource: crates/lpe-exchange/src/mapi/identity.rs#L760-L762
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/identity/folder_entry_id_from_object_id
---

# Signature

`fn raw_folder_entry_id_from_object_id(mailbox_guid: Uuid, object_id: u64) -> Option<Vec<u8>>`

# Called by

- [folder_entry_id_from_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/folder_entry_id_from_object_id.md)