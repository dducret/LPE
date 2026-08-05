---
type: Rust Function
title: additional_ren_entry_ids
resource: crates/lpe-exchange/src/mapi/properties/folder.rs#L189-L200
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/folder/special_folder_entry_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/folder/special_folder_identification_property_value
---

# Signature

`fn additional_ren_entry_ids(mailbox_guid: Uuid) -> Vec<Vec<u8>>`

# Calls

- [special_folder_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/special_folder_entry_id.md)

# Called by

- [special_folder_identification_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/special_folder_identification_property_value.md)