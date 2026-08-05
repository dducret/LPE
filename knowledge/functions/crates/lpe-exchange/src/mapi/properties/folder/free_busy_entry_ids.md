---
type: Rust Function
title: free_busy_entry_ids
resource: crates/lpe-exchange/src/mapi/properties/folder.rs#L230-L244
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/folder/special_folder_identification_property_value
---

# Signature

`fn free_busy_entry_ids(mailbox_guid: Uuid) -> Vec<Vec<u8>>`

# Called by

- [special_folder_identification_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/special_folder_identification_property_value.md)