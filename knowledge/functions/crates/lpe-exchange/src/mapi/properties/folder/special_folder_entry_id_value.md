---
type: Rust Function
title: special_folder_entry_id_value
resource: crates/lpe-exchange/src/mapi/properties/folder.rs#L185-L187
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

`fn special_folder_entry_id_value(mailbox_guid: Uuid, folder_id: u64) -> MapiValue`

# Calls

- [special_folder_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/special_folder_entry_id.md)

# Called by

- [special_folder_identification_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/special_folder_identification_property_value.md)