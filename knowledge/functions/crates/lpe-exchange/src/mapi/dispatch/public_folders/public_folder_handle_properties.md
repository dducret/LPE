---
type: Rust Function
title: public_folder_handle_properties
resource: crates/lpe-exchange/src/mapi/dispatch/public_folders.rs#L141-L177
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/public_folder_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folder_create/append_create_folder_response
---

# Signature

`pub(super) fn public_folder_handle_properties( folder: &lpe_storage::PublicFolder, folder_id: u64, ) -> HashMap<u32, MapiValue>`

# Calls

- [public_folder_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/public_folder_property_value.md)

# Called by

- [append_create_folder_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folder_create/append_create_folder_response.md)