---
type: Rust Function
title: search_folder_handle_properties
resource: crates/lpe-exchange/src/mapi/dispatch/search_folders.rs#L58-L102
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/hydrate_folder_handle_properties_for_request
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_properties_for_open_from_mailboxes
---

# Signature

`pub(super) fn search_folder_handle_properties( definition: &SearchFolderDefinition, folder_id: u64, mailbox_guid: Uuid, ) -> HashMap<u32, MapiValue>`

# Calls

- [search_folder_definition_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_property_value.md)

# Called by

- [hydrate_folder_handle_properties_for_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/hydrate_folder_handle_properties_for_request.md)
- [folder_properties_for_open_from_mailboxes](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_properties_for_open_from_mailboxes.md)