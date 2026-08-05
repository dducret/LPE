---
type: Rust Function
title: merge_additional_ren_entry_ids
resource: crates/lpe-exchange/src/mapi/dispatch/default_folders.rs#L225-L264
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/folder/special_folder_identification_property_value
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_identification_safe_property_value
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/hydrate_folder_handle_properties_for_request
---

# Signature

`pub(super) fn merge_additional_ren_entry_ids( principal: &AccountPrincipal, existing: Option<&MapiValue>, value: MapiValue, ) -> Option<MapiValue>`

# Calls

- [special_folder_identification_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/special_folder_identification_property_value.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [default_folder_identification_safe_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_identification_safe_property_value.md)
- [hydrate_folder_handle_properties_for_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/hydrate_folder_handle_properties_for_request.md)