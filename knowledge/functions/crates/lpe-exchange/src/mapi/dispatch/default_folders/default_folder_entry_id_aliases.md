---
type: Rust Function
title: default_folder_entry_id_aliases
resource: crates/lpe-exchange/src/mapi/dispatch/default_folders.rs#L290-L333
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/strips_any_default_folder_identification_values
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/indexed_special_folder_aliases
  - functions/crates/lpe-exchange/src/mapi/properties/folder/is_scalar_default_folder_entry_id_property_tag
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_entry_id_expected_folder_id
  - functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/special_folder_alias
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response
---

# Signature

`pub(super) fn default_folder_entry_id_aliases( object: Option<&MapiObject>, values: &[(u32, MapiValue)], ) -> Vec<MapiSpecialFolderAlias>`

# Calls

- [strips_any_default_folder_identification_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/strips_any_default_folder_identification_values.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [indexed_special_folder_aliases](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/indexed_special_folder_aliases.md)
- [is_scalar_default_folder_entry_id_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/is_scalar_default_folder_entry_id_property_tag.md)
- [default_folder_entry_id_expected_folder_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/default_folder_entry_id_expected_folder_id.md)
- [special_folder_alias](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/default_folders/special_folder_alias.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [append_set_properties_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/property_mutations/append_set_properties_response.md)