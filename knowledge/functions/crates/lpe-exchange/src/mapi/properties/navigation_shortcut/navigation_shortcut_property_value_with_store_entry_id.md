---
type: Rust Function
title: navigation_shortcut_property_value_with_store_entry_id
resource: crates/lpe-exchange/src/mapi/properties/navigation_shortcut.rs#L124-L293
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number
  - functions/crates/lpe-exchange/src/mapi/properties/wlink_save_stamp
  - functions/crates/lpe-exchange/src/mapi/properties/wlink_group_name
  - functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/is_sharing_local_folder_id_property_tag
  - functions/crates/lpe-exchange/src/mapi/properties/wlink_folder_type_guid
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_property_value_for_principal
---

# Signature

`fn navigation_shortcut_property_value_with_store_entry_id( message: &MapiNavigationShortcutMessage, account_id: Uuid, store_entry_id: Option<&[u8]>, property_tag: u32, ) -> Option<MapiValue>`

# Calls

- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [source_key_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)
- [change_number_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)
- [filetime_from_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number.md)
- [wlink_save_stamp](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/wlink_save_stamp.md)
- [wlink_group_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/wlink_group_name.md)
- [is_sharing_local_folder_id_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/is_sharing_local_folder_id_property_tag.md)
- [wlink_folder_type_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/wlink_folder_type_guid.md)

# Called by

- [navigation_shortcut_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_property_value.md)
- [navigation_shortcut_property_value_for_principal](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/navigation_shortcut/navigation_shortcut_property_value_for_principal.md)