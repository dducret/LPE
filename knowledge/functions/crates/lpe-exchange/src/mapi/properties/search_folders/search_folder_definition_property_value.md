---
type: Rust Function
title: search_folder_definition_property_value
resource: crates/lpe-exchange/src/mapi/properties/search_folders.rs#L3-L76
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_container_class_for_result_kind
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  - functions/crates/lpe-exchange/src/mapi/permissions/owner_rights
  - functions/crates/lpe-exchange/src/mapi/properties/extended_folder_flags_for_search_folder
  - functions/crates/lpe-exchange/src/mapi/properties/views/default_view_supported_folder
  - functions/crates/lpe-exchange/src/mapi/properties/views/default_folder_view_entry_id
  - functions/crates/lpe-exchange/src/mapi/properties/default_post_message_class_for_container_class
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/search_folder_handle_properties
  - functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_oxocfg_search_folder_flags_match_search_folder_id_property
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_session_folder_row
---

# Signature

`pub(in crate::mapi) fn search_folder_definition_property_value( definition: &SearchFolderDefinition, folder_id: u64, property_tag: u32, mailbox_guid: Uuid, ) -> Option<MapiValue>`

# Calls

- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [search_folder_container_class_for_result_kind](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_container_class_for_result_kind.md)
- [change_number_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)
- [source_key_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)
- [owner_rights](../../../../../../../functions/crates/lpe-exchange/src/mapi/permissions/owner_rights.md)
- [extended_folder_flags_for_search_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/extended_folder_flags_for_search_folder.md)
- [default_view_supported_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/default_view_supported_folder.md)
- [default_folder_view_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/default_folder_view_entry_id.md)
- [default_post_message_class_for_container_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/default_post_message_class_for_container_class.md)
- [filetime_from_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number.md)

# Called by

- [search_folder_handle_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/search_folders/search_folder_handle_properties.md)
- [microsoft_oxocfg_search_folder_flags_match_search_folder_id_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_oxocfg_search_folder_flags_match_search_folder_id_property.md)
- [serialize_session_folder_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_session_folder_row.md)