---
type: Rust Function
title: collaboration_folder_property_value
resource: crates/lpe-exchange/src/mapi/properties.rs#L722-L802
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  - functions/crates/lpe-exchange/src/mapi/properties/collaboration_folder_access
  - functions/crates/lpe-exchange/src/mapi/properties/collaboration_folder_rights
  - functions/crates/lpe-exchange/src/mapi/properties/extended_folder_flags
  - functions/crates/lpe-exchange/src/mapi/tables/folders/collaboration_folder_message_class
  - functions/crates/lpe-exchange/src/mapi/properties/default_post_message_class_for_container_class
  - functions/crates/lpe-exchange/src/mapi/properties/views/default_view_supported_folder
  - functions/crates/lpe-exchange/src/mapi/properties/views/default_folder_view_entry_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialized_replid_guid_map
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/collaboration_folder_handle_properties
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_collaboration_folder
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_folder_type_getprops_contract
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_collaboration_folder_row_with_context
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_property_value
---

# Signature

`pub(in crate::mapi) fn collaboration_folder_property_value( folder: &MapiCollaborationFolder, property_tag: u32, ) -> Option<MapiValue>`

# Calls

- [canonical_property_storage_tag](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [change_number_for_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)
- [collaboration_folder_access](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/collaboration_folder_access.md)
- [collaboration_folder_rights](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/collaboration_folder_rights.md)
- [extended_folder_flags](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/extended_folder_flags.md)
- [collaboration_folder_message_class](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/collaboration_folder_message_class.md)
- [default_post_message_class_for_container_class](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/default_post_message_class_for_container_class.md)
- [default_view_supported_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/default_view_supported_folder.md)
- [default_folder_view_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/default_folder_view_entry_id.md)
- [source_key_for_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)
- [filetime_from_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number.md)
- [serialized_replid_guid_map](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialized_replid_guid_map.md)

# Called by

- [collaboration_folder_handle_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/collaboration_folder_handle_properties.md)
- [restriction_matches_collaboration_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_collaboration_folder.md)
- [format_folder_type_getprops_contract](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_folder_type_getprops_contract.md)
- [serialize_collaboration_folder_row_with_context](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_collaboration_folder_row_with_context.md)
- [hierarchy_row_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_property_value.md)