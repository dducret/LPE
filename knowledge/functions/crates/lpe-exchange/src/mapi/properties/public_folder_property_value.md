---
type: Rust Function
title: public_folder_property_value
resource: crates/lpe-exchange/src/mapi/properties.rs#L836-L908
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  - functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id
  - functions/crates/lpe-exchange/src/mapi/permissions/rights_from_grant
  - functions/crates/lpe-exchange/src/mapi/properties/extended_folder_flags
  - functions/crates/lpe-exchange/src/mapi/properties/default_post_message_class_for_container_class
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialized_replid_guid_map
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_properties_for_open_from_mailboxes
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/public_folder_handle_properties
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_public_folder
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_folder_type_getprops_contract
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/public_folders/serialize_public_folder_row
---

# Signature

`pub(in crate::mapi) fn public_folder_property_value( folder: &MapiPublicFolder, property_tag: u32, ) -> Option<MapiValue>`

# Calls

- [canonical_property_storage_tag](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [change_number_for_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)
- [mapped_mapi_object_id](../../../../../../functions/crates/lpe-exchange/src/mapi/identity/mapped_mapi_object_id.md)
- [rights_from_grant](../../../../../../functions/crates/lpe-exchange/src/mapi/permissions/rights_from_grant.md)
- [extended_folder_flags](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/extended_folder_flags.md)
- [default_post_message_class_for_container_class](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/default_post_message_class_for_container_class.md)
- [source_key_for_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)
- [filetime_from_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number.md)
- [serialized_replid_guid_map](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialized_replid_guid_map.md)

# Called by

- [folder_properties_for_open_from_mailboxes](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_properties_for_open_from_mailboxes.md)
- [public_folder_handle_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/public_folder_handle_properties.md)
- [restriction_matches_public_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_public_folder.md)
- [format_folder_type_getprops_contract](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_folder_type_getprops_contract.md)
- [hierarchy_row_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_property_value.md)
- [serialize_public_folder_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/public_folders/serialize_public_folder_row.md)