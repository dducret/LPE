---
type: Rust Function
title: special_folder_property_value_with_change_number
resource: crates/lpe-exchange/src/mapi/tables/hierarchy.rs#L610-L692
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/tables/folders/special_folder_metadata
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  - functions/crates/lpe-exchange/src/mapi/tables/folders/special_folder_type
  - functions/crates/lpe-exchange/src/mapi/permissions/owner_rights
  - functions/crates/lpe-exchange/src/mapi/properties/extended_folder_flags_for_folder
  - functions/crates/lpe-exchange/src/mapi/properties/views/default_view_supported_folder
  - functions/crates/lpe-exchange/src/mapi/properties/views/default_folder_view_entry_id
  - functions/crates/lpe-exchange/src/mapi/properties/default_post_message_class_for_container_class
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number
  - functions/crates/lpe-exchange/src/mapi/properties/folder/special_folder_identification_property_value
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_properties_for_open_from_mailboxes
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_folder_property_value
---

# Signature

`pub(in crate::mapi) fn special_folder_property_value_with_change_number( folder_id: u64, property_tag: u32, mailbox_guid: Uuid, change_number: u64, ) -> Option<MapiValue>`

# Calls

- [special_folder_metadata](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/special_folder_metadata.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [source_key_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)
- [special_folder_type](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/special_folder_type.md)
- [owner_rights](../../../../../../../functions/crates/lpe-exchange/src/mapi/permissions/owner_rights.md)
- [extended_folder_flags_for_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/extended_folder_flags_for_folder.md)
- [default_view_supported_folder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/default_view_supported_folder.md)
- [default_folder_view_entry_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/default_folder_view_entry_id.md)
- [default_post_message_class_for_container_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/default_post_message_class_for_container_class.md)
- [filetime_from_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number.md)
- [special_folder_identification_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/special_folder_identification_property_value.md)

# Called by

- [folder_properties_for_open_from_mailboxes](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_properties_for_open_from_mailboxes.md)
- [special_folder_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/special_folder_property_value.md)