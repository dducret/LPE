---
type: Rust Function
title: mailbox_property_value_with_context_for_account
resource: crates/lpe-exchange/src/mapi/properties.rs#L498-L600
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/properties/folder/special_folder_identification_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/mapi_mailbox_display_name
  - functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_value
  - functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_extended_value
  - functions/crates/lpe-exchange/src/mapi/permissions/owner_rights
  - functions/crates/lpe-exchange/src/mapi/properties/extended_folder_flags_for_folder
  - functions/crates/lpe-exchange/src/mapi/properties/views/default_view_supported_folder
  - functions/crates/lpe-exchange/src/mapi/tables/folders/folder_message_class
  - functions/crates/lpe-exchange/src/mapi/properties/views/default_folder_view_entry_id
  - functions/crates/lpe-exchange/src/mapi/tables/folders/mailbox_projects_hidden_attribute
  - functions/crates/lpe-exchange/src/mapi/properties/default_post_message_class_for_container_class
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_mailbox_folder
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/canonical_folder_change_number
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialized_replid_guid_map
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_properties_for_open_from_mailboxes
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_mailbox_with_context_for_account
  - functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context
  - functions/crates/lpe-exchange/src/mapi/properties/streams/property_stream_data
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_folder_type_getprops_contract
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_folder_row_with_context
  - functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_property_value
---

# Signature

`pub(in crate::mapi) fn mailbox_property_value_with_context_for_account( mailbox: &JmapMailbox, mailboxes: &[JmapMailbox], property_tag: u32, mailbox_guid: Uuid, ) -> Option<MapiValue>`

# Calls

- [canonical_property_storage_tag](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [special_folder_identification_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/special_folder_identification_property_value.md)
- [mapi_mailbox_display_name](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/mapi_mailbox_display_name.md)
- [mapi_message_size_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_value.md)
- [mapi_message_size_extended_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_extended_value.md)
- [owner_rights](../../../../../../functions/crates/lpe-exchange/src/mapi/permissions/owner_rights.md)
- [extended_folder_flags_for_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/extended_folder_flags_for_folder.md)
- [default_view_supported_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/default_view_supported_folder.md)
- [folder_message_class](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/folder_message_class.md)
- [default_folder_view_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/default_folder_view_entry_id.md)
- [mailbox_projects_hidden_attribute](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/mailbox_projects_hidden_attribute.md)
- [default_post_message_class_for_container_class](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/default_post_message_class_for_container_class.md)
- [source_key_for_mailbox_folder](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_mailbox_folder.md)
- [filetime_from_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number.md)
- [canonical_folder_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/canonical_folder_change_number.md)
- [serialized_replid_guid_map](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialized_replid_guid_map.md)
- [source_key_for_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)

# Called by

- [folder_properties_for_open_from_mailboxes](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/folders/folder_properties_for_open_from_mailboxes.md)
- [restriction_matches_mailbox_with_context_for_account](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_mailbox_with_context_for_account.md)
- [mailbox_property_value_with_context](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context.md)
- [property_stream_data](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/property_stream_data.md)
- [format_folder_type_getprops_contract](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_folder_type_getprops_contract.md)
- [serialize_folder_row_with_context](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_folder_row_with_context.md)
- [hierarchy_row_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/hierarchy/hierarchy_row_property_value.md)