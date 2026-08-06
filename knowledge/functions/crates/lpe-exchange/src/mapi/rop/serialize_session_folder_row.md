---
type: Rust Function
title: serialize_session_folder_row
resource: crates/lpe-exchange/src/mapi/rop.rs#L1478-L1610
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/properties/views/persisted_default_folder_view_entry_id
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/search_folder_definition_for_folder_id
  - functions/crates/lpe-exchange/src/mapi/properties/folder/folder_local_commit_time_max_property_value
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_version
  - functions/crates/lpe-exchange/src/mapi/properties/folder_version_property_value
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/tables/folders/is_advertised_special_folder
  - functions/crates/lpe-exchange/src/mapi/tables/counts/folder_message_count
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/folder/ipm_subtree_ost_ostid
  - functions/crates/lpe-exchange/src/mapi/properties/folder/special_folder_identification_property_value
  - functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_folder_row_with_context_and_version
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_collaboration_folder_row_with_context_and_version
  - functions/crates/lpe-exchange/src/mapi/tables/counts/associated_folder_message_count
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/public_folders/serialize_public_folder_row
  - functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_special_folder_row_with_version
  called_by:
  - functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property
---

# Signature

`fn serialize_session_folder_row( folder_id: u64, properties: &HashMap<u32, MapiValue>, principal: &AccountPrincipal, mailboxes: &[JmapMailbox], snapshot: &MapiMailStoreSnapshot, columns: &[u32], ) -> Vec<u8>`

# Calls

- [canonical_property_storage_tag](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [persisted_default_folder_view_entry_id](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/persisted_default_folder_view_entry_id.md)
- [write_mapi_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [search_folder_definition_for_folder_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/search_folder_definition_for_folder_id.md)
- [folder_local_commit_time_max_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/folder_local_commit_time_max_property_value.md)
- [folder_version](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/folder_version.md)
- [folder_version_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder_version_property_value.md)
- [get](../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [is_advertised_special_folder](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/is_advertised_special_folder.md)
- [folder_message_count](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/folder_message_count.md)
- [search_folder_definition_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_property_value.md)
- [ipm_subtree_ost_ostid](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/ipm_subtree_ost_ostid.md)
- [special_folder_identification_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/folder/special_folder_identification_property_value.md)
- [folder_row_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/folder_row_for_id.md)
- [serialize_folder_row_with_context_and_version](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_folder_row_with_context_and_version.md)
- [collaboration_folder_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/collaboration_folder_for_id.md)
- [serialize_collaboration_folder_row_with_context_and_version](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_collaboration_folder_row_with_context_and_version.md)
- [associated_folder_message_count](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/counts/associated_folder_message_count.md)
- [public_folder_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_for_id.md)
- [serialize_public_folder_row](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/public_folders/serialize_public_folder_row.md)
- [serialize_special_folder_row_with_version](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/folders/serialize_special_folder_row_with_version.md)

# Called by

- [rop_get_properties_specific_response_with_custom](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/rop_get_properties_specific_response_with_custom.md)
- [serialize_object_property](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property.md)