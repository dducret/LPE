---
type: Rust Function
title: search_folder_definition_message_property_value
resource: crates/lpe-exchange/src/mapi/properties/search_folders.rs#L87-L159
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_message_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_uuid
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_template_id
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_tag
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_last_used
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_expiration
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_storage_type
  - functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_blob
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_values_inner
  - functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_oxosrch_search_folder_definition_blob_header_is_little_endian
  - functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_oxosrch_large_messages_template_projects_text_and_numerical_search
  - functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_oxosrch_old_mail_template_projects_big_endian_age_numerical_search
  - functions/crates/lpe-exchange/src/mapi/sync/search_folder_definition_sync_object
  - functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_search_folder_definition_row_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/restriction_matches_common_views_message
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/common_views_message_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/common_views_message_property_value_for_principal
---

# Signature

`pub(in crate::mapi) fn search_folder_definition_message_property_value( definition: &SearchFolderDefinition, account_id: Uuid, property_tag: u32, ) -> Option<MapiValue>`

# Calls

- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [search_folder_definition_message_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_message_id.md)
- [change_number_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)
- [source_key_for_uuid](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_uuid.md)
- [source_key_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)
- [search_folder_template_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_template_id.md)
- [search_folder_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_tag.md)
- [search_folder_last_used](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_last_used.md)
- [search_folder_expiration](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_expiration.md)
- [search_folder_storage_type](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_storage_type.md)
- [search_folder_definition_blob](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/search_folders/search_folder_definition_blob.md)
- [filetime_from_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number.md)

# Called by

- [format_outlook_query_row_values_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_values_inner.md)
- [microsoft_oxosrch_search_folder_definition_blob_header_is_little_endian](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_oxosrch_search_folder_definition_blob_header_is_little_endian.md)
- [microsoft_oxosrch_large_messages_template_projects_text_and_numerical_search](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_oxosrch_large_messages_template_projects_text_and_numerical_search.md)
- [microsoft_oxosrch_old_mail_template_projects_big_endian_age_numerical_search](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_oxosrch_old_mail_template_projects_big_endian_age_numerical_search.md)
- [search_folder_definition_sync_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/search_folder_definition_sync_object.md)
- [rop_find_row_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/rop_find_row_response.md)
- [serialize_search_folder_definition_row_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_search_folder_definition_row_with_mailbox_guid.md)
- [restriction_matches_common_views_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/restriction_matches_common_views_message.md)
- [common_views_message_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/common_views_message_property_value.md)
- [common_views_message_property_value_for_principal](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/common_views_message_property_value_for_principal.md)