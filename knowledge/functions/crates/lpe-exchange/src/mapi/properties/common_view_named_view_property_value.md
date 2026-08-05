---
type: Rust Function
title: common_view_named_view_property_value
resource: crates/lpe-exchange/src/mapi/properties.rs#L887-L1012
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number
  - functions/crates/lpe-exchange/src/mapi/properties/views/outlook_folder_view_definition
  - functions/crates/lpe-exchange/src/mapi/properties/views/log_view_definition_diagnostics
  - functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_binary
  - functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_strings_binary
  - functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_strings
  - functions/crates/lpe-exchange/src/mapi/properties/property_tag_id
  - functions/crates/lpe-exchange/src/mapi/properties/wlink_guid_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/views/outlook_view_descriptor_clsid
  - functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_folder_type_guid
  - functions/crates/lpe-exchange/src/mapi/properties/default_wlink_group_guid
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_row_property_value
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_values_inner
  - functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_common_view_named_view
  - functions/crates/lpe-exchange/src/mapi/properties/streams/property_stream_data
  - functions/crates/lpe-exchange/src/mapi/properties/tests/common_view_named_view_projects_descriptor_properties_for_outlook
  - functions/crates/lpe-exchange/src/mapi/rop/debug/format_common_view_descriptor_response_values
  - functions/crates/lpe-exchange/src/mapi/sync/common_view_named_view_sync_object
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_common_view_named_view_row_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_row_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/common_views_message_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/common_views_message_property_value_for_principal
---

# Signature

`pub(in crate::mapi) fn common_view_named_view_property_value( message: &MapiCommonViewNamedViewMessage, account_id: Uuid, property_tag: u32, ) -> Option<MapiValue>`

# Calls

- [canonical_property_storage_tag](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [source_key_for_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)
- [change_number_for_store_id](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)
- [filetime_from_change_number](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number.md)
- [outlook_folder_view_definition](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/outlook_folder_view_definition.md)
- [log_view_definition_diagnostics](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/log_view_definition_diagnostics.md)
- [view_descriptor_binary](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_binary.md)
- [view_descriptor_strings_binary](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_strings_binary.md)
- [view_descriptor_strings](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/view_descriptor_strings.md)
- [property_tag_id](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/property_tag_id.md)
- [wlink_guid_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/wlink_guid_property_value.md)
- [outlook_view_descriptor_clsid](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/views/outlook_view_descriptor_clsid.md)
- [common_view_named_view_folder_type_guid](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_folder_type_guid.md)
- [default_wlink_group_guid](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/default_wlink_group_guid.md)

# Called by

- [debug_associated_row_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_row_property_value.md)
- [format_outlook_query_row_values_inner](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_values_inner.md)
- [restriction_matches_common_view_named_view](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/restriction_matches_common_view_named_view.md)
- [property_stream_data](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/property_stream_data.md)
- [common_view_named_view_projects_descriptor_properties_for_outlook](../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/common_view_named_view_projects_descriptor_properties_for_outlook.md)
- [format_common_view_descriptor_response_values](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/format_common_view_descriptor_response_values.md)
- [common_view_named_view_sync_object](../../../../../../functions/crates/lpe-exchange/src/mapi/sync/common_view_named_view_sync_object.md)
- [serialize_common_view_named_view_row_with_mailbox_guid](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_common_view_named_view_row_with_mailbox_guid.md)
- [associated_table_row_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_row_property_value.md)
- [common_views_message_property_value](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/common_views_message_property_value.md)
- [common_views_message_property_value_for_principal](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/common_views_message_property_value_for_principal.md)