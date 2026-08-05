---
type: Rust Function
title: associated_config_property_value_with_mailbox_guid
resource: crates/lpe-exchange/src/mapi/tables/associated_contents.rs#L539-L856
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_configuration_message_class_name
  - functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_from_json
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_id
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_associated_config_read_only_property_tag
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/sanitize_configuration_property_value
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_umolk_user_options_message_class
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/is_umolk_computed_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_last_modified_filetime
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_creation_filetime
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_last_modifier_name
  - functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_value
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_message_size
  - functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_extended_value
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id
  - functions/crates/lpe-exchange/src/mapi/identity/generated_message_search_key
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_configuration_message_class
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/configuration_roaming_datatypes
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/configuration_uses_xml_stream
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/minimal_roaming_dictionary_stream
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/minimal_working_hours_roaming_xml_stream
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/minimal_category_list_roaming_xml_stream
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/minimal_mrm_roaming_xml_stream
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/minimal_custom_action_roaming_xml_stream
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/is_outlook_virtual_sharing_state_config
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/outlook_configuration_stamp
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_row_property_value
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_values_inner
  - functions/crates/lpe-exchange/src/mapi/properties/streams/property_stream_data
  - functions/crates/lpe-exchange/src/mapi/rop/associated_config_modeled_property
  - functions/crates/lpe-exchange/src/mapi/rop/debug/semantic_property_shape_for_debug
  - functions/crates/lpe-exchange/src/mapi/sync/associated_config/associated_config_direct_fast_transfer_object
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_associated_config_row_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_row_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/common_views_message_property_value
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/common_views_message_property_value_for_principal
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value
---

# Signature

`pub(in crate::mapi) fn associated_config_property_value_with_mailbox_guid( message: &MapiAssociatedConfigMessage, mailbox_guid: Uuid, property_tag: u32, ) -> Option<MapiValue>`

# Calls

- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [is_outlook_configuration_message_class_name](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_configuration_message_class_name.md)
- [mapi_properties_from_json](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/mapi_properties_from_json.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [property_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/MapiPropertyTag/property_id.md)
- [is_associated_config_read_only_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_associated_config_read_only_property_tag.md)
- [sanitize_configuration_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/sanitize_configuration_property_value.md)
- [is_outlook_umolk_user_options_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_umolk_user_options_message_class.md)
- [is_umolk_computed_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/is_umolk_computed_property.md)
- [change_number_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/change_number_for_store_id.md)
- [associated_config_last_modified_filetime](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_last_modified_filetime.md)
- [filetime_from_change_number](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/filetime_from_change_number.md)
- [associated_config_creation_filetime](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_creation_filetime.md)
- [associated_config_last_modifier_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_last_modifier_name.md)
- [mapi_message_size_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_value.md)
- [associated_config_message_size](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_message_size.md)
- [mapi_message_size_extended_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/mapi_message_size_extended_value.md)
- [source_key_for_store_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/source_key_for_store_id.md)
- [generated_message_search_key](../../../../../../../functions/crates/lpe-exchange/src/mapi/identity/generated_message_search_key.md)
- [is_outlook_configuration_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_configuration_message_class.md)
- [configuration_roaming_datatypes](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/configuration_roaming_datatypes.md)
- [configuration_uses_xml_stream](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/configuration_uses_xml_stream.md)
- [minimal_roaming_dictionary_stream](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/minimal_roaming_dictionary_stream.md)
- [minimal_working_hours_roaming_xml_stream](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/minimal_working_hours_roaming_xml_stream.md)
- [minimal_category_list_roaming_xml_stream](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/minimal_category_list_roaming_xml_stream.md)
- [minimal_mrm_roaming_xml_stream](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/minimal_mrm_roaming_xml_stream.md)
- [minimal_custom_action_roaming_xml_stream](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/minimal_custom_action_roaming_xml_stream.md)
- [is_outlook_virtual_sharing_state_config](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/is_outlook_virtual_sharing_state_config.md)
- [outlook_configuration_stamp](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/outlook_configuration_stamp.md)

# Called by

- [debug_associated_row_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/table_diagnostics/debug_associated_row_property_value.md)
- [format_outlook_query_row_values_inner](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_outlook_query_row_values_inner.md)
- [property_stream_data](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/property_stream_data.md)
- [associated_config_modeled_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/associated_config_modeled_property.md)
- [semantic_property_shape_for_debug](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/debug/semantic_property_shape_for_debug.md)
- [associated_config_direct_fast_transfer_object](../../../../../../../functions/crates/lpe-exchange/src/mapi/sync/associated_config/associated_config_direct_fast_transfer_object.md)
- [serialize_associated_config_row_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/serialize_associated_config_row_with_mailbox_guid.md)
- [associated_table_row_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_table_row_property_value.md)
- [common_views_message_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/common_views_message_property_value.md)
- [common_views_message_property_value_for_principal](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/common_views_message_property_value_for_principal.md)
- [associated_config_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value.md)