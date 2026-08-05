---
type: Rust Function
title: write_u16
resource: crates/lpe-exchange/src/mapi/rop/buffer.rs#L96-L98
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/notifications/append_notification_data
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_rop_binary
  - functions/crates/lpe-exchange/src/mapi/properties/values/write_multi_i16
  - functions/crates/lpe-exchange/src/mapi/rop/write_flagged_property_row
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_copy_properties_success_response
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_options_data_response
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_expand_row_success_response
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_collapse_state_success_response
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_set_collapse_state_success_response
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_fast_transfer_put_buffer_response
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_owning_servers_response
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_public_folder_is_ghosted_response
  - functions/crates/lpe-exchange/src/mapi/rop/serialize/serialize_rop_request
  - functions/crates/lpe-exchange/src/mapi/rop/tests/modify_recipients_parses_outlook_flagged_recipient_property_row
  - functions/crates/lpe-exchange/src/mapi/rop/tests/get_properties_specific_preserves_values_and_flags_absent_message_deadlines
  - functions/crates/lpe-exchange/src/mapi/rop/tests/get_properties_specific_resolves_unspecified_modeled_message_properties
  - functions/crates/lpe-exchange/src/mapi/rop/tests/get_properties_specific_returns_not_enough_memory_for_size_limited_value
  - functions/crates/lpe-exchange/src/mapi/rop/tests/get_properties_specific_size_limit_preserves_unspecified_property_type
  - functions/crates/lpe-exchange/src/mapi/rop/tests/modify_recipients_accepts_microsoft_message_example_columns
  - functions/crates/lpe-exchange/src/mapi/tables/collapse/rop_get_collapse_state_response
  - functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default
  - functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_table_bookmark_and_collapse_rops_require_set_columns
  - functions/crates/lpe-exchange/src/mapi/transport/connect_auxiliary_buffer
---

# Signature

`pub(in crate::mapi) fn write_u16(body: &mut Vec<u8>, value: u16)`

# Called by

- [append_notification_data](../../../../../../../functions/crates/lpe-exchange/src/mapi/notifications/append_notification_data.md)
- [write_mapi_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_mapi_value.md)
- [write_rop_binary](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_rop_binary.md)
- [write_multi_i16](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/write_multi_i16.md)
- [write_flagged_property_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/write_flagged_property_row.md)
- [rop_copy_properties_success_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_copy_properties_success_response.md)
- [rop_options_data_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_options_data_response.md)
- [rop_expand_row_success_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_expand_row_success_response.md)
- [rop_get_collapse_state_success_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_collapse_state_success_response.md)
- [rop_set_collapse_state_success_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_set_collapse_state_success_response.md)
- [rop_fast_transfer_put_buffer_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_fast_transfer_put_buffer_response.md)
- [rop_get_owning_servers_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_get_owning_servers_response.md)
- [rop_public_folder_is_ghosted_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_public_folder_is_ghosted_response.md)
- [serialize_rop_request](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize/serialize_rop_request.md)
- [modify_recipients_parses_outlook_flagged_recipient_property_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/modify_recipients_parses_outlook_flagged_recipient_property_row.md)
- [get_properties_specific_preserves_values_and_flags_absent_message_deadlines](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/get_properties_specific_preserves_values_and_flags_absent_message_deadlines.md)
- [get_properties_specific_resolves_unspecified_modeled_message_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/get_properties_specific_resolves_unspecified_modeled_message_properties.md)
- [get_properties_specific_returns_not_enough_memory_for_size_limited_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/get_properties_specific_returns_not_enough_memory_for_size_limited_value.md)
- [get_properties_specific_size_limit_preserves_unspecified_property_type](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/get_properties_specific_size_limit_preserves_unspecified_property_type.md)
- [modify_recipients_accepts_microsoft_message_example_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/modify_recipients_accepts_microsoft_message_example_columns.md)
- [rop_get_collapse_state_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/collapse/rop_get_collapse_state_response.md)
- [write_property_default](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/row_codecs/write_property_default.md)
- [microsoft_table_bookmark_and_collapse_rops_require_set_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/microsoft_table_bookmark_and_collapse_rops_require_set_columns.md)
- [connect_auxiliary_buffer](../../../../../../../functions/crates/lpe-exchange/src/mapi/transport/connect_auxiliary_buffer.md)