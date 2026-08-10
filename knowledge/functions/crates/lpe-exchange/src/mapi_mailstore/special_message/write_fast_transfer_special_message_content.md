---
type: Rust Function
title: write_fast_transfer_special_message_content
resource: crates/lpe-exchange/src/mapi_mailstore/special_message.rs#L265-L437
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_source_key
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_change_key
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_predecessor_change_list
  - functions/crates/lpe-exchange/src/mapi_mailstore/FastTransferDirectPropertyFilter/includes
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_binary_property
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_configuration_message_class
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_parent_source_key
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_i32_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_access
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_access_level
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_bool_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_has_attachments
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_status
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_search_key
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_i64
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_flags
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_utf16_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/write_string8_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_property_is_copy_identity
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_property_is_server_projected
  - functions/crates/lpe-exchange/src/mapi_mailstore/provider_defined_internal_property
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/write_special_message_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi_mailstore/special_message/fast_transfer_message_content_buffer_with_special_object
---

# Signature

`fn write_fast_transfer_special_message_content( buffer: &mut Vec<u8>, entry_id: Option<&[u8]>, parent_entry_id: Option<&[u8]>, object: &SpecialMessageSyncFact, send_options: u8, property_filter: FastTransferDirectPropertyFilter<'_>, message_children: FastTransferMessageChildren, )`

# Calls

- [special_message_source_key](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_source_key.md)
- [special_message_change_key](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_change_key.md)
- [special_message_predecessor_change_list](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_predecessor_change_list.md)
- [includes](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/FastTransferDirectPropertyFilter/includes.md)
- [write_binary_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_binary_property.md)
- [is_outlook_configuration_message_class](../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_configuration_message_class.md)
- [special_message_parent_source_key](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_parent_source_key.md)
- [write_i32_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_i32_property.md)
- [special_message_access](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_access.md)
- [special_message_access_level](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_access_level.md)
- [write_bool_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_bool_property.md)
- [special_message_has_attachments](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_has_attachments.md)
- [special_message_status](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_status.md)
- [special_message_search_key](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_search_key.md)
- [write_i64](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_i64.md)
- [special_message_flags](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_flags.md)
- [write_utf16_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_utf16_property.md)
- [write_string8_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/write_string8_property.md)
- [special_message_property_is_copy_identity](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_property_is_copy_identity.md)
- [special_message_property_is_server_projected](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/special_message_property_is_server_projected.md)
- [provider_defined_internal_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/provider_defined_internal_property.md)
- [write_special_message_property](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/write_special_message_property.md)

# Called by

- [fast_transfer_message_content_buffer_with_special_object](../../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/special_message/fast_transfer_message_content_buffer_with_special_object.md)