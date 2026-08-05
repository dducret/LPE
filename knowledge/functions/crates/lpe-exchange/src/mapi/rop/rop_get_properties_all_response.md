---
type: Rust Function
title: rop_get_properties_all_response
resource: crates/lpe-exchange/src/mapi/rop.rs#L1023-L1113
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response
  - functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id
  - functions/crates/lpe-exchange/src/mapi/rop/property_limits/request_property_size_limit
  - functions/crates/lpe-exchange/src/mapi/rop/request_get_properties_all_want_unicode
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_store_property_tags
  - functions/crates/lpe-exchange/src/mapi/rop/default_folder_property_tags_with_identity
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_attachment_columns
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_message_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_contact_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_event_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_task_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_note_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_journal_entry_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_conversation_action_property_tags
  - functions/crates/lpe-exchange/src/mapi/tables/columns/default_folder_property_tags
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_named_property_tags
  - functions/crates/lpe-exchange/src/mapi/rop/get_properties_all_response_tag
  - functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property
  - functions/crates/lpe-exchange/src/mapi/rop/property_error_tag
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_all_response
  - functions/crates/lpe-exchange/src/mapi/rop/tests/associated_config_get_properties_all_returns_its_named_properties
  - functions/crates/lpe-exchange/src/mapi/rop/tests/get_properties_all_honors_non_unicode_string_request
  - functions/crates/lpe-exchange/src/mapi/rop/tests/get_properties_all_returns_error_tag_for_size_limited_value
  - functions/crates/lpe-exchange/src/mapi/rop/tests/calendar_event_getprops_all_rejects_missing_event_handle
---

# Signature

`pub(in crate::mapi) fn rop_get_properties_all_response( request: &RopRequest, object: Option<&MapiObject>, principal: &AccountPrincipal, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, ) -> Vec<u8>`

# Calls

- [rop_error_response](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/errors/rop_error_response.md)
- [input_handle_index](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/parse/RopRequest/input_handle_index.md)
- [event_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id.md)
- [request_property_size_limit](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/property_limits/request_property_size_limit.md)
- [request_get_properties_all_want_unicode](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/request_get_properties_all_want_unicode.md)
- [default_store_property_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_store_property_tags.md)
- [default_folder_property_tags_with_identity](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/default_folder_property_tags_with_identity.md)
- [default_attachment_columns](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_attachment_columns.md)
- [default_message_property_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_message_property_tags.md)
- [default_contact_property_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_contact_property_tags.md)
- [default_event_property_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_event_property_tags.md)
- [default_task_property_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_task_property_tags.md)
- [default_note_property_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_note_property_tags.md)
- [default_journal_entry_property_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_journal_entry_property_tags.md)
- [default_conversation_action_property_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_conversation_action_property_tags.md)
- [default_folder_property_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/columns/default_folder_property_tags.md)
- [associated_config_message_for_id](../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_id.md)
- [associated_config_named_property_tags](../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_named_property_tags.md)
- [get_properties_all_response_tag](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/get_properties_all_response_tag.md)
- [serialize_object_property](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/serialize_object_property.md)
- [property_error_tag](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/property_error_tag.md)

# Called by

- [append_get_properties_all_response](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/properties/append_get_properties_all_response.md)
- [associated_config_get_properties_all_returns_its_named_properties](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/associated_config_get_properties_all_returns_its_named_properties.md)
- [get_properties_all_honors_non_unicode_string_request](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/get_properties_all_honors_non_unicode_string_request.md)
- [get_properties_all_returns_error_tag_for_size_limited_value](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/get_properties_all_returns_error_tag_for_size_limited_value.md)
- [calendar_event_getprops_all_rejects_missing_event_handle](../../../../../../functions/crates/lpe-exchange/src/mapi/rop/tests/calendar_event_getprops_all_rejects_missing_event_handle.md)