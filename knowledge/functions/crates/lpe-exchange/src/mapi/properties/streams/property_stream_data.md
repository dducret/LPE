---
type: Rust Function
title: property_stream_data
resource: crates/lpe-exchange/src/mapi/properties/streams.rs#L105-L296
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/properties/streams/common_view_named_view_stream_property_is_writable
  - functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag
  - functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context_for_account
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/named_view_message_for_folder_and_id
  - functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_property_value
  - functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_local_freebusy_message_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_reminder
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/reminder_for_source
  - functions/crates/lpe-exchange/src/mapi/properties/streams/mapi_value_stream_bytes
  - functions/crates/lpe-exchange/src/mapi/properties/streams/empty_stream_bytes_for_property_tag
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/open_stream_data
  - functions/crates/lpe-exchange/src/mapi/properties/tests/already_open_common_view_missing_descriptor_uses_empty_stream_semantics
  - functions/crates/lpe-exchange/src/mapi/properties/tests/common_view_named_view_descriptor_accepts_microsoft_write_stream_sequence
  - functions/crates/lpe-exchange/src/mapi/properties/tests/associated_config_missing_binary_property_opens_writable_stream
---

# Signature

`pub(super) fn property_stream_data( session: &mut MapiSession, input_handle: u32, property_tag: u32, open_mode: u8, mailboxes: &[JmapMailbox], mailbox_guid: Uuid, snapshot: &MapiMailStoreSnapshot, ) -> Option<(Vec<u8>, Option<StreamWriteTarget>)>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [common_view_named_view_stream_property_is_writable](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/common_view_named_view_stream_property_is_writable.md)
- [canonical_property_storage_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tags/canonical_property_storage_tag.md)
- [mailbox_property_value_with_context_for_account](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/mailbox_property_value_with_context_for_account.md)
- [associated_config_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_id.md)
- [associated_config_property_value_with_mailbox_guid](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value_with_mailbox_guid.md)
- [named_view_message_for_folder_and_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/named_view_message_for_folder_and_id.md)
- [common_view_named_view_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/common_view_named_view_property_value.md)
- [is_outlook_local_freebusy_message_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/associated_config/is_outlook_local_freebusy_message_id.md)
- [event_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id.md)
- [event_property_value_with_reminder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_reminder.md)
- [reminder_for_source](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/reminder_for_source.md)
- [mapi_value_stream_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/mapi_value_stream_bytes.md)
- [empty_stream_bytes_for_property_tag](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/empty_stream_bytes_for_property_tag.md)

# Called by

- [open_stream_data](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/open_stream_data.md)
- [already_open_common_view_missing_descriptor_uses_empty_stream_semantics](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/already_open_common_view_missing_descriptor_uses_empty_stream_semantics.md)
- [common_view_named_view_descriptor_accepts_microsoft_write_stream_sequence](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/common_view_named_view_descriptor_accepts_microsoft_write_stream_sequence.md)
- [associated_config_missing_binary_property_opens_writable_stream](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/associated_config_missing_binary_property_opens_writable_stream.md)