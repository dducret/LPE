---
type: Rust Function
title: message_body_stream_data
resource: crates/lpe-exchange/src/mapi/properties/streams.rs#L339-L511
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/properties/streams/pending_text_property
  - functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property
  - functions/crates/lpe-exchange/src/mapi/properties/streams/pending_html_binary_property
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_item_for_id
  - functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_id
  - functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value
  - functions/crates/lpe-exchange/src/mapi/properties/message/html_body_from_plain_text
  - functions/crates/lpe-exchange/src/mapi/properties/streams/string8z_bytes
  - functions/crates/lpe-exchange/src/mapi/properties/message/uncompressed_rtf_body
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/streams/open_stream_data
  - functions/crates/lpe-exchange/src/mapi/properties/streams/rtf_compressed_body_stream_is_read_only_projection
---

# Signature

`pub(in crate::mapi) fn message_body_stream_data( session: &MapiSession, input_handle: u32, property_tag: u32, open_mode: u8, mailboxes: &[JmapMailbox], emails: &[JmapEmail], snapshot: &MapiMailStoreSnapshot, ) -> Option<(Vec<u8>, Option<StreamWriteTarget>)>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [pending_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/pending_text_property.md)
- [optional_pending_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/optional_pending_text_property.md)
- [pending_html_binary_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/pending_html_binary_property.md)
- [event_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/event_for_id.md)
- [public_folder_item_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/public_folder_item_for_id.md)
- [associated_config_message_for_id](../../../../../../../functions/crates/lpe-exchange/src/mapi_store/snapshot/MapiMailStoreSnapshot/associated_config_message_for_id.md)
- [associated_config_property_value](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/associated_contents/associated_config_property_value.md)
- [html_body_from_plain_text](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/html_body_from_plain_text.md)
- [string8z_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/string8z_bytes.md)
- [uncompressed_rtf_body](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/uncompressed_rtf_body.md)

# Called by

- [open_stream_data](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/open_stream_data.md)
- [rtf_compressed_body_stream_is_read_only_projection](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/rtf_compressed_body_stream_is_read_only_projection.md)