---
type: Rust Function
title: pending_text_property
resource: crates/lpe-exchange/src/mapi/properties/streams.rs#L890-L901
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/into_text
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/apply_canonical_public_folder_item_property_values
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/clearable_pending_text_property
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/clearable_pending_html_property
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/attendees_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/message/jmap_import_from_pending_message
  - functions/crates/lpe-exchange/src/mapi/properties/message/mapi_submit_from_pending_message
  - functions/crates/lpe-exchange/src/mapi/properties/message/meeting_request_attachment
  - functions/crates/lpe-exchange/src/mapi/properties/streams/message_body_stream_data
  - functions/crates/lpe-exchange/src/mapi/properties/streams/pending_message_size
  - functions/crates/lpe-exchange/src/mapi/properties/streams/pending_body_text_property
  - functions/crates/lpe-exchange/src/mapi/rop/responses/rop_reload_cached_information_response
---

# Signature

`pub(in crate::mapi) fn pending_text_property( properties: &HashMap<u32, MapiValue>, tags: &[u32], ) -> String`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [into_text](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/into_text.md)

# Called by

- [append_save_changes_message_route_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)
- [apply_canonical_public_folder_item_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/apply_canonical_public_folder_item_property_values.md)
- [clearable_pending_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/clearable_pending_text_property.md)
- [clearable_pending_html_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/clearable_pending_html_property.md)
- [attendees_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/attendees_from_mapi.md)
- [jmap_import_from_pending_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/jmap_import_from_pending_message.md)
- [mapi_submit_from_pending_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/mapi_submit_from_pending_message.md)
- [meeting_request_attachment](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/meeting_request_attachment.md)
- [message_body_stream_data](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/message_body_stream_data.md)
- [pending_message_size](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/pending_message_size.md)
- [pending_body_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/pending_body_text_property.md)
- [rop_reload_cached_information_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/rop/responses/rop_reload_cached_information_response.md)