---
type: Rust Function
title: optional_pending_text_property
resource: crates/lpe-exchange/src/mapi/properties/streams.rs#L903-L914
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/into_text
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/embedded_message_open_subject
  - functions/crates/lpe-exchange/src/mapi/dispatch/attachments/pending_embedded_message_attachment_upload
  - functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response
  - functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/pending_common_views_message_is_navigation_shortcut
  - functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/apply_canonical_public_folder_item_property_values
  - functions/crates/lpe-exchange/src/mapi/properties/attachments/pending_attachment_upload
  - functions/crates/lpe-exchange/src/mapi/properties/attachments/pending_attachment_file_name
  - functions/crates/lpe-exchange/src/mapi/properties/attachments/pending_attachment_media_type
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/meeting_response_event_input_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/organizer_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/bounded_meeting_cancellation_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/reject_unsupported_calendar_message_class
  - functions/crates/lpe-exchange/src/mapi/properties/contact/contact_input_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/message/jmap_import_from_pending_message
  - functions/crates/lpe-exchange/src/mapi/properties/message/mapi_submit_from_pending_message
  - functions/crates/lpe-exchange/src/mapi/properties/message/meeting_request_attachment
  - functions/crates/lpe-exchange/src/mapi/properties/message/optional_pending_submit_address
  - functions/crates/lpe-exchange/src/mapi/properties/notes/note_input_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/notes/journal_entry_input_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/streams/message_body_stream_data
  - functions/crates/lpe-exchange/src/mapi/properties/streams/pending_html_property
  - functions/crates/lpe-exchange/src/mapi/properties/task/task_input_from_mapi
  - functions/crates/lpe-exchange/src/mapi/tables/attachments/pending_attachment_content_id
---

# Signature

`pub(in crate::mapi) fn optional_pending_text_property( properties: &HashMap<u32, MapiValue>, tags: &[u32], ) -> Option<String>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [into_text](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/values/MapiValue/into_text.md)

# Called by

- [embedded_message_open_subject](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/embedded_message_open_subject.md)
- [pending_embedded_message_attachment_upload](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/attachments/pending_embedded_message_attachment_upload.md)
- [append_save_changes_message_route_response](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/message_save/append_save_changes_message_route_response.md)
- [pending_common_views_message_is_navigation_shortcut](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/navigation_shortcut_save/pending_common_views_message_is_navigation_shortcut.md)
- [apply_canonical_public_folder_item_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/public_folders/apply_canonical_public_folder_item_property_values.md)
- [pending_attachment_upload](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/attachments/pending_attachment_upload.md)
- [pending_attachment_file_name](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/attachments/pending_attachment_file_name.md)
- [pending_attachment_media_type](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/attachments/pending_attachment_media_type.md)
- [calendar_time_zone_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone_from_mapi.md)
- [meeting_response_event_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/meeting_response_event_input_from_mapi.md)
- [organizer_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/organizer_from_mapi.md)
- [bounded_meeting_cancellation_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/bounded_meeting_cancellation_from_mapi.md)
- [reject_unsupported_calendar_message_class](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/reject_unsupported_calendar_message_class.md)
- [contact_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/contact/contact_input_from_mapi.md)
- [jmap_import_from_pending_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/jmap_import_from_pending_message.md)
- [mapi_submit_from_pending_message](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/mapi_submit_from_pending_message.md)
- [meeting_request_attachment](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/meeting_request_attachment.md)
- [optional_pending_submit_address](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/optional_pending_submit_address.md)
- [note_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/note_input_from_mapi.md)
- [journal_entry_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/notes/journal_entry_input_from_mapi.md)
- [message_body_stream_data](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/message_body_stream_data.md)
- [pending_html_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/streams/pending_html_property.md)
- [task_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/task/task_input_from_mapi.md)
- [pending_attachment_content_id](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/attachments/pending_attachment_content_id.md)