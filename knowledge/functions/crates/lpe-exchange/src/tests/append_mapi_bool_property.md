---
type: Rust Function
title: append_mapi_bool_property
resource: crates/lpe-exchange/src/tests/mod.rs#L15067-L15070
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_delete_reminder_delta_reports_problem_without_hiding_reminder
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_keep_open_handle_accepts_second_update_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_save_projects_committed_far_future_reminder_without_query_reread
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_calendar_create_resolves_mailbox_named_property_ids
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_create_inline_attachment_preserves_content_id_metadata
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_attach_text_file_stream_saves_canonical_attachment
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/save_changes_handles/mapi_over_http_fai_save_then_get_change_key_keeps_same_input_handle_in_buffer
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcmsg_insert_html_embedded_image_is_imported_on_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_message_list_settings_import_preserves_outlook_identity_and_content
---

# Signature

`fn append_mapi_bool_property(values: &mut Vec<u8>, property_tag: u32, value: bool)`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [mapi_over_http_calendar_delete_reminder_delta_reports_problem_without_hiding_reminder](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_delete_reminder_delta_reports_problem_without_hiding_reminder.md)
- [mapi_over_http_calendar_keep_open_handle_accepts_second_update_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_keep_open_handle_accepts_second_update_save.md)
- [mapi_over_http_calendar_save_projects_committed_far_future_reminder_without_query_reread](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_save_projects_committed_far_future_reminder_without_query_reread.md)
- [mapi_over_http_outlook_calendar_create_resolves_mailbox_named_property_ids](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_calendar_create_resolves_mailbox_named_property_ids.md)
- [mapi_over_http_create_inline_attachment_preserves_content_id_metadata](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_create_inline_attachment_preserves_content_id_metadata.md)
- [mapi_over_http_microsoft_attach_text_file_stream_saves_canonical_attachment](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_attach_text_file_stream_saves_canonical_attachment.md)
- [mapi_over_http_fai_save_then_get_change_key_keeps_same_input_handle_in_buffer](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/save_changes_handles/mapi_over_http_fai_save_then_get_change_key_keeps_same_input_handle_in_buffer.md)
- [mapi_over_http_microsoft_oxcmsg_insert_html_embedded_image_is_imported_on_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcmsg_insert_html_embedded_image_is_imported_on_save.md)
- [mapi_over_http_message_list_settings_import_preserves_outlook_identity_and_content](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_message_list_settings_import_preserves_outlook_identity_and_content.md)