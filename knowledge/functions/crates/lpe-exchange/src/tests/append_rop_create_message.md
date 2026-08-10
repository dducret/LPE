---
type: Rust Function
title: append_rop_create_message
resource: crates/lpe-exchange/src/tests/mod.rs#L15295-L15300
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/append_mapi_wire_id
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_save_maps_store_outcomes_and_preserves_pending_handle
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_keep_open_handle_accepts_second_update_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_default_save_closes_created_updated_and_noop_handles
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_second_save_without_global_object_id_uses_distinct_uid
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_calendar_create_accepts_html_stream_and_object_ids
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_calendar_create_resolves_mailbox_named_property_ids
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_empty_advertised_calendar_create_uses_default_collection
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_pending_event_modify_recipients_succeeds
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_reports_malformed_recurrence_and_saves_valid_properties
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_canonicalizes_bounded_meeting_request
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_commits_event_and_attachment_together
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_task_crud_uses_canonical_tasks
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_uses_postgresql_custom_calendar_collection
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_shared_calendar_read_only_rights_reject_mutations
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_mail_lifecycle_uses_canonical_state_end_to_end
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_notification_wait_reports_content_event_after_registered_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_outlook_contact_create_resolves_named_email_addresses
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contact_crud_uses_canonical_contacts
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/permissions/mapi_over_http_denies_mutation_without_folder_write_permission
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_custom_named_property_set_before_save_persists_on_created_item
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_oxcmsg_setting_message_properties_preserves_html_cid_body
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_string8_property_tags_round_trip_through_canonical_unicode_property
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_create_message_initializes_documented_properties
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_pending_message_display_recipients_follow_modify_recipients
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_set_properties_accepts_ptyp_server_id_on_pending_message
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_string8_body_stream_writes_canonical_message_body
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/submit_new_mapi_message_with_identities
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_microsoft_modify_recipients_accepts_type_flags_and_rejects_invalid_type
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_modify_recipients_wrapped_recipient_rows_save_canonically
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_modify_recipients_x500_rows_save_canonically
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_microsoft_modify_recipients_example_saves_canonically
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_transport_send_uses_canonical_submission
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_transport_send_target_entry_id_uses_outbox_mirror_and_import_move
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_replayed_execute_request_id_does_not_resubmit_message
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcmsg_save_message_keep_open_read_write_imports_canonical_email
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcmsg_insert_html_embedded_image_is_imported_on_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_associated_message_uploads_do_not_create_visible_items
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_microsoft_oxcmsg_name_to_id_mapping_works_on_message_object
  - functions/crates/lpe-exchange/src/tests/mapi_submit_execute_body
---

# Signature

`fn append_rop_create_message(rops: &mut Vec<u8>, input: u8, output: u8, folder_id: u64)`

# Calls

- [append_mapi_wire_id](../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_wire_id.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [mapi_over_http_calendar_create_save_maps_store_outcomes_and_preserves_pending_handle](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_save_maps_store_outcomes_and_preserves_pending_handle.md)
- [mapi_over_http_calendar_keep_open_handle_accepts_second_update_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_keep_open_handle_accepts_second_update_save.md)
- [mapi_over_http_calendar_default_save_closes_created_updated_and_noop_handles](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_default_save_closes_created_updated_and_noop_handles.md)
- [mapi_over_http_calendar_second_save_without_global_object_id_uses_distinct_uid](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_second_save_without_global_object_id_uses_distinct_uid.md)
- [mapi_over_http_outlook_calendar_create_accepts_html_stream_and_object_ids](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_calendar_create_accepts_html_stream_and_object_ids.md)
- [mapi_over_http_outlook_calendar_create_resolves_mailbox_named_property_ids](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_outlook_calendar_create_resolves_mailbox_named_property_ids.md)
- [mapi_over_http_empty_advertised_calendar_create_uses_default_collection](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_empty_advertised_calendar_create_uses_default_collection.md)
- [mapi_over_http_calendar_pending_event_modify_recipients_succeeds](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_pending_event_modify_recipients_succeeds.md)
- [mapi_over_http_calendar_create_reports_malformed_recurrence_and_saves_valid_properties](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_reports_malformed_recurrence_and_saves_valid_properties.md)
- [mapi_over_http_calendar_create_canonicalizes_bounded_meeting_request](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_canonicalizes_bounded_meeting_request.md)
- [mapi_over_http_calendar_create_commits_event_and_attachment_together](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_commits_event_and_attachment_together.md)
- [mapi_over_http_task_crud_uses_canonical_tasks](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_task_crud_uses_canonical_tasks.md)
- [mapi_over_http_calendar_create_uses_postgresql_custom_calendar_collection](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_create_uses_postgresql_custom_calendar_collection.md)
- [mapi_over_http_shared_calendar_read_only_rights_reject_mutations](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_shared_calendar_read_only_rights_reject_mutations.md)
- [mapi_over_http_mail_lifecycle_uses_canonical_state_end_to_end](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_mail_lifecycle_uses_canonical_state_end_to_end.md)
- [mapi_over_http_notification_wait_reports_content_event_after_registered_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/connect/mapi_over_http_notification_wait_reports_content_event_after_registered_save.md)
- [mapi_over_http_outlook_contact_create_resolves_named_email_addresses](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_outlook_contact_create_resolves_named_email_addresses.md)
- [mapi_over_http_contact_crud_uses_canonical_contacts](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/contacts/mapi_over_http_contact_crud_uses_canonical_contacts.md)
- [mapi_over_http_denies_mutation_without_folder_write_permission](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/permissions/mapi_over_http_denies_mutation_without_folder_write_permission.md)
- [mapi_over_http_custom_named_property_set_before_save_persists_on_created_item](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_custom_named_property_set_before_save_persists_on_created_item.md)
- [mapi_over_http_microsoft_oxcmsg_setting_message_properties_preserves_html_cid_body](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_oxcmsg_setting_message_properties_preserves_html_cid_body.md)
- [mapi_over_http_string8_property_tags_round_trip_through_canonical_unicode_property](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_string8_property_tags_round_trip_through_canonical_unicode_property.md)
- [mapi_over_http_microsoft_create_message_initializes_documented_properties](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_microsoft_create_message_initializes_documented_properties.md)
- [mapi_over_http_pending_message_display_recipients_follow_modify_recipients](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_pending_message_display_recipients_follow_modify_recipients.md)
- [mapi_over_http_set_properties_accepts_ptyp_server_id_on_pending_message](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_set_properties_accepts_ptyp_server_id_on_pending_message.md)
- [mapi_over_http_string8_body_stream_writes_canonical_message_body](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_string8_body_stream_writes_canonical_message_body.md)
- [submit_new_mapi_message_with_identities](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/submit_new_mapi_message_with_identities.md)
- [mapi_over_http_microsoft_modify_recipients_accepts_type_flags_and_rejects_invalid_type](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_microsoft_modify_recipients_accepts_type_flags_and_rejects_invalid_type.md)
- [mapi_over_http_modify_recipients_wrapped_recipient_rows_save_canonically](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_modify_recipients_wrapped_recipient_rows_save_canonically.md)
- [mapi_over_http_modify_recipients_x500_rows_save_canonically](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_modify_recipients_x500_rows_save_canonically.md)
- [mapi_over_http_microsoft_modify_recipients_example_saves_canonically](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_microsoft_modify_recipients_example_saves_canonically.md)
- [mapi_over_http_transport_send_uses_canonical_submission](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_transport_send_uses_canonical_submission.md)
- [mapi_over_http_transport_send_target_entry_id_uses_outbox_mirror_and_import_move](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_transport_send_target_entry_id_uses_outbox_mirror_and_import_move.md)
- [mapi_over_http_replayed_execute_request_id_does_not_resubmit_message](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/submission/mapi_over_http_replayed_execute_request_id_does_not_resubmit_message.md)
- [mapi_over_http_microsoft_oxcmsg_save_message_keep_open_read_write_imports_canonical_email](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcmsg_save_message_keep_open_read_write_imports_canonical_email.md)
- [mapi_over_http_microsoft_oxcmsg_insert_html_embedded_image_is_imported_on_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_microsoft_oxcmsg_insert_html_embedded_image_is_imported_on_save.md)
- [mapi_over_http_associated_message_uploads_do_not_create_visible_items](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_associated_message_uploads_do_not_create_visible_items.md)
- [mapi_over_http_microsoft_oxcmsg_name_to_id_mapping_works_on_message_object](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/transport/mapi_over_http_microsoft_oxcmsg_name_to_id_mapping_works_on_message_object.md)
- [mapi_submit_execute_body](../../../../../functions/crates/lpe-exchange/src/tests/mapi_submit_execute_body.md)