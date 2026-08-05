---
type: Rust Function
title: append_rop_open_message_with_flags
resource: crates/lpe-exchange/src/tests/mod.rs#L15485-L15498
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/append_mapi_wire_id
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/mapi_over_http_set_properties_updates_canonical_event_and_task_reminders
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_custom_properties_survive_restart_style_session
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_delete_properties_clears_canonical_and_custom_fields
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_delete_reminder_delta_reports_problem_without_hiding_reminder
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_read_only_handle_rejects_every_save_disposition
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_custom_property_get_uses_same_handle_transaction_overlay
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_default_save_closes_created_updated_and_noop_handles
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_save_projects_committed_far_future_reminder_without_query_reread
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_event_handle_stages_until_save_and_release_discards
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_existing_calendar_body_stream_uses_parent_event_transaction
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_selective_reopen_uses_durable_event_modseq
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_concurrent_rw_handles_require_force_save
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_advertised_calendar_update_delete_uses_default_collection_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_meeting_cancel_save_fails_closed_without_atomic_delete
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_meeting_response_updates_canonical_attendee_status
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_attendee_named_properties_update_canonical_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_display_cc_updates_optional_attendees
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_time_zone_description_updates_canonical_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_whole_start_end_update_canonical_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_common_start_end_update_canonical_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_state_flags_cancel_updates_canonical_event_status
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_attachment_waits_for_parent_save_and_is_handle_local
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_delete_attachment_is_handle_local_and_release_abandons
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_reopen_update_uses_postgresql_custom_calendar_collection
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_custom_named_properties_round_trip_on_canonical_item_kinds
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_existing_common_views_wlink_stages_until_atomic_save_in_postgresql
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_existing_common_views_wlink_entry_id_replacement_is_staged_until_atomic_save_in_postgresql
  - functions/crates/lpe-exchange/src/tests/append_rop_open_message
---

# Signature

`fn append_rop_open_message_with_flags( rops: &mut Vec<u8>, input: u8, output: u8, folder_id: u64, message_id: u64, open_mode_flags: u8, )`

# Calls

- [append_mapi_wire_id](../../../../../functions/crates/lpe-exchange/src/tests/append_mapi_wire_id.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [mapi_over_http_set_properties_updates_canonical_event_and_task_reminders](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/mapi_over_http_set_properties_updates_canonical_event_and_task_reminders.md)
- [mapi_over_http_calendar_custom_properties_survive_restart_style_session](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_custom_properties_survive_restart_style_session.md)
- [mapi_over_http_calendar_delete_properties_clears_canonical_and_custom_fields](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_delete_properties_clears_canonical_and_custom_fields.md)
- [mapi_over_http_calendar_delete_reminder_delta_reports_problem_without_hiding_reminder](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_delete_reminder_delta_reports_problem_without_hiding_reminder.md)
- [mapi_over_http_calendar_read_only_handle_rejects_every_save_disposition](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_read_only_handle_rejects_every_save_disposition.md)
- [mapi_over_http_calendar_custom_property_get_uses_same_handle_transaction_overlay](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_custom_property_get_uses_same_handle_transaction_overlay.md)
- [mapi_over_http_calendar_default_save_closes_created_updated_and_noop_handles](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_default_save_closes_created_updated_and_noop_handles.md)
- [mapi_over_http_calendar_save_projects_committed_far_future_reminder_without_query_reread](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_save_projects_committed_far_future_reminder_without_query_reread.md)
- [mapi_over_http_calendar_event_handle_stages_until_save_and_release_discards](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_event_handle_stages_until_save_and_release_discards.md)
- [mapi_over_http_existing_calendar_body_stream_uses_parent_event_transaction](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_existing_calendar_body_stream_uses_parent_event_transaction.md)
- [mapi_over_http_calendar_selective_reopen_uses_durable_event_modseq](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_selective_reopen_uses_durable_event_modseq.md)
- [mapi_over_http_calendar_concurrent_rw_handles_require_force_save](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_concurrent_rw_handles_require_force_save.md)
- [mapi_over_http_advertised_calendar_update_delete_uses_default_collection_event](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_advertised_calendar_update_delete_uses_default_collection_event.md)
- [mapi_over_http_calendar_meeting_cancel_save_fails_closed_without_atomic_delete](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_meeting_cancel_save_fails_closed_without_atomic_delete.md)
- [mapi_over_http_calendar_meeting_response_updates_canonical_attendee_status](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_meeting_response_updates_canonical_attendee_status.md)
- [mapi_over_http_calendar_attendee_named_properties_update_canonical_event](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_attendee_named_properties_update_canonical_event.md)
- [mapi_over_http_calendar_display_cc_updates_optional_attendees](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_display_cc_updates_optional_attendees.md)
- [mapi_over_http_calendar_time_zone_description_updates_canonical_event](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_time_zone_description_updates_canonical_event.md)
- [mapi_over_http_calendar_whole_start_end_update_canonical_event](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_whole_start_end_update_canonical_event.md)
- [mapi_over_http_calendar_common_start_end_update_canonical_event](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_common_start_end_update_canonical_event.md)
- [mapi_over_http_calendar_state_flags_cancel_updates_canonical_event_status](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_state_flags_cancel_updates_canonical_event_status.md)
- [mapi_over_http_calendar_attachment_waits_for_parent_save_and_is_handle_local](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_attachment_waits_for_parent_save_and_is_handle_local.md)
- [mapi_over_http_calendar_delete_attachment_is_handle_local_and_release_abandons](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_delete_attachment_is_handle_local_and_release_abandons.md)
- [mapi_over_http_calendar_reopen_update_uses_postgresql_custom_calendar_collection](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_reopen_update_uses_postgresql_custom_calendar_collection.md)
- [mapi_over_http_custom_named_properties_round_trip_on_canonical_item_kinds](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/properties/mapi_over_http_custom_named_properties_round_trip_on_canonical_item_kinds.md)
- [mapi_over_http_existing_common_views_wlink_stages_until_atomic_save_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_existing_common_views_wlink_stages_until_atomic_save_in_postgresql.md)
- [mapi_over_http_existing_common_views_wlink_entry_id_replacement_is_staged_until_atomic_save_in_postgresql](../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/sync/mapi_over_http_existing_common_views_wlink_entry_id_replacement_is_staged_until_atomic_save_in_postgresql.md)
- [append_rop_open_message](../../../../../functions/crates/lpe-exchange/src/tests/append_rop_open_message.md)