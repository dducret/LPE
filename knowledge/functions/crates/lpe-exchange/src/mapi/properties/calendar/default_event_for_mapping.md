---
type: Rust Function
title: default_event_for_mapping
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L453-L485
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/default_mapping_rights
  - functions/crates/lpe-storage/src/calendar/serialize_calendar_participants_metadata
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_pending_event
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/validate_pending_event_property_values
  - functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_oxprops_message_size_projects_integer32_property
  - functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_oxcdata_reminder_restriction_matches_recurring_calendar_items
  - functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_oxcdata_reminder_restriction_example_parses_and_matches
  - functions/crates/lpe-exchange/src/mapi/properties/tests/calendar_projection_does_not_mark_an_appointment_without_attendees_as_a_meeting
  - functions/crates/lpe-exchange/src/mapi/properties/tests/calendar_projection_keeps_a_named_exchange_recipient_as_a_meeting
  - functions/crates/lpe-exchange/src/mapi/properties/tests/calendar_projection_keeps_meeting_state_after_all_attendees_are_removed
  - functions/crates/lpe-exchange/src/mapi/properties/tests/calendar_projection_backs_outlook_table_identity_and_status_columns
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_writes_map_supported_mapi_fields_to_canonical_event_fields
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_outlook_free_appointment_is_not_imported_as_cancelled
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_w_europe_all_day_import_preserves_canonical_local_midnight
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_w_europe_all_day_projection_emits_utc_filetimes
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_state_flags_map_bounded_cancel_state
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_whole_start_end_write_to_canonical_start_duration
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_common_start_end_write_to_canonical_start_duration
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_meeting_response_classes_map_to_partstat
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_maps_month_end_rule
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_rejects_unsupported_shapes
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_binary_maps_to_canonical_daily_rule
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_binary_maps_monthly_and_yearly_rules
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_binary_maps_deleted_instances_to_overrides
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_binary_maps_modified_instances_to_overrides
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_binary_maps_subject_location_exceptions
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_binary_maps_mixed_deleted_and_modified_instances
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_projects_back_to_mapi_binary
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_modified_recurrence_projects_back_to_mapi_binary
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_subject_location_recurrence_projects_back_to_mapi_binary
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_mixed_recurrence_overrides_project_back_to_mapi_binary
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_month_end_recurrence_projects_back_to_mapi_binary
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_yearly_recurrence_projects_back_to_mapi_binary_with_month
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_yearly_nth_recurrence_projects_back_to_mapi_binary_with_month
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_event_row
  - functions/crates/lpe-exchange/src/mapi/tables/tests/calendar_contents_find_row_matches_outlook_date_window
---

# Signature

`pub(in crate::mapi) fn default_event_for_mapping( account_id: Uuid, collection_id: &str, ) -> AccessibleEvent`

# Calls

- [default_mapping_rights](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/default_mapping_rights.md)
- [serialize_calendar_participants_metadata](../../../../../../../functions/crates/lpe-storage/src/calendar/serialize_calendar_participants_metadata.md)

# Called by

- [save_pending_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_pending_event.md)
- [validate_pending_event_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/validate_pending_event_property_values.md)
- [microsoft_oxprops_message_size_projects_integer32_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_oxprops_message_size_projects_integer32_property.md)
- [microsoft_oxcdata_reminder_restriction_matches_recurring_calendar_items](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_oxcdata_reminder_restriction_matches_recurring_calendar_items.md)
- [microsoft_oxcdata_reminder_restriction_example_parses_and_matches](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/microsoft_oxcdata_reminder_restriction_example_parses_and_matches.md)
- [calendar_projection_does_not_mark_an_appointment_without_attendees_as_a_meeting](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/calendar_projection_does_not_mark_an_appointment_without_attendees_as_a_meeting.md)
- [calendar_projection_keeps_a_named_exchange_recipient_as_a_meeting](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/calendar_projection_keeps_a_named_exchange_recipient_as_a_meeting.md)
- [calendar_projection_keeps_meeting_state_after_all_attendees_are_removed](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/calendar_projection_keeps_meeting_state_after_all_attendees_are_removed.md)
- [calendar_projection_backs_outlook_table_identity_and_status_columns](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/calendar_projection_backs_outlook_table_identity_and_status_columns.md)
- [mapi_over_http_calendar_writes_map_supported_mapi_fields_to_canonical_event_fields](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_writes_map_supported_mapi_fields_to_canonical_event_fields.md)
- [mapi_over_http_outlook_free_appointment_is_not_imported_as_cancelled](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_outlook_free_appointment_is_not_imported_as_cancelled.md)
- [mapi_over_http_w_europe_all_day_import_preserves_canonical_local_midnight](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_w_europe_all_day_import_preserves_canonical_local_midnight.md)
- [mapi_over_http_w_europe_all_day_projection_emits_utc_filetimes](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_w_europe_all_day_projection_emits_utc_filetimes.md)
- [mapi_over_http_calendar_state_flags_map_bounded_cancel_state](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_state_flags_map_bounded_cancel_state.md)
- [mapi_over_http_calendar_whole_start_end_write_to_canonical_start_duration](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_whole_start_end_write_to_canonical_start_duration.md)
- [mapi_over_http_calendar_common_start_end_write_to_canonical_start_duration](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_common_start_end_write_to_canonical_start_duration.md)
- [mapi_over_http_calendar_meeting_response_classes_map_to_partstat](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_meeting_response_classes_map_to_partstat.md)
- [mapi_over_http_calendar_recurrence_maps_month_end_rule](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_maps_month_end_rule.md)
- [mapi_over_http_calendar_recurrence_rejects_unsupported_shapes](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_rejects_unsupported_shapes.md)
- [mapi_over_http_calendar_recurrence_binary_maps_to_canonical_daily_rule](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_binary_maps_to_canonical_daily_rule.md)
- [mapi_over_http_calendar_recurrence_binary_maps_monthly_and_yearly_rules](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_binary_maps_monthly_and_yearly_rules.md)
- [mapi_over_http_calendar_recurrence_binary_maps_deleted_instances_to_overrides](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_binary_maps_deleted_instances_to_overrides.md)
- [mapi_over_http_calendar_recurrence_binary_maps_modified_instances_to_overrides](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_binary_maps_modified_instances_to_overrides.md)
- [mapi_over_http_calendar_recurrence_binary_maps_subject_location_exceptions](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_binary_maps_subject_location_exceptions.md)
- [mapi_over_http_calendar_recurrence_binary_maps_mixed_deleted_and_modified_instances](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_binary_maps_mixed_deleted_and_modified_instances.md)
- [mapi_over_http_calendar_recurrence_projects_back_to_mapi_binary](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_projects_back_to_mapi_binary.md)
- [mapi_over_http_calendar_modified_recurrence_projects_back_to_mapi_binary](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_modified_recurrence_projects_back_to_mapi_binary.md)
- [mapi_over_http_calendar_subject_location_recurrence_projects_back_to_mapi_binary](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_subject_location_recurrence_projects_back_to_mapi_binary.md)
- [mapi_over_http_calendar_mixed_recurrence_overrides_project_back_to_mapi_binary](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_mixed_recurrence_overrides_project_back_to_mapi_binary.md)
- [mapi_over_http_calendar_month_end_recurrence_projects_back_to_mapi_binary](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_month_end_recurrence_projects_back_to_mapi_binary.md)
- [mapi_over_http_calendar_yearly_recurrence_projects_back_to_mapi_binary_with_month](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_yearly_recurrence_projects_back_to_mapi_binary_with_month.md)
- [mapi_over_http_calendar_yearly_nth_recurrence_projects_back_to_mapi_binary_with_month](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_yearly_nth_recurrence_projects_back_to_mapi_binary_with_month.md)
- [serialize_pending_event_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_event_row.md)
- [calendar_contents_find_row_matches_outlook_date_window](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/calendar_contents_find_row_matches_outlook_date_window.md)