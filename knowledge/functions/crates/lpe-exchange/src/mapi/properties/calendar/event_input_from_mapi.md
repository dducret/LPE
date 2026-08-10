---
type: Rust Function
title: event_input_from_mapi
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L669-L769
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/reject_unsupported_mapi_event_properties
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_participants_from_mapi
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/appointment_recurrence_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone_from_mapi
  - functions/crates/lpe-exchange/src/mapi/tables/time/filetime_to_date_time_in_time_zone
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_status_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/clearable_pending_text_property
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/clearable_pending_html_property
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_pending_event
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/validate_pending_event_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/validate_staged_event_property_values
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/staged_event_commit_input
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/apply_canonical_event_property_values
  - functions/crates/lpe-exchange/src/mapi/properties/tests/calendar_projection_does_not_mark_an_appointment_without_attendees_as_a_meeting
  - functions/crates/lpe-exchange/src/mapi/properties/tests/calendar_projection_keeps_a_named_exchange_recipient_as_a_meeting
  - functions/crates/lpe-exchange/src/mapi/properties/tests/calendar_projection_keeps_meeting_state_after_all_attendees_are_removed
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_writes_map_supported_mapi_fields_to_canonical_event_fields
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_outlook_free_appointment_is_not_imported_as_cancelled
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_w_europe_all_day_import_preserves_canonical_local_midnight
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_state_flags_map_bounded_cancel_state
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_whole_start_end_write_to_canonical_start_duration
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_common_start_end_write_to_canonical_start_duration
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_maps_month_end_rule
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_rejects_unsupported_shapes
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_binary_maps_to_canonical_daily_rule
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_binary_maps_monthly_and_yearly_rules
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_binary_maps_deleted_instances_to_overrides
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_binary_maps_modified_instances_to_overrides
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_binary_maps_subject_location_exceptions
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_binary_maps_mixed_deleted_and_modified_instances
  - functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_event_row
---

# Signature

`pub(in crate::mapi) fn event_input_from_mapi( account_id: Uuid, id: Option<Uuid>, existing: &AccessibleEvent, properties: &HashMap<u32, MapiValue>, ) -> Result<UpsertClientEventInput>`

# Calls

- [reject_unsupported_mapi_event_properties](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/reject_unsupported_mapi_event_properties.md)
- [event_participants_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_participants_from_mapi.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [appointment_recurrence_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/appointment_recurrence_from_mapi.md)
- [calendar_time_zone_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_time_zone_from_mapi.md)
- [filetime_to_date_time_in_time_zone](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/filetime_to_date_time_in_time_zone.md)
- [calendar_status_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/calendar_status_from_mapi.md)
- [clearable_pending_text_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/clearable_pending_text_property.md)
- [clearable_pending_html_property](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/clearable_pending_html_property.md)

# Called by

- [save_pending_event](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_pending_event.md)
- [validate_pending_event_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/validate_pending_event_property_values.md)
- [validate_staged_event_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/validate_staged_event_property_values.md)
- [staged_event_commit_input](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_transactions/staged_event_commit_input.md)
- [apply_canonical_event_property_values](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/apply_canonical_event_property_values.md)
- [calendar_projection_does_not_mark_an_appointment_without_attendees_as_a_meeting](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/calendar_projection_does_not_mark_an_appointment_without_attendees_as_a_meeting.md)
- [calendar_projection_keeps_a_named_exchange_recipient_as_a_meeting](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/calendar_projection_keeps_a_named_exchange_recipient_as_a_meeting.md)
- [calendar_projection_keeps_meeting_state_after_all_attendees_are_removed](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/calendar_projection_keeps_meeting_state_after_all_attendees_are_removed.md)
- [mapi_over_http_calendar_writes_map_supported_mapi_fields_to_canonical_event_fields](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_writes_map_supported_mapi_fields_to_canonical_event_fields.md)
- [mapi_over_http_outlook_free_appointment_is_not_imported_as_cancelled](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_outlook_free_appointment_is_not_imported_as_cancelled.md)
- [mapi_over_http_w_europe_all_day_import_preserves_canonical_local_midnight](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_w_europe_all_day_import_preserves_canonical_local_midnight.md)
- [mapi_over_http_calendar_state_flags_map_bounded_cancel_state](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_state_flags_map_bounded_cancel_state.md)
- [mapi_over_http_calendar_whole_start_end_write_to_canonical_start_duration](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_whole_start_end_write_to_canonical_start_duration.md)
- [mapi_over_http_calendar_common_start_end_write_to_canonical_start_duration](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_common_start_end_write_to_canonical_start_duration.md)
- [mapi_over_http_calendar_recurrence_maps_month_end_rule](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_maps_month_end_rule.md)
- [mapi_over_http_calendar_recurrence_rejects_unsupported_shapes](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_rejects_unsupported_shapes.md)
- [mapi_over_http_calendar_recurrence_binary_maps_to_canonical_daily_rule](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_binary_maps_to_canonical_daily_rule.md)
- [mapi_over_http_calendar_recurrence_binary_maps_monthly_and_yearly_rules](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_binary_maps_monthly_and_yearly_rules.md)
- [mapi_over_http_calendar_recurrence_binary_maps_deleted_instances_to_overrides](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_binary_maps_deleted_instances_to_overrides.md)
- [mapi_over_http_calendar_recurrence_binary_maps_modified_instances_to_overrides](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_binary_maps_modified_instances_to_overrides.md)
- [mapi_over_http_calendar_recurrence_binary_maps_subject_location_exceptions](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_binary_maps_subject_location_exceptions.md)
- [mapi_over_http_calendar_recurrence_binary_maps_mixed_deleted_and_modified_instances](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_binary_maps_mixed_deleted_and_modified_instances.md)
- [serialize_pending_event_row](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/pending/serialize_pending_event_row.md)