---
type: Rust Function
title: save_staged_calendar_event
resource: crates/lpe-exchange/src/tests/mapi_over_http/calendar.rs#L25-L45
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/tests/append_rop_save_changes_message
  - functions/crates/lpe-exchange/src/tests/renew_mapi_request_id
  - functions/crates/lpe-exchange/src/tests/execute_body
  - functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response
  called_by:
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_custom_properties_survive_restart_style_session
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_delete_properties_clears_canonical_and_custom_fields
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_delete_reminder_delta_reports_problem_without_hiding_reminder
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_meeting_cancel_save_fails_closed_without_atomic_delete
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_meeting_response_updates_canonical_attendee_status
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_attendee_named_properties_update_canonical_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_display_cc_updates_optional_attendees
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_time_zone_description_updates_canonical_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_whole_start_end_update_canonical_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_common_start_end_update_canonical_event
  - functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_state_flags_cancel_updates_canonical_event_status
---

# Signature

`async fn save_staged_calendar_event( service: &ExchangeService<FakeStore>, execute_headers: &mut HeaderMap, handle_slots: &[u32], ) -> Vec<u8>`

# Calls

- [append_rop_save_changes_message](../../../../../../../functions/crates/lpe-exchange/src/tests/append_rop_save_changes_message.md)
- [renew_mapi_request_id](../../../../../../../functions/crates/lpe-exchange/src/tests/renew_mapi_request_id.md)
- [execute_body](../../../../../../../functions/crates/lpe-exchange/src/tests/execute_body.md)
- [response_rops_from_execute_response](../../../../../../../functions/crates/lpe-exchange/src/tests/response_rops_from_execute_response.md)

# Called by

- [mapi_over_http_calendar_custom_properties_survive_restart_style_session](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_custom_properties_survive_restart_style_session.md)
- [mapi_over_http_calendar_delete_properties_clears_canonical_and_custom_fields](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_delete_properties_clears_canonical_and_custom_fields.md)
- [mapi_over_http_calendar_delete_reminder_delta_reports_problem_without_hiding_reminder](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_delete_reminder_delta_reports_problem_without_hiding_reminder.md)
- [mapi_over_http_calendar_meeting_cancel_save_fails_closed_without_atomic_delete](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_meeting_cancel_save_fails_closed_without_atomic_delete.md)
- [mapi_over_http_calendar_meeting_response_updates_canonical_attendee_status](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_meeting_response_updates_canonical_attendee_status.md)
- [mapi_over_http_calendar_attendee_named_properties_update_canonical_event](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_attendee_named_properties_update_canonical_event.md)
- [mapi_over_http_calendar_display_cc_updates_optional_attendees](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_display_cc_updates_optional_attendees.md)
- [mapi_over_http_calendar_time_zone_description_updates_canonical_event](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_time_zone_description_updates_canonical_event.md)
- [mapi_over_http_calendar_whole_start_end_update_canonical_event](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_whole_start_end_update_canonical_event.md)
- [mapi_over_http_calendar_common_start_end_update_canonical_event](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_common_start_end_update_canonical_event.md)
- [mapi_over_http_calendar_state_flags_cancel_updates_canonical_event_status](../../../../../../../functions/crates/lpe-exchange/src/tests/mapi_over_http/calendar/mapi_over_http_calendar_state_flags_cancel_updates_canonical_event_status.md)