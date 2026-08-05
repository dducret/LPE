---
type: Rust Function
title: event_property_value
resource: crates/lpe-exchange/src/mapi/properties/calendar.rs#L6-L13
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_reminder
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_calendar_event_query_position_summary
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_projects_back_to_mapi_binary
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_modified_recurrence_projects_back_to_mapi_binary
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_subject_location_recurrence_projects_back_to_mapi_binary
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_mixed_recurrence_overrides_project_back_to_mapi_binary
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_month_end_recurrence_projects_back_to_mapi_binary
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_yearly_recurrence_projects_back_to_mapi_binary_with_month
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_yearly_nth_recurrence_projects_back_to_mapi_binary_with_month
  - functions/crates/lpe-exchange/src/mapi/tables/tests/calendar_contents_find_row_matches_outlook_date_window
---

# Signature

`pub(in crate::mapi) fn event_property_value( event: &AccessibleEvent, item_id: u64, folder_id: u64, property_tag: u32, ) -> Option<MapiValue>`

# Calls

- [event_property_value_with_reminder](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_property_value_with_reminder.md)

# Called by

- [format_calendar_event_query_position_summary](../../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/tables/format_calendar_event_query_position_summary.md)
- [mapi_over_http_calendar_recurrence_projects_back_to_mapi_binary](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_projects_back_to_mapi_binary.md)
- [mapi_over_http_calendar_modified_recurrence_projects_back_to_mapi_binary](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_modified_recurrence_projects_back_to_mapi_binary.md)
- [mapi_over_http_calendar_subject_location_recurrence_projects_back_to_mapi_binary](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_subject_location_recurrence_projects_back_to_mapi_binary.md)
- [mapi_over_http_calendar_mixed_recurrence_overrides_project_back_to_mapi_binary](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_mixed_recurrence_overrides_project_back_to_mapi_binary.md)
- [mapi_over_http_calendar_month_end_recurrence_projects_back_to_mapi_binary](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_month_end_recurrence_projects_back_to_mapi_binary.md)
- [mapi_over_http_calendar_yearly_recurrence_projects_back_to_mapi_binary_with_month](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_yearly_recurrence_projects_back_to_mapi_binary_with_month.md)
- [mapi_over_http_calendar_yearly_nth_recurrence_projects_back_to_mapi_binary_with_month](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_yearly_nth_recurrence_projects_back_to_mapi_binary_with_month.md)
- [calendar_contents_find_row_matches_outlook_date_window](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/tests/calendar_contents_find_row_matches_outlook_date_window.md)