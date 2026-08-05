---
type: Rust Function
title: appointment_recurrence_from_mapi
resource: crates/lpe-exchange/src/mapi/properties/recurrence.rs#L386-L531
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_u16
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_u32
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_pattern
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_dates
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_exception_infos
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_extended_exceptions
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_date_yyyymmdd
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_date_string
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_datetime_string
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/calendar/event_input_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_projects_back_to_mapi_binary
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_modified_recurrence_projects_back_to_mapi_binary
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_subject_location_recurrence_projects_back_to_mapi_binary
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_mixed_recurrence_overrides_project_back_to_mapi_binary
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_month_end_recurrence_projects_back_to_mapi_binary
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_yearly_recurrence_projects_back_to_mapi_binary_with_month
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_yearly_nth_recurrence_projects_back_to_mapi_binary_with_month
---

# Signature

`pub(super) fn appointment_recurrence_from_mapi(value: &[u8]) -> Result<MapiAppointmentRecurrence>`

# Calls

- [read_recur_u16](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_u16.md)
- [read_recur_u32](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_u32.md)
- [read_recur_pattern](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_pattern.md)
- [read_recur_dates](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_dates.md)
- [read_recur_exception_infos](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_exception_infos.md)
- [read_recur_extended_exceptions](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_extended_exceptions.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)
- [recurrence_date_yyyymmdd](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_date_yyyymmdd.md)
- [recurrence_date_string](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_date_string.md)
- [recurrence_datetime_string](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_datetime_string.md)

# Called by

- [event_input_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/calendar/event_input_from_mapi.md)
- [mapi_over_http_calendar_recurrence_projects_back_to_mapi_binary](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_recurrence_projects_back_to_mapi_binary.md)
- [mapi_over_http_calendar_modified_recurrence_projects_back_to_mapi_binary](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_modified_recurrence_projects_back_to_mapi_binary.md)
- [mapi_over_http_calendar_subject_location_recurrence_projects_back_to_mapi_binary](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_subject_location_recurrence_projects_back_to_mapi_binary.md)
- [mapi_over_http_calendar_mixed_recurrence_overrides_project_back_to_mapi_binary](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_mixed_recurrence_overrides_project_back_to_mapi_binary.md)
- [mapi_over_http_calendar_month_end_recurrence_projects_back_to_mapi_binary](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_month_end_recurrence_projects_back_to_mapi_binary.md)
- [mapi_over_http_calendar_yearly_recurrence_projects_back_to_mapi_binary_with_month](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_yearly_recurrence_projects_back_to_mapi_binary_with_month.md)
- [mapi_over_http_calendar_yearly_nth_recurrence_projects_back_to_mapi_binary_with_month](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_yearly_nth_recurrence_projects_back_to_mapi_binary_with_month.md)