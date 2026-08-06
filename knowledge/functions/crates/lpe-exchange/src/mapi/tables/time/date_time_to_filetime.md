---
type: Rust Function
title: date_time_to_filetime
resource: crates/lpe-exchange/src/mapi/tables/time.rs#L17-L44
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-exchange/src/mapi/tables/time/unix_seconds_to_filetime
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_writes_map_supported_mapi_fields_to_canonical_event_fields
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_outlook_free_appointment_is_not_imported_as_cancelled
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_w_europe_all_day_import_preserves_canonical_local_midnight
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_w_europe_time_conversion_observes_standard_and_daylight_biases
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_whole_start_end_write_to_canonical_start_duration
  - functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_common_start_end_write_to_canonical_start_duration
  - functions/crates/lpe-exchange/src/mapi/tables/time/date_time_to_filetime_in_time_zone
---

# Signature

`pub(in crate::mapi) fn date_time_to_filetime(date: &str, time: &str) -> u64`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [unix_seconds_to_filetime](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/unix_seconds_to_filetime.md)

# Called by

- [mapi_over_http_calendar_writes_map_supported_mapi_fields_to_canonical_event_fields](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_writes_map_supported_mapi_fields_to_canonical_event_fields.md)
- [mapi_over_http_outlook_free_appointment_is_not_imported_as_cancelled](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_outlook_free_appointment_is_not_imported_as_cancelled.md)
- [mapi_over_http_w_europe_all_day_import_preserves_canonical_local_midnight](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_w_europe_all_day_import_preserves_canonical_local_midnight.md)
- [mapi_over_http_w_europe_time_conversion_observes_standard_and_daylight_biases](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_w_europe_time_conversion_observes_standard_and_daylight_biases.md)
- [mapi_over_http_calendar_whole_start_end_write_to_canonical_start_duration](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_whole_start_end_write_to_canonical_start_duration.md)
- [mapi_over_http_calendar_common_start_end_write_to_canonical_start_duration](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/tests/mapi_over_http_calendar_common_start_end_write_to_canonical_start_duration.md)
- [date_time_to_filetime_in_time_zone](../../../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/date_time_to_filetime_in_time_zone.md)