---
type: Rust Function
title: days_from_civil
resource: crates/lpe-domain/src/civil_time.rs#L14-L22
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/service/application_data/date_time_to_minutes
  - functions/crates/lpe-activesync/src/snapshot/add_minutes_to_compact
  - functions/crates/lpe-domain/src/mail_format/weekday_name
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_minutes_since_1601
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_date_string
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_month_from_minutes
  - functions/crates/lpe-exchange/src/mapi/tables/time/date_time_to_filetime
  - functions/crates/lpe-exchange/src/mapi/tables/time/western_europe_transition_seconds
  - functions/crates/lpe-exchange/src/mapi_mailstore/manifest/parse_rfc3339_utc_filetime
---

# Signature

`pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64`

# Called by

- [date_time_to_minutes](../../../../../functions/crates/lpe-activesync/src/service/application_data/date_time_to_minutes.md)
- [add_minutes_to_compact](../../../../../functions/crates/lpe-activesync/src/snapshot/add_minutes_to_compact.md)
- [weekday_name](../../../../../functions/crates/lpe-domain/src/mail_format/weekday_name.md)
- [recurrence_minutes_since_1601](../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_minutes_since_1601.md)
- [recurrence_date_string](../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_date_string.md)
- [recurrence_month_from_minutes](../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_month_from_minutes.md)
- [date_time_to_filetime](../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/date_time_to_filetime.md)
- [western_europe_transition_seconds](../../../../../functions/crates/lpe-exchange/src/mapi/tables/time/western_europe_transition_seconds.md)
- [parse_rfc3339_utc_filetime](../../../../../functions/crates/lpe-exchange/src/mapi_mailstore/manifest/parse_rfc3339_utc_filetime.md)