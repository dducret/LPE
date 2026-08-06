---
type: Rust Function
title: ews_datetime_minutes
resource: crates/lpe-exchange/src/service/ews/calendar.rs#L654-L657
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/calendar/ews_datetime_parts
  - functions/crates/lpe-exchange/src/service/ews/calendar/ews_date_parts
  - functions/crates/lpe-exchange/src/service/ews/calendar/time_minutes
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/calendar/ews_duration_minutes
---

# Signature

`fn ews_datetime_minutes(value: &str) -> Option<i64>`

# Calls

- [ews_datetime_parts](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/ews_datetime_parts.md)
- [ews_date_parts](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/ews_date_parts.md)
- [time_minutes](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/time_minutes.md)

# Called by

- [ews_duration_minutes](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/ews_duration_minutes.md)