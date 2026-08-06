---
type: Rust Function
title: time_minutes
resource: crates/lpe-exchange/src/service/ews/calendar.rs#L647-L652
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/calendar/event_end_datetime
  - functions/crates/lpe-exchange/src/service/ews/calendar/ews_datetime_minutes
---

# Signature

`fn time_minutes(value: &str) -> Option<i32>`

# Called by

- [event_end_datetime](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/event_end_datetime.md)
- [ews_datetime_minutes](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/ews_datetime_minutes.md)