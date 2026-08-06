---
type: Rust Function
title: ews_date_after_days
resource: crates/lpe-exchange/src/service/ews/calendar.rs#L659-L663
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/calendar/ews_date_parts
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/calendar/event_end_datetime
---

# Signature

`fn ews_date_after_days(date: &str, days: i64) -> Option<String>`

# Calls

- [ews_date_parts](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/ews_date_parts.md)

# Called by

- [event_end_datetime](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/event_end_datetime.md)