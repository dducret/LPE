---
type: Rust Function
title: ews_datetime
resource: crates/lpe-exchange/src/service/ews/calendar.rs#L93-L95
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/availability/ExchangeService/get_user_availability
  - functions/crates/lpe-exchange/src/service/ews/availability/event_overlaps_window
  - functions/crates/lpe-exchange/src/service/ews/calendar/event_end_datetime
---

# Signature

`pub(in crate::service) fn ews_datetime(date: &str, time: &str) -> String`

# Called by

- [get_user_availability](../../../../../../../functions/crates/lpe-exchange/src/service/ews/availability/ExchangeService/get_user_availability.md)
- [event_overlaps_window](../../../../../../../functions/crates/lpe-exchange/src/service/ews/availability/event_overlaps_window.md)
- [event_end_datetime](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/event_end_datetime.md)