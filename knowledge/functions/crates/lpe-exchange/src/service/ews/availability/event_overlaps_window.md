---
type: Rust Function
title: event_overlaps_window
resource: crates/lpe-exchange/src/service/ews/availability.rs#L141-L150
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/calendar/ews_datetime
  - functions/crates/lpe-exchange/src/service/ews/calendar/event_end_datetime
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/availability/ExchangeService/get_user_availability
---

# Signature

`pub(in crate::service) fn event_overlaps_window( event: &AccessibleEvent, start: Option<&str>, end: Option<&str>, ) -> bool`

# Calls

- [ews_datetime](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/ews_datetime.md)
- [event_end_datetime](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/event_end_datetime.md)

# Called by

- [get_user_availability](../../../../../../../functions/crates/lpe-exchange/src/service/ews/availability/ExchangeService/get_user_availability.md)