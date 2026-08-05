---
type: Rust Function
title: event_end_datetime
resource: crates/lpe-exchange/src/service/ews/calendar.rs#L97-L110
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/calendar/ews_datetime
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/availability/event_overlaps_window
---

# Signature

`pub(in crate::service) fn event_end_datetime(event: &AccessibleEvent) -> String`

# Calls

- [ews_datetime](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/ews_datetime.md)

# Called by

- [event_overlaps_window](../../../../../../../functions/crates/lpe-exchange/src/service/ews/availability/event_overlaps_window.md)