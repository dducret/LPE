---
type: Rust Function
title: record_mapi_calendar_event_save
resource: crates/lpe-exchange/src/mapi/event_metrics.rs#L35-L60
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_existing_event
  - functions/crates/lpe-exchange/src/mapi/event_metrics/calendar_event_save_metrics_preserve_direct_and_ics_outcomes
---

# Signature

`pub(crate) fn record_mapi_calendar_event_save( flow: MapiCalendarEventSaveFlow, outcome: MapiCalendarEventSaveOutcome, )`

# Called by

- [save_existing_event](../../../../../../functions/crates/lpe-exchange/src/mapi/dispatch/event_save/save_existing_event.md)
- [calendar_event_save_metrics_preserve_direct_and_ics_outcomes](../../../../../../functions/crates/lpe-exchange/src/mapi/event_metrics/calendar_event_save_metrics_preserve_direct_and_ics_outcomes.md)