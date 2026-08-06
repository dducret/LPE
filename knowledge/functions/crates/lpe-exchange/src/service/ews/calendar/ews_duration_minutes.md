---
type: Rust Function
title: ews_duration_minutes
resource: crates/lpe-exchange/src/service/ews/calendar.rs#L639-L645
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/service/ews/calendar/ews_datetime_minutes
  - functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/calendar/parse_create_event_input
  - functions/crates/lpe-exchange/src/service/ews/calendar/parse_update_event_input
---

# Signature

`fn ews_duration_minutes(start: &str, end: &str) -> Option<i32>`

# Calls

- [ews_datetime_minutes](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/ews_datetime_minutes.md)
- [try_from](../../../../../../../functions/crates/lpe-activesync/src/protocol/WbxmlCodePage/tryfrom-u8/try_from.md)

# Called by

- [parse_create_event_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/parse_create_event_input.md)
- [parse_update_event_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/parse_update_event_input.md)