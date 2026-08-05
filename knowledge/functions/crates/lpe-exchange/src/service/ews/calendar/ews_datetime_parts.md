---
type: Rust Function
title: ews_datetime_parts
resource: crates/lpe-exchange/src/service/ews/calendar.rs#L632-L640
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/service/ews/calendar/parse_create_event_input
  - functions/crates/lpe-exchange/src/service/ews/calendar/ews_duration_minutes
---

# Signature

`fn ews_datetime_parts(value: &str) -> Option<(String, String)>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [parse_create_event_input](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/parse_create_event_input.md)
- [ews_duration_minutes](../../../../../../../functions/crates/lpe-exchange/src/service/ews/calendar/ews_duration_minutes.md)