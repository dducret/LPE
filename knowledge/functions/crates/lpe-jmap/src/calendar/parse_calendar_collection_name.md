---
type: Rust Function
title: parse_calendar_collection_name
resource: crates/lpe-jmap/src/calendar.rs#L725-L736
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-jmap/src/parse/parse_required_string
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_set
  - functions/crates/lpe-jmap/src/calendar/JmapService/calendar_update_name
---

# Signature

`fn parse_calendar_collection_name(value: &Value) -> Result<String>`

# Calls

- [parse_required_string](../../../../../functions/crates/lpe-jmap/src/parse/parse_required_string.md)
- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [handle_calendar_set](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/handle_calendar_set.md)
- [calendar_update_name](../../../../../functions/crates/lpe-jmap/src/calendar/JmapService/calendar_update_name.md)