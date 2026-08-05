---
type: Rust Function
title: recurrence_to_rrule
resource: crates/lpe-activesync/src/service/application_data.rs#L360-L425
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/service/application_data/day_of_week_to_rrule
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-activesync/src/service/application_data/parse_event_input
---

# Signature

`fn recurrence_to_rrule(recurrence: &WbxmlNode) -> Result<String>`

# Calls

- [day_of_week_to_rrule](../../../../../../functions/crates/lpe-activesync/src/service/application_data/day_of_week_to_rrule.md)
- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [parse_event_input](../../../../../../functions/crates/lpe-activesync/src/service/application_data/parse_event_input.md)