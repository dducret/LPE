---
type: Rust Function
title: day_of_week_to_rrule
resource: crates/lpe-activesync/src/service/application_data.rs#L427-L444
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-activesync/src/service/application_data/recurrence_to_rrule
---

# Signature

`fn day_of_week_to_rrule(value: &str) -> Option<String>`

# Calls

- [push](../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [recurrence_to_rrule](../../../../../../functions/crates/lpe-activesync/src/service/application_data/recurrence_to_rrule.md)