---
type: Rust Function
title: insert_json_if
resource: crates/lpe-jmap/src/calendar.rs#L940-L952
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str
  called_by:
  - functions/crates/lpe-jmap/src/calendar/calendar_event_to_value
---

# Signature

`fn insert_json_if( properties: &HashSet<String>, object: &mut Map<String, Value>, key: &str, raw: &str, )`

# Calls

- [from_str](../../../../../functions/crates/lpe-storage/src/change/CanonicalChangeCategory/from_str.md)

# Called by

- [calendar_event_to_value](../../../../../functions/crates/lpe-jmap/src/calendar/calendar_event_to_value.md)