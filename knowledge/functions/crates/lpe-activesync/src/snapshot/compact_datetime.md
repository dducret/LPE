---
type: Rust Function
title: compact_datetime
resource: crates/lpe-activesync/src/snapshot.rs#L327-L329
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/snapshot/calendar_application_data
  - functions/crates/lpe-activesync/src/snapshot/add_minutes_to_compact
---

# Signature

`fn compact_datetime(date: &str, time: &str) -> String`

# Called by

- [calendar_application_data](../../../../../functions/crates/lpe-activesync/src/snapshot/calendar_application_data.md)
- [add_minutes_to_compact](../../../../../functions/crates/lpe-activesync/src/snapshot/add_minutes_to_compact.md)