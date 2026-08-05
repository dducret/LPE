---
type: Rust Function
title: add_minutes_to_compact
resource: crates/lpe-activesync/src/snapshot.rs#L331-L349
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/snapshot/parse_date
  - functions/crates/lpe-activesync/src/snapshot/compact_datetime
  - functions/crates/lpe-activesync/src/snapshot/parse_time
  - functions/crates/lpe-domain/src/civil_time/days_from_civil
  - functions/crates/lpe-domain/src/civil_time/civil_from_days
  called_by:
  - functions/crates/lpe-activesync/src/snapshot/calendar_application_data
---

# Signature

`fn add_minutes_to_compact(date: &str, time: &str, duration_minutes: i32) -> String`

# Calls

- [parse_date](../../../../../functions/crates/lpe-activesync/src/snapshot/parse_date.md)
- [compact_datetime](../../../../../functions/crates/lpe-activesync/src/snapshot/compact_datetime.md)
- [parse_time](../../../../../functions/crates/lpe-activesync/src/snapshot/parse_time.md)
- [days_from_civil](../../../../../functions/crates/lpe-domain/src/civil_time/days_from_civil.md)
- [civil_from_days](../../../../../functions/crates/lpe-domain/src/civil_time/civil_from_days.md)

# Called by

- [calendar_application_data](../../../../../functions/crates/lpe-activesync/src/snapshot/calendar_application_data.md)