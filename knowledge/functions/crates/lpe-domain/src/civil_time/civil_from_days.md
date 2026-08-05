---
type: Rust Function
title: civil_from_days
resource: crates/lpe-domain/src/civil_time.rs#L24-L36
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/snapshot/add_minutes_to_compact
  - functions/crates/lpe-domain/src/civil_time/utc_from_unix_seconds
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_date_string
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_month_from_minutes
---

# Signature

`pub fn civil_from_days(days_since_epoch: i64) -> (i64, i64, i64)`

# Called by

- [add_minutes_to_compact](../../../../../functions/crates/lpe-activesync/src/snapshot/add_minutes_to_compact.md)
- [utc_from_unix_seconds](../../../../../functions/crates/lpe-domain/src/civil_time/utc_from_unix_seconds.md)
- [recurrence_date_string](../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_date_string.md)
- [recurrence_month_from_minutes](../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_month_from_minutes.md)