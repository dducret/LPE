---
type: Rust Function
title: recurrence_month_from_minutes
resource: crates/lpe-exchange/src/mapi/properties/recurrence.rs#L850-L859
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/civil_time/days_from_civil
  - functions/crates/lpe-domain/src/civil_time/civil_from_days
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_pattern
---

# Signature

`fn recurrence_month_from_minutes(minutes_since_1601: u32) -> Result<u32>`

# Calls

- [days_from_civil](../../../../../../../functions/crates/lpe-domain/src/civil_time/days_from_civil.md)
- [civil_from_days](../../../../../../../functions/crates/lpe-domain/src/civil_time/civil_from_days.md)

# Called by

- [read_recur_pattern](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_pattern.md)