---
type: Rust Function
title: recurrence_date_string
resource: crates/lpe-exchange/src/mapi/properties/recurrence.rs#L843-L848
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-domain/src/civil_time/days_from_civil
  - functions/crates/lpe-domain/src/civil_time/civil_from_days
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/appointment_recurrence_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_date_yyyymmdd
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_datetime_string
---

# Signature

`pub(super) fn recurrence_date_string(minutes_since_1601: u32) -> Result<String>`

# Calls

- [days_from_civil](../../../../../../../functions/crates/lpe-domain/src/civil_time/days_from_civil.md)
- [civil_from_days](../../../../../../../functions/crates/lpe-domain/src/civil_time/civil_from_days.md)

# Called by

- [appointment_recurrence_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/appointment_recurrence_from_mapi.md)
- [recurrence_date_yyyymmdd](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_date_yyyymmdd.md)
- [recurrence_datetime_string](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_datetime_string.md)