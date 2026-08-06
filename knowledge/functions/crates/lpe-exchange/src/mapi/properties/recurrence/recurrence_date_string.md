---
type: Rust Function
title: recurrence_date_string
resource: crates/lpe-exchange/src/mapi/properties/recurrence.rs#L843-L848
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/appointment_recurrence_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_date_yyyymmdd
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_datetime_string
---

# Signature

`pub(super) fn recurrence_date_string(minutes_since_1601: u32) -> Result<String>`

# Called by

- [appointment_recurrence_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/appointment_recurrence_from_mapi.md)
- [recurrence_date_yyyymmdd](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_date_yyyymmdd.md)
- [recurrence_datetime_string](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_datetime_string.md)