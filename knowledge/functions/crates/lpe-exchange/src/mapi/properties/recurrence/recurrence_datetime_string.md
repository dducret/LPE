---
type: Rust Function
title: recurrence_datetime_string
resource: crates/lpe-exchange/src/mapi/properties/recurrence.rs#L861-L865
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_date_string
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/appointment_recurrence_from_mapi
---

# Signature

`fn recurrence_datetime_string(minutes_since_1601: u32) -> Result<String>`

# Calls

- [recurrence_date_string](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_date_string.md)

# Called by

- [appointment_recurrence_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/appointment_recurrence_from_mapi.md)