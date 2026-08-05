---
type: Rust Function
title: read_recur_pattern
resource: crates/lpe-exchange/src/mapi/properties/recurrence.rs#L542-L637
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_u32
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_days_from_mask
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_month_from_minutes
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/appointment_recurrence_from_mapi
---

# Signature

`fn read_recur_pattern( value: &[u8], offset: &mut usize, frequency: u16, pattern_type: u16, period: u32, first_date_time: u32, ) -> Result<MapiRecurPattern>`

# Calls

- [read_recur_u32](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_u32.md)
- [recurrence_days_from_mask](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_days_from_mask.md)
- [recurrence_month_from_minutes](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/recurrence_month_from_minutes.md)

# Called by

- [appointment_recurrence_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/appointment_recurrence_from_mapi.md)