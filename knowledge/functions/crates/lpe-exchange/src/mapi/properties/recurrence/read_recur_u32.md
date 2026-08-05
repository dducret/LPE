---
type: Rust Function
title: read_recur_u32
resource: crates/lpe-exchange/src/mapi/properties/recurrence.rs#L805-L811
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/appointment_recurrence_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_pattern
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_dates
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_exception_infos
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_extended_exceptions
---

# Signature

`fn read_recur_u32(value: &[u8], offset: &mut usize) -> Result<u32>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [appointment_recurrence_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/appointment_recurrence_from_mapi.md)
- [read_recur_pattern](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_pattern.md)
- [read_recur_dates](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_dates.md)
- [read_recur_exception_infos](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_exception_infos.md)
- [read_recur_extended_exceptions](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_extended_exceptions.md)