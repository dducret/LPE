---
type: Rust Function
title: read_recur_u16
resource: crates/lpe-exchange/src/mapi/properties/recurrence.rs#L797-L803
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/appointment_recurrence_from_mapi
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_exception_infos
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_ansi_string
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_wide_string
---

# Signature

`fn read_recur_u16(value: &[u8], offset: &mut usize) -> Result<u16>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [appointment_recurrence_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/appointment_recurrence_from_mapi.md)
- [read_recur_exception_infos](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_exception_infos.md)
- [read_recur_ansi_string](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_ansi_string.md)
- [read_recur_wide_string](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_wide_string.md)