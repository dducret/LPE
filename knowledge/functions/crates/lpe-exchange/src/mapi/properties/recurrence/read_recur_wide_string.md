---
type: Rust Function
title: read_recur_wide_string
resource: crates/lpe-exchange/src/mapi/properties/recurrence.rs#L760-L772
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_u16
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_extended_exceptions
---

# Signature

`fn read_recur_wide_string(value: &[u8], offset: &mut usize) -> Result<String>`

# Calls

- [read_recur_u16](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_u16.md)
- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [read_recur_extended_exceptions](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_extended_exceptions.md)