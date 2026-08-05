---
type: Rust Function
title: read_recur_extended_exceptions
resource: crates/lpe-exchange/src/mapi/properties/recurrence.rs#L706-L745
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_u32
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/skip_recur_bytes
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_wide_string
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/appointment_recurrence_from_mapi
---

# Signature

`fn read_recur_extended_exceptions( value: &[u8], offset: &mut usize, writer_version2: u32, exceptions: &[MapiRecurrenceException], ) -> Result<()>`

# Calls

- [read_recur_u32](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_u32.md)
- [skip_recur_bytes](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/skip_recur_bytes.md)
- [read_recur_wide_string](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_wide_string.md)

# Called by

- [appointment_recurrence_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/appointment_recurrence_from_mapi.md)