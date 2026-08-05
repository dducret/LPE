---
type: Rust Function
title: read_recur_dates
resource: crates/lpe-exchange/src/mapi/properties/recurrence.rs#L658-L665
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_u32
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/appointment_recurrence_from_mapi
---

# Signature

`fn read_recur_dates(value: &[u8], offset: &mut usize) -> Result<Vec<u32>>`

# Calls

- [read_recur_u32](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_u32.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [appointment_recurrence_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/appointment_recurrence_from_mapi.md)