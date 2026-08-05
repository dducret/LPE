---
type: Rust Function
title: read_recur_exception_infos
resource: crates/lpe-exchange/src/mapi/properties/recurrence.rs#L667-L704
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_u32
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_u16
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_ansi_string
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/recurrence/appointment_recurrence_from_mapi
---

# Signature

`fn read_recur_exception_infos( value: &[u8], offset: &mut usize, count: usize, ) -> Result<Vec<MapiRecurrenceException>>`

# Calls

- [read_recur_u32](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_u32.md)
- [read_recur_u16](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_u16.md)
- [read_recur_ansi_string](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/read_recur_ansi_string.md)
- [push](../../../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [appointment_recurrence_from_mapi](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/recurrence/appointment_recurrence_from_mapi.md)