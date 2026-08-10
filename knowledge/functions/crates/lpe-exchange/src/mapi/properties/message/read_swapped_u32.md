---
type: Rust Function
title: read_swapped_u32
resource: crates/lpe-exchange/src/mapi/properties/message.rs#L1153-L1160
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/message/parse_swapped_todo_data
  - functions/crates/lpe-exchange/src/mapi/properties/message/swapped_todo_minutes
---

# Signature

`fn read_swapped_u32(bytes: &[u8], offset: usize) -> Result<u32>`

# Calls

- [get](../../../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)

# Called by

- [parse_swapped_todo_data](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/parse_swapped_todo_data.md)
- [swapped_todo_minutes](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/swapped_todo_minutes.md)