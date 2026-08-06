---
type: Rust Function
title: swapped_todo_minutes
resource: crates/lpe-exchange/src/mapi/properties/message.rs#L997-L1003
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/properties/message/read_swapped_u32
  called_by:
  - functions/crates/lpe-exchange/src/mapi/properties/message/parse_swapped_todo_data
---

# Signature

`fn swapped_todo_minutes(bytes: &[u8], offset: usize, valid: bool) -> Result<Option<u32>>`

# Calls

- [read_swapped_u32](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/read_swapped_u32.md)

# Called by

- [parse_swapped_todo_data](../../../../../../../functions/crates/lpe-exchange/src/mapi/properties/message/parse_swapped_todo_data.md)