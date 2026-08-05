---
type: Rust Method
title: take_exact
resource: crates/lpe-activesync/src/types.rs#L237-L244
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/types/parse_base64_query
  - functions/crates/lpe-activesync/src/types/ByteCursor/take_u8
  - functions/crates/lpe-activesync/src/types/ByteCursor/take_array
  - functions/crates/lpe-activesync/src/types/ByteCursor/take_string
---

# Signature

`fn take_exact(&mut self, len: usize) -> Result<&'a [u8]>`

# Called by

- [parse_base64_query](../../../../../../functions/crates/lpe-activesync/src/types/parse_base64_query.md)
- [take_u8](../../../../../../functions/crates/lpe-activesync/src/types/ByteCursor/take_u8.md)
- [take_array](../../../../../../functions/crates/lpe-activesync/src/types/ByteCursor/take_array.md)
- [take_string](../../../../../../functions/crates/lpe-activesync/src/types/ByteCursor/take_string.md)