---
type: Rust Method
title: take_string
resource: crates/lpe-activesync/src/types.rs#L233-L235
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/types/decode_parameter_value
  - functions/crates/lpe-activesync/src/types/ByteCursor/take_exact
  called_by:
  - functions/crates/lpe-activesync/src/types/parse_base64_query
---

# Signature

`fn take_string(&mut self, len: usize) -> Result<String>`

# Calls

- [decode_parameter_value](../../../../../../functions/crates/lpe-activesync/src/types/decode_parameter_value.md)
- [take_exact](../../../../../../functions/crates/lpe-activesync/src/types/ByteCursor/take_exact.md)

# Called by

- [parse_base64_query](../../../../../../functions/crates/lpe-activesync/src/types/parse_base64_query.md)