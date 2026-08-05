---
type: Rust Function
title: decode_parameter_value
resource: crates/lpe-activesync/src/types.rs#L153-L155
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/types/parse_base64_query
  - functions/crates/lpe-activesync/src/types/ByteCursor/take_string
---

# Signature

`fn decode_parameter_value(value: &[u8]) -> Result<String>`

# Called by

- [parse_base64_query](../../../../../functions/crates/lpe-activesync/src/types/parse_base64_query.md)
- [take_string](../../../../../functions/crates/lpe-activesync/src/types/ByteCursor/take_string.md)