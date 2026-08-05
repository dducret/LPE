---
type: Rust Function
title: parse_base64_query
resource: crates/lpe-activesync/src/types.rs#L92-L151
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/types/decode_query_component
  - functions/crates/lpe-activesync/src/types/decode_protocol_version
  - functions/crates/lpe-activesync/src/types/ByteCursor/take_u8
  - functions/crates/lpe-activesync/src/types/ByteCursor/take_exact
  - functions/crates/lpe-activesync/src/types/ByteCursor/take_string
  - functions/crates/lpe-activesync/src/types/ByteCursor/take_array
  - functions/crates/lpe-activesync/src/types/ByteCursor/has_remaining
  - functions/crates/lpe-activesync/src/types/decode_parameter_value
  called_by:
  - functions/crates/lpe-activesync/src/types/ParsedActiveSyncQuery/from_raw_query
---

# Signature

`fn parse_base64_query(raw_query: &str) -> Result<ParsedActiveSyncQuery>`

# Calls

- [decode_query_component](../../../../../functions/crates/lpe-activesync/src/types/decode_query_component.md)
- [decode_protocol_version](../../../../../functions/crates/lpe-activesync/src/types/decode_protocol_version.md)
- [take_u8](../../../../../functions/crates/lpe-activesync/src/types/ByteCursor/take_u8.md)
- [take_exact](../../../../../functions/crates/lpe-activesync/src/types/ByteCursor/take_exact.md)
- [take_string](../../../../../functions/crates/lpe-activesync/src/types/ByteCursor/take_string.md)
- [take_array](../../../../../functions/crates/lpe-activesync/src/types/ByteCursor/take_array.md)
- [has_remaining](../../../../../functions/crates/lpe-activesync/src/types/ByteCursor/has_remaining.md)
- [decode_parameter_value](../../../../../functions/crates/lpe-activesync/src/types/decode_parameter_value.md)

# Called by

- [from_raw_query](../../../../../functions/crates/lpe-activesync/src/types/ParsedActiveSyncQuery/from_raw_query.md)