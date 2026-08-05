---
type: Rust Function
title: decode_query_component
resource: crates/lpe-activesync/src/types.rs#L164-L190
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-activesync/src/types/parse_plain_query
  - functions/crates/lpe-activesync/src/types/parse_base64_query
---

# Signature

`fn decode_query_component(value: &str, plus_as_space: bool) -> Result<String>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [parse_plain_query](../../../../../functions/crates/lpe-activesync/src/types/parse_plain_query.md)
- [parse_base64_query](../../../../../functions/crates/lpe-activesync/src/types/parse_base64_query.md)