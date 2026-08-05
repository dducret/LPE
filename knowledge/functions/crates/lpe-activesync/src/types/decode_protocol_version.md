---
type: Rust Function
title: decode_protocol_version
resource: crates/lpe-activesync/src/types.rs#L157-L162
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-activesync/src/types/parse_base64_query
---

# Signature

`fn decode_protocol_version(value: u8) -> Result<&'static str>`

# Called by

- [parse_base64_query](../../../../../functions/crates/lpe-activesync/src/types/parse_base64_query.md)