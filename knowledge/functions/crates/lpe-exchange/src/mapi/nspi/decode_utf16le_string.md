---
type: Rust Function
title: decode_utf16le_string
resource: crates/lpe-exchange/src/mapi/nspi.rs#L1485-L1495
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/crates/lpe-exchange/src/mapi/nspi/parse_resolve_names_values
---

# Signature

`pub(in crate::mapi) fn decode_utf16le_string(bytes: &[u8]) -> Option<String>`

# Called by

- [parse_resolve_names_values](../../../../../../functions/crates/lpe-exchange/src/mapi/nspi/parse_resolve_names_values.md)