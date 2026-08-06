---
type: Rust Function
title: hex_to_bytes
resource: crates/lpe-exchange/src/mapi_store.rs#L330-L341
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi_store/hex_digit
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn hex_to_bytes(value: &str) -> Option<Vec<u8>>`

# Calls

- [hex_digit](../../../../../functions/crates/lpe-exchange/src/mapi_store/hex_digit.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)