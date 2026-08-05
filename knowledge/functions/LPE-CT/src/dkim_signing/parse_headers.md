---
type: Rust Function
title: parse_headers
resource: LPE-CT/src/dkim_signing.rs#L87-L118
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn parse_headers(message: &[u8]) -> Vec<(String, String)>`

# Calls

- [position](../../../../functions/crates/lpe-exchange/src/mapi/rop/buffer/Cursor/position.md)
- [push](../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)