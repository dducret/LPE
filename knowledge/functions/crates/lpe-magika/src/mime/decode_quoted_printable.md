---
type: Rust Function
title: decode_quoted_printable
resource: crates/lpe-magika/src/mime.rs#L345-L374
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/LPE-CT/web/app/smoke/test/MockFormData/get
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
---

# Signature

`fn decode_quoted_printable(body: &[u8]) -> Result<Vec<u8>>`

# Calls

- [get](../../../../../functions/LPE-CT/web/app/smoke/test/MockFormData/get.md)
- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)