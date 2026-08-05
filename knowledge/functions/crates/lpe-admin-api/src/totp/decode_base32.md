---
type: Rust Function
title: decode_base32
resource: crates/lpe-admin-api/src/totp.rs#L83-L102
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-admin-api/src/totp/generate_code
---

# Signature

`fn decode_base32(input: &str) -> Option<Vec<u8>>`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [generate_code](../../../../../functions/crates/lpe-admin-api/src/totp/generate_code.md)