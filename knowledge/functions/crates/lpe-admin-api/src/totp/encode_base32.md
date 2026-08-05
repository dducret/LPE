---
type: Rust Function
title: encode_base32
resource: crates/lpe-admin-api/src/totp.rs#L63-L81
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  calls:
  - functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push
  called_by:
  - functions/crates/lpe-admin-api/src/totp/generate_secret
  - functions/crates/lpe-admin-api/src/totp/current_code_verifies
---

# Signature

`fn encode_base32(bytes: &[u8]) -> String`

# Calls

- [push](../../../../../functions/crates/lpe-activesync/src/wbxml/WbxmlNode/push.md)

# Called by

- [generate_secret](../../../../../functions/crates/lpe-admin-api/src/totp/generate_secret.md)
- [current_code_verifies](../../../../../functions/crates/lpe-admin-api/src/totp/current_code_verifies.md)