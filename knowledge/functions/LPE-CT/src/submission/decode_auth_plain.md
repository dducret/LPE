---
type: Rust Function
title: decode_auth_plain
resource: LPE-CT/src/submission.rs#L565-L580
visibility: private
generated:
  by: okf-rs/0.3.0
relationships:
  called_by:
  - functions/LPE-CT/src/submission/parse_auth_plain
  - functions/LPE-CT/src/submission/auth_plain_decodes_username_and_password
---

# Signature

`fn decode_auth_plain(value: &str) -> Result<(String, String)>`

# Called by

- [parse_auth_plain](../../../../functions/LPE-CT/src/submission/parse_auth_plain.md)
- [auth_plain_decodes_username_and_password](../../../../functions/LPE-CT/src/submission/auth_plain_decodes_username_and_password.md)